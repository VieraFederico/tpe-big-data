"""Step gold: chequea que los counts cierren y, si cierran, pisa los marts de Gold."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DataType, StructField, StructType

from src.bronze.batch import ingest as bronze_batch
from src.common.columns import RUN_ID
from src.common.lake import log_storage_stats, read_dataset, write_dataset
from src.common.manifests import read_manifest, write_manifest
from src.common.schemas import DATE, DOUBLE, INTEGER, LONG, STRING, USAGE_EVENTS
from src.config import Settings, Zone
from src.gold.anomalies import COST_ANOMALY_MART, cost_anomaly_mart, write_anomaly_summary_csv
from src.gold.marts import (
    AS_OF_DATE,
    ORG_DAILY_USAGE_BY_SERVICE,
    TOP_SERVICES_BY_ORG_14D,
    USAGE_DATE,
    org_daily_usage_by_service,
    top_services_by_org_14d,
)
from src.gold.reconciliation import ReconciliationError, check, write_reconciliation_csv
from src.silver import build as silver

log = logging.getLogger(__name__)

STEP_NAME = "gold"


def _mart_schema(*field_specs: tuple[str, DataType]) -> StructType:
    """Esquema de un mart tal como queda en Gold: sus columnas más ``run_id``."""
    fields = [*field_specs, (RUN_ID, STRING)]
    return StructType([StructField(name, data_type) for name, data_type in fields])


# Columnas y tipos de cada mart en Gold; las tablas de Cassandra tienen exactamente estas
# columnas, porque el serving inserta las columnas del mart tal cual.
MART_SCHEMAS: dict[str, StructType] = {
    ORG_DAILY_USAGE_BY_SERVICE: _mart_schema(
        ("org_id", STRING),
        ("service", STRING),
        (USAGE_DATE, DATE),
        ("daily_cost_usd", DOUBLE),
        ("requests", DOUBLE),
        ("cpu_hours", DOUBLE),
        ("storage_gb_hours", DOUBLE),
        ("carbon_kg", DOUBLE),
        ("genai_tokens", LONG),
        ("event_count", INTEGER),
        ("cost_anomaly_events", INTEGER),
    ),
    TOP_SERVICES_BY_ORG_14D: _mart_schema(
        ("org_id", STRING),
        ("service", STRING),
        (AS_OF_DATE, DATE),
        ("window_start_date", DATE),
        ("rank", INTEGER),
        ("cost_14d_usd", DOUBLE),
        ("requests_14d", DOUBLE),
    ),
    COST_ANOMALY_MART: _mart_schema(
        ("org_id", STRING),
        ("service", STRING),
        (USAGE_DATE, DATE),
        ("daily_cost_usd", DOUBLE),
        ("p99_usd", DOUBLE),
        ("threshold_usd", DOUBLE),
        ("anomaly_score", DOUBLE),
        ("robust_z", DOUBLE),
        ("detection_method", STRING),
    ),
}


def run(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Step ``gold``: si la reconciliation cierra, pisa los marts y devuelve sus rows.

    Siempre deja ``reconciliation.csv`` en evidence y el ``_manifests/gold.json``; si el gate
    pasa, también ``anomaly_summary.csv``.

    Raises:
        ReconciliationError: Si algún check no cierra; ahí no se toca ``gold/``.
    """
    results = check(spark, settings)
    write_reconciliation_csv(settings, settings.run_id, results)
    failed = [result for result in results if not result.passed]
    if failed:
        for result in failed:
            log.error(
                "%s (%s) no concilia: esperado %d, real %d",
                result.check_id,
                result.dataset,
                result.expected,
                result.actual,
            )
        failed_checks = list(dict.fromkeys(result.check_id for result in failed))
        write_manifest(
            settings,
            STEP_NAME,
            {"status": "gate_failed", "failed_checks": failed_checks, "marts": {}},
        )
        raise ReconciliationError(failed)
    log.info("Conciliación ok: %d chequeos", len(results))

    silver_events = read_dataset(spark, settings, Zone.SILVER, USAGE_EVENTS.name)
    daily = org_daily_usage_by_service(silver_events)
    anomalies, summary = cost_anomaly_mart(daily, settings.anomaly_p99_multiplier)
    to_write = {
        ORG_DAILY_USAGE_BY_SERVICE: (daily, USAGE_DATE),
        TOP_SERVICES_BY_ORG_14D: (top_services_by_org_14d(daily), AS_OF_DATE),
        COST_ANOMALY_MART: (anomalies, USAGE_DATE),
    }
    marts = {
        name: _replace(df.withColumn(RUN_ID, lit(settings.run_id)), settings, name, (partition,))
        for name, (df, partition) in to_write.items()
    }
    write_anomaly_summary_csv(settings, settings.run_id, summary)
    inputs = {
        f"{step}_run_id": (read_manifest(settings, step) or {}).get("run_id")
        for step in (bronze_batch.STEP_NAME, silver.STEP_NAME)
    }
    write_manifest(settings, STEP_NAME, {"status": "ok", "inputs": inputs, "marts": marts})
    log.info("gold: %s", marts)
    return marts


def _replace(
    df: DataFrame, settings: Settings, dataset: str, partition_by: tuple[str, ...]
) -> int:
    # Overwrite completo, así no quedan partitions viejas de otros runs.
    rows = df.cache()
    try:
        write_dataset(
            rows,
            settings,
            Zone.GOLD,
            dataset,
            partition_by=partition_by,
            replace_all_partitions=True,
        )
        count = rows.count()
    finally:
        rows.unpersist()
    log_storage_stats(settings, dataset, (Zone.GOLD,))
    return count
