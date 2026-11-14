"""Step gold: chequea que los counts cierren y, si cierran, pisa los marts de Gold."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.bronze.batch import ingest as bronze_batch
from src.common.columns import RUN_ID
from src.common.lake import log_storage_stats, read_dataset, write_dataset
from src.common.manifests import read_manifest, write_manifest
from src.common.schemas import USAGE_EVENTS
from src.config import Settings, Zone
from src.gold.marts import ORG_DAILY_USAGE_BY_SERVICE, USAGE_DATE, org_daily_usage_by_service
from src.gold.reconciliation import ReconciliationError, check, write_reconciliation_csv
from src.silver import build as silver

log = logging.getLogger(__name__)

STEP_NAME = "gold"


def run(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Step ``gold``: si la reconciliation cierra, pisa los marts y devuelve sus rows.

    Siempre deja ``reconciliation.csv`` en evidence y el ``_manifests/gold.json``.

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
    daily = org_daily_usage_by_service(silver_events).withColumn(RUN_ID, lit(settings.run_id))
    marts = {
        ORG_DAILY_USAGE_BY_SERVICE: _replace(
            daily, settings, ORG_DAILY_USAGE_BY_SERVICE, (USAGE_DATE,)
        ),
    }
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
