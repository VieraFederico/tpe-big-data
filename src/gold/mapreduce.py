"""Mart diario de Gold armado a mano como job MapReduce sobre RDDs.

El pipeline usa ``marts.org_daily_usage_by_service`` (DataFrames); esto queda como referencia de
las fases map -> shuffle -> reduce y para comparar las dos. Se corre con
``python -m src.gold.mapreduce``.
"""

import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import coalesce, col, lit, when
from pyspark.sql.functions import max as spark_max

from src.common.columns import EVENT_DATE
from src.common.lake import read_dataset
from src.common.logging_setup import setup_logging
from src.common.schemas import USAGE_EVENTS
from src.common.spark import get_spark
from src.config import Settings, Zone, load_env_file, load_settings
from src.gold.anomalies import ANALYTICS_DIR
from src.gold.marts import USAGE_DATE, org_daily_usage_by_service
from src.silver.anomalies import IS_COST_ANOMALY

log = logging.getLogger(__name__)

MAPREDUCE_CHECK_FILE = "mapreduce_check.txt"

Key = tuple[str, date, str]
Vector = tuple[Any, ...]

KEY_COLUMNS = ("org_id", USAGE_DATE, "service")
# Mismo orden que el vector: columna de Silver que entra y métrica del mart que sale.
EVENT_COLUMNS = (
    "cost_usd_increment",
    "requests",
    "cpu_hours",
    "storage_gb_hours",
    "carbon_kg",
    "genai_tokens",
)
METRICS = (
    "daily_cost_usd",
    "requests",
    "cpu_hours",
    "storage_gb_hours",
    "carbon_kg",
    "genai_tokens",
    "event_count",
    "cost_anomaly_events",
)
SCHEMA = (
    "org_id string, service string, usage_date date, daily_cost_usd double, requests double, "
    "cpu_hours double, storage_gb_hours double, carbon_kg double, genai_tokens bigint, "
    "event_count int, cost_anomaly_events int"
)
MAX_KEYS_LISTED = 20


def map_event(row: Row) -> tuple[Key, Vector]:
    """Map: cada event emite su key ``(org_id, usage_date, service)`` y su vector de métricas.

    El vector trae las métricas tal cual (con sus null), un 1 para contar el event y un 1/0
    si está marcado como anomalía de cost.
    """
    flag = row[IS_COST_ANOMALY]
    is_anomaly = None if flag is None else int(flag)
    key = (row["org_id"], row[EVENT_DATE], row["service"])
    return key, (*(row[column] for column in EVENT_COLUMNS), 1, is_anomaly)


def reduce_vectors(left: Vector, right: Vector) -> Vector:
    """Reduce: suma componente a componente; si un lado es null queda el otro, como ``sum``."""
    return tuple(
        a if b is None else b if a is None else a + b for a, b in zip(left, right, strict=True)
    )


def _to_row(item: tuple[Key, Vector]) -> tuple[Any, ...]:
    (org_id, usage_date, service), vector = item
    return (org_id, service, usage_date, *vector)


def org_daily_usage_mapreduce(silver_df: DataFrame) -> DataFrame:
    """Mismo contenido que ``org_daily_usage_by_service``, pero con ``map`` y ``reduceByKey``.

    El shuffle lo hace ``reduceByKey``: junta en la misma partition todos los vectores de una
    key (y ya los va sumando de cada lado antes de mandarlos). No agrega ``run_id``.
    """
    events = silver_df.select("org_id", "service", EVENT_DATE, *EVENT_COLUMNS, IS_COST_ANOMALY).rdd
    reduced = events.map(map_event).reduceByKey(reduce_vectors)
    return silver_df.sparkSession.createDataFrame(reduced.map(_to_row), SCHEMA)


def compare(dataframe_mart: DataFrame, mapreduce_mart: DataFrame) -> str:
    """Compara las dos implementaciones y devuelve el reporte en texto.

    Una métrica null de un lado y con valor del otro cuenta como diferencia ``inf``.
    """
    both = dataframe_mart.alias("df").join(mapreduce_mart.alias("mr"), list(KEY_COLUMNS), "full")
    only_dataframe = both.filter(col("mr.event_count").isNull()).select(*KEY_COLUMNS)
    only_mapreduce = both.filter(col("df.event_count").isNull()).select(*KEY_COLUMNS)
    matched = both.filter(col("df.event_count").isNotNull() & col("mr.event_count").isNotNull())
    differences = [spark_max(_difference(metric)).alias(metric) for metric in METRICS]
    max_differences = matched.agg(*differences).first()

    lines = [
        f"rows_dataframe: {dataframe_mart.count()}",
        f"rows_mapreduce: {mapreduce_mart.count()}",
    ]
    for name, keys in (("only_dataframe", only_dataframe), ("only_mapreduce", only_mapreduce)):
        lines.append(f"keys_{name}: {keys.count()}")
        lines += [f"  {tuple(row)}" for row in keys.limit(MAX_KEYS_LISTED).collect()]
    for metric in METRICS:
        value = max_differences[metric] if max_differences else None
        lines.append(f"max_abs_diff_{metric}: {0.0 if value is None else value}")
    return "\n".join(lines) + "\n"


def _difference(metric: str) -> Column:
    left, right = col(f"df.{metric}"), col(f"mr.{metric}")
    gap = coalesce(spark_abs(left - right), lit(float("inf")))
    return when(left.eqNullSafe(right), 0.0).otherwise(gap)


def write_check(settings: Settings, report: str) -> Path:
    """Guarda el reporte en ``<evidence>/analytics/<run_id>/mapreduce_check.txt``."""
    path = Path(settings.evidence_path) / ANALYTICS_DIR / settings.run_id / MAPREDUCE_CHECK_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
    return path


def main() -> int:
    """Lee Silver, arma el mart diario de las dos formas, las compara y guarda el reporte."""
    load_env_file()
    settings = load_settings()
    spark = get_spark("mapreduce-check")
    try:
        silver_events = read_dataset(spark, settings, Zone.SILVER, USAGE_EVENTS.name).cache()
        report = compare(
            org_daily_usage_by_service(silver_events), org_daily_usage_mapreduce(silver_events)
        )
    finally:
        spark.stop()
    path = write_check(settings, report)
    print(report, end="")
    log.info("Reporte MapReduce vs DataFrame: %s", path)
    return 0


if __name__ == "__main__":
    setup_logging("mapreduce_check")
    sys.exit(main())
