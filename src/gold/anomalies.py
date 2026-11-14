"""Mart de anomalías de cost diario: p99 exacto por service, con robust z (MAD) como referencia."""

import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, count, lit, percentile, when

from src.config import Settings
from src.gold.marts import USAGE_DATE

COST_ANOMALY_MART = "cost_anomaly_mart"
ANALYTICS_DIR = "analytics"
ANOMALY_SUMMARY_FILE = "anomaly_summary.csv"

COST_PERCENTILE = 0.99
ROBUST_Z_FACTOR = 0.6745
ROBUST_Z_LIMIT = 3.5

_MEDIAN = "_median_usd"
_MAD = "_mad_usd"


def detection_method(multiplier: float) -> str:
    """``p99_x<multiplier>`` sin ceros de más: ``3.0`` da ``p99_x3`` y ``2.5`` da ``p99_x2.5``."""
    return f"p99_x{str(multiplier).removesuffix('.0')}"


def cost_anomaly_mart(daily: DataFrame, multiplier: float) -> tuple[DataFrame, DataFrame]:
    """Rows del mart diario cuyo cost supera ``multiplier × p99`` de su service.

    Returns:
        El mart (sin ``run_id``) y el summary con una row por service, para calibrar el
        multiplier.
    """
    cost = col("daily_cost_usd")
    stats = daily.groupBy("service").agg(
        count(lit(1)).alias("days_rows"),
        percentile(cost, COST_PERCENTILE).alias("p99_usd"),
        percentile(cost, 0.5).alias(_MEDIAN),
    )
    # MAD: la mediana de cuánto se aleja cada día de la mediana del service.
    mad = (
        daily.join(stats, "service")
        .groupBy("service")
        .agg(percentile(spark_abs(cost - col(_MEDIAN)), 0.5).alias(_MAD))
    )
    p99 = col("p99_usd")
    scored = (
        daily.join(stats.join(mad, "service"), "service")
        .withColumns(
            {
                "threshold_usd": p99 * lit(float(multiplier)),
                "robust_z": when(
                    col(_MAD) > 0, ROBUST_Z_FACTOR * (cost - col(_MEDIAN)) / col(_MAD)
                ),
            }
        )
        .withColumn("is_anomaly", (p99 > 0) & (cost > col("threshold_usd")))
    )
    mart = scored.filter("is_anomaly").select(
        "org_id",
        "service",
        USAGE_DATE,
        "daily_cost_usd",
        "p99_usd",
        "threshold_usd",
        (cost / p99).alias("anomaly_score"),
        "robust_z",
        lit(detection_method(multiplier)).alias("detection_method"),
    )
    summary = scored.groupBy("service", "days_rows", "p99_usd", "threshold_usd").agg(
        count(when(col("is_anomaly"), 1)).alias("anomalies_p99"),
        count(when(col("robust_z") > ROBUST_Z_LIMIT, 1)).alias("anomalies_robust_z"),
    )
    return mart, summary.select(
        "service",
        "days_rows",
        "p99_usd",
        lit(float(multiplier)).alias("multiplier"),
        "threshold_usd",
        "anomalies_p99",
        "anomalies_robust_z",
    )


def write_anomaly_summary_csv(settings: Settings, run_id: str, summary: DataFrame) -> Path:
    """Escribe ``<evidence>/analytics/<run_id>/anomaly_summary.csv``, una row por service."""
    path = Path(settings.evidence_path) / ANALYTICS_DIR / run_id / ANOMALY_SUMMARY_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")
        writer.writerow(summary.columns)
        writer.writerows(summary.orderBy("service").collect())
    return path
