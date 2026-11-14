"""Marts de Gold, salen de los events de Silver."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    date_sub,
    explode,
    lit,
    row_number,
    sequence,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum

from src.common.columns import EVENT_DATE
from src.silver.anomalies import IS_COST_ANOMALY
from src.silver.events import METRIC_COLUMNS

ORG_DAILY_USAGE_BY_SERVICE = "org_daily_usage_by_service"
TOP_SERVICES_BY_ORG_14D = "top_services_by_org_14d"
USAGE_DATE = "usage_date"
AS_OF_DATE = "as_of_date"
WINDOW_DAYS = 14


def org_daily_usage_by_service(silver_events: DataFrame) -> DataFrame:
    """Una row por ``(org_id, service, usage_date)`` con el cost y el uso del día.

    No agrega ``run_id``. ``sum`` ignora los nulls y da null si son todos null, que es justo lo
    que queremos para ``carbon_kg`` y ``genai_tokens`` (solo vienen en los events v2).
    """
    groups = silver_events.groupBy("org_id", "service", col(EVENT_DATE).alias(USAGE_DATE))
    return groups.agg(
        spark_sum("cost_usd_increment").alias("daily_cost_usd"),
        *(spark_sum(metric).alias(metric) for metric in METRIC_COLUMNS),
        spark_sum("carbon_kg").alias("carbon_kg"),
        spark_sum("genai_tokens").alias("genai_tokens"),
        count(lit(1)).cast("int").alias("event_count"),
        spark_sum(col(IS_COST_ANOMALY).cast("int")).cast("int").alias("cost_anomaly_events"),
    )


def top_services_by_org_14d(daily: DataFrame) -> DataFrame:
    """Cost y requests de 14 días por org y service, con su ``rank``, para cada ``as_of_date``.

    Las ``as_of_date`` cubren todo el rango de ``usage_date`` del mart diario (no el de cada
    org). No agrega ``run_id``.
    """
    as_of_dates = daily.agg(
        explode(sequence(spark_min(USAGE_DATE), spark_max(USAGE_DATE))).alias(AS_OF_DATE)
    )
    window_start = date_sub(col(AS_OF_DATE), WINDOW_DAYS - 1)
    in_window = col(USAGE_DATE).between(window_start, col(AS_OF_DATE))
    totals = (
        daily.join(broadcast(as_of_dates), in_window)
        .groupBy("org_id", "service", AS_OF_DATE)
        .agg(
            spark_sum("daily_cost_usd").alias("cost_14d_usd"),
            spark_sum("requests").alias("requests_14d"),
        )
    )
    ranking = Window.partitionBy("org_id", AS_OF_DATE).orderBy(
        col("cost_14d_usd").desc(), col("service")
    )
    return totals.select(
        "org_id",
        "service",
        AS_OF_DATE,
        window_start.alias("window_start_date"),
        row_number().over(ranking).alias("rank"),
        "cost_14d_usd",
        "requests_14d",
    )
