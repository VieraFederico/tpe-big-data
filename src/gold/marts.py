"""Marts de Gold, salen de los events de Silver."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit
from pyspark.sql.functions import sum as spark_sum

from src.common.columns import EVENT_DATE
from src.silver.anomalies import IS_COST_ANOMALY
from src.silver.events import METRIC_COLUMNS

ORG_DAILY_USAGE_BY_SERVICE = "org_daily_usage_by_service"
USAGE_DATE = "usage_date"


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
