"""Picos de costo por evento contra el percentil 99 exacto del costo de su servicio."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, coalesce, col, lit, percentile, when

SERVICE = "service"
COST = "cost_usd_increment"
ANOMALY_SCORE = "anomaly_score"
IS_COST_ANOMALY = "is_cost_anomaly"

COST_PERCENTILE = 0.99
ANOMALY_FACTOR = 3.0

_THRESHOLD_SERVICE_COL = "_threshold_service"
_SERVICE_P99_COL = "_service_cost_p99"


def with_cost_anomalies(events: DataFrame) -> DataFrame:
    """Agrega ``anomaly_score`` e ``is_cost_anomaly`` según el p99 exacto de su servicio."""
    thresholds = events.groupBy(col(SERVICE).alias(_THRESHOLD_SERVICE_COL)).agg(
        percentile(col(COST), COST_PERCENTILE).alias(_SERVICE_P99_COL)
    )
    with_threshold = events.join(
        broadcast(thresholds), col(SERVICE) == col(_THRESHOLD_SERVICE_COL), "left"
    )
    p99 = col(_SERVICE_P99_COL)
    cost = col(COST)
    has_positive_p99 = p99 > 0
    is_spike = has_positive_p99 & (cost > p99 * ANOMALY_FACTOR)
    scored = with_threshold.withColumns(
        {
            ANOMALY_SCORE: when(has_positive_p99, cost / p99),
            IS_COST_ANOMALY: coalesce(is_spike, lit(False)),
        }
    )
    return scored.drop(_THRESHOLD_SERVICE_COL, _SERVICE_P99_COL)
