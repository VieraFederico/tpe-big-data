import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import AnalysisException
from pyspark.sql.functions import coalesce, col, sha2, struct, to_date, to_json, try_to_timestamp

from src.common.columns import EVENT_DATE, EVENT_TS, with_technical_columns
from src.common.lake import dataset_storage_stats, read_dataset, write_dataset
from src.common.readers import read_usage_events_stream
from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS
from src.config import Settings
from src.quality.catalog import bronze_event_rules
from src.quality.rules import split_by_rules

log = logging.getLogger(__name__)

WATERMARK = "61 days"
MAX_FILES_PER_TRIGGER = 10


def build_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    source_cols = [field.name for field in USAGE_EVENTS.read_schema().fields]
    return (
        with_technical_columns(read_usage_events_stream(spark, settings, MAX_FILES_PER_TRIGGER), settings.run_id)
        .withColumn(EVENT_TS, try_to_timestamp(col("timestamp")))
        .withColumn(EVENT_DATE, to_date(EVENT_TS))
        .withColumn(
            "_dedupe_key",
            coalesce(col("event_id"), sha2(to_json(struct(*source_cols)), 256)),
        )
        .withWatermark(EVENT_TS, WATERMARK)
        .dropDuplicatesWithinWatermark(["_dedupe_key"])
        .drop("_dedupe_key")
    )


def write_micro_batch(settings: Settings):
    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        valid, quarantine = split_by_rules(batch_df, bronze_event_rules())
        write_dataset(valid.drop(CORRUPT_RECORD_COL), settings, "bronze", USAGE_EVENTS.name,
                      partition_by=(EVENT_DATE,), mode="append", use_rebalance=False)
        write_dataset(quarantine, settings, "quarantine", USAGE_EVENTS.name, mode="append", use_rebalance=False)
        log.info("Micro-lote %d: %d a bronze, %d a quarantine", batch_id, valid.count(), quarantine.count())
        batch_df.unpersist()

    return _write


def _count_or_zero(spark: SparkSession, settings: Settings, zone: str) -> int:
    try:
        return read_dataset(spark, settings, zone, USAGE_EVENTS.name).count()
    except AnalysisException:
        return 0


def run(spark: SparkSession, settings: Settings) -> dict:
    query = (
        build_stream(spark, settings)
        .writeStream.foreachBatch(write_micro_batch(settings))
        .option("checkpointLocation", f"{settings.zone_path('checkpoints')}/bronze_{USAGE_EVENTS.name}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    progress = query.recentProgress
    state_ops = [op for p in progress for op in p.get("stateOperators", [])]
    stats = {
        "micro_batches": len(progress),
        "rows_read": sum(p["numInputRows"] for p in progress),
        "dropped_by_watermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in state_ops),
        "dropped_duplicates": sum(op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for op in state_ops),
        "bronze_total": _count_or_zero(spark, settings, "bronze"),
        "quarantine_total": _count_or_zero(spark, settings, "quarantine"),
    }
    log.info("bronze/%s: %s", USAGE_EVENTS.name, stats)
    for zone in ("bronze", "quarantine"):
        log.info("%s/%s (en disco) -> %s", zone, USAGE_EVENTS.name, dataset_storage_stats(settings, zone, USAGE_EVENTS.name))
    return stats
