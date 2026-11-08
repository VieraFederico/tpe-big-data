import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.columns import with_technical_columns
from src.common.lake import write_dataset
from src.common.readers import read_usage_events_stream
from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS
from src.config import Settings
from src.quality.catalog import bronze_structural_rules
from src.quality.rules import split_by_rules

log = logging.getLogger(__name__)

WATERMARK = "61 days"
MAX_FILES_PER_TRIGGER = 10


def build_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    source_cols = [f.name for f in USAGE_EVENTS.read_schema().fields]
    return (
        with_technical_columns(read_usage_events_stream(spark, settings, MAX_FILES_PER_TRIGGER))
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn(
            "_dedupe_key",
            F.coalesce(F.col("event_id"), F.sha2(F.to_json(F.struct(*source_cols)), 256)),
        )
        .withWatermark("event_ts", WATERMARK)
        .dropDuplicatesWithinWatermark(["_dedupe_key"])
        .drop("_dedupe_key")
    )


def write_micro_batch(settings: Settings):
    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        valid, quarantine = split_by_rules(batch_df, bronze_structural_rules(USAGE_EVENTS))
        write_dataset(valid.drop(CORRUPT_RECORD_COL), settings, "bronze", USAGE_EVENTS.name,
                      partition_by=("event_date",), mode="append")
        write_dataset(quarantine, settings, "quarantine", USAGE_EVENTS.name, mode="append")
        log.info("Micro-lote %d: %d a bronze, %d a quarantine", batch_id, valid.count(), quarantine.count())
        batch_df.unpersist()

    return _write


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
    stats = {
        "micro_batches": len(progress),
        "rows_read": sum(p["numInputRows"] for p in progress),
        "dropped_by_watermark": sum(
            op.get("numRowsDroppedByWatermark", 0) for p in progress for op in p.get("stateOperators", [])
        ),
    }
    log.info("bronze/%s: %s", USAGE_EVENTS.name, stats)
    return stats
