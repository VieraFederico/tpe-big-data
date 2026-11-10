"""Paso bronze_stream: ingesta incremental de los eventos de uso de Landing a Bronze."""
import logging
from collections.abc import Callable
from typing import TypedDict

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, sha2, struct, to_date, to_json, try_to_timestamp

from src.common.columns import EVENT_DATE, EVENT_TS, with_technical_columns
from src.common.lake import dataset_storage_stats, read_dataset, write_dataset
from src.common.readers import read_usage_events_stream
from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS
from src.config import Settings, Zone
from src.quality.catalog import bronze_event_rules
from src.quality.rules import split_by_rules

log = logging.getLogger(__name__)

WATERMARK = "61 days"
MAX_FILES_PER_TRIGGER = 10


class BronzeStreamStats(TypedDict):
    """Métricas de una corrida del stream de eventos a Bronze.

    Attributes:
        micro_batches: Micro-lotes procesados en esta corrida.
        rows_read: Filas leídas de Landing en esta corrida.
        dropped_by_watermark: Eventos descartados por llegar detrás del watermark.
        dropped_duplicates: Eventos descartados por duplicados.
        bronze_total: Filas acumuladas en Bronze (todas las corridas).
        quarantine_total: Filas acumuladas en quarantine (todas las corridas).
    """

    micro_batches: int
    rows_read: int
    dropped_by_watermark: int
    dropped_duplicates: int
    bronze_total: int
    quarantine_total: int


def build_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    """Arma el stream de eventos con timestamp parseado y deduplicado dentro del watermark.

    Los eventos sin ``event_id`` se deduplican por un hash de todo su contenido.
    """
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


def write_micro_batch(settings: Settings) -> Callable[[DataFrame, int], None]:
    """Devuelve la función de ``foreachBatch`` que separa cada micro-lote en Bronze y quarantine."""

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            valid, quarantine = split_by_rules(batch_df, bronze_event_rules())
            write_dataset(valid.drop(CORRUPT_RECORD_COL), settings, Zone.BRONZE, USAGE_EVENTS.name,
                          partition_by=(EVENT_DATE,), mode="append", use_rebalance=False)
            write_dataset(quarantine, settings, Zone.QUARANTINE, USAGE_EVENTS.name,
                          mode="append", use_rebalance=False)
            log.info("Micro-lote %d: %d a bronze, %d a quarantine",
                     batch_id, valid.count(), quarantine.count())
        finally:
            batch_df.unpersist()

    return _write


def _count_or_zero(spark: SparkSession, settings: Settings, zone: Zone) -> int:
    """Filas de eventos escritas en la zona; 0 si todavía no existe el dataset."""
    try:
        return read_dataset(spark, settings, zone, USAGE_EVENTS.name).count()
    except AnalysisException:
        return 0


def run(spark: SparkSession, settings: Settings) -> BronzeStreamStats:
    """Paso ``bronze_stream``: procesa los archivos nuevos (availableNow) y devuelve métricas.

    El checkpoint recuerda qué archivos ya se leyeron y el estado de deduplicación,
    así que volver a correr el paso solo procesa lo nuevo.

    Returns:
        Métricas de la corrida: micro-lotes, filas leídas, descartes por watermark y
        por duplicado, y totales acumulados en Bronze y quarantine.
    """
    query = (
        build_stream(spark, settings)
        .writeStream.foreachBatch(write_micro_batch(settings))
        .option("checkpointLocation", f"{settings.zone_path(Zone.CHECKPOINTS)}/{Zone.BRONZE}_{USAGE_EVENTS.name}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    batch_progresses = query.recentProgress
    state_operators = [
        state_operator
        for batch_progress in batch_progresses
        for state_operator in batch_progress.get("stateOperators", [])
    ]
    stats: BronzeStreamStats = {
        "micro_batches": len(batch_progresses),
        "rows_read": sum(batch_progress["numInputRows"] for batch_progress in batch_progresses),
        "dropped_by_watermark": sum(
            state_operator.get("numRowsDroppedByWatermark", 0) for state_operator in state_operators
        ),
        "dropped_duplicates": sum(
            state_operator.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
            for state_operator in state_operators
        ),
        "bronze_total": _count_or_zero(spark, settings, Zone.BRONZE),
        "quarantine_total": _count_or_zero(spark, settings, Zone.QUARANTINE),
    }
    log.info("bronze/%s: %s", USAGE_EVENTS.name, stats)
    for zone in (Zone.BRONZE, Zone.QUARANTINE):
        log.info("%s/%s (en disco) -> %s", zone, USAGE_EVENTS.name, dataset_storage_stats(settings, zone, USAGE_EVENTS.name))
    return stats
