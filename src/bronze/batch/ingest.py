"""Paso bronze_batch: ingesta de las fuentes CSV de Landing a Bronze."""

import logging
from datetime import UTC, datetime
from typing import TypedDict

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    count,
    count_distinct,
    lit,
    monotonically_increasing_id,
    row_number,
)

from src.common.columns import SOURCE_FILE, with_technical_columns
from src.common.lake import log_storage_stats, write_dataset
from src.common.readers import read_csv_source
from src.common.schemas import CORRUPT_RECORD_COL, SOURCES, Source
from src.config import Settings, Zone
from src.quality.catalog import bronze_structural_rules
from src.quality.rules import split_by_rules

log = logging.getLogger(__name__)

# Posición de la fila en el archivo de origen. Sirve para desempatar de forma idempotente
# entre versiones de la misma clave dentro de un mismo archivo.
ROW_ORDER_COL = "_row_order"


def dedupe_last_wins(df: DataFrame, natural_key: tuple[str, ...]) -> DataFrame:
    """Deja una fila por clave natural: la del último archivo y, dentro de él, la última fila.

    Args:
        df: Datos con SOURCE_FILE y ROW_ORDER_COL.
        natural_key: Columnas que identifican la fila.

    Returns:
        Una fila por valor de la clave natural.
    """
    window = Window.partitionBy(*natural_key).orderBy(
        col(SOURCE_FILE).desc(), col(ROW_ORDER_COL).desc()
    )
    return (
        df.withColumn("_position_in_key", row_number().over(window))
        .filter(col("_position_in_key") == 1)
        .drop("_position_in_key")
    )


class BronzeBatchCounts(TypedDict):
    """Conteos de la ingesta de una fuente a Bronze.

    Attributes:
        read: Filas leídas de Landing.
        quarantine: Filas mandadas a quarantine.
        written: Filas escritas en Bronze.
        exact_duplicates: Filas válidas repetidas en todas sus columnas.
        key_conflicts: Versiones distintas de una misma clave descartadas (gana la última).
        duplicates: exact_duplicates + key_conflicts.
    """

    read: int
    quarantine: int
    written: int
    exact_duplicates: int
    key_conflicts: int
    duplicates: int


def ingest_source(
    spark: SparkSession, settings: Settings, source: Source, ingest_ts: Column
) -> BronzeBatchCounts:
    """Ingesta una fuente CSV de Landing a Bronze, mandando las filas inválidas a quarantine.

    Args:
        spark: Sesión de Spark.
        settings: Configuración de la corrida.
        source: Fuente a ingestar.
        ingest_ts: Momento de ingesta, compartido por todas las fuentes de la corrida.

    Returns:
        Conteos de la ingesta.
    """
    raw = (
        with_technical_columns(
            read_csv_source(spark, source, settings), settings.run_id, ingest_ts
        )
        .withColumn(ROW_ORDER_COL, monotonically_increasing_id())
        .cache()
    )
    try:
        valid, quarantine = split_by_rules(raw, bronze_structural_rules(source))
        bronze = dedupe_last_wins(valid, source.natural_key).drop(
            CORRUPT_RECORD_COL, ROW_ORDER_COL
        )

        write_dataset(bronze, settings, Zone.BRONZE, source.name)
        write_dataset(quarantine.drop(ROW_ORDER_COL), settings, Zone.QUARANTINE, source.name)
        counts = _count_ingest(valid, quarantine, source)
    finally:
        raw.unpersist()

    _log_ingest(settings, source, counts)
    return counts


def _count_ingest(valid: DataFrame, quarantine: DataFrame, source: Source) -> BronzeBatchCounts:
    """Calcula los conteos de la ingesta.

    No recuenta lo que ya se deduce de dos invariantes:
    - split_by_rules manda cada fila a una sola salida: leídas = válidas + quarantine.
    - Las válidas no tienen clave nula y el dedupe deja una fila por clave:
      escritas = claves distintas entre las válidas.
    """
    valid_rows = valid.count()
    quarantine_rows = quarantine.count()
    data_cols = [field.name for field in source.schema.fields]
    # Una agregación sin groupBy devuelve siempre exactamente una fila.
    (distinct_summary,) = (
        valid.select(*data_cols)
        .distinct()
        .agg(
            count(lit(1)).alias("distinct_rows"),
            count_distinct(*source.natural_key).alias("distinct_keys"),
        )
        .collect()
    )
    distinct_rows = distinct_summary["distinct_rows"]
    written_rows = distinct_summary["distinct_keys"]

    exact_duplicates = valid_rows - distinct_rows
    key_conflicts = distinct_rows - written_rows
    return BronzeBatchCounts(
        read=valid_rows + quarantine_rows,
        quarantine=quarantine_rows,
        written=written_rows,
        exact_duplicates=exact_duplicates,
        key_conflicts=key_conflicts,
        duplicates=exact_duplicates + key_conflicts,
    )


def _log_ingest(settings: Settings, source: Source, counts: BronzeBatchCounts) -> None:
    """Loguea los conteos de la ingesta, avisa si hubo conflictos de clave y el tamaño en disco."""
    if counts["key_conflicts"]:
        log.warning(
            "bronze/%s: %d versiones descartadas por conflicto de clave %s (gana la última)",
            source.name,
            counts["key_conflicts"],
            source.natural_key,
        )
    log.info("bronze/%s: %s", source.name, counts)
    log_storage_stats(settings, source.name, (Zone.BRONZE, Zone.QUARANTINE))


def run(spark: SparkSession, settings: Settings) -> dict[str, BronzeBatchCounts]:
    """Paso ``bronze_batch``: ingesta todas las fuentes CSV y devuelve sus conteos por nombre."""
    ingest_ts = lit(datetime.now(UTC)).cast("timestamp")
    return {
        name: ingest_source(spark, settings, source, ingest_ts) for name, source in SOURCES.items()
    }
