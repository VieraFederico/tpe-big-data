"""Paso bronze_batch: ingesta de las fuentes CSV de Landing a Bronze."""
import logging
from datetime import datetime, timezone
from typing import TypedDict

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, monotonically_increasing_id, row_number

from src.common.columns import SOURCE_FILE, with_technical_columns
from src.common.lake import dataset_storage_stats, write_dataset
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
    window = Window.partitionBy(*natural_key).orderBy(col(SOURCE_FILE).desc(), col(ROW_ORDER_COL).desc())
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
        with_technical_columns(read_csv_source(spark, source, settings), settings.run_id, ingest_ts)
        .withColumn(ROW_ORDER_COL, monotonically_increasing_id())
        .cache()
    )
    try:
        valid, quarantine = split_by_rules(raw, bronze_structural_rules(source))
        bronze = dedupe_last_wins(valid, source.natural_key).drop(CORRUPT_RECORD_COL, ROW_ORDER_COL)

        write_dataset(bronze, settings, Zone.BRONZE, source.name)
        write_dataset(quarantine.drop(ROW_ORDER_COL), settings, Zone.QUARANTINE, source.name)

        data_cols = [field.name for field in source.schema.fields]
        valid_rows = valid.count()
        distinct_rows = valid.select(*data_cols).distinct().count()
        read_rows = raw.count()
        quarantine_rows = quarantine.count()
        written_rows = bronze.count()
    finally:
        raw.unpersist()
    exact_duplicates = valid_rows - distinct_rows
    key_conflicts = distinct_rows - written_rows
    counts = BronzeBatchCounts(
        read=read_rows,
        quarantine=quarantine_rows,
        written=written_rows,
        exact_duplicates=exact_duplicates,
        key_conflicts=key_conflicts,
        duplicates=exact_duplicates + key_conflicts,
    )
    if counts["key_conflicts"]:
        log.warning("bronze/%s: %d versiones descartadas por conflicto de clave %s (gana la última)",
                    source.name, counts["key_conflicts"], source.natural_key)
    log.info("bronze/%s: %s", source.name, counts)
    for zone in (Zone.BRONZE, Zone.QUARANTINE):
        log.info("%s/%s (en disco) -> %s", zone, source.name, dataset_storage_stats(settings, zone, source.name))
    return counts


def run(spark: SparkSession, settings: Settings) -> dict[str, BronzeBatchCounts]:
    """Paso ``bronze_batch``: ingesta todas las fuentes CSV y devuelve sus conteos por nombre."""
    ingest_ts = lit(datetime.now(timezone.utc)).cast("timestamp")
    return {name: ingest_source(spark, settings, source, ingest_ts) for name, source in SOURCES.items()}
