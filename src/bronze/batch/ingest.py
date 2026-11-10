"""Paso bronze_batch: ingesta de las fuentes CSV de Landing a Bronze."""
import logging
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import col, lit, monotonically_increasing_id, row_number

from src.common.columns import SOURCE_FILE, with_technical_columns
from src.common.lake import dataset_storage_stats, write_dataset
from src.common.readers import read_csv_source
from src.common.schemas import CORRUPT_RECORD_COL, SOURCES, Source
from src.config import Settings
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


def ingest_source(spark: SparkSession, settings: Settings, source: Source, ingest_ts: Column) -> dict:
    """Ingesta una fuente CSV de Landing a Bronze, mandando las filas inválidas a quarantine.

    Args:
        spark: Sesión de Spark.
        settings: Configuración de la corrida.
        source: Fuente a ingestar.
        ingest_ts: Momento de ingesta, compartido por todas las fuentes de la corrida.

    Returns:
        Conteos de la ingesta: ``read``, ``quarantine``, ``written``,
        ``exact_duplicates``, ``key_conflicts`` y ``duplicates``.
    """
    raw = (
        with_technical_columns(read_csv_source(spark, source, settings), settings.run_id, ingest_ts)
        .withColumn(ROW_ORDER_COL, monotonically_increasing_id())
        .cache()
    )
    try:
        valid, quarantine = split_by_rules(raw, bronze_structural_rules(source))
        bronze = dedupe_last_wins(valid, source.natural_key).drop(CORRUPT_RECORD_COL, ROW_ORDER_COL)

        write_dataset(bronze, settings, "bronze", source.name)
        write_dataset(quarantine.drop(ROW_ORDER_COL), settings, "quarantine", source.name)

        data_cols = [field.name for field in source.schema.fields]
        valid_rows = valid.count()
        distinct_rows = valid.select(*data_cols).distinct().count()
        counts = {
            "read": raw.count(),
            "quarantine": quarantine.count(),
            "written": bronze.count(),
        }
    finally:
        raw.unpersist()
    counts["exact_duplicates"] = valid_rows - distinct_rows
    counts["key_conflicts"] = distinct_rows - counts["written"]
    counts["duplicates"] = counts["exact_duplicates"] + counts["key_conflicts"]
    if counts["key_conflicts"]:
        log.warning("bronze/%s: %d versiones descartadas por conflicto de clave %s (gana la última)",
                    source.name, counts["key_conflicts"], source.natural_key)
    log.info("bronze/%s: %s", source.name, counts)
    for zone in ("bronze", "quarantine"):
        log.info("%s/%s (en disco) -> %s", zone, source.name, dataset_storage_stats(settings, zone, source.name))
    return counts


def run(spark: SparkSession, settings: Settings) -> dict[str, dict]:
    """Paso ``bronze_batch``: ingesta todas las fuentes CSV y devuelve sus conteos por nombre."""
    ingest_ts = lit(datetime.now(timezone.utc)).cast("timestamp")
    return {name: ingest_source(spark, settings, source, ingest_ts) for name, source in SOURCES.items()}
