"""Lectura de las fuentes de Landing con esquema explícito.

Todas las lecturas son PERMISSIVE: las filas que no respetan el esquema dejan el texto
original en CORRUPT_RECORD_COL y el resto de las columnas en null, así se pueden mandar
a quarantine sin perder el dato.
"""
from pyspark.sql import DataFrame, SparkSession

from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS, Source
from src.config import Settings


def read_csv_source(spark: SparkSession, source: Source, settings: Settings) -> DataFrame:
    """Lee una fuente CSV de Landing con su esquema más la columna de registro corrupto."""
    return (
        spark.read.schema(source.read_schema())
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .option("escape", '"')
        .csv(f"{settings.zone_path('landing')}/{source.path}")
    )


def read_usage_events(spark: SparkSession, settings: Settings) -> DataFrame:
    """Lee en batch todos los archivos JSONL de eventos de uso."""
    return (
        spark.read.schema(USAGE_EVENTS.read_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .json(_events_glob(settings))
    )


def read_usage_events_stream(
    spark: SparkSession, settings: Settings, max_files_per_trigger: int = 10
) -> DataFrame:
    """Lee los eventos de uso como stream, procesando de a ``max_files_per_trigger`` archivos."""
    return (
        spark.readStream.schema(USAGE_EVENTS.read_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .json(_events_glob(settings))
    )


def _events_glob(settings: Settings) -> str:
    return f"{settings.zone_path('landing')}/{USAGE_EVENTS.path}/*.jsonl"
