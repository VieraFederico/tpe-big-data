from pyspark.sql import DataFrame, SparkSession

from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS, Source
from src.config import Settings

# Las filas que no respetan el esquema original
# Dejan el texto en CORRUPT_RECORD_COL y el resto de las cols en null
# Asi se pueden mandar a quarantine facilmente.
def read_csv_source(spark: SparkSession, source: Source, settings: Settings) -> DataFrame:
    return (
        spark.read.schema(source.read_schema())
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .option("escape", '"')
        .csv(f"{settings.zone_path('landing')}/{source.path}")
    )


def read_usage_events(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.read.schema(USAGE_EVENTS.read_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .json(_events_glob(settings))
    )


def read_usage_events_stream(
    spark: SparkSession, settings: Settings, max_files_per_trigger: int = 10
) -> DataFrame:
    return (
        spark.readStream.schema(USAGE_EVENTS.read_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .json(_events_glob(settings))
    )


def _events_glob(settings: Settings) -> str:
    return f"{settings.zone_path('landing')}/{USAGE_EVENTS.path}/*.jsonl"
