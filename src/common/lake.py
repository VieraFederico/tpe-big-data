from pyspark.sql import DataFrame, SparkSession

from src.config import Settings

def dataset_path(settings: Settings, zone: str, dataset: str) -> str:
    return f"{settings.zone_path(zone)}/{dataset}"

def write_dataset(
    df: DataFrame,
    settings: Settings,
    zone: str,
    dataset: str,
    partition_by: tuple[str, ...] = ("ingest_date",),
    mode: str = "overwrite",
) -> str:
    path = dataset_path(settings, zone, dataset)
    df.write.mode(mode).partitionBy(*partition_by).parquet(path)
    return path


def read_dataset(spark: SparkSession, settings: Settings, zone: str, dataset: str) -> DataFrame:
    return spark.read.parquet(dataset_path(settings, zone, dataset))
