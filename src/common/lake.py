from dataclasses import dataclass
from pathlib import Path

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


@dataclass(frozen=True)
class DatasetStats:
    path: str
    partitions: int
    files: int
    bytes: int

    def __str__(self) -> str:
        avg = self.bytes / self.files if self.files else 0
        return (f"{self.path} | {self.partitions} particiones | {self.files} archivos | "
                f"{self.bytes / 1e6:.1f} MB (prom. {avg / 1e6:.2f} MB/archivo)")


def describe_dataset(settings: Settings, zone: str, dataset: str) -> DatasetStats:
    path = dataset_path(settings, zone, dataset)
    files = list(Path(path).rglob("*.parquet")) if Path(path).is_dir() else []
    return DatasetStats(
        path=path,
        partitions=len({f.parent for f in files}),
        files=len(files),
        bytes=sum(f.stat().st_size for f in files),
    )
