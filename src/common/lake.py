from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.columns import INGEST_DATE
from src.config import Settings

def dataset_path(settings: Settings, zone: str, dataset: str) -> str:
    return f"{settings.zone_path(zone)}/{dataset}"

# df.hint("rebalance", "col") toma los nombres como literales y falla.
# Con esto evitamos eso y podemos pasar una tupla de nombres de columna.
def rebalance_by(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    if not columns:
        return df.hint("rebalance")
    column_list = ", ".join(f"`{name}`" for name in columns)
    return df.sparkSession.sql(f"SELECT /*+ REBALANCE({column_list}) */ * FROM {{df}}", df=df)


def write_dataset(
    df: DataFrame,
    settings: Settings,
    zone: str,
    dataset: str,
    partition_by: tuple[str, ...] = (INGEST_DATE,),
    mode: str = "overwrite",
    use_rebalance: bool = True,
) -> str:
    path = dataset_path(settings, zone, dataset)
    distributed = rebalance_by(df, partition_by) if use_rebalance else df.repartition(*partition_by)
    distributed.write.mode(mode).partitionBy(*partition_by).parquet(path)
    return path


def read_dataset(spark: SparkSession, settings: Settings, zone: str, dataset: str) -> DataFrame:
    return spark.read.parquet(dataset_path(settings, zone, dataset))


@dataclass(frozen=True)
class StorageStats:
    path: str
    partitions: int
    files: int
    bytes: int

    def __str__(self) -> str:
        avg = self.bytes / self.files if self.files else 0
        return (f"{self.path} | {self.partitions} particiones | {self.files} archivos | "
                f"{self.bytes / 1e6:.1f} MB (prom. {avg / 1e6:.2f} MB/archivo)")


def dataset_storage_stats(settings: Settings, zone: str, dataset: str) -> StorageStats:
    path = dataset_path(settings, zone, dataset)
    files = list(Path(path).rglob("*.parquet")) if Path(path).is_dir() else []
    return StorageStats(
        path=path,
        partitions=len({f.parent for f in files}),
        files=len(files),
        bytes=sum(f.stat().st_size for f in files),
    )
