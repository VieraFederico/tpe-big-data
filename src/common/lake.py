"""Lectura y escritura de datasets del lake, organizados por zona (bronze, quarantine, ...)."""
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.columns import INGEST_DATE
from src.config import Settings, Zone

BYTES_PER_MB = 1_000_000


def dataset_path(settings: Settings, zone: Zone, dataset: str) -> str:
    """Ruta de un dataset dentro de una zona del lake, p. ej. ``<lake>/bronze/users``."""
    return f"{settings.zone_path(zone)}/{dataset}"


def rebalance_by(df: DataFrame, columns: tuple[str, ...]) -> DataFrame:
    """Aplica el hint REBALANCE de Spark por las columnas dadas.

    ``df.hint("rebalance", "col")`` toma los nombres como literales y falla, por eso el
    hint se arma en SQL. Sin columnas, rebalancea sin clave.

    Args:
        df: Datos a redistribuir.
        columns: Nombres de las columnas por las que rebalancear.

    Returns:
        El mismo DataFrame con el hint aplicado.
    """
    if not columns:
        return df.hint("rebalance")
    column_list = ", ".join(f"`{name}`" for name in columns)
    return df.sparkSession.sql(f"SELECT /*+ REBALANCE({column_list}) */ * FROM {{df}}", df=df)


def write_dataset(
    df: DataFrame,
    settings: Settings,
    zone: Zone,
    dataset: str,
    partition_by: tuple[str, ...] = (INGEST_DATE,),
    mode: str = "overwrite",
    use_rebalance: bool = True,
) -> str:
    """Escribe un dataset como parquet particionado.

    Antes de escribir agrupa las filas por las columnas de partición, para que cada
    partición en disco quede en pocos archivos.

    Args:
        df: Datos a escribir.
        settings: Configuración de la corrida (define la raíz del lake).
        zone: Zona del lake, p. ej. ``bronze``.
        dataset: Nombre del dataset dentro de la zona.
        partition_by: Columnas de partición en disco.
        mode: Modo de escritura de Spark (``overwrite`` o ``append``).
        use_rebalance: True usa REBALANCE (AQE ajusta el tamaño de los archivos);
            False usa ``repartition``, que es lo que conviene en micro-lotes chicos.

    Returns:
        Ruta donde quedó escrito el dataset.
    """
    path = dataset_path(settings, zone, dataset)
    distributed = rebalance_by(df, partition_by) if use_rebalance else df.repartition(*partition_by)
    distributed.write.mode(mode).partitionBy(*partition_by).parquet(path)
    return path


def read_dataset(spark: SparkSession, settings: Settings, zone: Zone, dataset: str) -> DataFrame:
    """Lee un dataset parquet de una zona del lake."""
    return spark.read.parquet(dataset_path(settings, zone, dataset))


@dataclass(frozen=True)
class StorageStats:
    """Cómo quedó un dataset en disco, para revisar el tamaño de los archivos.

    Attributes:
        path: Ruta del dataset.
        partitions: Cantidad de carpetas de partición con archivos parquet.
        files: Cantidad de archivos parquet.
        bytes: Tamaño total en bytes.
    """

    path: str
    partitions: int
    files: int
    bytes: int

    def __str__(self) -> str:
        average_bytes_per_file = self.bytes / self.files if self.files else 0
        return (f"{self.path} | {self.partitions} particiones | {self.files} archivos | "
                f"{self.bytes / BYTES_PER_MB:.1f} MB (prom. {average_bytes_per_file / BYTES_PER_MB:.2f} MB/archivo)")


def dataset_storage_stats(settings: Settings, zone: Zone, dataset: str) -> StorageStats:
    """Cuenta particiones, archivos y bytes de un dataset local; da ceros si no existe."""
    path = dataset_path(settings, zone, dataset)
    parquet_files = list(Path(path).rglob("*.parquet")) if Path(path).is_dir() else []
    return StorageStats(
        path=path,
        partitions=len({parquet_file.parent for parquet_file in parquet_files}),
        files=len(parquet_files),
        bytes=sum(parquet_file.stat().st_size for parquet_file in parquet_files),
    )
