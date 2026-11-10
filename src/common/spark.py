"""Creación de la SparkSession del pipeline."""
import os
from collections.abc import Mapping

from pyspark.sql import SparkSession

SEMANTIC_CONFIG: dict[str, str] = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
}


def get_spark(
    app_name: str = "cloud-analytics",
    *,
    master: str | None = None,
    config_overrides: Mapping[str, str] | None = None,
    log_level: str | None = None,
) -> SparkSession:
    """Crea (o reutiliza) la SparkSession, configurable por variables de entorno.

    Args:
        app_name: Nombre de la aplicación en Spark.
        master: Master de Spark; por defecto SPARK_MASTER o ``local[*]``.
        config_overrides: Configuración extra que pisa la de recursos (no la semántica
            de SEMANTIC_CONFIG, que siempre se aplica al final).
        log_level: Nivel de log de Spark; por defecto SPARK_LOG_LEVEL o ``WARN``.

    Returns:
        La SparkSession activa.
    """
    config = {
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        **(config_overrides or {}),
        **SEMANTIC_CONFIG,
    }
    builder = SparkSession.builder.appName(app_name).master(
        master or os.getenv("SPARK_MASTER", "local[*]")
    )
    for key, value in config.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level or os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
