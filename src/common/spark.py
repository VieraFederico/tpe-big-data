"""Creación de la SparkSession del pipeline."""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "cloud-analytics") -> SparkSession:
    """Crea (o reutiliza) la SparkSession, configurable por variables de entorno.

    Fija la zona horaria en UTC y el overwrite dinámico de particiones, para que
    reescribir un dataset solo reemplace las particiones presentes en los datos nuevos.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
