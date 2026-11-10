"""Nombres de columnas técnicas compartidas entre capas y helper para agregarlas."""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_date

# Columnas técnicas que se agregan a todos los DataFrames de Landing y se usan en Bronze/Silver
INGEST_TS = "ingest_ts"
INGEST_DATE = "ingest_date"
SOURCE_FILE = "source_file"
RUN_ID = "run_id"

# Columnas derivadas del timestamp de los eventos
EVENT_TS = "event_ts"
EVENT_DATE = "event_date"


def with_technical_columns(df: DataFrame, run_id: str, ingest_ts: Column | None = None) -> DataFrame:
    """Agrega las columnas de trazabilidad de la ingesta.

    Args:
        df: Datos leídos de Landing.
        run_id: Id de la corrida que escribe los datos.
        ingest_ts: Momento de ingesta a usar; por defecto, el timestamp actual. Pasarlo
            fijo permite que todas las fuentes de una corrida compartan el mismo valor.

    Returns:
        El DataFrame con INGEST_TS, INGEST_DATE, SOURCE_FILE y RUN_ID.
    """
    timestamp = ingest_ts if ingest_ts is not None else current_timestamp()
    return (
        df.withColumn(INGEST_TS, timestamp)
        .withColumn(INGEST_DATE, to_date(col(INGEST_TS)))
        .withColumn(SOURCE_FILE, input_file_name())
        .withColumn(RUN_ID, lit(run_id))
    )
