from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, current_timestamp, input_file_name, to_date

# Columnas tecnicas que se agregan a todos los DataFrames de Landing y se usan en Bronze/Silver
INGEST_TS = "ingest_ts"
INGEST_DATE = "ingest_date"
SOURCE_FILE = "source_file"


def with_technical_columns(df: DataFrame, ingest_ts: Column | None = None) -> DataFrame:
    ts = ingest_ts if ingest_ts is not None else current_timestamp()
    return (
        df.withColumn(INGEST_TS, ts)
        .withColumn(INGEST_DATE, to_date(col(INGEST_TS)))
        .withColumn(SOURCE_FILE, input_file_name())
    )
