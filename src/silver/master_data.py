"""Conformación de los datos maestros de Bronze para Silver."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lower, trim, upper
from pyspark.sql.functions import max as spark_max

from src.common.columns import INGEST_DATE, INGEST_TS, SOURCE_FILE
from src.common.schemas import BILLING_MONTHLY, RESOURCES, SOURCES
from src.quality.catalog import silver_master_fixes, silver_master_flags, tags_from_json
from src.quality.flags import DQ_FLAGS_COL, with_dq_flags

LOWERCASE_CATEGORICAL_COLUMNS = ("service", "region", "severity", "plan_tier", "channel", "state")
UPPERCASE_CATEGORICAL_COLUMNS = ("currency",)

TAGS = "tags"
REVENUE_USD = "revenue_usd"
BILLING_MONTH = "billing_month"

DERIVED_COLUMNS: dict[str, tuple[str, ...]] = {
    RESOURCES.name: (TAGS,),
    BILLING_MONTHLY.name: (REVENUE_USD, BILLING_MONTH),
}

_LATEST_INGEST_DATE_COL = "_latest_ingest_date"


def latest_snapshot(df: DataFrame) -> DataFrame:
    """Filas de la partición con el ``ingest_date`` más reciente."""
    latest = df.agg(spark_max(INGEST_DATE).alias(_LATEST_INGEST_DATE_COL))
    return df.join(latest, col(INGEST_DATE) == col(_LATEST_INGEST_DATE_COL)).drop(
        _LATEST_INGEST_DATE_COL
    )


def conformed_master_columns(name: str) -> tuple[str, ...]:
    """Columnas, en orden, de un dato maestro conformado (sin el ``run_id`` de Silver)."""
    _ensure_master_dataset(name)
    data_columns = tuple(field.name for field in SOURCES[name].schema.fields)
    return (*data_columns, *DERIVED_COLUMNS.get(name, ()), DQ_FLAGS_COL, INGEST_TS, SOURCE_FILE)


def conform_master(name: str, df: DataFrame) -> DataFrame:
    """Conforma un snapshot de Bronze del dato maestro ``name``.

    Devuelve exactamente ``conformed_master_columns(name)``.
    """
    return apply_master_rules(name, df).select(*conformed_master_columns(name))


def apply_master_rules(name: str, df: DataFrame) -> DataFrame:
    """Normaliza, marca, corrige y deriva las columnas de ``name`` sin quitar ninguna."""
    _ensure_master_dataset(name)
    flagged = with_dq_flags(_normalize_categoricals(df), silver_master_flags()[name])
    fixed = flagged.withColumns(silver_master_fixes()[name])
    return _with_derived_columns(name, fixed)


def _ensure_master_dataset(name: str) -> None:
    if name not in SOURCES:
        raise ValueError(f"Dato maestro desconocido: {name}")


def _normalize_categoricals(df: DataFrame) -> DataFrame:
    for name in LOWERCASE_CATEGORICAL_COLUMNS:
        if name in df.columns:
            df = df.withColumn(name, lower(trim(col(name))))
    for name in UPPERCASE_CATEGORICAL_COLUMNS:
        if name in df.columns:
            df = df.withColumn(name, upper(trim(col(name))))
    return df


def _with_derived_columns(name: str, df: DataFrame) -> DataFrame:
    if name == RESOURCES.name:
        return df.withColumn(TAGS, tags_from_json())
    if name == BILLING_MONTHLY.name:
        revenue = (col("subtotal") - col("credits") + col("taxes")) * col("exchange_rate_to_usd")
        return df.withColumns(
            {REVENUE_USD: revenue, BILLING_MONTH: date_format(col("month"), "yyyy-MM")}
        )
    return df
