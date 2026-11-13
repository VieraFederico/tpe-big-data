"""Conformación de los eventos de uso de Bronze para Silver.

Normaliza las columnas categóricas, descarta duplicados por ``event_id``, separa los
eventos que violan las reglas de calidad y, sobre los válidos, convierte ``value``,
imputa ``unit``, arma las columnas por métrica y las marcas de calidad.
"""

from itertools import chain
from typing import NamedTuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import col, create_map, lit, lower, row_number, trim, when

from src.common.columns import EVENT_DATE, EVENT_TS, INGEST_TS, SOURCE_FILE
from src.quality.catalog import (
    METRIC_UNITS,
    silver_event_flags,
    silver_event_rules,
    value_as_double,
)
from src.quality.flags import DQ_FLAGS_COL, with_dq_flags
from src.quality.rules import RAW_COL, original_record, split_by_rules

EVENT_ID = "event_id"
VALUE_NUMERIC = "value_numeric"

# Columnas categóricas que se normalizan (sin espacios en los extremos y en minúsculas).
CATEGORICAL_COLUMNS = ("service", "region", "metric", "unit")

# Una columna por métrica conocida, con el mismo nombre que la métrica.
METRIC_COLUMNS = tuple(METRIC_UNITS)

# Columnas de un evento conformado, en orden. El enriquecimiento con datos maestros,
# la detección de anomalías y el run_id de Silver se agregan después.
CONFORMED_COLUMNS = (
    EVENT_ID,
    "org_id",
    "resource_id",
    "service",
    "region",
    "metric",
    "unit",
    EVENT_TS,
    EVENT_DATE,
    "schema_version",
    VALUE_NUMERIC,
    *METRIC_COLUMNS,
    "cost_usd_increment",
    "carbon_kg",
    "genai_tokens",
    DQ_FLAGS_COL,
    INGEST_TS,
    SOURCE_FILE,
)

# Columna interna con el registro de Bronze antes de normalizar.
_BRONZE_RECORD_COL = "_bronze_record"
_RANK_COL = "_duplicate_rank"


class ConformedEvents(NamedTuple):
    """Resultado de conformar los eventos de Bronze.

    Attributes:
        valid: Eventos válidos con las columnas CONFORMED_COLUMNS.
        quarantine: Eventos que violan alguna regla: columnas de Bronze (normalizadas) más
            las columnas de cuarentena. ``_raw`` guarda el registro de Bronze sin normalizar.
        duplicates: Eventos descartados por repetir ``event_id``, con las columnas de
            Bronze (normalizadas). No van a cuarentena; su cantidad es ``duplicates.count()``.
    """

    valid: DataFrame
    quarantine: DataFrame
    duplicates: DataFrame


def conform_events(bronze_events: DataFrame) -> ConformedEvents:
    """Conforma los eventos de uso leídos de Bronze.

    Pasos, en orden: normaliza las columnas categóricas, descarta duplicados por
    ``event_id``, separa los eventos que violan las reglas de Silver y, sobre los
    válidos, agrega ``value_numeric``, imputa ``unit``, agrega las columnas por métrica y
    ``dq_flags``. No ejecuta ninguna acción de Spark.

    Args:
        bronze_events: Eventos tal como están en Bronze (columnas de datos, técnicas,
            ``event_ts`` y ``event_date``).

    Returns:
        Eventos válidos, en cuarentena y duplicados descartados.
    """
    with_record = bronze_events.withColumn(_BRONZE_RECORD_COL, original_record(bronze_events))
    unique, duplicates = drop_duplicate_events(normalize_categoricals(with_record))
    valid, quarantine = split_by_rules(unique, silver_event_rules())
    quarantine = quarantine.withColumn(RAW_COL, col(_BRONZE_RECORD_COL)).drop(_BRONZE_RECORD_COL)
    return ConformedEvents(
        valid=_conform_valid_events(valid),
        quarantine=quarantine,
        duplicates=duplicates.drop(_BRONZE_RECORD_COL),
    )


def normalize_categoricals(df: DataFrame) -> DataFrame:
    for name in CATEGORICAL_COLUMNS:
        df = df.withColumn(name, lower(trim(col(name))))
    return df


def drop_duplicate_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy(EVENT_ID).orderBy(
        col(INGEST_TS).asc_nulls_last(),
        col(SOURCE_FILE).asc_nulls_last(),
        col(_BRONZE_RECORD_COL).asc_nulls_last(),
    )
    ranked = df.withColumn(
        _RANK_COL,
        when(col(EVENT_ID).isNotNull(), row_number().over(window)).otherwise(lit(1)),
    )
    unique = ranked.filter(col(_RANK_COL) == 1).drop(_RANK_COL)
    duplicates = ranked.filter(col(_RANK_COL) > 1).drop(_RANK_COL)
    return unique, duplicates


def _conform_valid_events(valid: DataFrame) -> DataFrame:
    flagged = with_dq_flags(
        valid.withColumn(VALUE_NUMERIC, value_as_double()), silver_event_flags()
    )
    needs_unit = col("unit").isNull() & col("value").isNotNull()
    imputed_unit = when(needs_unit, _metric_units()[col("metric")]).otherwise(col("unit"))
    conformed = flagged.withColumn("unit", imputed_unit)
    for metric in METRIC_COLUMNS:
        conformed = conformed.withColumn(
            metric,
            when(col("metric") == metric, col(VALUE_NUMERIC)).otherwise(lit(0.0)),
        )
    return conformed.select(*CONFORMED_COLUMNS)


def _metric_units() -> Column:
    pairs = ((lit(metric), lit(unit)) for metric, unit in METRIC_UNITS.items())
    return create_map(*chain.from_iterable(pairs))
