"""Reglas de calidad declarativas y separación de filas válidas / en cuarentena."""

from collections.abc import Sequence
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, struct, to_json, when
from pyspark.sql.types import StringType

from src.common.schemas import CORRUPT_RECORD_COL

# Columnas que se agregan a toda fila en cuarentena, en cualquier capa.
RULE_ID_COL = "_rule_id"
REASON_COL = "_quarantine_reason"
RAW_COL = "_raw"
QUARANTINE_TS_COL = "_quarantine_ts"

QUARANTINE_COLUMNS = (RULE_ID_COL, REASON_COL, RAW_COL, QUARANTINE_TS_COL)


@dataclass(frozen=True)
class Rule:
    """Regla de calidad.

    Attributes:
        rule_id: Código corto que queda en la cuarentena, p. ej. ``BRZ_PARSE``.
        reason: Descripción legible de la violación.
        violation: Condición que es verdadera cuando la fila viola la regla.
    """

    rule_id: str
    reason: str
    violation: Column


def split_by_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Separa las filas válidas de las que violan alguna regla.

    Cada fila en cuarentena se etiqueta con la primera regla que viola (en el orden
    de ``rules``). Una violación que evalúa a null no cuenta como violación.

    Args:
        df: Datos a validar.
        rules: Reglas a aplicar, en orden de prioridad. Puede estar vacía.

    Returns:
        Tupla (válidas, cuarentena). La cuarentena siempre trae las columnas
        RULE_ID_COL, REASON_COL, RAW_COL y QUARANTINE_TS_COL, aunque no haya reglas.
        RAW_COL guarda el registro original (ver ``original_record``).
    """
    record = original_record(df)
    first_violated_rule_id = lit(None).cast(StringType())
    first_violated_reason = lit(None).cast(StringType())
    # Se arma de atrás para adelante para que la primera regla quede como la condición externa.
    for rule in reversed(rules):
        is_violated = coalesce(rule.violation, lit(False))
        first_violated_rule_id = when(is_violated, lit(rule.rule_id)).otherwise(
            first_violated_rule_id
        )
        first_violated_reason = when(is_violated, lit(rule.reason)).otherwise(
            first_violated_reason
        )

    tagged = df.withColumn(RULE_ID_COL, first_violated_rule_id)
    valid = tagged.filter(col(RULE_ID_COL).isNull()).drop(RULE_ID_COL)
    quarantine = (
        tagged.filter(col(RULE_ID_COL).isNotNull())
        .withColumn(REASON_COL, first_violated_reason)
        .withColumn(RAW_COL, record)
        .withColumn(QUARANTINE_TS_COL, current_timestamp())
    )
    return valid, quarantine


def original_record(df: DataFrame) -> Column:
    """Expresión con el registro original de cada fila, para guardar en la cuarentena.

    Si la fila no parseó, es el texto de CORRUPT_RECORD_COL. Si parseó, es el JSON de
    todas las columnas de entrada (sin las columnas técnicas de cuarentena ni
    CORRUPT_RECORD_COL, que en ese caso es null), con las columnas nulas explícitas.

    Args:
        df: Datos de entrada, tal como llegan a ``split_by_rules``.

    Returns:
        Columna de texto con el registro original.
    """
    excluded = {*QUARANTINE_COLUMNS, CORRUPT_RECORD_COL}
    input_columns = [col(f"`{name}`") for name in df.columns if name not in excluded]
    record_json = to_json(struct(*input_columns), {"ignoreNullFields": "false"})
    if CORRUPT_RECORD_COL in df.columns:
        return coalesce(col(CORRUPT_RECORD_COL), record_json)
    return record_json


def any_null(columns: Sequence[str]) -> Column:
    """Condición verdadera si al menos una de las columnas es null.

    Raises:
        ValueError: Si no se pasa ninguna columna.
    """
    if not columns:
        raise ValueError("any_null necesita al menos una columna")
    return reduce(
        lambda condition, name: condition | col(name).isNull(),
        columns[1:],
        col(columns[0]).isNull(),
    )
