"""Flags de calidad declarativos y armado de la columna ``dq_flags``."""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    array_compact,
    array_distinct,
    array_sort,
    array_union,
    coalesce,
    col,
    lit,
    when,
)
from pyspark.sql.types import ArrayType, StringType

DQ_FLAGS_COL = "dq_flags"

_FLAGS_TYPE = ArrayType(StringType())


@dataclass(frozen=True)
class Flag:
    """Marca de calidad que no saca la fila del dataset.

    Attributes:
        flag_id: Código que queda en ``dq_flags``, p. ej. ``VALUE_NULL``.
        reason: Descripción legible de lo que indica la marca.
        condition: Condición que es verdadera cuando la marca aplica a la fila.
    """

    flag_id: str
    reason: str
    condition: Column


def with_dq_flags(df: DataFrame, flags: list[Flag]) -> DataFrame:
    """Agrega a cada fila las marcas que le aplican en la columna DQ_FLAGS_COL.

    ``dq_flags`` es un ``array<string>`` ordenado alfabéticamente, sin repetidos y vacío
    (no null) cuando no aplica ninguna marca. Una condición que evalúa a null no aplica.
    Si ``df`` ya trae DQ_FLAGS_COL, las marcas nuevas se suman a las existentes.

    Args:
        df: Datos a marcar.
        flags: Marcas a evaluar. Puede estar vacía.

    Returns:
        El DataFrame con DQ_FLAGS_COL.
    """
    flag_ids = [when(coalesce(flag.condition, lit(False)), lit(flag.flag_id)) for flag in flags]
    new_flags = array_compact(array(*flag_ids).cast(_FLAGS_TYPE))
    if DQ_FLAGS_COL in df.columns:
        existing = coalesce(col(DQ_FLAGS_COL), array().cast(_FLAGS_TYPE))
        new_flags = array_union(existing, new_flags)
    return df.withColumn(DQ_FLAGS_COL, array_sort(array_distinct(new_flags)))
