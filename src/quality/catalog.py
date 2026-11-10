"""Catálogo de reglas de calidad por capa."""
from pyspark.sql.functions import col

from src.common.columns import EVENT_TS
from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS, Source
from src.quality.rules import Rule, any_null


def bronze_structural_rules(source: Source) -> list[Rule]:
    """Reglas base de Bronze: la fila parsea con el esquema y tiene clave natural."""
    return [
        Rule("BRZ_PARSE", "La fila no respeta el esquema", col(CORRUPT_RECORD_COL).isNotNull()),
        Rule("BRZ_NULL_KEY", f"Clave natural nula {source.natural_key}", any_null(source.natural_key)),
    ]


def bronze_event_rules() -> list[Rule]:
    """Reglas de Bronze para el stream de eventos: las estructurales más un timestamp válido."""
    return [
        *bronze_structural_rules(USAGE_EVENTS),
        Rule("BRZ_BAD_TS", "timestamp nulo o no parseable", col(EVENT_TS).isNull()),
    ]


def silver_event_rules() -> list[Rule]:
    """Reglas de Silver para los eventos de uso."""
    return [
        Rule("SLV_NULL_UNIT", "unit nulo con value presente", col("value").isNotNull() & col("unit").isNull()),
    ]
