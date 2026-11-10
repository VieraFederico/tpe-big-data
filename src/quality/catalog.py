from pyspark.sql.functions import col

from src.common.columns import EVENT_TS
from src.common.schemas import CORRUPT_RECORD_COL, USAGE_EVENTS, Source
from src.quality.rules import Rule, any_null

# Reglas de base
def bronze_structural_rules(source: Source) -> list[Rule]:
    return [
        Rule("BRZ_PARSE", "La fila no respeta el esquema", col(CORRUPT_RECORD_COL).isNotNull()),
        Rule("BRZ_NULL_KEY", f"Clave natural nula {source.natural_key}", any_null(source.natural_key)),
    ]

# Reglas para streaming de eventos
def bronze_event_rules() -> list[Rule]:
    return [
        *bronze_structural_rules(USAGE_EVENTS),
        Rule("BRZ_BAD_TS", "timestamp nulo o no parseable", col(EVENT_TS).isNull()),
    ]


def silver_event_rules() -> list[Rule]:
    return [
        Rule("SLV_NULL_UNIT", "unit nulo con value presente", col("value").isNotNull() & col("unit").isNull()),
    ]
