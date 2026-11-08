from pyspark.sql import functions as F

from src.common.schemas import CORRUPT_RECORD_COL, Source
from src.quality.rules import Rule, any_null


def bronze_structural_rules(source: Source) -> list[Rule]:
    return [
        Rule("BRZ_PARSE", "La fila no respeta el esquema", F.col(CORRUPT_RECORD_COL).isNotNull()),
        Rule("BRZ_NULL_KEY", f"Clave natural nula {source.natural_key}", any_null(source.natural_key)),
    ]


def silver_event_rules() -> list[Rule]:
    return [
        Rule("SLV_NULL_UNIT", "unit nulo con value presente", F.col("value").isNotNull() & F.col("unit").isNull()),
    ]
