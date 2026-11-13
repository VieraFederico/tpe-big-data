"""Catálogo de reglas y marcas de calidad por capa."""

from pyspark.sql import Column
from pyspark.sql.functions import coalesce, col, expr, from_json, lit, when
from pyspark.sql.types import ArrayType, StringType

from src.common.columns import EVENT_TS
from src.common.schemas import (
    BILLING_MONTHLY,
    CORRUPT_RECORD_COL,
    CUSTOMERS_ORGS,
    MARKETING_TOUCHES,
    NPS_SURVEYS,
    RESOURCES,
    SUPPORT_TICKETS,
    USAGE_EVENTS,
    USERS,
    Source,
)
from src.quality.flags import Flag
from src.quality.rules import Rule, any_null

# Unidad de cada métrica conocida; se usa para imputar ``unit`` cuando falta.
METRIC_UNITS: dict[str, str] = {
    "requests": "count",
    "cpu_hours": "hours",
    "storage_gb_hours": "gb_hours",
}

# Costo mínimo aceptado: entre este valor y 0 es un ajuste por redondeo y se marca;
# por debajo, el evento restaría costo real y va a cuarentena.
MIN_ACCEPTED_COST = -0.01

GENAI_SERVICE = "genai"

NPS_SCORE_RANGE = (-100.0, 100.0)
CSAT_RANGE = (1.0, 5.0)
USD_CURRENCY = "USD"


def bronze_structural_rules(source: Source) -> list[Rule]:
    """Reglas base de Bronze: la fila parsea con el esquema y tiene clave natural."""
    return [
        Rule("BRZ_PARSE", "La fila no respeta el esquema", col(CORRUPT_RECORD_COL).isNotNull()),
        Rule(
            "BRZ_NULL_KEY",
            f"Clave natural nula {source.natural_key}",
            any_null(source.natural_key),
        ),
    ]


def bronze_event_rules() -> list[Rule]:
    return [
        *bronze_structural_rules(USAGE_EVENTS),
        Rule("BRZ_BAD_TS", "timestamp nulo o no parseable", col(EVENT_TS).isNull()),
    ]


def value_as_double() -> Column:
    return expr("try_cast(`value` AS DOUBLE)")


def silver_event_rules() -> list[Rule]:
    value = col("value")
    unit = col("unit")
    metric = col("metric")
    cost = col("cost_usd_increment")
    schema_version = col("schema_version")
    carbon_kg = col("carbon_kg")
    genai_tokens = col("genai_tokens")
    return [
        Rule("SLV_NULL_EVENT_ID", "event_id nulo", col("event_id").isNull()),
        Rule(
            "SLV_NULL_DIMENSION",
            "org_id, service o metric nulo",
            any_null(("org_id", "service", "metric")),
        ),
        Rule(
            "SLV_SCHEMA_VERSION",
            "schema_version inválido o campos v2 incoherentes con la versión o el servicio",
            schema_version.isNull()
            | ~schema_version.isin(1, 2)
            | ((schema_version == 1) & (carbon_kg.isNotNull() | genai_tokens.isNotNull()))
            | ((schema_version == 2) & carbon_kg.isNull())
            | (genai_tokens.isNotNull() & (col("service") != GENAI_SERVICE)),
        ),
        Rule(
            "SLV_VALUE_NOT_NUMERIC",
            "value no se puede convertir a número",
            value.isNotNull() & value_as_double().isNull(),
        ),
        Rule(
            "SLV_UNIT_UNKNOWN",
            "unit nulo con value presente y metric sin unidad conocida",
            value.isNotNull() & unit.isNull() & ~metric.isin(list(METRIC_UNITS)),
        ),
        Rule(
            "SLV_COST_INVALID",
            f"cost_usd_increment nulo o menor a {MIN_ACCEPTED_COST}",
            cost.isNull() | (cost < MIN_ACCEPTED_COST),
        ),
    ]


def silver_event_flags() -> list[Flag]:
    value = col("value")
    cost = col("cost_usd_increment")
    return [
        Flag("VALUE_NULL", "value nulo", value.isNull()),
        Flag(
            "UNIT_IMPUTED",
            "unit imputado a partir de metric",
            value.isNotNull() & col("unit").isNull() & col("metric").isin(list(METRIC_UNITS)),
        ),
        Flag(
            "COST_NEGATIVE_SMALL",
            f"cost_usd_increment negativo pero mayor o igual a {MIN_ACCEPTED_COST}",
            (cost >= MIN_ACCEPTED_COST) & (cost < 0),
        ),
    ]


def tags_from_json() -> Column:
    return from_json(col("tags_json"), ArrayType(StringType()))


def silver_master_flags() -> dict[str, list[Flag]]:
    nps_out_of_range = Flag(
        "NPS_OUT_OF_RANGE",
        f"nps_score fuera de {_range_text(NPS_SCORE_RANGE)}; se reemplaza por null",
        _outside(col("nps_score"), NPS_SCORE_RANGE),
    )
    exchange_rate = col("exchange_rate_to_usd")
    return {
        CUSTOMERS_ORGS.name: [nps_out_of_range],
        USERS.name: [
            Flag(
                "LOGIN_BEFORE_CREATED",
                "last_login anterior a created_at",
                col("last_login") < col("created_at"),
            ),
        ],
        RESOURCES.name: [
            Flag(
                "TAGS_INVALID_JSON",
                "tags_json no es un array JSON válido; tags queda en null",
                col("tags_json").isNotNull() & tags_from_json().isNull(),
            ),
        ],
        BILLING_MONTHLY.name: [
            Flag("CREDITS_IMPUTED", "credits nulo; se reemplaza por 0.0", col("credits").isNull()),
            Flag("SUBTOTAL_NEGATIVE", "subtotal negativo", col("subtotal") < 0),
            Flag(
                "FX_USD_NOT_ONE",
                f"currency {USD_CURRENCY} con exchange_rate_to_usd distinto de 1",
                (col("currency") == USD_CURRENCY) & (exchange_rate != 1),
            ),
        ],
        SUPPORT_TICKETS.name: [
            Flag(
                "CSAT_OUT_OF_RANGE",
                f"csat fuera de {_range_text(CSAT_RANGE)}; se reemplaza por null",
                _outside(col("csat"), CSAT_RANGE),
            ),
        ],
        MARKETING_TOUCHES.name: [],
        NPS_SURVEYS.name: [nps_out_of_range],
    }


def silver_master_fixes() -> dict[str, dict[str, Column]]:
    nps_within_range = {"nps_score": _within_range_or_null(col("nps_score"), NPS_SCORE_RANGE)}
    return {
        CUSTOMERS_ORGS.name: nps_within_range,
        USERS.name: {},
        RESOURCES.name: {},
        BILLING_MONTHLY.name: {"credits": coalesce(col("credits"), lit(0.0))},
        SUPPORT_TICKETS.name: {"csat": _within_range_or_null(col("csat"), CSAT_RANGE)},
        MARKETING_TOUCHES.name: {},
        NPS_SURVEYS.name: nps_within_range,
    }


def _outside(column: Column, bounds: tuple[float, float]) -> Column:
    return ~column.between(*bounds)


def _within_range_or_null(column: Column, bounds: tuple[float, float]) -> Column:
    return when(column.between(*bounds), column)


def _range_text(bounds: tuple[float, float]) -> str:
    low, high = bounds
    return f"[{low:g}, {high:g}]"
