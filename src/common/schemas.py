from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_RECORD_COL = "_corrupt_record"

# Fuente de datos de landing, con un esquema explicito
# Con una clave natural para deduplicar datos
@dataclass(frozen=True)
class Source:
    name: str
    path: str
    schema: StructType
    natural_key: tuple[str, ...]

    def read_schema(self) -> StructType:
        return StructType([*self.schema.fields, StructField(CORRUPT_RECORD_COL, StringType())])


def _fields(*specs: tuple[str, object]) -> StructType:
    return StructType([StructField(name, dtype, nullable=True) for name, dtype in specs])

# Type checks
S, D, F, B, I, L = StringType(), DateType(), DoubleType(), BooleanType(), IntegerType(), LongType()

CUSTOMERS_ORGS = Source(
    "customers_orgs",
    "customers_orgs.csv",
    _fields(
        ("org_id", S), ("org_name", S), ("industry", S), ("hq_region", S), ("plan_tier", S),
        ("is_enterprise", B), ("signup_date", D), ("sales_rep", S), ("lifecycle_stage", S),
        ("marketing_source", S), ("nps_score", F),
    ),
    ("org_id",),
)

USERS = Source(
    "users",
    "users.csv",
    _fields(
        ("user_id", S), ("org_id", S), ("email", S), ("role", S), ("active", B),
        ("created_at", D), ("last_login", D),
    ),
    ("user_id",),
)

RESOURCES = Source(
    "resources",
    "resources.csv",
    _fields(
        ("resource_id", S), ("org_id", S), ("service", S), ("region", S), ("created_at", D),
        ("state", S), ("tags_json", S),
    ),
    ("resource_id",),
)

BILLING_MONTHLY = Source(
    "billing_monthly",
    "billing_monthly.csv",
    _fields(
        ("invoice_id", S), ("org_id", S), ("month", D), ("subtotal", F), ("credits", F),
        ("taxes", F), ("currency", S), ("exchange_rate_to_usd", F),
    ),
    ("invoice_id",),
)

SUPPORT_TICKETS = Source(
    "support_tickets",
    "support_tickets.csv",
    _fields(
        ("ticket_id", S), ("org_id", S), ("category", S), ("severity", S), ("created_at", D),
        ("resolved_at", D), ("csat", F), ("sla_breached", B),
    ),
    ("ticket_id",),
)

MARKETING_TOUCHES = Source(
    "marketing_touches",
    "marketing_touches.csv",
    _fields(
        # "timestamp" en el CSV es solo fecha (yyyy-MM-dd).
        ("touch_id", S), ("org_id", S), ("campaign", S), ("channel", S), ("timestamp", D),
        ("clicked", B), ("converted", B),
    ),
    ("touch_id",),
)

NPS_SURVEYS = Source(
    "nps_surveys",
    "nps_surveys.csv",
    _fields(("org_id", S), ("survey_date", D), ("nps_score", F), ("comment", S)),
    # No trae id propio: una encuesta por organización y fecha.
    ("org_id", "survey_date"),
)

SOURCES: dict[str, Source] = {
    s.name: s
    for s in (
        CUSTOMERS_ORGS, USERS, RESOURCES, BILLING_MONTHLY,
        SUPPORT_TICKETS, MARKETING_TOUCHES, NPS_SURVEYS,
    )
}

# Esquema unificado v1 + v2
USAGE_EVENTS = Source(
    "usage_events",
    "usage_events_stream",
    _fields(
        ("event_id", S), ("timestamp", S), ("org_id", S), ("resource_id", S), ("service", S),
        ("region", S), ("metric", S), ("value", S), ("unit", S), ("cost_usd_increment", F),
        ("schema_version", I), ("carbon_kg", F), ("genai_tokens", L),
    ),
    ("event_id",),
)
