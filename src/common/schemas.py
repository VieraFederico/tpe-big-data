"""Esquemas explícitos de las fuentes de Landing y sus claves naturales."""
from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_RECORD_COL = "_corrupt_record"


@dataclass(frozen=True)
class Source:
    """Fuente de datos de Landing con esquema explícito.

    Attributes:
        name: Nombre del dataset en las zonas del lake.
        path: Archivo o carpeta dentro de Landing.
        schema: Esquema de los datos (sin la columna de registro corrupto).
        natural_key: Columnas que identifican una fila; se usan para deduplicar.
    """

    name: str
    path: str
    schema: StructType
    natural_key: tuple[str, ...]

    def read_schema(self) -> StructType:
        """Esquema de lectura: el de los datos más la columna de registro corrupto."""
        return StructType([*self.schema.fields, StructField(CORRUPT_RECORD_COL, StringType())])


def _fields(*field_specs: tuple[str, DataType]) -> StructType:
    """Arma un StructType nullable a partir de pares (nombre, tipo)."""
    return StructType(
        [StructField(name, data_type, nullable=True) for name, data_type in field_specs]
    )


STRING, DATE, DOUBLE, BOOLEAN, INTEGER, LONG = (
    StringType(), DateType(), DoubleType(), BooleanType(), IntegerType(), LongType()
)

CUSTOMERS_ORGS = Source(
    "customers_orgs",
    "customers_orgs.csv",
    _fields(
        ("org_id", STRING), ("org_name", STRING), ("industry", STRING), ("hq_region", STRING), ("plan_tier", STRING),
        ("is_enterprise", BOOLEAN), ("signup_date", DATE), ("sales_rep", STRING), ("lifecycle_stage", STRING),
        ("marketing_source", STRING), ("nps_score", DOUBLE),
    ),
    ("org_id",),
)

USERS = Source(
    "users",
    "users.csv",
    _fields(
        ("user_id", STRING), ("org_id", STRING), ("email", STRING), ("role", STRING), ("active", BOOLEAN),
        ("created_at", DATE), ("last_login", DATE),
    ),
    ("user_id",),
)

RESOURCES = Source(
    "resources",
    "resources.csv",
    _fields(
        ("resource_id", STRING), ("org_id", STRING), ("service", STRING), ("region", STRING), ("created_at", DATE),
        ("state", STRING), ("tags_json", STRING),
    ),
    ("resource_id",),
)

BILLING_MONTHLY = Source(
    "billing_monthly",
    "billing_monthly.csv",
    _fields(
        ("invoice_id", STRING), ("org_id", STRING), ("month", DATE), ("subtotal", DOUBLE), ("credits", DOUBLE),
        ("taxes", DOUBLE), ("currency", STRING), ("exchange_rate_to_usd", DOUBLE),
    ),
    ("invoice_id",),
)

SUPPORT_TICKETS = Source(
    "support_tickets",
    "support_tickets.csv",
    _fields(
        ("ticket_id", STRING), ("org_id", STRING), ("category", STRING), ("severity", STRING), ("created_at", DATE),
        ("resolved_at", DATE), ("csat", DOUBLE), ("sla_breached", BOOLEAN),
    ),
    ("ticket_id",),
)

MARKETING_TOUCHES = Source(
    "marketing_touches",
    "marketing_touches.csv",
    _fields(
        # "timestamp" en el CSV es solo fecha (yyyy-MM-dd).
        ("touch_id", STRING), ("org_id", STRING), ("campaign", STRING), ("channel", STRING), ("timestamp", DATE),
        ("clicked", BOOLEAN), ("converted", BOOLEAN),
    ),
    ("touch_id",),
)

NPS_SURVEYS = Source(
    "nps_surveys",
    "nps_surveys.csv",
    _fields(("org_id", STRING), ("survey_date", DATE), ("nps_score", DOUBLE), ("comment", STRING)),
    # No trae id propio: una encuesta por organización y fecha.
    ("org_id", "survey_date"),
)

SOURCES: dict[str, Source] = {
    source.name: source
    for source in (
        CUSTOMERS_ORGS, USERS, RESOURCES, BILLING_MONTHLY,
        SUPPORT_TICKETS, MARKETING_TOUCHES, NPS_SURVEYS,
    )
}

# Esquema unificado v1 + v2
USAGE_EVENTS = Source(
    "usage_events",
    "usage_events_stream",
    _fields(
        ("event_id", STRING), ("timestamp", STRING), ("org_id", STRING), ("resource_id", STRING), ("service", STRING),
        ("region", STRING), ("metric", STRING), ("value", STRING), ("unit", STRING), ("cost_usd_increment", DOUBLE),
        ("schema_version", INTEGER), ("carbon_kg", DOUBLE), ("genai_tokens", LONG),
    ),
    ("event_id",),
)
