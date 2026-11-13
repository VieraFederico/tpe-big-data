from datetime import date, datetime, time

import pytest
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.schemas import SOURCES
from src.quality.catalog import silver_master_fixes, silver_master_flags
from src.quality.flags import DQ_FLAGS_COL
from src.silver.master_data import conform_master, conformed_master_columns, latest_snapshot

FIRST_SNAPSHOT = date(2025, 9, 1)
SECOND_SNAPSHOT = date(2025, 9, 2)

DEFAULT_ROWS = {
    "customers_orgs": {
        "org_id": "org_a",
        "org_name": "Nimbus Labs",
        "industry": "Education",
        "hq_region": "sa-east",
        "plan_tier": "standard",
        "is_enterprise": False,
        "signup_date": date(2025, 5, 26),
        "sales_rep": "rep_c",
        "lifecycle_stage": "active",
        "marketing_source": "partner",
        "nps_score": 10.0,
    },
    "users": {
        "user_id": "user_a",
        "org_id": "org_a",
        "email": "user_a@example.com",
        "role": "devops",
        "active": True,
        "created_at": date(2025, 7, 10),
        "last_login": date(2025, 8, 10),
    },
    "resources": {
        "resource_id": "res_a",
        "org_id": "org_a",
        "service": "compute",
        "region": "sa-east",
        "created_at": date(2025, 8, 14),
        "state": "running",
        "tags_json": '["env:prod"]',
    },
    "billing_monthly": {
        "invoice_id": "inv_a",
        "org_id": "org_a",
        "month": date(2025, 6, 1),
        "subtotal": 100.0,
        "credits": 5.0,
        "taxes": 21.0,
        "currency": "ARS",
        "exchange_rate_to_usd": 0.5,
    },
    "support_tickets": {
        "ticket_id": "tkt_a",
        "org_id": "org_a",
        "category": "performance",
        "severity": "low",
        "created_at": date(2025, 7, 2),
        "resolved_at": date(2025, 7, 8),
        "csat": 4.0,
        "sla_breached": False,
    },
    "marketing_touches": {
        "touch_id": "mkt_a",
        "org_id": "org_a",
        "campaign": "webinar_finops",
        "channel": "email",
        "timestamp": date(2025, 7, 23),
        "clicked": False,
        "converted": False,
    },
    "nps_surveys": {
        "org_id": "org_a",
        "survey_date": date(2025, 8, 22),
        "nps_score": 11.0,
        "comment": "Complex billing",
    },
}

TECHNICAL_FIELDS = [
    StructField("_corrupt_record", StringType()),
    StructField("ingest_ts", TimestampType()),
    StructField("ingest_date", DateType()),
    StructField("source_file", StringType()),
    StructField("run_id", StringType()),
]


def _row(name, ingest_date=SECOND_SNAPSHOT, **overrides):
    fields = {**DEFAULT_ROWS[name], **overrides}
    assert set(fields) == set(DEFAULT_ROWS[name])
    return (
        *fields.values(),
        None,
        datetime.combine(ingest_date, time(10)),
        ingest_date,
        f"file:/landing/{ingest_date}/{name}.csv",
        "bronze-run",
    )


def _bronze(spark, name, *rows):
    schema = StructType([*SOURCES[name].schema.fields, *TECHNICAL_FIELDS])
    return spark.createDataFrame(list(rows), schema)


def _conformed_by(spark, name, key, *rows):
    conformed = conform_master(name, _bronze(spark, name, *rows))
    return {row[key]: row for row in conformed.collect()}


@pytest.mark.parametrize("name", ["customers_orgs", "nps_surveys"])
def test_nps_fuera_de_rango_queda_nulo_con_marca(spark, name):
    """Dado nps_score 150, -100, 100 y nulo.

    Cuando se conforma, entonces 150 pasa a nulo con NPS_OUT_OF_RANGE y el resto queda igual
    y sin marcas.
    """
    rows = _conformed_by(
        spark,
        name,
        "org_id",
        _row(name, org_id="a", nps_score=150.0),
        _row(name, org_id="b", nps_score=-100.0),
        _row(name, org_id="c", nps_score=100.0),
        _row(name, org_id="d", nps_score=None),
    )

    assert {org_id: (row.nps_score, row[DQ_FLAGS_COL]) for org_id, row in rows.items()} == {
        "a": (None, ["NPS_OUT_OF_RANGE"]),
        "b": (-100.0, []),
        "c": (100.0, []),
        "d": (None, []),
    }


def test_csat_fuera_de_rango_queda_nulo_con_marca(spark):
    """Dado tickets con csat 0, 1, 5, 6 y nulo.

    Cuando se conforman, entonces 0 y 6 pasan a nulo con CSAT_OUT_OF_RANGE; 1, 5 y nulo
    quedan igual y sin marcas.
    """
    name = "support_tickets"
    rows = _conformed_by(
        spark,
        name,
        "ticket_id",
        *[_row(name, ticket_id=str(csat), csat=csat) for csat in (0.0, 1.0, 5.0, 6.0, None)],
    )

    assert {ticket_id: (row.csat, row[DQ_FLAGS_COL]) for ticket_id, row in rows.items()} == {
        "0.0": (None, ["CSAT_OUT_OF_RANGE"]),
        "1.0": (1.0, []),
        "5.0": (5.0, []),
        "6.0": (None, ["CSAT_OUT_OF_RANGE"]),
        "None": (None, []),
    }


def test_login_anterior_al_alta_queda_con_marca(spark):
    """Dado usuarios con last_login anterior, igual y nulo respecto de created_at.

    Cuando se conforman, entonces solo el anterior tiene LOGIN_BEFORE_CREATED y ninguno
    cambia sus fechas.
    """
    name = "users"
    created_at = date(2025, 7, 10)
    rows = _conformed_by(
        spark,
        name,
        "user_id",
        _row(name, user_id="before", created_at=created_at, last_login=date(2025, 7, 1)),
        _row(name, user_id="same", created_at=created_at, last_login=created_at),
        _row(name, user_id="null", created_at=created_at, last_login=None),
    )

    assert {user_id: (row.last_login, row[DQ_FLAGS_COL]) for user_id, row in rows.items()} == {
        "before": (date(2025, 7, 1), ["LOGIN_BEFORE_CREATED"]),
        "same": (created_at, []),
        "null": (None, []),
    }


def test_credits_nulo_se_imputa_y_revenue_usa_el_valor_corregido(spark):
    """Dado una factura subtotal 100, credits nulo, taxes 21, tipo de cambio 0.5 en ARS.

    Cuando se conforma, entonces credits es 0.0, revenue_usd es 60.5, tiene
    CREDITS_IMPUTED y billing_month es "2025-06".
    """
    row = conform_master(
        "billing_monthly",
        _bronze(
            spark,
            "billing_monthly",
            _row(
                "billing_monthly",
                month=date(2025, 6, 1),
                subtotal=100.0,
                credits=None,
                taxes=21.0,
                currency="ARS",
                exchange_rate_to_usd=0.5,
            ),
        ),
    ).first()

    assert row.credits == 0.0
    assert row.revenue_usd == pytest.approx(60.5)
    assert row[DQ_FLAGS_COL] == ["CREDITS_IMPUTED"]
    assert row.billing_month == "2025-06"


def test_revenue_resta_credits_informados(spark):
    """Dado subtotal 100, credits 5, taxes 21 y tipo de cambio 2, entonces revenue_usd es 232."""
    row = conform_master(
        "billing_monthly",
        _bronze(spark, "billing_monthly", _row("billing_monthly", exchange_rate_to_usd=2.0)),
    ).first()

    assert row.revenue_usd == pytest.approx(232.0)
    assert row[DQ_FLAGS_COL] == []


def test_marcas_de_facturacion_en_orden_alfabetico(spark):
    """Dado una factura en USD con tipo de cambio 0.97684 y subtotal -10, y otra en USD con
    tipo de cambio 1.

    Cuando se conforman, entonces la primera tiene FX_USD_NOT_ONE y SUBTOTAL_NEGATIVE en
    orden alfabético y la segunda no tiene marcas.
    """
    name = "billing_monthly"
    rows = _conformed_by(
        spark,
        name,
        "invoice_id",
        _row(name, invoice_id="a", currency="USD", exchange_rate_to_usd=0.97684, subtotal=-10.0),
        _row(name, invoice_id="b", currency="USD", exchange_rate_to_usd=1.0),
    )

    assert rows["a"][DQ_FLAGS_COL] == ["FX_USD_NOT_ONE", "SUBTOTAL_NEGATIVE"]
    assert rows["b"][DQ_FLAGS_COL] == []


def test_currency_se_normaliza_en_mayusculas_antes_de_marcar(spark):
    """Dado currency " usd " con tipo de cambio 0.9.

    Cuando se conforma, entonces currency es "USD" y tiene FX_USD_NOT_ONE.
    """
    row = conform_master(
        "billing_monthly",
        _bronze(
            spark,
            "billing_monthly",
            _row("billing_monthly", currency=" usd ", exchange_rate_to_usd=0.9),
        ),
    ).first()

    assert row.currency == "USD"
    assert row[DQ_FLAGS_COL] == ["FX_USD_NOT_ONE"]


@pytest.mark.parametrize(
    ("name", "column", "value", "expected"),
    [
        ("customers_orgs", "plan_tier", " Enterprise ", "enterprise"),
        ("resources", "service", " Compute ", "compute"),
        ("resources", "region", "SA-East ", "sa-east"),
        ("resources", "state", " Running", "running"),
        ("support_tickets", "severity", "HIGH", "high"),
        ("marketing_touches", "channel", " Email ", "email"),
    ],
)
def test_categoricas_se_normalizan_en_minusculas(spark, name, column, value, expected):
    """Dado una categórica con espacios y mayúsculas, entonces queda sin espacios y en
    minúsculas."""
    row = conform_master(name, _bronze(spark, name, _row(name, **{column: value}))).first()

    assert row[column] == expected


def test_tags_se_arman_desde_tags_json(spark):
    """Dado recursos con tags_json válido, inválido, un objeto JSON y nulo.

    Cuando se conforman, entonces el válido trae su array de tags sin marcas, el inválido y
    el objeto traen tags nulo con TAGS_INVALID_JSON y el nulo trae tags nulo sin marcas.
    """
    name = "resources"
    rows = _conformed_by(
        spark,
        name,
        "resource_id",
        _row(name, resource_id="valid", tags_json='["env:prod","team:a"]'),
        _row(name, resource_id="invalid", tags_json="{no json"),
        _row(name, resource_id="object", tags_json='{"env": "prod"}'),
        _row(name, resource_id="null", tags_json=None),
    )

    assert {resource_id: (row.tags, row[DQ_FLAGS_COL]) for resource_id, row in rows.items()} == {
        "valid": (["env:prod", "team:a"], []),
        "invalid": (None, ["TAGS_INVALID_JSON"]),
        "object": (None, ["TAGS_INVALID_JSON"]),
        "null": (None, []),
    }


@pytest.mark.parametrize("name", list(SOURCES))
def test_conformado_tiene_las_columnas_de_silver_y_flags_vacios(spark, name):
    """Dado una fila de Bronze sin problemas de cada dato maestro.

    Cuando se conforma, entonces no trae _corrupt_record, ingest_date ni el run_id de Bronze,
    y dq_flags es un array<string> vacío.
    """
    conformed = conform_master(name, _bronze(spark, name, _row(name)))

    assert tuple(conformed.columns) == conformed_master_columns(name)
    assert not {"_corrupt_record", "ingest_date", "run_id"} & set(conformed.columns)
    assert conformed.schema[DQ_FLAGS_COL].dataType == ArrayType(StringType())
    assert conformed.first()[DQ_FLAGS_COL] == []


def test_columnas_derivadas_de_recursos_y_facturacion(spark):
    """Dado recursos y facturas, entonces traen tags, revenue_usd y billing_month tipados."""
    resources = conform_master("resources", _bronze(spark, "resources", _row("resources")))
    billing = conform_master(
        "billing_monthly", _bronze(spark, "billing_monthly", _row("billing_monthly"))
    )

    assert resources.schema["tags"].dataType == ArrayType(StringType())
    assert billing.schema["revenue_usd"].dataType == DoubleType()
    assert billing.schema["billing_month"].dataType == StringType()
    assert tuple(billing.columns) == (
        "invoice_id",
        "org_id",
        "month",
        "subtotal",
        "credits",
        "taxes",
        "currency",
        "exchange_rate_to_usd",
        "revenue_usd",
        "billing_month",
        DQ_FLAGS_COL,
        "ingest_ts",
        "source_file",
    )


def test_latest_snapshot_se_queda_con_la_ultima_particion(spark):
    """Dado dos snapshots de organizaciones, entonces solo quedan las filas del más reciente."""
    name = "customers_orgs"
    bronze = _bronze(
        spark,
        name,
        _row(name, FIRST_SNAPSHOT, org_id="a"),
        _row(name, FIRST_SNAPSHOT, org_id="b"),
        _row(name, SECOND_SNAPSHOT, org_id="a"),
    )

    rows = latest_snapshot(bronze).collect()

    assert [(row.org_id, row.ingest_date) for row in rows] == [("a", SECOND_SNAPSHOT)]


def test_dato_maestro_desconocido_falla(spark):
    """Dado un nombre que no es un dato maestro, entonces conform_master falla."""
    with pytest.raises(ValueError, match="desconocido"):
        conform_master("orgs", _bronze(spark, "users", _row("users")))


def test_catalogo_de_marcas_por_dato_maestro(spark):
    """Dado el catálogo de datos maestros, entonces cada fuente tiene marcas y correcciones."""
    flags_by_name = silver_master_flags()

    assert {name: [flag.flag_id for flag in flags] for name, flags in flags_by_name.items()} == {
        "customers_orgs": ["NPS_OUT_OF_RANGE"],
        "users": ["LOGIN_BEFORE_CREATED"],
        "resources": ["TAGS_INVALID_JSON"],
        "billing_monthly": ["CREDITS_IMPUTED", "SUBTOTAL_NEGATIVE", "FX_USD_NOT_ONE"],
        "support_tickets": ["CSAT_OUT_OF_RANGE"],
        "marketing_touches": [],
        "nps_surveys": ["NPS_OUT_OF_RANGE"],
    }
    assert {name: set(fixes) for name, fixes in silver_master_fixes().items()} == {
        "customers_orgs": {"nps_score"},
        "users": set(),
        "resources": set(),
        "billing_monthly": {"credits"},
        "support_tickets": {"csat"},
        "marketing_touches": set(),
        "nps_surveys": {"nps_score"},
    }
