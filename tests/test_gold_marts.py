from datetime import date

import pytest

from src.gold.marts import org_daily_usage_by_service

SILVER_EVENTS_SCHEMA = (
    "org_id string, service string, event_date date, cost_usd_increment double, "
    "requests double, cpu_hours double, storage_gb_hours double, carbon_kg double, "
    "genai_tokens bigint, is_cost_anomaly boolean"
)

DAY = date(2025, 8, 1)


def _event(service="compute", cost=1.0, requests=0.0, **overrides):
    fields = {
        "org_id": "org_a",
        "service": service,
        "event_date": DAY,
        "cost_usd_increment": cost,
        "requests": requests,
        "cpu_hours": 0.0,
        "storage_gb_hours": 0.0,
        "carbon_kg": None,
        "genai_tokens": None,
        "is_cost_anomaly": False,
        **overrides,
    }
    return tuple(fields.values())


def _daily(spark, *events):
    silver = spark.createDataFrame(list(events), SILVER_EVENTS_SCHEMA)
    return {row.service: row for row in org_daily_usage_by_service(silver).collect()}


def test_una_fila_por_organizacion_dia_y_servicio_con_costos_negativos_incluidos(spark):
    daily = _daily(
        spark,
        _event(cost=1.0, requests=10.0),
        _event(cost=2.0, requests=0.0),
        _event(cost=-0.005, requests=5.0),
        _event(service="storage", cost=3.0),
    )

    assert set(daily) == {"compute", "storage"}
    compute = daily["compute"]
    assert compute.usage_date == DAY
    assert compute.daily_cost_usd == pytest.approx(2.995, abs=1e-9)
    assert compute.requests == pytest.approx(15.0, abs=1e-9)
    assert compute.event_count == 3
    assert daily["storage"].event_count == 1


def test_columnas_y_tipos_del_mart(spark):
    silver = spark.createDataFrame([_event()], SILVER_EVENTS_SCHEMA)

    assert org_daily_usage_by_service(silver).dtypes == [
        ("org_id", "string"),
        ("service", "string"),
        ("usage_date", "date"),
        ("daily_cost_usd", "double"),
        ("requests", "double"),
        ("cpu_hours", "double"),
        ("storage_gb_hours", "double"),
        ("carbon_kg", "double"),
        ("genai_tokens", "bigint"),
        ("event_count", "int"),
        ("cost_anomaly_events", "int"),
    ]


def test_carbon_y_tokens_son_nulos_solo_si_todos_los_eventos_son_nulos(spark):
    daily = _daily(
        spark,
        _event(service="compute"),
        _event(service="compute"),
        _event(service="genai", carbon_kg=0.1, genai_tokens=100),
        _event(service="genai"),
        _event(service="genai", carbon_kg=0.2, genai_tokens=50),
    )

    assert daily["compute"].carbon_kg is None
    assert daily["compute"].genai_tokens is None
    assert daily["genai"].carbon_kg == pytest.approx(0.3, abs=1e-9)
    assert daily["genai"].genai_tokens == 150


def test_cuenta_los_eventos_marcados_como_anomalia_de_costo(spark):
    daily = _daily(
        spark,
        _event(is_cost_anomaly=True),
        _event(is_cost_anomaly=True),
        _event(),
        _event(),
        _event(),
    )

    assert daily["compute"].event_count == 5
    assert daily["compute"].cost_anomaly_events == 2
