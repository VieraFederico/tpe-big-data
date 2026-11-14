from datetime import date

import pytest

from src.gold.marts import org_daily_usage_by_service, top_services_by_org_14d

SILVER_EVENTS_SCHEMA = (
    "org_id string, service string, event_date date, cost_usd_increment double, "
    "requests double, cpu_hours double, storage_gb_hours double, carbon_kg double, "
    "genai_tokens bigint, is_cost_anomaly boolean"
)
DAILY_SCHEMA = (
    "org_id string, service string, usage_date date, daily_cost_usd double, requests double"
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


def _top(spark, rows):
    daily = spark.createDataFrame(rows, DAILY_SCHEMA)
    return top_services_by_org_14d(daily)


def test_ventana_de_14_dias_para_cada_fecha_del_rango_global(spark):
    days = [date(2025, 8, day) for day in range(1, 21)]
    rows = [("org_a", "compute", day, float(day.day), 1.0) for day in days]
    rows += [("org_a", "storage", day, 10.0 * day.day, 2.0) for day in days]
    rows.append(("org_b", "compute", date(2025, 8, 20), 5.0, 1.0))

    top = _top(spark, rows).collect()
    by_key = {(row.org_id, row.service, row.as_of_date): row for row in top}

    assert {row.as_of_date for row in top} == set(days)
    last = by_key[("org_a", "compute", date(2025, 8, 20))]
    assert last.window_start_date == date(2025, 8, 7)
    assert last.cost_14d_usd == pytest.approx(sum(range(7, 21)), abs=1e-9)
    assert last.requests_14d == pytest.approx(14.0, abs=1e-9)
    storage = by_key[("org_a", "storage", date(2025, 8, 20))]
    assert storage.cost_14d_usd == pytest.approx(10.0 * sum(range(7, 21)), abs=1e-9)
    first = by_key[("org_a", "compute", date(2025, 8, 1))]
    assert first.window_start_date == date(2025, 7, 19)
    assert first.cost_14d_usd == pytest.approx(1.0, abs=1e-9)
    # org_b solo tiene el 20, así que no aparece en fechas sin días en la ventana.
    assert {row.as_of_date for row in top if row.org_id == "org_b"} == {date(2025, 8, 20)}


def test_rank_por_costo_desempata_por_servicio_sin_repetir(spark):
    rows = [
        ("org_a", "storage", DAY, 5.0, 0.0),
        ("org_a", "compute", DAY, 5.0, 0.0),
        ("org_a", "genai", DAY, 9.0, 0.0),
    ]

    ranks = {row.service: row.rank for row in _top(spark, rows).collect()}

    assert ranks == {"genai": 1, "compute": 2, "storage": 3}


def test_columnas_y_tipos_del_top_de_servicios(spark):
    assert _top(spark, [("org_a", "compute", DAY, 1.0, 1.0)]).dtypes == [
        ("org_id", "string"),
        ("service", "string"),
        ("as_of_date", "date"),
        ("window_start_date", "date"),
        ("rank", "int"),
        ("cost_14d_usd", "double"),
        ("requests_14d", "double"),
    ]
