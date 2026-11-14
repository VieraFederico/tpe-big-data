from datetime import date, timedelta

import pytest

from src.gold.mapreduce import METRICS, compare, org_daily_usage_mapreduce
from src.gold.marts import org_daily_usage_by_service

SILVER_EVENTS_SCHEMA = (
    "org_id string, service string, event_date date, cost_usd_increment double, "
    "requests double, cpu_hours double, storage_gb_hours double, carbon_kg double, "
    "genai_tokens bigint, is_cost_anomaly boolean"
)
SERVICES = ("compute", "storage", "genai", "network")
FIRST_DAY = date(2025, 8, 1)


def _events():
    # 50 events: 3 orgs, 4 services, 5 días. Dos de cada tres son v2 (carbon salvo en compute,
    # tokens en genai); hay costs negativos chicos y algún is_cost_anomaly null.
    events = []
    for index in range(50):
        service = SERVICES[index % 4]
        is_v2 = index % 3 != 0
        events.append(
            (
                f"org_{index % 3}",
                service,
                FIRST_DAY + timedelta(days=index % 5),
                -0.005 if index % 11 == 0 else (index * 7 % 13) * 0.37,
                float(index % 7),
                0.25 * (index % 3),
                1.5 if service == "storage" else 0.0,
                0.01 * index if is_v2 and service != "compute" else None,
                index * 10 if is_v2 and service == "genai" else None,
                None if index % 17 == 0 else index % 6 == 0,
            )
        )
    return events


def _by_key(df):
    return {(row.org_id, row.usage_date, row.service): row for row in df.collect()}


def test_mapreduce_da_las_mismas_keys_y_metricas_que_el_mart_diario(spark):
    silver = spark.createDataFrame(_events(), SILVER_EVENTS_SCHEMA)

    expected = _by_key(org_daily_usage_by_service(silver))
    actual = _by_key(org_daily_usage_mapreduce(silver))

    assert set(actual) == set(expected)
    assert any(row.carbon_kg is None for row in expected.values())
    assert any(row.genai_tokens is not None for row in expected.values())
    for key, row in expected.items():
        for metric in METRICS:
            if row[metric] is None:
                assert actual[key][metric] is None, (key, metric)
            else:
                assert actual[key][metric] == pytest.approx(row[metric], abs=1e-9), (key, metric)


def test_mapreduce_tiene_las_mismas_columnas_y_tipos(spark):
    silver = spark.createDataFrame(_events(), SILVER_EVENTS_SCHEMA)

    assert org_daily_usage_mapreduce(silver).dtypes == org_daily_usage_by_service(silver).dtypes


def test_el_reporte_cuenta_las_keys_que_estan_en_una_sola_implementacion(spark):
    silver = spark.createDataFrame(_events(), SILVER_EVENTS_SCHEMA)
    daily = org_daily_usage_by_service(silver)
    mapreduce = org_daily_usage_mapreduce(silver)

    same = compare(daily, mapreduce)
    missing = compare(daily, mapreduce.filter("org_id != 'org_0'"))

    assert "keys_only_dataframe: 0" in same
    assert "keys_only_mapreduce: 0" in same
    assert "max_abs_diff_event_count: 0" in same
    assert "keys_only_dataframe: 0" not in missing
    assert "keys_only_mapreduce: 0" in missing
