import csv
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from src.gold.anomalies import cost_anomaly_mart, detection_method, write_anomaly_summary_csv

DAILY_SCHEMA = "org_id string, service string, usage_date date, daily_cost_usd double"

FIRST_DAY = date(2025, 5, 1)


def _days(service, costs):
    return [
        ("org_a", service, FIRST_DAY + timedelta(days=index), float(cost))
        for index, cost in enumerate(costs)
    ]


def _anomalies(spark, rows, multiplier=3.0):
    daily = spark.createDataFrame(rows, DAILY_SCHEMA)
    mart, summary = cost_anomaly_mart(daily, multiplier)
    return mart.collect(), {row.service: row for row in summary.collect()}


# 100 días con costs 1..100 y un pico de 400: el p99 exacto da 100.
SPIKE = [*range(1, 101), 400]


def test_solo_entra_el_dia_que_supera_el_umbral_del_p99(spark):
    mart, summary = _anomalies(spark, _days("compute", SPIKE))

    (row,) = mart
    assert row.daily_cost_usd == 400
    assert row.p99_usd == pytest.approx(100.0, abs=1e-9)
    assert row.threshold_usd == pytest.approx(300.0, abs=1e-9)
    assert row.anomaly_score == pytest.approx(4.0, abs=1e-9)
    assert row.detection_method == "p99_x3"
    # Mediana 51 y MAD 25.
    assert row.robust_z == pytest.approx(0.6745 * 349 / 25, abs=1e-9)
    compute = summary["compute"]
    assert (compute.days_rows, compute.anomalies_p99, compute.anomalies_robust_z) == (101, 1, 1)


def test_multiplicador_configurable_cambia_el_umbral(spark):
    mart, summary = _anomalies(spark, _days("compute", SPIKE), multiplier=5.0)

    assert mart == []
    compute = summary["compute"]
    assert compute.multiplier == 5.0
    assert compute.threshold_usd == pytest.approx(500.0, abs=1e-9)
    assert compute.anomalies_p99 == 0


def test_robust_z_es_nulo_si_el_mad_es_cero(spark):
    mart, _ = _anomalies(spark, _days("compute", [*[1] * 100, 400]))

    (row,) = mart
    assert row.daily_cost_usd == 400
    assert row.robust_z is None


def test_servicio_con_p99_cero_no_tiene_anomalias(spark):
    rows = _days("storage", [*[0] * 100, 5]) + _days("compute", SPIKE)

    mart, summary = _anomalies(spark, rows)

    assert {row.service for row in mart} == {"compute"}
    assert summary["storage"].p99_usd == 0
    assert summary["storage"].anomalies_p99 == 0


@pytest.mark.parametrize(("multiplier", "expected"), [(2.5, "p99_x2.5"), (3.0, "p99_x3")])
def test_metodo_de_deteccion_sin_ceros_de_mas(spark, multiplier, expected):
    mart, _ = _anomalies(spark, _days("compute", SPIKE), multiplier=multiplier)

    assert detection_method(multiplier) == expected
    assert {row.detection_method for row in mart} == {expected}


def test_columnas_y_tipos_del_mart_de_anomalias(spark):
    daily = spark.createDataFrame(_days("compute", SPIKE), DAILY_SCHEMA)

    mart, _ = cost_anomaly_mart(daily, 3.0)

    assert mart.dtypes == [
        ("org_id", "string"),
        ("service", "string"),
        ("usage_date", "date"),
        ("daily_cost_usd", "double"),
        ("p99_usd", "double"),
        ("threshold_usd", "double"),
        ("anomaly_score", "double"),
        ("robust_z", "double"),
        ("detection_method", "string"),
    ]


def test_anomaly_summary_csv_tiene_una_fila_por_servicio(spark, settings, tmp_path):
    lake = replace(settings, evidence_path=str(tmp_path / "evidence"))
    rows = _days("compute", SPIKE) + _days("storage", [2] * 10)
    _, summary = cost_anomaly_mart(spark.createDataFrame(rows, DAILY_SCHEMA), 3.0)

    path = write_anomaly_summary_csv(lake, lake.run_id, summary)

    assert path == Path(lake.evidence_path, "analytics", lake.run_id, "anomaly_summary.csv")
    with path.open(encoding="utf-8", newline="") as csv_file:
        written = list(csv.DictReader(csv_file))
    assert list(written[0]) == [
        "service",
        "days_rows",
        "p99_usd",
        "multiplier",
        "threshold_usd",
        "anomalies_p99",
        "anomalies_robust_z",
    ]
    assert [(row["service"], row["days_rows"], row["anomalies_p99"]) for row in written] == [
        ("compute", "101", "1"),
        ("storage", "10", "0"),
    ]
