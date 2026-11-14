import csv
import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest

from src.common.lake import dataset_path
from src.common.manifests import read_manifest, write_manifest
from src.common.schemas import USAGE_EVENTS
from src.config import Zone
from src.gold import build

SILVER_EVENTS_SCHEMA = (
    "org_id string, service string, event_date date, cost_usd_increment double, "
    "requests double, cpu_hours double, storage_gb_hours double, carbon_kg double, "
    "genai_tokens bigint, is_cost_anomaly boolean"
)
MARTS = {
    "org_daily_usage_by_service": "usage_date",
    "top_services_by_org_14d": "as_of_date",
    "cost_anomaly_mart": "usage_date",
}
FIRST_DAY = date(2025, 8, 1)


@pytest.fixture
def lake(settings, tmp_path):
    return replace(settings, evidence_path=str(tmp_path / "evidence"))


def _silver_events(first_day):
    # compute tiene un pico de 400 el 1/8 y después 1..100; storage cuesta 2 todos los días.
    days = [FIRST_DAY + timedelta(days=index) for index in range(101)]
    costs = {"compute": [400, *range(1, 101)], "storage": [2] * 101}
    return [
        ("org_a", service, day, float(cost), 1.0, 0.0, 0.0, None, None, False)
        for service, service_costs in costs.items()
        for day, cost in zip(days, service_costs, strict=True)
        if day >= first_day
    ]


def _consistent_lake(spark, lake, first_day=FIRST_DAY):
    """Lake donde todos los checks cierran: Landing, Bronze y Silver con las mismas rows."""
    events = _silver_events(first_day)
    folder = Path(lake.zone_path(Zone.LANDING), USAGE_EVENTS.path)
    folder.mkdir(exist_ok=True)
    lines = [json.dumps({"event_id": f"e{index}"}) for index in range(len(events))]
    (folder / "events_part_0000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    ids = spark.createDataFrame([(f"e{index}",) for index in range(len(events))], "id string")
    ids.write.mode("overwrite").parquet(dataset_path(lake, Zone.BRONZE, USAGE_EVENTS.name))
    silver = spark.createDataFrame(events, SILVER_EVENTS_SCHEMA)
    silver.write.mode("overwrite").parquet(dataset_path(lake, Zone.SILVER, USAGE_EVENTS.name))
    counts = {
        "bronze_rows": len(events),
        "silver_rows": len(events),
        "quarantine_rows": 0,
        "duplicates_dropped": 0,
    }
    write_manifest(lake, "bronze_batch", {"ingest_date": "2026-11-14", "datasets": {}})
    write_manifest(lake, "silver", {"datasets": {USAGE_EVENTS.name: counts}})


def _mart_rows(spark, lake, mart):
    rows = spark.read.parquet(dataset_path(lake, Zone.GOLD, mart)).drop("run_id").collect()
    return sorted(tuple(row) for row in rows)


def _partitions(lake, mart):
    return {path.name for path in Path(dataset_path(lake, Zone.GOLD, mart)).iterdir()}


def test_lake_consistente_escribe_los_tres_marts_y_el_manifiesto(spark, lake):
    _consistent_lake(spark, lake)

    marts = build.run(spark, lake)

    manifest = read_manifest(lake, "gold")
    assert manifest["status"] == "ok"
    assert manifest["marts"] == marts
    assert set(marts) == set(MARTS)
    for mart in MARTS:
        rows = spark.read.parquet(dataset_path(lake, Zone.GOLD, mart))
        assert marts[mart] == rows.count()
        assert {row.run_id for row in rows.select("run_id").collect()} == {lake.run_id}
    assert marts == {
        "org_daily_usage_by_service": 202,
        "top_services_by_org_14d": 202,
        "cost_anomaly_mart": 1,
    }


def test_cada_mart_escrito_tiene_las_columnas_y_tipos_de_su_esquema(spark, lake):
    """Lo que el serving lee de Gold coincide con MART_SCHEMAS (la partición incluida)."""
    _consistent_lake(spark, lake)

    build.run(spark, lake)

    assert set(build.MART_SCHEMAS) == set(MARTS)
    for mart, schema in build.MART_SCHEMAS.items():
        written = spark.read.parquet(dataset_path(lake, Zone.GOLD, mart))
        expected = {field.name: field.dataType.simpleString() for field in schema}
        assert dict(written.dtypes) == expected
        assert len(written.columns) == len(schema.fields)


def test_anomaly_summary_coincide_con_el_mart_de_anomalias(spark, lake):
    _consistent_lake(spark, lake)
    build.run(spark, lake)

    path = Path(lake.evidence_path, "analytics", lake.run_id, "anomaly_summary.csv")
    with path.open(encoding="utf-8", newline="") as csv_file:
        summary = {row["service"]: row for row in csv.DictReader(csv_file)}
    anomalies = spark.read.parquet(dataset_path(lake, Zone.GOLD, "cost_anomaly_mart"))

    assert set(summary) == {"compute", "storage"}
    assert len(summary["compute"]) == 7
    for service, row in summary.items():
        assert int(row["anomalies_p99"]) == anomalies.filter(f"service = '{service}'").count()
        assert int(row["days_rows"]) == 101


def test_dos_corridas_dan_lo_mismo_y_no_quedan_fechas_viejas(spark, lake):
    _consistent_lake(spark, lake)
    build.run(spark, lake)
    first = {mart: _mart_rows(spark, lake, mart) for mart in MARTS}

    build.run(spark, replace(lake, run_id="segunda"))

    assert {mart: _mart_rows(spark, lake, mart) for mart in MARTS} == first

    _consistent_lake(spark, lake, first_day=FIRST_DAY + timedelta(days=1))
    build.run(spark, replace(lake, run_id="tercera"))

    for mart, partition in MARTS.items():
        assert f"{partition}={FIRST_DAY}" not in _partitions(lake, mart)
    assert _partitions(lake, "org_daily_usage_by_service")
    assert _partitions(lake, "top_services_by_org_14d")
