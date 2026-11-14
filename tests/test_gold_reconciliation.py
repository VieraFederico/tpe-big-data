import csv
import json
from dataclasses import replace
from pathlib import Path

import pytest

from src.common.lake import dataset_path, quarantine_dataset
from src.common.manifests import read_manifest, write_manifest
from src.common.schemas import USAGE_EVENTS, USERS
from src.config import Zone
from src.gold import build
from src.gold.reconciliation import (
    BRONZE_INPUT_ON_DISK,
    BRZ_SILVER_BALANCE,
    LANDING_BRONZE_BATCH,
    LANDING_BRONZE_STREAM,
    MANIFESTS_PRESENT,
    SILVER_QUARANTINE_ON_DISK,
    SILVER_ROWS_ON_DISK,
    ReconciliationError,
    check,
    write_reconciliation_csv,
)

INGEST_DATE = "2026-11-14"
SILVER_CHECKS = {
    BRZ_SILVER_BALANCE,
    SILVER_ROWS_ON_DISK,
    SILVER_QUARANTINE_ON_DISK,
    BRONZE_INPUT_ON_DISK,
}


@pytest.fixture
def lake(settings, tmp_path):
    """Lake temporal, con evidence también adentro de la carpeta del test."""
    return replace(settings, evidence_path=str(tmp_path / "evidence"))


def _write_rows(spark, path, rows):
    data = [(f"id_{index}",) for index in range(rows)]
    spark.createDataFrame(data, "id string").write.parquet(str(path))


def _counts(bronze_rows, silver_rows, quarantine_rows, duplicates_dropped):
    return {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "duplicates_dropped": duplicates_dropped,
    }


def _by_dataset(results, check_id):
    return {result.dataset: result for result in results if result.check_id == check_id}


def _gold_files(settings):
    root = Path(settings.zone_path(Zone.GOLD))
    return {
        (str(path.relative_to(root)), path.read_bytes())
        for path in root.rglob("*")
        if path.is_file()
    }


def _event(event_id):
    return json.dumps(
        {
            "event_id": event_id,
            "timestamp": "2025-08-01T10:00:00Z",
            "org_id": "org_a",
            "service": "compute",
            "metric": "requests",
            "value": "1",
            "cost_usd_increment": 0.5,
            "schema_version": 1,
        }
    )


@pytest.mark.parametrize(("duplicates_dropped", "passed"), [(0, False), (1, True)])
def test_balance_bronze_silver_suma_silver_cuarentena_y_duplicados(
    spark, lake, duplicates_dropped, passed
):
    write_manifest(
        lake, "silver", {"datasets": {"usage_events": _counts(10, 7, 2, duplicates_dropped)}}
    )

    balance = _by_dataset(check(spark, lake), BRZ_SILVER_BALANCE)["usage_events"]

    assert balance.expected == 10
    assert balance.actual == 9 + duplicates_dropped
    assert balance.passed is passed


def test_conteos_en_disco_que_coinciden_con_el_manifiesto_concilian(spark, lake):
    _write_rows(spark, dataset_path(lake, Zone.SILVER, "usage_events"), 3)
    _write_rows(spark, dataset_path(lake, Zone.QUARANTINE, "silver/usage_events"), 1)
    _write_rows(spark, dataset_path(lake, Zone.BRONZE, "usage_events"), 5)
    _write_rows(spark, dataset_path(lake, Zone.SILVER, "users"), 2)
    # Silver solo lee de Bronze el último snapshot de master data.
    bronze_users = dataset_path(lake, Zone.BRONZE, "users")
    _write_rows(spark, f"{bronze_users}/ingest_date=2026-11-01", 4)
    _write_rows(spark, f"{bronze_users}/ingest_date={INGEST_DATE}", 2)
    write_manifest(
        lake,
        "silver",
        {
            "datasets": {
                "usage_events": _counts(5, 3, 1, 1),
                "users": _counts(2, 2, 0, 0),
                "orgs_scd2": {"silver_rows": 0},
            },
        },
    )

    results = [result for result in check(spark, lake) if result.check_id in SILVER_CHECKS]

    assert len(results) == 9
    assert all(result.passed for result in results)


def test_manifiesto_de_silver_distinto_a_lo_que_hay_en_disco_falla(spark, lake):
    _write_rows(spark, dataset_path(lake, Zone.SILVER, "usage_events"), 9)
    write_manifest(lake, "silver", {"datasets": {"usage_events": _counts(10, 10, 0, 0)}})

    rows_on_disk = _by_dataset(check(spark, lake), SILVER_ROWS_ON_DISK)["usage_events"]

    assert (rows_on_disk.expected, rows_on_disk.actual, rows_on_disk.passed) == (10, 9, False)


def test_gate_fallido_no_toca_gold_y_lo_deja_en_el_manifiesto(spark, lake):
    _write_rows(spark, dataset_path(lake, Zone.SILVER, "usage_events"), 9)
    write_manifest(lake, "silver", {"datasets": {"usage_events": _counts(10, 10, 0, 0)}})
    previous_partition = Path(
        dataset_path(lake, Zone.GOLD, "org_daily_usage_by_service"), "usage_date=2025-08-01"
    )
    previous_partition.mkdir(parents=True)
    (previous_partition / "part-00000.parquet").write_bytes(b"corrida anterior")
    gold_before = _gold_files(lake)

    with pytest.raises(ReconciliationError):
        build.run(spark, lake)

    assert _gold_files(lake) == gold_before
    manifest = read_manifest(lake, "gold")
    assert manifest["status"] == "gate_failed"
    assert SILVER_ROWS_ON_DISK in manifest["failed_checks"]
    assert manifest["marts"] == {}
    assert Path(lake.evidence_path, "quality", lake.run_id, "reconciliation.csv").is_file()


def test_landing_batch_falla_si_el_manifiesto_no_cubre_las_filas_de_landing(spark, lake):
    users = [f"u{index},org_a,a@b.com,admin,true,2025-01-01,2025-02-01" for index in range(5)]
    Path(lake.zone_path(Zone.LANDING), USERS.path).write_text(
        "\n".join(["user_id,org_id,email,role,active,created_at,last_login", *users]) + "\n",
        encoding="utf-8",
    )
    bronze = dataset_path(lake, Zone.BRONZE, "users")
    _write_rows(spark, f"{bronze}/ingest_date={INGEST_DATE}", 3)
    _write_rows(spark, f"{bronze}/ingest_date=2026-11-01", 7)  # otra fecha: no cuenta
    quarantine = dataset_path(lake, Zone.QUARANTINE, "bronze/users")
    _write_rows(spark, f"{quarantine}/ingest_date={INGEST_DATE}", 1)
    write_manifest(
        lake,
        "bronze_batch",
        {
            "ingest_date": INGEST_DATE,
            "datasets": {"users": {"read": 4, "written": 3, "quarantine": 1, "duplicates": 0}},
        },
    )

    results = _by_dataset(check(spark, lake), LANDING_BRONZE_BATCH)

    landing = results["users"]
    assert (landing.expected, landing.actual, landing.passed) == (5, 4, False)
    assert results["bronze/users"].passed
    assert results["quarantine/bronze/users"].passed


def test_landing_stream_falla_si_bronze_y_cuarentena_no_cubren_todas_las_claves(spark, lake):
    folder = Path(lake.zone_path(Zone.LANDING), USAGE_EVENTS.path)
    folder.mkdir()
    # Keys distintas: e1, e2, el hash del event sin id (repetido) y el de la línea rota.
    lines = [_event("e1"), _event("e2"), _event("e1"), _event(None), _event(None), "{no es json"]
    (folder / "events_part_0000.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    _write_rows(spark, dataset_path(lake, Zone.BRONZE, USAGE_EVENTS.name), 2)
    _write_rows(
        spark,
        dataset_path(lake, Zone.QUARANTINE, quarantine_dataset(Zone.BRONZE, USAGE_EVENTS.name)),
        1,
    )

    (stream,) = _by_dataset(check(spark, lake), LANDING_BRONZE_STREAM).values()

    assert (stream.expected, stream.actual, stream.passed) == (4, 3, False)


def test_sin_manifiesto_de_silver_falla_y_no_evalua_los_chequeos_de_silver(spark, lake):
    write_manifest(lake, "bronze_batch", {"ingest_date": INGEST_DATE, "datasets": {}})

    results = check(spark, lake)

    present = _by_dataset(results, MANIFESTS_PRESENT)
    assert {name: result.passed for name, result in present.items()} == {
        "bronze_batch": True,
        "silver": False,
    }
    assert not SILVER_CHECKS & {result.check_id for result in results}


def test_reconciliation_csv_tiene_una_fila_por_chequeo(spark, lake):
    write_manifest(lake, "silver", {"datasets": {"usage_events": _counts(10, 7, 2, 0)}})
    results = check(spark, lake)

    path = write_reconciliation_csv(lake, lake.run_id, results)

    assert path == Path(lake.evidence_path, "quality", lake.run_id, "reconciliation.csv")
    with path.open(encoding="utf-8", newline="") as csv_file:
        rows = list(csv.DictReader(csv_file))
    assert len(rows) == len(results)
    assert list(rows[0]) == ["check_id", "dataset", "expected", "actual", "passed"]
    assert {
        "check_id": BRZ_SILVER_BALANCE,
        "dataset": "usage_events",
        "expected": "10",
        "actual": "9",
        "passed": "false",
    } in rows
