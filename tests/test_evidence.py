import csv
import dataclasses
import json
from datetime import date

import pytest

from src.common.lake import quarantine_dataset, write_dataset
from src.common.manifests import write_manifest
from src.config import Zone
from src.evidence import collect as collect_module
from src.evidence import idempotence
from src.evidence.collect import collect, sanitize, storage_rows
from src.evidence.idempotence import compare_counts, count_lake, exit_code

USERS_SCHEMA = "user_id string, org_id string, ingest_date date"
EVENTS_SCHEMA = "event_id string, org_id string, event_date date"
FIRST_DAY = date(2026, 11, 14)
NEXT_DAY = date(2026, 11, 15)


@pytest.fixture
def lake(settings, tmp_path):
    """Settings con la evidencia también dentro del directorio temporario del test."""
    return dataclasses.replace(settings, evidence_path=str(tmp_path / "evidence"))


def _write_users(spark, settings, ingest_date, zone=Zone.BRONZE, dataset="users"):
    users = spark.createDataFrame(
        [(f"u{index}", "org_a", ingest_date) for index in range(3)], USERS_SCHEMA
    )
    write_dataset(users, settings, zone, dataset)


def _write_events(spark, settings, rows):
    events = spark.createDataFrame(
        [(f"e{index}", "org_a", FIRST_DAY) for index in range(rows)], EVENTS_SCHEMA
    )
    write_dataset(events, settings, Zone.BRONZE, "usage_events", partition_by=("event_date",))


def _read_csv(path):
    with path.open(encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def test_un_conteo_distinto_queda_con_igual_false_y_sale_con_1():
    comparisons = compare_counts(
        {"bronze/users": 3, "cassandra/top_services_by_org_14d": 10},
        {"bronze/users": 3, "cassandra/top_services_by_org_14d": 11},
    )

    assert [(item.objeto, item.igual) for item in comparisons] == [
        ("bronze/users", True),
        ("cassandra/top_services_by_org_14d", False),
    ]
    assert exit_code(comparisons) == 1


def test_conteos_iguales_salen_con_0_y_lo_que_falta_cuenta_0():
    comparisons = compare_counts({"a": 1, "b": 0}, {"a": 1})

    assert [(item.objeto, item.antes, item.despues) for item in comparisons] == [
        ("a", 1, 1),
        ("b", 0, 0),
    ]
    assert exit_code(comparisons) == 0


def test_main_con_un_conteo_distinto_sale_con_1_y_escribe_la_evidencia(
    monkeypatch, spark, lake, tmp_path
):
    lake_counts = iter([{"bronze/users": 3}, {"bronze/users": 4}])
    pipeline_runs = []
    monkeypatch.setattr(idempotence, "load_env_file", lambda: None)
    monkeypatch.setattr(idempotence, "load_settings", lambda run_id: lake)
    monkeypatch.setattr(idempotence, "get_spark", lambda app_name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(idempotence, "count_lake", lambda spark, settings: next(lake_counts))
    monkeypatch.setattr(
        idempotence, "count_serving", lambda settings, spark: {"cassandra/tabla": 5}
    )
    monkeypatch.setattr(
        idempotence, "run_steps", lambda steps, run_id: pipeline_runs.append(steps)
    )
    output = tmp_path / "entrega"

    exit_status = idempotence.main(output, run_id="run_2")

    assert exit_status == 1
    assert pipeline_runs == [list(idempotence.STEPS)]
    assert _read_csv(output / "idempotencia.csv") == [
        {"objeto": "bronze/users", "antes": "3", "despues": "4", "igual": "false"},
        {"objeto": "cassandra/tabla", "antes": "5", "despues": "5", "igual": "true"},
    ]
    report = (output / "idempotencia.md").read_text(encoding="utf-8")
    assert "`local`" in report
    assert "`bronze/users`" in report.split("**Conclusión:**")[1]


def test_maestros_de_bronze_cuentan_solo_el_ultimo_snapshot(spark, lake):
    quarantine_users = quarantine_dataset(Zone.BRONZE, "users")
    _write_users(spark, lake, FIRST_DAY)
    _write_users(spark, lake, FIRST_DAY, Zone.QUARANTINE, quarantine_users)
    _write_events(spark, lake, rows=4)
    before = count_lake(spark, lake)

    # Segunda corrida otro día: agrega un snapshot nuevo con el mismo contenido.
    _write_users(spark, lake, NEXT_DAY)
    _write_users(spark, lake, NEXT_DAY, Zone.QUARANTINE, quarantine_users)
    after = count_lake(spark, lake)

    assert before == {
        "bronze/usage_events": 4,
        "bronze/users": 3,
        "quarantine/bronze/users": 3,
    }
    assert after == before


def test_dataset_sin_archivos_cuenta_0(spark, lake, tmp_path):
    (tmp_path / "quarantine" / "bronze" / "nps_surveys").mkdir(parents=True)

    assert count_lake(spark, lake) == {"quarantine/bronze/nps_surveys": 0}


def test_storage_tiene_rutas_relativas_al_lake(spark, lake, tmp_path):
    _write_users(spark, lake, FIRST_DAY)
    (tmp_path / "quarantine" / "bronze" / "users").mkdir(parents=True)

    rows = storage_rows(lake)

    assert [row[:4] for row in rows] == [
        ("bronze", "users", "bronze/users", 1),
        ("quarantine", "bronze/users", "quarantine/bronze/users", 0),
    ]
    assert rows[0][4] >= 1
    assert all(not row[2].startswith("/") and str(tmp_path) not in row[2] for row in rows)


def test_collect_junta_la_evidencia_de_la_ultima_corrida(spark, lake, tmp_path):
    _write_users(spark, lake, FIRST_DAY)
    write_manifest(dataclasses.replace(lake, run_id="run_1"), "gold", {"marts": {"mart_a": 7}})
    write_manifest(
        dataclasses.replace(lake, run_id="run_1"),
        "bronze_batch",
        {"datasets": {"users": {"read": 3, "written": 3}}},
    )
    evidence = tmp_path / "evidence"
    (evidence / "quality" / "run_1" / "samples").mkdir(parents=True)
    (evidence / "quality" / "run_1" / "rule_counts.csv").write_text("rule_id\n")
    (evidence / "analytics" / "run_1").mkdir(parents=True)
    (evidence / "analytics" / "run_1" / "anomaly_summary.csv").write_text("x\n")
    for run_id, text in (("run_0", "vieja"), ("run_9", "nueva")):
        (evidence / "analytics" / run_id).mkdir(parents=True, exist_ok=True)
        (evidence / "analytics" / run_id / "mapreduce_check.txt").write_text(text)
    (evidence / "logs").mkdir()
    (evidence / "logs" / "pipeline_20261114T000000Z-aaaaaa.log").write_text("vieja\n")
    (evidence / "logs" / "pipeline_20261115T000000Z-bbbbbb.log").write_text(
        "token AstraCS:abc:123 bundle /app/secrets/secure-connect-db.zip en /home/ana/lake\n"
    )
    output = evidence / "entrega-2"

    run_id = collect(spark, lake, output)

    assert run_id == "run_1"
    counts = [tuple(row.values()) for row in _read_csv(output / "conteos.csv")]
    assert counts == [
        ("run_1", "bronze_batch", "users", "read", "3"),
        ("run_1", "bronze_batch", "users", "written", "3"),
        ("run_1", "gold", "mart_a", "filas", "7"),
    ]
    assert [row["ruta"] for row in _read_csv(output / "storage.csv")] == ["bronze/users"]
    schemas = json.loads((output / "schemas.json").read_text(encoding="utf-8"))
    assert schemas["bronze/users"] == [
        {"columna": "user_id", "tipo": "string"},
        {"columna": "org_id", "tipo": "string"},
        {"columna": "ingest_date", "tipo": "date"},
    ]
    assert (output / "quality" / "rule_counts.csv").is_file()
    assert (output / "quality" / "samples").is_dir()
    assert (output / "analytics" / "anomaly_summary.csv").is_file()
    assert (output / "analytics" / "mapreduce_check.txt").read_text() == "nueva"
    copied_logs = sorted(path.name for path in (output / "logs").iterdir())
    assert copied_logs == ["pipeline_20261115T000000Z-bbbbbb.log"]
    log_text = (output / "logs" / copied_logs[0]).read_text(encoding="utf-8")
    assert "AstraCS:" not in log_text
    assert "secure-connect" not in log_text
    assert "/home/" not in log_text


def test_collect_sin_manifiesto_de_gold_falla(spark, lake, tmp_path):
    with pytest.raises(FileNotFoundError, match="gold.json"):
        collect(spark, lake, tmp_path / "entrega-2")


def test_sanitize_saca_tokens_bundles_y_usuarios():
    text = sanitize(r"AstraCS:x:y secure-connect-abc.zip /home/ana/x C:\Users\ana\x")

    assert text == r"<token> <bundle> <home>/x <home>\x"


def test_main_de_collect_sin_gold_sale_con_1(monkeypatch, spark, lake, capsys):
    monkeypatch.setattr(collect_module, "load_env_file", lambda: None)
    monkeypatch.setattr(collect_module, "load_settings", lambda: lake)
    monkeypatch.setattr(collect_module, "get_spark", lambda app_name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert collect_module.main() == 1
    assert "gold.json" in capsys.readouterr().err
