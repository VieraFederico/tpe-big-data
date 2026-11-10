import json
from dataclasses import replace
from pathlib import Path

from pyspark.sql.functions import col

from src.bronze.streaming import ingest
from src.common.columns import RUN_ID
from src.common.lake import read_dataset
from src.common.schemas import USAGE_EVENTS
from src.quality.rules import RULE_ID_COL


def _event(event_id, ts="2025-08-01T10:00:00Z", **extra):
    return {
        "event_id": event_id, "timestamp": ts, "org_id": "org_1", "resource_id": "res_1",
        "service": "compute", "region": "us-east", "metric": "cpu_hours", "value": "1.5",
        "unit": "h", "cost_usd_increment": 0.5, "schema_version": 1, **extra,
    }


def _write_part(settings, name, lines):
    folder = Path(settings.zone_path("landing"), USAGE_EVENTS.path)
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(
        "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines) + "\n",
        encoding="utf-8",
    )


def _bronze_ids(spark, settings):
    return sorted(r.event_id for r in read_dataset(spark, settings, "bronze", USAGE_EVENTS.name).collect())


def test_dedupe_y_quarantine_en_un_micro_lote(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [
        _event("e1"),
        _event("e2", schema_version=2, carbon_kg=0.1),
        _event("e1"),            # duplicado
        "{esto no es json",      # corrupta
    ])

    stats = ingest.run(spark, settings)

    assert _bronze_ids(spark, settings) == ["e1", "e2"]
    assert stats["dropped_duplicates"] == 1
    assert stats["bronze_total"] == 2
    quarantine = read_dataset(spark, settings, "quarantine", USAGE_EVENTS.name)
    assert [r[RULE_ID_COL] for r in quarantine.collect()] == ["BRZ_PARSE"]


def test_esquema_v2_conserva_campos_nuevos(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [
        _event("v1"),
        _event("v2", service="genai", schema_version=2, carbon_kg=0.2, genai_tokens=1000),
    ])

    ingest.run(spark, settings)

    rows = {r.event_id: r for r in read_dataset(spark, settings, "bronze", USAGE_EVENTS.name).collect()}
    assert rows["v1"].genai_tokens is None and rows["v1"].carbon_kg is None
    assert rows["v2"].genai_tokens == 1000 and rows["v2"].carbon_kg == 0.2


def test_reejecucion_es_idempotente_y_recuerda_duplicados(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [_event("e1"), _event("e2")])
    ingest.run(spark, settings)

    # Sin archivos nuevos: el checkpoint no vuelve a leer nada.
    stats = ingest.run(spark, settings)
    assert stats["rows_read"] == 0
    assert stats["bronze_total"] == 2

    # Archivo nuevo con un duplicado de la corrida anterior: el estado lo descarta.
    _write_part(settings, "events_part_0001.jsonl", [_event("e1"), _event("e3")])
    ingest.run(spark, settings)
    assert _bronze_ids(spark, settings) == ["e1", "e2", "e3"]


def test_timestamp_invalido_o_nulo_va_a_quarantine(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [
        _event("ok"),
        _event("fecha_rota", ts="no-es-una-fecha"),
        _event("sin_fecha", ts=None),
        "{esto no es json",      # corrupta: se reporta como BRZ_PARSE, no como BRZ_BAD_TS
    ])

    ingest.run(spark, settings)

    assert _bronze_ids(spark, settings) == ["ok"]
    quarantine = read_dataset(spark, settings, "quarantine", USAGE_EVENTS.name)
    rules_by_event = {row.event_id: row[RULE_ID_COL] for row in quarantine.collect()}
    assert rules_by_event == {"fecha_rota": "BRZ_BAD_TS", "sin_fecha": "BRZ_BAD_TS", None: "BRZ_PARSE"}


def test_evento_tardio_se_descarta_por_watermark(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [_event("nuevo", ts="2025-09-30T00:00:00Z")])
    ingest.run(spark, settings)

    # Más de 61 días antes del máximo visto: queda detrás del watermark.
    _write_part(settings, "events_part_0001.jsonl", [_event("viejo", ts="2025-07-01T00:00:00Z")])
    stats = ingest.run(spark, settings)

    assert stats["dropped_by_watermark"] == 1
    assert _bronze_ids(spark, settings) == ["nuevo"]
    assert read_dataset(spark, settings, "bronze", USAGE_EVENTS.name).filter(col("event_id") == "viejo").count() == 0


def test_cada_fila_guarda_el_run_id_de_la_corrida_que_la_escribio(spark, settings):
    _write_part(settings, "events_part_0000.jsonl", [_event("e1")])
    ingest.run(spark, replace(settings, run_id="corrida_a"))

    _write_part(settings, "events_part_0001.jsonl", [_event("e2")])
    ingest.run(spark, replace(settings, run_id="corrida_b"))

    bronze = read_dataset(spark, settings, "bronze", USAGE_EVENTS.name)
    assert {row.event_id: row[RUN_ID] for row in bronze.collect()} == {"e1": "corrida_a", "e2": "corrida_b"}
