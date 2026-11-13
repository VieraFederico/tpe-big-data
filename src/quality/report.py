"""Reporte de calidad de cada corrida de Silver: filas afectadas y muestras por regla."""

import csv
import json
import logging
import shutil
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import NamedTuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import array_contains, col

from src.common.schemas import USAGE_EVENTS
from src.config import Settings, Zone
from src.quality.catalog import (
    DUPLICATE_EVENT_RULE_ID,
    silver_enrichment_flags,
    silver_event_flags,
    silver_event_rules,
    silver_master_flags,
)
from src.quality.flags import DQ_FLAGS_COL, Flag
from src.quality.rules import RULE_ID_COL

log = logging.getLogger(__name__)

QUALITY_DIR = "quality"
RULE_COUNTS_FILE = "rule_counts.csv"
SAMPLES_DIR = "samples"
SAMPLE_SIZE = 20
RULE_COUNTS_COLUMNS = ("layer", "dataset", "rule_id", "action", "rows_affected")

QUARANTINE_ACTION = "quarantine"
DROP_ACTION = "drop"
FLAG_ACTION = "flag"
FIX_AND_FLAG_ACTION = "fix_and_flag"


class DatasetOutcome(NamedTuple):
    """Filas de un dataset de Silver que mira el reporte: válidas, en cuarentena y descartadas.

    ``None`` en ``quarantine`` o ``dropped`` significa que no hay filas.
    """

    silver: DataFrame
    quarantine: DataFrame | None = None
    dropped: DataFrame | None = None


class _RuleRows(NamedTuple):
    dataset: str
    rule_id: str
    action: str
    rows: DataFrame | None


def write_quality_report(
    settings: Settings, run_id: str, results: Mapping[str, DatasetOutcome]
) -> Path:
    """Escribe ``rule_counts.csv`` y ``samples/`` en ``<evidencia>/quality/<run_id>/``.

    ``results`` trae ``usage_events`` y cada dato maestro; devuelve la carpeta del reporte.
    """
    report_folder = Path(settings.evidence_path) / QUALITY_DIR / run_id
    shutil.rmtree(report_folder, ignore_errors=True)
    samples_folder = report_folder / SAMPLES_DIR
    samples_folder.mkdir(parents=True)
    rule_counts: list[tuple[object, ...]] = []
    for rule in _rows_by_rule(results):
        rows_affected = rule.rows.count() if rule.rows is not None else 0
        if rule.rows is not None and rows_affected > 0:
            _write_sample(samples_folder / f"{rule.dataset}__{rule.rule_id}.csv", rule.rows)
        rule_counts.append(
            (Zone.SILVER.value, rule.dataset, rule.rule_id, rule.action, rows_affected)
        )
    _write_csv(report_folder / RULE_COUNTS_FILE, RULE_COUNTS_COLUMNS, rule_counts)
    log.info("Reporte de calidad en %s", report_folder)
    return report_folder


def _rows_by_rule(results: Mapping[str, DatasetOutcome]) -> Iterator[_RuleRows]:
    events_name = USAGE_EVENTS.name
    events = results[events_name]
    for rule in silver_event_rules():
        quarantined = _rows_with_rule_id(events.quarantine, rule.rule_id)
        yield _RuleRows(events_name, rule.rule_id, QUARANTINE_ACTION, quarantined)
    yield _RuleRows(events_name, DUPLICATE_EVENT_RULE_ID, DROP_ACTION, events.dropped)
    for flag in (*silver_event_flags(), *silver_enrichment_flags()):
        yield _flagged_rows(events_name, events.silver, flag)
    for name, flags in silver_master_flags().items():
        for flag in flags:
            yield _flagged_rows(name, results[name].silver, flag)


def _rows_with_rule_id(quarantine: DataFrame | None, rule_id: str) -> DataFrame | None:
    if quarantine is None:
        return None
    return quarantine.filter(col(RULE_ID_COL) == rule_id)


def _flagged_rows(dataset: str, silver: DataFrame, flag: Flag) -> _RuleRows:
    action = FIX_AND_FLAG_ACTION if flag.fixes_value else FLAG_ACTION
    flagged = silver.filter(array_contains(col(DQ_FLAGS_COL), flag.flag_id))
    return _RuleRows(dataset, flag.flag_id, action, flagged)


def _write_sample(path: Path, rows: DataFrame) -> None:
    sample = rows.orderBy(*rows.columns).limit(SAMPLE_SIZE).collect()
    values = [tuple(_csv_value(value) for value in row) for row in sample]
    _write_csv(path, tuple(rows.columns), values)


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, list | dict):
        return json.dumps(value, ensure_ascii=False, default=str)
    return value


def _write_csv(path: Path, header: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
