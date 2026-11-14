"""Reconciliation de counts entre Landing, Bronze, Silver y quarantine antes de escribir Gold."""

import csv
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any, NamedTuple

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from src.bronze.batch import ingest as bronze_batch
from src.bronze.streaming.ingest import event_dedupe_key
from src.common.columns import INGEST_DATE
from src.common.lake import dataset_path, quarantine_dataset, read_dataset
from src.common.manifests import read_manifest
from src.common.readers import read_csv_source, read_usage_events
from src.common.schemas import SOURCES, USAGE_EVENTS
from src.config import Settings, Zone
from src.quality.report import QUALITY_DIR
from src.silver import build as silver
from src.silver.master_data import latest_snapshot

RECONCILIATION_FILE = "reconciliation.csv"

MANIFESTS_PRESENT = "MANIFESTS_PRESENT"
BRZ_SILVER_BALANCE = "BRZ_SILVER_BALANCE"
SILVER_ROWS_ON_DISK = "SILVER_ROWS_ON_DISK"
SILVER_QUARANTINE_ON_DISK = "SILVER_QUARANTINE_ON_DISK"
BRONZE_INPUT_ON_DISK = "BRONZE_INPUT_ON_DISK"
LANDING_BRONZE_BATCH = "LANDING_BRONZE_BATCH"
LANDING_BRONZE_STREAM = "LANDING_BRONZE_STREAM"


class CheckResult(NamedTuple):
    """Resultado de un check; ``expected`` y ``actual`` son cantidades de rows."""

    check_id: str
    dataset: str
    expected: int
    actual: int
    passed: bool


class ReconciliationError(Exception):
    """Algún check no cerró, así que Gold no se escribe."""

    def __init__(self, failed: list[CheckResult]) -> None:
        names = ", ".join(f"{result.check_id} ({result.dataset})" for result in failed)
        super().__init__(f"Chequeos que no concilian: {names}")
        self.failed = failed


def check(spark: SparkSession, settings: Settings) -> list[CheckResult]:
    """Corre los checks de reconciliation sobre el lake.

    Si falta un manifest, los checks que dependen de él se saltean (igual ya falla
    ``MANIFESTS_PRESENT``).
    """
    bronze_manifest = read_manifest(settings, bronze_batch.STEP_NAME)
    silver_manifest = read_manifest(settings, silver.STEP_NAME)
    results = [
        _result(MANIFESTS_PRESENT, step, 1, int(manifest is not None))
        for step, manifest in (
            (bronze_batch.STEP_NAME, bronze_manifest),
            (silver.STEP_NAME, silver_manifest),
        )
    ]
    if silver_manifest is not None:
        results += _silver_checks(spark, settings, silver_manifest["datasets"])
    if bronze_manifest is not None:
        results += _landing_batch_checks(spark, settings, bronze_manifest)
    results.append(_landing_stream_check(spark, settings))
    return results


def write_reconciliation_csv(settings: Settings, run_id: str, results: list[CheckResult]) -> Path:
    """Escribe ``<evidence>/quality/<run_id>/reconciliation.csv`` con una row por check."""
    path = Path(settings.evidence_path) / QUALITY_DIR / run_id / RECONCILIATION_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")
        writer.writerow(CheckResult._fields)
        for result in results:
            writer.writerow([*result[:-1], str(result.passed).lower()])
    return path


def _silver_checks(
    spark: SparkSession, settings: Settings, datasets: dict[str, dict[str, int]]
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for name, counts in datasets.items():
        silver_rows = _count_or_zero(partial(read_dataset, spark, settings, Zone.SILVER, name))
        results.append(_result(SILVER_ROWS_ON_DISK, name, counts["silver_rows"], silver_rows))
        # orgs_scd2 no tiene una row de Bronze por cada row de Silver.
        if name == silver.ORGS_SCD2:
            continue
        quarantine = quarantine_dataset(Zone.SILVER, name)
        quarantine_rows = _count_or_zero(
            partial(read_dataset, spark, settings, Zone.QUARANTINE, quarantine)
        )
        bronze_rows = _count_or_zero(partial(_bronze_input, spark, settings, name))
        balance = counts["silver_rows"] + counts["quarantine_rows"] + counts["duplicates_dropped"]
        results += [
            _result(BRZ_SILVER_BALANCE, name, counts["bronze_rows"], balance),
            _result(SILVER_QUARANTINE_ON_DISK, name, counts["quarantine_rows"], quarantine_rows),
            _result(BRONZE_INPUT_ON_DISK, name, counts["bronze_rows"], bronze_rows),
        ]
    return results


def _bronze_input(spark: SparkSession, settings: Settings, name: str) -> DataFrame:
    """Lo que Silver lee de Bronze: el último snapshot de master data o todos los events."""
    bronze = read_dataset(spark, settings, Zone.BRONZE, name)
    return bronze if name == USAGE_EVENTS.name else latest_snapshot(bronze)


def _landing_batch_checks(
    spark: SparkSession, settings: Settings, manifest: dict[str, Any]
) -> list[CheckResult]:
    """Por source: Landing contra el manifest y lo que se escribió en su ingest date."""
    partition = f"{INGEST_DATE}={manifest['ingest_date']}"
    results: list[CheckResult] = []
    for name, source in SOURCES.items():
        counts = manifest["datasets"].get(name, {})
        written, quarantine = counts.get("written", 0), counts.get("quarantine", 0)
        landing_rows = _count_or_zero(partial(read_csv_source, spark, source, settings))
        quarantine_name = quarantine_dataset(Zone.BRONZE, name)
        results += [
            _result(
                LANDING_BRONZE_BATCH,
                name,
                landing_rows,
                written + quarantine + counts.get("duplicates", 0),
            ),
            _result(
                LANDING_BRONZE_BATCH,
                f"{Zone.BRONZE}/{name}",
                written,
                _partition_rows(spark, settings, Zone.BRONZE, name, partition),
            ),
            _result(
                LANDING_BRONZE_BATCH,
                f"{Zone.QUARANTINE}/{quarantine_name}",
                quarantine,
                _partition_rows(spark, settings, Zone.QUARANTINE, quarantine_name, partition),
            ),
        ]
    return results


def _partition_rows(
    spark: SparkSession, settings: Settings, zone: Zone, dataset: str, partition: str
) -> int:
    path = f"{dataset_path(settings, zone, dataset)}/{partition}"
    return _count_or_zero(partial(spark.read.parquet, path))


def _landing_stream_check(spark: SparkSession, settings: Settings) -> CheckResult:
    """Keys distintas de events en Landing contra lo que quedó en Bronze y su quarantine."""
    landing_keys = _count_or_zero(partial(_distinct_event_keys, spark, settings))
    bronze_rows = _count_or_zero(
        partial(read_dataset, spark, settings, Zone.BRONZE, USAGE_EVENTS.name)
    )
    quarantine = quarantine_dataset(Zone.BRONZE, USAGE_EVENTS.name)
    quarantine_rows = _count_or_zero(
        partial(read_dataset, spark, settings, Zone.QUARANTINE, quarantine)
    )
    return _result(
        LANDING_BRONZE_STREAM, USAGE_EVENTS.name, landing_keys, bronze_rows + quarantine_rows
    )


def _distinct_event_keys(spark: SparkSession, settings: Settings) -> DataFrame:
    return read_usage_events(spark, settings).select(event_dedupe_key()).distinct()


def _count_or_zero(read: Callable[[], DataFrame]) -> int:
    """Rows de lo que devuelve ``read``; 0 si el file o el dataset no existe o está vacío."""
    try:
        return read().count()
    except AnalysisException:
        return 0


def _result(check_id: str, dataset: str, expected: int, actual: int) -> CheckResult:
    return CheckResult(check_id, dataset, expected, actual, expected == actual)
