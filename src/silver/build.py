import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import NotRequired, TypedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from src.common.columns import EVENT_DATE, INGEST_DATE, INGEST_TS, RUN_ID, SOURCE_FILE
from src.common.lake import log_storage_stats, quarantine_dataset, read_dataset, write_dataset
from src.common.manifests import write_manifest
from src.common.schemas import BILLING_MONTHLY, CUSTOMERS_ORGS, RESOURCES, SOURCES, USAGE_EVENTS
from src.config import Settings, Zone
from src.quality.flags import DQ_FLAGS_COL
from src.quality.report import DatasetOutcome, write_quality_report
from src.silver.anomalies import ANOMALY_SCORE, IS_COST_ANOMALY, with_cost_anomalies
from src.silver.enrichment import ORGANIZATION_COLUMNS, RESOURCE_STATE, enrich_events
from src.silver.events import CONFORMED_COLUMNS, conform_events
from src.silver.master_data import BILLING_MONTH, conform_master, latest_snapshot
from src.silver.scd2 import build_orgs_scd2

log = logging.getLogger(__name__)

STEP_NAME = "silver"
ORGS_SCD2 = "orgs_scd2"

SILVER_EVENT_COLUMNS = (
    *CONFORMED_COLUMNS[: CONFORMED_COLUMNS.index(DQ_FLAGS_COL)],
    *ORGANIZATION_COLUMNS,
    RESOURCE_STATE,
    IS_COST_ANOMALY,
    ANOMALY_SCORE,
    DQ_FLAGS_COL,
    INGEST_TS,
    SOURCE_FILE,
    RUN_ID,
)

SILVER_PARTITION_COLUMNS: dict[str, tuple[str, ...]] = {
    USAGE_EVENTS.name: (EVENT_DATE,),
    BILLING_MONTHLY.name: (BILLING_MONTH,),
}
QUARANTINE_PARTITION_COLUMNS = (INGEST_DATE,)


class SilverCounts(TypedDict):
    """Conteos de un dataset de Silver; ``orgs_scd2`` solo trae ``silver_rows``."""

    bronze_rows: NotRequired[int]
    silver_rows: int
    quarantine_rows: NotRequired[int]
    duplicates_dropped: NotRequired[int]


def run(spark: SparkSession, settings: Settings) -> dict[str, SilverCounts]:
    """Paso ``silver``: reemplaza Silver y su cuarentena, escribe el reporte de calidad en
    ``<evidencia>/quality/<run_id>/`` y, al final, ``_manifests/silver.json``."""
    master_counts = {name: _build_master(spark, settings, name) for name in SOURCES}
    orgs_scd2_counts = _build_orgs_scd2(spark, settings)
    events_counts, dropped_events = _build_usage_events(spark, settings)
    try:
        outcomes = _quality_outcomes(spark, settings, events_counts, dropped_events)
        write_quality_report(settings, settings.run_id, outcomes)
    finally:
        dropped_events.unpersist()
    counts: dict[str, SilverCounts] = {
        USAGE_EVENTS.name: events_counts,
        **master_counts,
        ORGS_SCD2: orgs_scd2_counts,
    }
    write_manifest(settings, STEP_NAME, {"datasets": counts})
    return counts


def _build_master(spark: SparkSession, settings: Settings, name: str) -> SilverCounts:
    bronze = read_dataset(spark, settings, Zone.BRONZE, name)
    with _cached(latest_snapshot(bronze)) as snapshot:
        bronze_rows = snapshot.count()
        silver = _with_run_id(conform_master(name, snapshot), settings)
        silver_rows = _replace(silver, settings, Zone.SILVER, name)
    counts = SilverCounts(
        bronze_rows=bronze_rows, silver_rows=silver_rows, quarantine_rows=0, duplicates_dropped=0
    )
    _log_counts(settings, name, counts)
    return counts


def _build_orgs_scd2(spark: SparkSession, settings: Settings) -> SilverCounts:
    all_snapshots = read_dataset(spark, settings, Zone.BRONZE, CUSTOMERS_ORGS.name)
    silver = _with_run_id(build_orgs_scd2(all_snapshots), settings)
    counts = SilverCounts(silver_rows=_replace(silver, settings, Zone.SILVER, ORGS_SCD2))
    _log_counts(settings, ORGS_SCD2, counts)
    return counts


def _build_usage_events(spark: SparkSession, settings: Settings) -> tuple[SilverCounts, DataFrame]:
    name = USAGE_EVENTS.name
    customers_orgs = read_dataset(spark, settings, Zone.SILVER, CUSTOMERS_ORGS.name)
    resources = read_dataset(spark, settings, Zone.SILVER, RESOURCES.name)
    with _cached(read_dataset(spark, settings, Zone.BRONZE, name)) as bronze_events:
        conformed = conform_events(bronze_events)
        dropped_events = conformed.duplicates.cache()
        enriched = enrich_events(conformed.valid, customers_orgs, resources)
        with _cached(enriched) as enriched_events:
            silver = _with_run_id(with_cost_anomalies(enriched_events), settings)
            quarantine = _with_run_id(conformed.quarantine, settings)
            counts = SilverCounts(
                bronze_rows=bronze_events.count(),
                silver_rows=_replace(
                    silver.select(*SILVER_EVENT_COLUMNS), settings, Zone.SILVER, name
                ),
                quarantine_rows=_replace(
                    quarantine, settings, Zone.QUARANTINE, quarantine_dataset(Zone.SILVER, name)
                ),
                duplicates_dropped=dropped_events.count(),
            )
    _log_counts(settings, name, counts)
    log_storage_stats(settings, quarantine_dataset(Zone.SILVER, name), (Zone.QUARANTINE,))
    return counts, dropped_events


def _quality_outcomes(
    spark: SparkSession, settings: Settings, events_counts: SilverCounts, dropped_events: DataFrame
) -> dict[str, DatasetOutcome]:
    events_name = USAGE_EVENTS.name
    events_quarantine = (
        read_dataset(
            spark, settings, Zone.QUARANTINE, quarantine_dataset(Zone.SILVER, events_name)
        )
        if events_counts.get("quarantine_rows", 0) > 0
        else None
    )
    events = DatasetOutcome(
        silver=read_dataset(spark, settings, Zone.SILVER, events_name),
        quarantine=events_quarantine,
        dropped=dropped_events,
    )
    masters = {
        name: DatasetOutcome(silver=read_dataset(spark, settings, Zone.SILVER, name))
        for name in SOURCES
    }
    return {events_name: events, **masters}


def _replace(df: DataFrame, settings: Settings, zone: Zone, dataset: str) -> int:
    partition_by = (
        QUARANTINE_PARTITION_COLUMNS
        if zone == Zone.QUARANTINE
        else SILVER_PARTITION_COLUMNS.get(dataset, ())
    )
    with _cached(df) as rows:
        write_dataset(
            rows, settings, zone, dataset, partition_by=partition_by, replace_all_partitions=True
        )
        return rows.count()


def _with_run_id(df: DataFrame, settings: Settings) -> DataFrame:
    return df.withColumn(RUN_ID, lit(settings.run_id))


@contextmanager
def _cached(df: DataFrame) -> Iterator[DataFrame]:
    cached = df.cache()
    try:
        yield cached
    finally:
        cached.unpersist()


def _log_counts(settings: Settings, name: str, counts: SilverCounts) -> None:
    log.info("silver/%s: %s", name, counts)
    log_storage_stats(settings, name, (Zone.SILVER,))
