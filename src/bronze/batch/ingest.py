import logging
from datetime import datetime, timezone

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from src.common.columns import with_technical_columns
from src.common.lake import dataset_storage_stats, write_dataset
from src.common.readers import read_csv_source
from src.common.schemas import CORRUPT_RECORD_COL, SOURCES, Source
from src.config import Settings
from src.quality.catalog import bronze_structural_rules
from src.quality.rules import split_by_rules

log = logging.getLogger(__name__)

MASTER_DATA = ("customers_orgs", "users", "billing_monthly")

def ingest_source(spark: SparkSession, settings: Settings, source: Source, ingest_ts: Column) -> dict:
    raw = with_technical_columns(read_csv_source(spark, source, settings), ingest_ts).cache()
    valid, quarantine = split_by_rules(raw, bronze_structural_rules(source))
    bronze = valid.dropDuplicates(list(source.natural_key)).drop(CORRUPT_RECORD_COL)

    write_dataset(bronze, settings, "bronze", source.name)
    write_dataset(quarantine, settings, "quarantine", source.name)

    counts = {
        "read": raw.count(),
        "quarantine": quarantine.count(),
        "written": bronze.count(),
    }
    counts["duplicates"] = counts["read"] - counts["quarantine"] - counts["written"]
    raw.unpersist()
    log.info("bronze/%s: %s", source.name, counts)
    for zone in ("bronze", "quarantine"):
        log.info("%s/%s (en disco) -> %s", zone, source.name, dataset_storage_stats(settings, zone, source.name))
    return counts


def run(spark: SparkSession, settings: Settings) -> dict[str, dict]:
    ingest_ts = F.lit(datetime.now(timezone.utc)).cast("timestamp")
    return {name: ingest_source(spark, settings, SOURCES[name], ingest_ts) for name in MASTER_DATA}
