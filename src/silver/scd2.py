"""Historial de versiones de las organizaciones (``orgs_scd2``)."""

from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.functions import col, lag, lead, row_number
from pyspark.sql.window import WindowSpec

from src.common.columns import INGEST_DATE, INGEST_TS, SOURCE_FILE
from src.common.schemas import CUSTOMERS_ORGS
from src.silver.master_data import apply_master_rules

ORG_ID = "org_id"
VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"

BUSINESS_COLUMNS = tuple(field.name for field in CUSTOMERS_ORGS.schema.fields)
TRACKED_COLUMNS = tuple(name for name in BUSINESS_COLUMNS if name != ORG_ID)

ORGS_SCD2_COLUMNS = (*BUSINESS_COLUMNS, VALID_FROM, VALID_TO, IS_CURRENT, INGEST_TS, SOURCE_FILE)

_STARTS_VERSION_COL = "_starts_version"


def build_orgs_scd2(all_snapshots: DataFrame) -> DataFrame:
    """Arma una fila por versión de cada organización a partir de todos los snapshots de Bronze.

    Devuelve exactamente ``ORGS_SCD2_COLUMNS`` (sin el ``run_id`` de Silver).
    """
    snapshots = apply_master_rules(CUSTOMERS_ORGS.name, all_snapshots)
    snapshots_by_org = Window.partitionBy(ORG_ID).orderBy(INGEST_DATE)
    marked = snapshots.withColumn(_STARTS_VERSION_COL, _starts_version(snapshots_by_org))
    version_starts = marked.filter(col(_STARTS_VERSION_COL))
    versions_by_org = Window.partitionBy(ORG_ID).orderBy(VALID_FROM)
    return (
        version_starts.withColumn(VALID_FROM, col(INGEST_DATE))
        .withColumn(VALID_TO, lead(VALID_FROM).over(versions_by_org))
        .withColumn(IS_CURRENT, col(VALID_TO).isNull())
        .select(*ORGS_SCD2_COLUMNS)
    )


def _starts_version(snapshots_by_org: WindowSpec) -> Column:
    is_first_snapshot = row_number().over(snapshots_by_org) == 1
    return reduce(
        lambda changed, name: changed | _differs_from_previous(name, snapshots_by_org),
        TRACKED_COLUMNS,
        is_first_snapshot,
    )


def _differs_from_previous(name: str, snapshots_by_org: WindowSpec) -> Column:
    return ~col(name).eqNullSafe(lag(name).over(snapshots_by_org))
