"""Enriquecimiento de los eventos de Silver con su organización y el estado de su recurso."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col

from src.quality.catalog import (
    MATCHED_ORG_ID_COL,
    MATCHED_RESOURCE_ID_COL,
    silver_enrichment_flags,
)
from src.quality.flags import with_dq_flags

ORGANIZATION_COLUMNS = ("org_name", "industry", "hq_region", "plan_tier", "is_enterprise")
RESOURCE_STATE = "resource_state"


def enrich_events(events: DataFrame, customers_orgs: DataFrame, resources: DataFrame) -> DataFrame:
    """Agrega los datos de la organización y del recurso, y marca los eventos huérfanos."""
    organizations = customers_orgs.select(
        col("org_id").alias(MATCHED_ORG_ID_COL), *ORGANIZATION_COLUMNS
    )
    resource_states = resources.select(
        col("resource_id").alias(MATCHED_RESOURCE_ID_COL), col("state").alias(RESOURCE_STATE)
    )
    with_organization = events.join(
        broadcast(organizations), col("org_id") == col(MATCHED_ORG_ID_COL), "left"
    )
    joined = with_organization.join(
        broadcast(resource_states), col("resource_id") == col(MATCHED_RESOURCE_ID_COL), "left"
    )
    return with_dq_flags(joined, silver_enrichment_flags()).drop(
        MATCHED_ORG_ID_COL, MATCHED_RESOURCE_ID_COL
    )
