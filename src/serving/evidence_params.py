"""Parámetros con los que se corren las consultas 1 y 2 para la evidencia, calculados desde Gold.

La fecha de corte es fija (``ANCHOR_DATE``) y la organización es la de mayor costo de 14 días
a esa fecha. Se corre con ``python -m src.serving.evidence_params`` e imprime un JSON, p. ej.
``{"org_id": "org_x", "from_date": "2025-08-18", "to_date": "2025-08-31", ...}``.
"""

import json
import logging
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

from src.common.lake import read_dataset
from src.common.logging_setup import setup_logging
from src.common.spark import get_spark
from src.config import Settings, Zone, load_env_file, load_settings
from src.gold.marts import AS_OF_DATE, TOP_SERVICES_BY_ORG_14D, WINDOW_DAYS
from src.serving.errors import ServingError

log = logging.getLogger(__name__)

ANCHOR_DATE = date(2025, 8, 31)
TOP_N = 5

_TOTAL_COST = "_total_cost_14d_usd"


class NoRowsForAnchorError(ServingError):
    """El top de servicios de Gold no tiene filas para la fecha de corte.

    Attributes:
        anchor: Fecha de corte buscada.
    """

    def __init__(self, anchor: date) -> None:
        super().__init__(
            f"gold/{TOP_SERVICES_BY_ORG_14D} no tiene filas con {AS_OF_DATE} = {anchor}; "
            "correr el pipeline hasta gold antes de calcular los parámetros"
        )
        self.anchor = anchor


def compute(
    spark: SparkSession, settings: Settings, anchor: date = ANCHOR_DATE
) -> dict[str, str | int]:
    """Calcula los parámetros de las consultas de evidencia.

    ``org_id`` es la organización que más suma de ``cost_14d_usd`` en el top de servicios con
    ``as_of_date = anchor``; si hay empate gana el ``org_id`` menor. El rango de la consulta 1
    son los 14 días que terminan en ``anchor``, los mismos que cubre el top.

    Args:
        spark: Sesión de Spark.
        settings: Configuración (define el lake).
        anchor: Fecha de corte; es ``to_date`` y ``as_of_date``.

    Returns:
        ``org_id``, ``from_date``, ``to_date``, ``as_of_date`` (fechas ISO) y ``n``.

    Raises:
        NoRowsForAnchorError: Si el mart no tiene filas para ``anchor``.
    """
    top = read_dataset(spark, settings, Zone.GOLD, TOP_SERVICES_BY_ORG_14D)
    leader = (
        top.filter(col(AS_OF_DATE) == anchor)
        .groupBy("org_id")
        .agg(spark_sum("cost_14d_usd").alias(_TOTAL_COST))
        .orderBy(col(_TOTAL_COST).desc(), col("org_id"))
        .first()
    )
    if leader is None:
        raise NoRowsForAnchorError(anchor)
    log.info(
        "Organización con más costo a %s: %s (%s USD)",
        anchor,
        leader["org_id"],
        leader[_TOTAL_COST],
    )
    return {
        "org_id": leader["org_id"],
        "from_date": (anchor - timedelta(days=WINDOW_DAYS - 1)).isoformat(),
        "to_date": anchor.isoformat(),
        "as_of_date": anchor.isoformat(),
        "n": TOP_N,
    }


def main() -> int:
    """Imprime los parámetros como JSON; devuelve 1 si Gold no tiene filas para la fecha."""
    load_env_file()
    settings = load_settings()
    spark = get_spark("evidence-params")
    try:
        params = compute(spark, settings)
    except NoRowsForAnchorError as error:
        print(error, file=sys.stderr)
        return 1
    finally:
        spark.stop()
    print(json.dumps(params))
    return 0


if __name__ == "__main__":
    setup_logging("evidence_params")
    sys.exit(main())
