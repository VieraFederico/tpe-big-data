"""Consultas 1 y 2 del serving con parámetros calculados desde Gold, sin tener que elegir un org_id.

Se corre con ``python -m src.evidence.queries [carpeta]``: calcula los parámetros con
``src.serving.evidence_params``, ejecuta las dos consultas, imprime sus tablas y guarda la
evidencia (``.txt`` y ``.csv``) en la carpeta indicada (default: ``evidence/local/``).
"""

import argparse
import logging
import sys
from collections.abc import Mapping
from pathlib import Path

from src.common.logging_setup import setup_logging
from src.common.spark import get_spark
from src.config import Settings, load_env_file, load_settings
from src.serving import evidence_params
from src.serving.cassandra_client import session_scope
from src.serving.evidence_params import NoRowsForAnchorError
from src.serving.queries import QueryResult, format_table, run_query, save_evidence

log = logging.getLogger("evidence_queries")

QUERIES_DIR = Path(__file__).resolve().parent.parent / "serving" / "cql" / "queries"
LOCAL_EVIDENCE_DIR = "local"

# Consulta -> parámetros (de los que calcula evidence_params) que usa.
EVIDENCE_QUERIES: dict[str, tuple[str, ...]] = {
    "q1_daily_usage_by_range": ("org_id", "from_date", "to_date"),
    "q2_top_services_14d": ("org_id", "as_of_date", "n"),
}


def run_evidence_queries(
    settings: Settings, params: Mapping[str, str | int], folder: Path
) -> list[QueryResult]:
    """Ejecuta las consultas 1 y 2 con los parámetros dados y guarda su evidencia en ``folder``.

    Args:
        settings: Configuración (define el destino del serving).
        params: Parámetros calculados por ``evidence_params.compute``.
        folder: Carpeta donde quedan los ``.txt`` y ``.csv``.

    Returns:
        El resultado de cada consulta, en orden.
    """
    results: list[QueryResult] = []
    with session_scope(settings) as session:
        for name, keys in EVIDENCE_QUERIES.items():
            cql = (QUERIES_DIR / f"{name}.cql").read_text(encoding="utf-8")
            result = run_query(session, cql, {key: params[key] for key in keys}, name=name)
            save_evidence(result, settings, folder)
            results.append(result)
    return results


def main(argv: list[str] | None = None) -> int:
    """CLI: calcula los parámetros, corre las consultas e imprime las tablas.

    Returns:
        0 si las dos consultas corrieron; 1 si Gold no tiene filas para la fecha de corte.
    """
    parser = argparse.ArgumentParser(
        description="Corre las consultas 1 y 2 y guarda la evidencia."
    )
    parser.add_argument(
        "folder", nargs="?", type=Path, help="Carpeta de la evidencia (default: evidence/local)"
    )
    arguments = parser.parse_args(argv)

    load_env_file()
    settings = load_settings()
    folder = arguments.folder or Path(settings.evidence_path) / LOCAL_EVIDENCE_DIR

    spark = get_spark("evidence-queries")
    try:
        params = evidence_params.compute(spark, settings)
    except NoRowsForAnchorError as error:
        print(error, file=sys.stderr)
        return 1
    finally:
        spark.stop()
    log.info("Parámetros: %s", params)

    for result in run_evidence_queries(settings, params, folder):
        print(f"\n{result.name} ({result.total_rows} filas) - {result.params}")
        print(format_table(result.columns, result.rows))
    print(f"\nEvidencia guardada en {folder}")
    return 0


if __name__ == "__main__":
    setup_logging("evidence_queries")
    sys.exit(main())
