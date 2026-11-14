"""Chequeos de las consultas de ``cql/queries/`` sin Cassandra: parámetros y claves."""

import re

import pytest

from src.serving.cassandra_client import read_cql_statements
from src.serving.cassandra_writer import SCHEMA_DIR
from tests.test_cql_schema import parse_ddl

QUERY_DIR = SCHEMA_DIR.parent / "queries"

# Parámetros con nombre que recibe cada consulta (los que se pasan con -p).
EXPECTED_PARAMS = {
    "q1_daily_usage_by_range": {"org_id", "from_date", "to_date"},
    "q2_top_services_14d": {"org_id", "as_of_date", "n"},
    "q_cost_anomalies_by_org": {"org_id", "from_date", "to_date"},
    "count_org_daily_usage_by_service": {"org_id"},
    "count_top_services_by_org_14d": {"org_id", "as_of_date"},
    "count_cost_anomaly_mart": {"org_id"},
}

_PARAM_RE = re.compile(r"%\((\w+)\)s")
_FROM_RE = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)


def _statement(query: str) -> str:
    """Única sentencia del archivo de la consulta, sin comentarios."""
    [statement] = read_cql_statements(QUERY_DIR / f"{query}.cql")
    return statement


def _fixes_column(where: str, column: str) -> bool:
    """True si el WHERE fija la columna por igualdad con su parámetro: ``col = %(col)s``."""
    return re.search(rf"\b{column}\s*=\s*%\({column}\)s", where) is not None


def test_hay_un_archivo_por_cada_consulta_esperada():
    assert {cql_path.stem for cql_path in QUERY_DIR.glob("*.cql")} == set(EXPECTED_PARAMS)


@pytest.mark.parametrize("query", sorted(EXPECTED_PARAMS))
def test_consulta_usa_sus_parametros_con_nombre(query):
    cql_text = (QUERY_DIR / f"{query}.cql").read_text(encoding="utf-8")

    assert set(_PARAM_RE.findall(_statement(query))) == EXPECTED_PARAMS[query]
    # El driver arma la consulta con %, así que el archivo entero (comentarios incluidos)
    # tiene que poder formatearse con sus parámetros, sin otros % sueltos.
    rendered = cql_text % dict.fromkeys(EXPECTED_PARAMS[query], "x")
    assert "%" not in rendered


@pytest.mark.parametrize("query", sorted(EXPECTED_PARAMS))
def test_consulta_no_usa_allow_filtering(query):
    cql_text = (QUERY_DIR / f"{query}.cql").read_text(encoding="utf-8")

    assert "ALLOW FILTERING" not in " ".join(cql_text.upper().split())


@pytest.mark.parametrize("query", sorted(EXPECTED_PARAMS))
def test_consulta_filtra_por_toda_la_clave_de_particion(query):
    statement = _statement(query)
    table_match = _FROM_RE.search(statement)
    where_match = _WHERE_RE.search(statement)
    assert table_match and where_match, f"{query}: falta FROM o WHERE"
    where = statement[where_match.end() :]

    partition_key = parse_ddl(SCHEMA_DIR / f"{table_match[1]}.cql").partition_key

    assert partition_key
    assert [column for column in partition_key if not _fixes_column(where, column)] == []
