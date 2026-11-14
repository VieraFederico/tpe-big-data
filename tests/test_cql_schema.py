import re
from pathlib import Path
from typing import NamedTuple

import pytest

from src.gold.build import MART_SCHEMAS
from src.serving.cassandra_client import read_cql_statements
from src.serving.cassandra_writer import SCHEMA_DIR


def test_read_cql_statements_separa_sentencias_e_ignora_comentarios(tmp_path):
    cql_file = tmp_path / "schema.cql"
    cql_file.write_text(
        "-- comentario\nCREATE TABLE a (id text PRIMARY KEY);\n\n"
        "CREATE TABLE b (\n    id text PRIMARY KEY\n);\n",
        encoding="utf-8",
    )

    assert read_cql_statements(cql_file) == [
        "CREATE TABLE a (id text PRIMARY KEY)",
        "CREATE TABLE b (\n    id text PRIMARY KEY\n)",
    ]


def test_cada_ddl_crea_la_tabla_con_el_nombre_del_archivo():
    for cql_path in SCHEMA_DIR.glob("*.cql"):
        statements = read_cql_statements(cql_path)
        assert statements[0].startswith(f"CREATE TABLE IF NOT EXISTS {cql_path.stem} (")


class TableDdl(NamedTuple):
    """Lo que interesa de un CREATE TABLE: columnas, clave primaria y orden de clustering."""

    name: str
    columns: dict[str, str]
    partition_key: tuple[str, ...]
    clustering: tuple[tuple[str, str], ...]


_CREATE_TABLE_RE = re.compile(
    r"CREATE TABLE IF NOT EXISTS (?P<name>\w+) \((?P<body>.*?)\)"
    r"(?:\s+WITH CLUSTERING ORDER BY \((?P<order>[^)]*)\))?$",
    re.DOTALL,
)
_PRIMARY_KEY_RE = re.compile(r"PRIMARY KEY \(\((?P<partition>[^)]*)\)(?P<clustering>[^)]*)\)")


def _names(text: str) -> tuple[str, ...]:
    return tuple(name.strip() for name in text.split(",") if name.strip())


def parse_ddl(cql_path: Path) -> TableDdl:
    """Parsea el CREATE TABLE de un .cql de ``schema/`` (una sola sentencia).

    Espera la clave como ``PRIMARY KEY ((particion...), clustering...)``; las columnas de
    clustering sin orden explícito quedan ``ASC``.
    """
    [statement] = read_cql_statements(cql_path)
    match = _CREATE_TABLE_RE.match(statement)
    assert match, f"{cql_path.name}: no es un CREATE TABLE IF NOT EXISTS reconocible"
    body = match["body"]
    primary_key = _PRIMARY_KEY_RE.search(body)
    assert primary_key, f"{cql_path.name}: falta PRIMARY KEY ((particion), clustering)"
    column_lines = body[: primary_key.start()].split(",")
    columns = dict(line.split() for line in column_lines if line.strip())
    order = dict(item.split() for item in _names(match["order"] or ""))
    return TableDdl(
        name=match["name"],
        columns=columns,
        partition_key=_names(primary_key["partition"]),
        clustering=tuple(
            (name, order.get(name, "ASC")) for name in _names(primary_key["clustering"])
        ),
    )


# Tipo de Spark (simpleString) -> tipo de CQL.
SPARK_TO_CQL = {
    "string": "text",
    "date": "date",
    "double": "double",
    "bigint": "bigint",
    "int": "int",
}

# Clave de cada tabla, pensada desde la consulta que la lee.
EXPECTED_KEYS = {
    "org_daily_usage_by_service": (("org_id",), (("usage_date", "DESC"), ("service", "ASC"))),
    "top_services_by_org_14d": (("org_id", "as_of_date"), (("rank", "ASC"),)),
    "cost_anomaly_mart": (("org_id",), (("usage_date", "DESC"), ("service", "ASC"))),
}


def test_hay_un_ddl_por_cada_mart_de_gold():
    assert {cql_path.stem for cql_path in SCHEMA_DIR.glob("*.cql")} == set(MART_SCHEMAS)


@pytest.mark.parametrize("mart", sorted(MART_SCHEMAS))
def test_ddl_tiene_las_columnas_y_tipos_del_mart(mart):
    ddl = parse_ddl(SCHEMA_DIR / f"{mart}.cql")

    assert ddl.name == mart
    assert ddl.columns == {
        field.name: SPARK_TO_CQL[field.dataType.simpleString()] for field in MART_SCHEMAS[mart]
    }


@pytest.mark.parametrize("mart", sorted(EXPECTED_KEYS))
def test_ddl_tiene_la_clave_primaria_y_el_orden_de_su_consulta(mart):
    ddl = parse_ddl(SCHEMA_DIR / f"{mart}.cql")

    assert (ddl.partition_key, ddl.clustering) == EXPECTED_KEYS[mart]


def test_parse_ddl_lee_columnas_clave_y_orden(tmp_path):
    cql_file = tmp_path / "usage.cql"
    cql_file.write_text(
        "-- comentario\nCREATE TABLE IF NOT EXISTS usage (\n    org_id text,\n    day date,\n"
        "    cost double,\n    PRIMARY KEY ((org_id), day)\n"
        ") WITH CLUSTERING ORDER BY (day DESC);\n",
        encoding="utf-8",
    )

    assert parse_ddl(cql_file) == TableDdl(
        name="usage",
        columns={"org_id": "text", "day": "date", "cost": "double"},
        partition_key=("org_id",),
        clustering=(("day", "DESC"),),
    )
