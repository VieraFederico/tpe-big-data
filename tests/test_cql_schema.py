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
