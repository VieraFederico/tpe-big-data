"""Carga de los marts y consultas de ``cql/queries/`` contra una Cassandra real.

Cada test publica un Gold de prueba con ``cassandra_writer.run`` (las tablas salen de los DDL
reales) y lee con ``run_query``, igual que el CLI de consultas. Se saltean si no hay Cassandra.
Levantarla con:

    docker compose -f infra/docker-compose.yml --profile local up -d --wait cassandra
"""

import os
from dataclasses import replace
from datetime import date, timedelta

import pytest
from cassandra.util import Date

from src.common.lake import write_dataset
from src.common.manifests import write_manifest
from src.config import Settings, Zone
from src.gold.anomalies import COST_ANOMALY_MART
from src.gold.build import MART_SCHEMAS, STEP_NAME
from src.gold.marts import ORG_DAILY_USAGE_BY_SERVICE, TOP_SERVICES_BY_ORG_14D
from src.serving import cassandra_writer
from src.serving.cassandra_client import get_session
from src.serving.cassandra_writer import SCHEMA_DIR
from src.serving.queries import parse_param, run_query

pytestmark = pytest.mark.integration

QUERY_DIR = SCHEMA_DIR.parent / "queries"
DAILY = ORG_DAILY_USAGE_BY_SERVICE
TOP = TOP_SERVICES_BY_ORG_14D
ANOMALIES = COST_ANOMALY_MART
# Gold particiona por fecha: así se lee de vuelta igual que el mart real.
PARTITION_BY = {DAILY: ("usage_date",), TOP: ("as_of_date",), ANOMALIES: ("usage_date",)}
RUN_ID = "run_test"
AS_OF = date(2025, 8, 31)
FROM_DATE = date(2025, 8, 18)
RANGE_PARAMS = {"org_id": "org_a", "from_date": "2025-08-18", "to_date": "2025-08-31"}

Q1_COLUMNS = [
    "usage_date",
    "service",
    "daily_cost_usd",
    "requests",
    "cpu_hours",
    "storage_gb_hours",
    "genai_tokens",
    "carbon_kg",
]
Q2_COLUMNS = ["rank", "service", "cost_14d_usd", "requests_14d", "window_start_date"]
ANOMALY_COLUMNS = ["usage_date", "service", "daily_cost_usd", "threshold_usd", "anomaly_score"]


def _daily_row(org_id, service, usage_date, cost):
    return (
        org_id,
        service,
        usage_date,
        cost,
        cost * 10,
        cost / 2,
        cost * 3,
        cost / 100,
        int(cost * 1000),
        4,
        0,
        RUN_ID,
    )


def _daily_rows(org_id, days, services, first_cost):
    rows = []
    for day in days:
        for service in services:
            rows.append(_daily_row(org_id, service, day, first_cost + len(rows)))
    return rows


def _top_row(org_id, as_of_date, rank, service, cost):
    window_start = as_of_date - timedelta(days=13)
    return (org_id, service, as_of_date, window_start, rank, cost, cost * 20, RUN_ID)


def _top_rows(org_id, as_of_date, services, top_cost):
    """Una fila por servicio: rank 1 para el primero y costo decreciente."""
    return [
        _top_row(org_id, as_of_date, rank, service, top_cost - rank)
        for rank, service in enumerate(services, start=1)
    ]


def _anomaly_row(org_id, service, usage_date, cost):
    return (org_id, service, usage_date, cost, cost / 4, cost / 2, 2.0, 5.5, "p99_x2", RUN_ID)


def _anomaly_rows(org_id, days, services, first_cost):
    rows = []
    for day in days:
        for service in services:
            rows.append(_anomaly_row(org_id, service, day, first_cost + len(rows)))
    return rows


def _days(last_day, count):
    return [last_day - timedelta(days=offset) for offset in range(count)]


def _records(mart, rows):
    """Filas de Gold como diccionarios columna -> valor."""
    names = MART_SCHEMAS[mart].fieldNames()
    return [dict(zip(names, row, strict=True)) for row in rows]


def _as_python(value):
    """Cassandra devuelve las fechas como ``cassandra.util.Date``; se pasan a ``date``."""
    return value.date() if isinstance(value, Date) else value


def _result_rows(result):
    return [tuple(_as_python(value) for value in row) for row in result.rows]


def _project(records, columns):
    return [tuple(record[column] for column in columns) for record in records]


def _newest_first(records):
    """Orden de la clustering: día más reciente primero y, dentro del día, por servicio."""
    by_service = sorted(records, key=lambda record: record["service"])
    return sorted(by_service, key=lambda record: record["usage_date"], reverse=True)


def _query(session, query, params):
    """Corre un .cql de ``queries/`` tal como lo lee el CLI (comentarios incluidos)."""
    cql = (QUERY_DIR / f"{query}.cql").read_text(encoding="utf-8")
    return run_query(session, cql, params, name=query)


def _publish_gold(spark, settings, lake_dir, rows_by_mart):
    """Escribe un Gold de prueba con su manifiesto ``ok`` y devuelve la config de ese lake."""
    lake_settings = replace(settings, datalake_path=str(lake_dir))
    for mart, rows in rows_by_mart.items():
        mart_df = spark.createDataFrame(rows, MART_SCHEMAS[mart])
        write_dataset(mart_df, lake_settings, Zone.GOLD, mart, partition_by=PARTITION_BY[mart])
    marts = {mart: len(rows) for mart, rows in rows_by_mart.items()}
    write_manifest(lake_settings, STEP_NAME, {"status": "ok", "marts": marts})
    return lake_settings


@pytest.fixture(scope="module")
def cassandra_settings():
    integration_settings = Settings(
        datalake_path="/tmp/unused",
        serving_target="local",
        keyspace="tpe_integration_tests",
        cassandra_host=os.getenv("CASSANDRA_HOST", "localhost"),
        cassandra_port=int(os.getenv("CASSANDRA_PORT", "9042")),
        astra_token=None,
        astra_api_endpoint=None,
        astra_bundle_path=None,
    )
    try:
        session = get_session(integration_settings)
    except Exception as error:  # sin Cassandra: NoHostAvailable, DNS, timeout...
        pytest.skip(f"Cassandra no disponible en {integration_settings.cassandra_host}: {error!r}")
    # Se borran una vez por módulo: run las recrea con el DDL actual y sin filas viejas.
    for mart in MART_SCHEMAS:
        session.execute(f"DROP TABLE IF EXISTS {mart}")
    yield integration_settings, session
    session.cluster.shutdown()


# Gold de carga: 10 filas por mart, todas en la misma partición de org_c.
LOAD_ORG = "org_c"
LOAD_GOLD = {
    DAILY: _daily_rows(LOAD_ORG, _days(AS_OF, 5), ["service_a", "service_b"], 1.25),
    TOP: _top_rows(LOAD_ORG, AS_OF, [f"service_{rank:02d}" for rank in range(1, 11)], 100.5),
    ANOMALIES: _anomaly_rows(LOAD_ORG, _days(AS_OF, 5), ["service_a", "service_b"], 50.75),
}
LOAD_PARTITION = {
    DAILY: {"org_id": LOAD_ORG},
    TOP: {"org_id": LOAD_ORG, "as_of_date": AS_OF.isoformat()},
    ANOMALIES: {"org_id": LOAD_ORG},
}


def _count(session, mart):
    result = _query(session, f"count_{mart}", LOAD_PARTITION[mart])
    assert result.columns == ["row_count"]
    [(row_count,)] = result.rows
    return row_count


def _stored_partition(session, mart):
    """Lee con un SELECT * toda la partición de carga de ``mart``, en el orden de Gold."""
    partition = LOAD_PARTITION[mart]
    where = " AND ".join(f"{column} = %({column})s" for column in partition)
    stored = run_query(session, f"SELECT * FROM {mart} WHERE {where}", partition)
    records = [dict(zip(stored.columns, row, strict=True)) for row in _result_rows(stored)]
    return _project(records, MART_SCHEMAS[mart].fieldNames())


def test_run_carga_cada_mart_completo_con_los_valores_de_gold(spark, cassandra_settings, tmp_path):
    settings, session = cassandra_settings
    lake_settings = _publish_gold(spark, settings, tmp_path / "lake", LOAD_GOLD)

    results = cassandra_writer.run(spark, lake_settings)

    loaded = {mart: (result.rows_written, result.rows_failed) for mart, result in results.items()}
    assert loaded == {mart: (10, 0) for mart in LOAD_GOLD}
    for mart, rows in LOAD_GOLD.items():
        assert sorted(_stored_partition(session, mart)) == sorted(rows)


def test_recargar_el_mismo_gold_no_duplica_filas(spark, cassandra_settings, tmp_path):
    settings, session = cassandra_settings
    lake_settings = _publish_gold(spark, settings, tmp_path / "lake", LOAD_GOLD)

    cassandra_writer.run(spark, lake_settings)
    counts_before = {mart: _count(session, mart) for mart in LOAD_GOLD}
    cassandra_writer.run(spark, lake_settings)
    counts_after = {mart: _count(session, mart) for mart in LOAD_GOLD}

    gold_counts = {mart: len(rows) for mart, rows in LOAD_GOLD.items()}
    assert counts_before == counts_after == gold_counts
    for mart, rows in LOAD_GOLD.items():
        assert sorted(_stored_partition(session, mart)) == sorted(rows)


# Gold para las consultas: org_a con datos dentro y fuera del rango, y otras particiones
# (org_b, otra fecha de corte) que las consultas no tienen que devolver.
TOP_SERVICES = ["compute", "storage", "network", "database", "genai", "analytics"]
QUERY_GOLD = {
    # org_a del 2025-08-10 al 2025-08-31 en dos servicios: 44 filas.
    DAILY: [
        *_daily_rows("org_a", _days(AS_OF, 22), ["compute", "storage"], 10.5),
        *_daily_rows("org_b", [date(2025, 8, 20)], ["compute"], 99.0),
    ],
    # Se escriben del último rank al primero: el orden lo tiene que dar la tabla.
    TOP: [
        *reversed(_top_rows("org_a", AS_OF, TOP_SERVICES, 600.0)),
        _top_row("org_a", AS_OF - timedelta(days=1), 1, "compute", 900.0),
        _top_row("org_b", AS_OF, 1, "storage", 800.0),
    ],
    # Tres anomalías de org_a, dos dentro del rango; una de org_b en el rango.
    ANOMALIES: [
        _anomaly_row("org_a", "compute", date(2025, 8, 5), 40.0),
        _anomaly_row("org_a", "storage", date(2025, 8, 20), 41.0),
        _anomaly_row("org_a", "compute", date(2025, 8, 25), 42.0),
        _anomaly_row("org_b", "compute", date(2025, 8, 25), 43.0),
    ],
}


def _in_range_for_org_a(records):
    return [
        record
        for record in records
        if record["org_id"] == "org_a" and FROM_DATE <= record["usage_date"] <= AS_OF
    ]


@pytest.fixture(scope="module")
def query_session(spark, cassandra_settings, tmp_path_factory):
    """Sesión con el Gold de consultas ya cargado (una sola carga para todo el módulo)."""
    settings, session = cassandra_settings
    lake_settings = _publish_gold(spark, settings, tmp_path_factory.mktemp("lake"), QUERY_GOLD)
    cassandra_writer.run(spark, lake_settings)
    return session


def test_consulta_1_devuelve_el_rango_de_la_org_del_dia_mas_reciente_al_mas_viejo(query_session):
    result = _query(query_session, "q1_daily_usage_by_range", RANGE_PARAMS)

    expected = _newest_first(_in_range_for_org_a(_records(DAILY, QUERY_GOLD[DAILY])))
    assert result.columns == Q1_COLUMNS
    assert result.total_rows == 28
    assert _result_rows(result)[0][0] == AS_OF
    assert _result_rows(result) == _project(expected, Q1_COLUMNS)


def test_consulta_2_devuelve_el_top_n_ordenado_por_rank(query_session):
    # Parámetros tipados igual que en el CLI: -p n:int=5.
    cli_params = ["org_id=org_a", "as_of_date=2025-08-31", "n:int=5"]
    params = dict(parse_param(item) for item in cli_params)

    result = _query(query_session, "q2_top_services_14d", params)

    partition = [
        record
        for record in _records(TOP, QUERY_GOLD[TOP])
        if record["org_id"] == "org_a" and record["as_of_date"] == AS_OF
    ]
    gold_top = sorted(partition, key=lambda record: record["rank"])[:5]
    assert result.columns == Q2_COLUMNS
    assert [row[0] for row in result.rows] == [1, 2, 3, 4, 5]
    assert [row[1] for row in result.rows] == TOP_SERVICES[:5]
    assert _result_rows(result) == _project(gold_top, Q2_COLUMNS)


def test_anomalias_de_la_org_devuelve_solo_las_del_rango(query_session):
    result = _query(query_session, "q_cost_anomalies_by_org", RANGE_PARAMS)

    expected = _newest_first(_in_range_for_org_a(_records(ANOMALIES, QUERY_GOLD[ANOMALIES])))
    assert result.columns == ANOMALY_COLUMNS
    assert result.total_rows == 2
    assert _result_rows(result) == _project(expected, ANOMALY_COLUMNS)
