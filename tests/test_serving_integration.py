"""Tests contra una Cassandra real; se saltean si no hay una disponible.

Levantarla con ``docker compose -f infra/docker-compose.yml --profile local up -d cassandra``.
"""

import os
from datetime import date

import pytest

from src.config import Settings
from src.serving.cassandra_client import get_session
from src.serving.errors import WriteFailedError
from src.serving.loader import write_dataframe
from src.serving.queries import run_query

pytestmark = pytest.mark.integration

TABLE = "usage_by_org"


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
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE} "
        "(org_id text, day date, cost double, PRIMARY KEY (org_id, day))"
    )
    yield integration_settings, session
    session.cluster.shutdown()


@pytest.fixture
def cassandra_session(cassandra_settings):
    settings, session = cassandra_settings
    session.execute(f"TRUNCATE {TABLE}")
    return settings, session


def _usage(spark, rows):
    return spark.createDataFrame(rows, "org_id string, day date, cost double")


def test_write_dataframe_escribe_y_se_puede_consultar(spark, cassandra_session):
    settings, session = cassandra_session
    usage = _usage(spark, [("org_1", date(2025, 8, 1), 1.5), ("org_1", date(2025, 8, 2), 2.0)])

    result = write_dataframe(usage, TABLE, settings)

    assert (result.rows_written, result.rows_failed) == (2, 0)
    query_result = run_query(
        session, f"SELECT day, cost FROM {TABLE} WHERE org_id = %(org_id)s;", {"org_id": "org_1"}
    )
    assert query_result.columns == ["day", "cost"]
    assert [(row[0].date(), row[1]) for row in query_result.rows] == [
        (date(2025, 8, 1), 1.5),
        (date(2025, 8, 2), 2.0),
    ]


def test_valor_nulo_se_escribe_sin_pisar_con_null(spark, cassandra_session):
    settings, session = cassandra_session
    write_dataframe(_usage(spark, [("org_1", date(2025, 8, 1), 1.5)]), TABLE, settings)

    # cost=None viaja como UNSET: no borra el valor que ya estaba.
    write_dataframe(_usage(spark, [("org_1", date(2025, 8, 1), None)]), TABLE, settings)

    stored_cost = session.execute(f"SELECT cost FROM {TABLE} WHERE org_id = 'org_1'").one().cost
    assert stored_cost == 1.5


def test_clave_nula_hace_fallar_la_carga(spark, cassandra_session):
    settings, _ = cassandra_session
    usage = _usage(spark, [("org_1", date(2025, 8, 1), 1.5), (None, date(2025, 8, 1), 2.0)])

    with pytest.raises(WriteFailedError) as error_info:
        write_dataframe(usage, TABLE, settings)

    assert (error_info.value.table, error_info.value.rows_failed) == (TABLE, 1)


def test_sin_fail_on_error_devuelve_las_filas_fallidas(spark, cassandra_session):
    settings, _ = cassandra_session
    usage = _usage(spark, [("org_1", date(2025, 8, 1), 1.5), (None, date(2025, 8, 1), 2.0)])

    result = write_dataframe(usage, TABLE, settings, fail_on_error=False)

    assert (result.rows_written, result.rows_failed) == (1, 1)
