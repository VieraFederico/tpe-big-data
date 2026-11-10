import urllib.error

import pytest
from cassandra.protocol import InvalidRequest

from src.serving import astra_bundle, cassandra_client
from src.serving.cassandra_writer import write_dataframe
from src.serving.errors import AstraApiError, KeyspaceNotFoundError, ServingError, WriteFailedError


class FakeSession:
    def __init__(self, failing_statement=None):
        self.failing_statement = failing_statement

    def execute(self, statement):
        if self.failing_statement == "execute":
            raise InvalidRequest("nombre de keyspace inválido")

    def set_keyspace(self, keyspace):
        if self.failing_statement == "set_keyspace":
            raise InvalidRequest(f"Keyspace '{keyspace}' does not exist")


class FakeCluster:
    def __init__(self, session):
        self.session = session
        self.was_shut_down = False

    def connect(self):
        return self.session

    def shutdown(self):
        self.was_shut_down = True


def _patch_cluster(monkeypatch, session):
    fake_cluster = FakeCluster(session)
    monkeypatch.setattr(cassandra_client, "Cluster", lambda *args, **kwargs: fake_cluster)
    return fake_cluster


def test_keyspace_inexistente_da_error_propio_y_cierra_el_cluster(monkeypatch, settings):
    fake_cluster = _patch_cluster(monkeypatch, FakeSession(failing_statement="set_keyspace"))

    with pytest.raises(KeyspaceNotFoundError, match="no existe") as error_info:
        cassandra_client.get_session(settings)

    assert error_info.value.keyspace == settings.keyspace
    assert fake_cluster.was_shut_down


def test_error_al_crear_keyspace_no_se_reporta_como_inexistente(monkeypatch, settings):
    fake_cluster = _patch_cluster(monkeypatch, FakeSession(failing_statement="execute"))

    with pytest.raises(InvalidRequest):
        cassandra_client.get_session(settings)

    assert fake_cluster.was_shut_down


def test_sesion_ok_no_cierra_el_cluster(monkeypatch, settings):
    fake_cluster = _patch_cluster(monkeypatch, FakeSession())

    assert cassandra_client.get_session(settings) is fake_cluster.session
    assert not fake_cluster.was_shut_down


def test_api_de_astra_con_403_da_error_propio(monkeypatch):
    def reject_request(request, timeout):
        raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.setattr(astra_bundle.urllib.request, "urlopen", reject_request)

    with pytest.raises(AstraApiError, match="token sin permisos") as error_info:
        astra_bundle._request_download_url("db-id", "token")

    assert error_info.value.status_code == 403


def test_write_failed_error_guarda_tabla_y_filas():
    error = WriteFailedError("usage_daily", 3)

    assert isinstance(error, ServingError)
    assert (error.table, error.rows_failed) == ("usage_daily", 3)
    assert "3 filas fallaron al escribir usage_daily" in str(error)


def test_write_dataframe_rechaza_identificadores_invalidos(spark, settings):
    invalid_columns = spark.createDataFrame([(1,)], "OrgId int")

    with pytest.raises(ValueError, match="Identificador inválido"):
        write_dataframe(invalid_columns, "usage_daily", settings)
