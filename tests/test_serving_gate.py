"""El serving solo publica un Gold cuyo manifiesto terminó en ``ok``; no necesita Cassandra."""

import pytest

from src.common.manifests import write_manifest
from src.serving import cassandra_writer
from src.serving.errors import GoldNotPublishableError, ServingError


class SessionOpenedError(Exception):
    """Se intentó abrir una sesión CQL."""


@pytest.fixture
def session_attempts(monkeypatch):
    """Reemplaza la apertura de sesión: registra el intento y corta el paso ahí."""
    attempts = []

    def open_session(settings):
        attempts.append(settings)
        raise SessionOpenedError

    monkeypatch.setattr(cassandra_writer, "session_scope", open_session)
    return attempts


@pytest.fixture
def schema_dir(tmp_path):
    directory = tmp_path / "schema"
    directory.mkdir()
    (directory / "mart_de_prueba.cql").write_text(
        "CREATE TABLE IF NOT EXISTS mart_de_prueba (id text PRIMARY KEY);\n", encoding="utf-8"
    )
    return directory


def test_sin_manifiesto_de_gold_no_abre_sesion(spark, settings, schema_dir, session_attempts):
    with pytest.raises(GoldNotPublishableError) as error_info:
        cassandra_writer.run(spark, settings, schema_dir)

    assert error_info.value.status == "missing"
    assert session_attempts == []


def test_gold_con_conciliacion_fallida_no_publica(spark, settings, schema_dir, session_attempts):
    write_manifest(
        settings,
        "gold",
        {"status": "gate_failed", "failed_checks": ["BRZ_SILVER_BALANCE"], "marts": {}},
    )

    with pytest.raises(GoldNotPublishableError, match="gate_failed") as error_info:
        cassandra_writer.run(spark, settings, schema_dir)

    assert error_info.value.status == "gate_failed"
    assert isinstance(error_info.value, ServingError)
    assert session_attempts == []


def test_manifiesto_sin_status_no_se_publica(settings):
    write_manifest(settings, "gold", {"marts": {}})

    with pytest.raises(GoldNotPublishableError):
        cassandra_writer.check_gold_publishable(settings)


def test_gold_ok_pasa_el_control_y_abre_la_sesion(spark, settings, schema_dir, session_attempts):
    write_manifest(settings, "gold", {"status": "ok", "marts": {"mart_de_prueba": 1}})

    with pytest.raises(SessionOpenedError):
        cassandra_writer.run(spark, settings, schema_dir)

    assert session_attempts == [settings]
