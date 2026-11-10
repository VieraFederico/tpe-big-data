import re
from dataclasses import replace

import pytest

from src.config import Zone, load_env_file, load_settings
from src.serving.astra_bundle import database_id_from_endpoint, ensure_bundle

DB_ID = "0123abcd-0123-4567-89ab-0123456789ab"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for variable_name in (
        "SERVING_TARGET",
        "ASTRA_DB_APPLICATION_TOKEN",
        "ASTRA_DB_API_ENDPOINT",
        "ASTRA_DB_SECURE_BUNDLE_PATH",
        "DATALAKE_PATH",
    ):
        monkeypatch.delenv(variable_name, raising=False)


def test_defaults_locales():
    loaded_settings = load_settings()

    assert loaded_settings.serving_target == "local"
    assert loaded_settings.zone_path(Zone.BRONZE).endswith("/bronze")


def test_zone_path_ignora_barra_final(monkeypatch):
    monkeypatch.setenv("DATALAKE_PATH", "/lake/")

    assert load_settings().zone_path(Zone.GOLD) == "/lake/gold"


def test_target_invalido(monkeypatch):
    monkeypatch.setenv("SERVING_TARGET", "mongo")

    with pytest.raises(ValueError, match="inválido"):
        load_settings()


def test_astra_sin_token(monkeypatch):
    monkeypatch.setenv("SERVING_TARGET", "astra")

    with pytest.raises(ValueError, match="ASTRA_DB_APPLICATION_TOKEN"):
        load_settings()


def test_astra_sin_endpoint_ni_bundle(monkeypatch):
    monkeypatch.setenv("SERVING_TARGET", "astra")
    monkeypatch.setenv("ASTRA_DB_APPLICATION_TOKEN", "AstraCS:x")

    with pytest.raises(ValueError, match="ASTRA_DB_API_ENDPOINT"):
        load_settings()


def test_astra_con_token_y_endpoint(monkeypatch):
    monkeypatch.setenv("SERVING_TARGET", "astra")
    monkeypatch.setenv("ASTRA_DB_APPLICATION_TOKEN", "AstraCS:x")
    monkeypatch.setenv(
        "ASTRA_DB_API_ENDPOINT", f"https://{DB_ID}-us-east1.apps.astra.datastax.com"
    )

    assert load_settings().serving_target == "astra"


def test_db_id_desde_endpoint():
    assert database_id_from_endpoint(f"https://{DB_ID}-us-east1.apps.astra.datastax.com") == DB_ID


def test_endpoint_invalido():
    with pytest.raises(ValueError):
        database_id_from_endpoint("https://algo.com")


def test_bundle_sin_endpoint_explica_que_falta(settings):
    with pytest.raises(ValueError, match="falta ASTRA_DB_API_ENDPOINT"):
        ensure_bundle(settings)


def test_bundle_sin_token_explica_que_falta(settings, tmp_path):
    astra_settings = replace(
        settings,
        astra_api_endpoint=f"https://{DB_ID}-us-east1.apps.astra.datastax.com",
        astra_bundle_path=str(tmp_path / "bundle.zip"),
    )

    with pytest.raises(ValueError, match="Falta ASTRA_DB_APPLICATION_TOKEN"):
        ensure_bundle(astra_settings)


def test_bundle_existente_se_usa_sin_descargar(settings, tmp_path):
    bundle = tmp_path / "bundle.zip"
    bundle.write_bytes(b"zip")

    assert ensure_bundle(replace(settings, astra_bundle_path=str(bundle))) == str(bundle)


def test_run_id_se_genera_si_no_se_pasa():
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", load_settings().run_id)
    assert load_settings().run_id != load_settings().run_id


def test_run_id_explicito_se_respeta():
    assert load_settings(run_id="corrida_x").run_id == "corrida_x"


def test_load_env_file_carga_el_archivo(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DATALAKE_PATH=/desde/archivo\n", encoding="utf-8")
    monkeypatch.setenv("ENV_FILE", str(env_file))
    # setenv + delenv: la variable arranca ausente y monkeypatch la borra al terminar.
    monkeypatch.setenv("DATALAKE_PATH", "temporal")
    monkeypatch.delenv("DATALAKE_PATH")

    load_env_file()

    assert load_settings().datalake_path == "/desde/archivo"


def test_load_env_file_no_pisa_variables_existentes(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DATALAKE_PATH=/desde/archivo\n", encoding="utf-8")
    monkeypatch.setenv("ENV_FILE", str(env_file))
    monkeypatch.setenv("DATALAKE_PATH", "/desde/entorno")

    load_env_file()

    assert load_settings().datalake_path == "/desde/entorno"
