import re

import pytest

from src.config import load_settings
from src.serving.astra_bundle import database_id_from_endpoint

DB_ID = "0123abcd-0123-4567-89ab-0123456789ab"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in ("SERVING_TARGET", "ASTRA_DB_APPLICATION_TOKEN", "ASTRA_DB_API_ENDPOINT",
                "ASTRA_DB_SECURE_BUNDLE_PATH", "DATALAKE_PATH"):
        monkeypatch.delenv(var, raising=False)


def test_defaults_locales():
    s = load_settings()

    assert s.serving_target == "local"
    assert s.zone_path("bronze").endswith("/bronze")


def test_zone_path_ignora_barra_final(monkeypatch):
    monkeypatch.setenv("DATALAKE_PATH", "/lake/")

    assert load_settings().zone_path("gold") == "/lake/gold"


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
    monkeypatch.setenv("ASTRA_DB_API_ENDPOINT", f"https://{DB_ID}-us-east1.apps.astra.datastax.com")

    assert load_settings().serving_target == "astra"


def test_db_id_desde_endpoint():
    assert database_id_from_endpoint(f"https://{DB_ID}-us-east1.apps.astra.datastax.com") == DB_ID


def test_endpoint_invalido():
    with pytest.raises(ValueError):
        database_id_from_endpoint("https://algo.com")


def test_run_id_se_genera_si_no_se_pasa():
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{6}", load_settings().run_id)
    assert load_settings().run_id != load_settings().run_id


def test_run_id_explicito_se_respeta():
    assert load_settings(run_id="corrida_x").run_id == "corrida_x"
