import io
import json
import urllib.request
from dataclasses import replace

import pytest

from src.serving import astra_bundle
from src.serving.astra_bundle import ensure_bundle

DB_ID = "0123abcd-0123-4567-89ab-0123456789ab"
DOWNLOAD_URL = "https://storage.example/bundle.zip"
BUNDLE_BYTES = b"contenido del zip"


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exception_info):
        self.close()


@pytest.fixture
def astra_settings(settings, tmp_path):
    return replace(
        settings,
        serving_target="astra",
        astra_token="token-falso",
        astra_api_endpoint=f"https://{DB_ID}-us-east1.apps.astra.datastax.com",
        astra_bundle_path=str(tmp_path / "secrets" / "bundle.zip"),
    )


def _fake_urlopen(download_error=None):
    def fake_urlopen(request, timeout):
        if isinstance(request, urllib.request.Request):
            return FakeResponse(json.dumps({"downloadURL": DOWNLOAD_URL}).encode())
        assert request == DOWNLOAD_URL
        if download_error:
            raise download_error
        return FakeResponse(BUNDLE_BYTES)

    return fake_urlopen


def test_descarga_el_bundle_si_no_existe(monkeypatch, astra_settings, tmp_path):
    monkeypatch.setattr(astra_bundle.urllib.request, "urlopen", _fake_urlopen())

    bundle_path = ensure_bundle(astra_settings)

    assert bundle_path == astra_settings.astra_bundle_path
    assert (tmp_path / "secrets" / "bundle.zip").read_bytes() == BUNDLE_BYTES
    assert list((tmp_path / "secrets").glob("*.part")) == []


def test_descarga_fallida_no_deja_archivos_a_medias(monkeypatch, astra_settings, tmp_path):
    monkeypatch.setattr(
        astra_bundle.urllib.request, "urlopen", _fake_urlopen(download_error=TimeoutError())
    )

    with pytest.raises(TimeoutError):
        ensure_bundle(astra_settings)

    assert list((tmp_path / "secrets").iterdir()) == []
