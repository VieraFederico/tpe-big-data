import json
import re
from pathlib import Path

import pytest

from src.common.manifests import read_manifest, write_manifest


def test_write_manifest_agrega_run_id_y_fecha_de_escritura(settings):
    path = write_manifest(settings, "paso", {"datasets": {"a": {"rows": 3}}})

    assert path == Path(settings.datalake_path) / "_manifests" / "paso.json"
    content = json.loads(path.read_text(encoding="utf-8"))
    assert content["run_id"] == settings.run_id
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", content["written_at"])
    assert content["datasets"] == {"a": {"rows": 3}}


def test_read_manifest_devuelve_lo_escrito(settings):
    write_manifest(settings, "paso", {"total": 7})

    manifest = read_manifest(settings, "paso")

    assert manifest is not None
    assert (manifest["run_id"], manifest["total"]) == (settings.run_id, 7)


def test_read_manifest_inexistente_da_none(settings):
    assert read_manifest(settings, "no_existe") is None


def test_write_manifest_reemplaza_el_anterior(settings):
    write_manifest(settings, "paso", {"total": 1})
    write_manifest(settings, "paso", {"total": 2})

    manifest = read_manifest(settings, "paso")

    assert manifest is not None and manifest["total"] == 2
    assert [path.name for path in (Path(settings.datalake_path) / "_manifests").iterdir()] == [
        "paso.json"
    ]


def test_write_manifest_rechaza_claves_reservadas(settings):
    with pytest.raises(ValueError, match="run_id"):
        write_manifest(settings, "paso", {"run_id": "otro"})
