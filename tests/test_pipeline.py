import pytest

from src import pipeline


def test_paso_con_modulo_inexistente_no_esta_implementado(monkeypatch):
    monkeypatch.setitem(pipeline.STEPS, "fantasma", "src.no_existe")

    with pytest.raises(NotImplementedError, match="src.no_existe.run"):
        pipeline._resolve("fantasma")


def test_paso_con_modulo_sin_run_no_esta_implementado(monkeypatch):
    monkeypatch.setitem(pipeline.STEPS, "sin_run", "src.common.columns")

    with pytest.raises(NotImplementedError, match="src.common.columns.run"):
        pipeline._resolve("sin_run")


def test_paso_implementado_devuelve_run():
    from src.bronze.batch import ingest

    assert pipeline._resolve("bronze_batch") is ingest.run
