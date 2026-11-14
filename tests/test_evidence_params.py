import json
from datetime import date

import pytest

from src.common.lake import write_dataset
from src.config import Zone
from src.serving import evidence_params
from src.serving.evidence_params import NoRowsForAnchorError, compute

TOP_SCHEMA = (
    "org_id string, service string, as_of_date date, window_start_date date, rank int, "
    "cost_14d_usd double, requests_14d double, run_id string"
)
ANCHOR = date(2025, 8, 31)
DAY_BEFORE = date(2025, 8, 30)


def _write_top(spark, settings, rows):
    """Escribe ``gold/top_services_by_org_14d`` con filas (org, service, as_of_date, cost)."""
    top = spark.createDataFrame(
        [
            (org_id, service, as_of_date, date(2025, 8, 18), rank, cost, 1.0, "run")
            for rank, (org_id, service, as_of_date, cost) in enumerate(rows, start=1)
        ],
        TOP_SCHEMA,
    )
    write_dataset(
        top, settings, Zone.GOLD, "top_services_by_org_14d", partition_by=("as_of_date",)
    )


def test_elige_la_organizacion_con_mas_costo_a_la_fecha_de_corte(spark, settings):
    _write_top(
        spark,
        settings,
        [
            ("org_a", "compute", ANCHOR, 30.0),
            ("org_b", "compute", ANCHOR, 20.0),
            ("org_b", "storage", ANCHOR, 30.0),
            # Otro día no cuenta, aunque sume más.
            ("org_a", "compute", DAY_BEFORE, 100.0),
        ],
    )

    assert compute(spark, settings) == {
        "org_id": "org_b",
        "from_date": "2025-08-18",
        "to_date": "2025-08-31",
        "as_of_date": "2025-08-31",
        "n": 5,
    }


def test_empate_exacto_gana_el_org_id_menor(spark, settings):
    _write_top(
        spark,
        settings,
        [
            ("org_c", "compute", ANCHOR, 40.0),
            ("org_b", "compute", ANCHOR, 25.0),
            ("org_b", "storage", ANCHOR, 15.0),
        ],
    )

    assert compute(spark, settings)["org_id"] == "org_b"


def test_sin_filas_para_la_fecha_de_corte_da_error(spark, settings):
    _write_top(spark, settings, [("org_a", "compute", DAY_BEFORE, 10.0)])

    with pytest.raises(NoRowsForAnchorError, match="2025-08-31"):
        compute(spark, settings)


@pytest.fixture
def cli(monkeypatch, spark, settings):
    """Corre ``main`` sobre el lake del test, sin cerrar la SparkSession compartida."""
    monkeypatch.setattr(evidence_params, "load_env_file", lambda: None)
    monkeypatch.setattr(evidence_params, "load_settings", lambda: settings)
    monkeypatch.setattr(evidence_params, "get_spark", lambda app_name: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    return evidence_params.main


def test_main_imprime_los_parametros_como_json(spark, settings, cli, capsys):
    _write_top(spark, settings, [("org_a", "compute", ANCHOR, 10.0)])

    exit_code = cli()

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "org_id": "org_a",
        "from_date": "2025-08-18",
        "to_date": "2025-08-31",
        "as_of_date": "2025-08-31",
        "n": 5,
    }


def test_main_sin_filas_para_la_fecha_de_corte_sale_con_1(spark, settings, cli, capsys):
    _write_top(spark, settings, [("org_a", "compute", DAY_BEFORE, 10.0)])

    exit_code = cli()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "no tiene filas" in captured.err
