import json

import pytest
from pyspark.sql.functions import col

from src.common.schemas import CORRUPT_RECORD_COL
from src.quality.rules import (
    QUARANTINE_TS_COL,
    RAW_COL,
    REASON_COL,
    RULE_ID_COL,
    Rule,
    any_null,
    split_by_rules,
)


def _sample_df(spark):
    return spark.createDataFrame(
        [("a", 1), ("b", -5), (None, 3), (None, -1), ("e", None)],
        "id string, amount int",
    )


def _rules():
    # Las Column necesitan una sesión activa: se arman adentro de cada test.
    return [
        Rule("NULL_ID", "id nulo", col("id").isNull()),
        Rule("NEG", "amount negativo", col("amount") < 0),
    ]


def test_split_separa_validos_e_invalidos(spark):
    valid, quarantine = split_by_rules(_sample_df(spark), _rules())

    assert sorted(row.id for row in valid.collect()) == ["a", "e"]
    assert quarantine.count() == 3


def test_primera_regla_que_falla_gana(spark):
    _, quarantine = split_by_rules(_sample_df(spark), _rules())

    rule_by_amount = {row.amount: row[RULE_ID_COL] for row in quarantine.collect()}
    assert rule_by_amount == {-5: "NEG", 3: "NULL_ID", -1: "NULL_ID"}


def test_violacion_nula_no_manda_a_quarantine(spark):
    # amount nulo hace que "amount < 0" sea null: no cuenta como violación.
    valid, _ = split_by_rules(_sample_df(spark), _rules())

    assert "e" in [row.id for row in valid.collect()]


def test_quarantine_trae_motivo_y_timestamp(spark):
    valid, quarantine = split_by_rules(_sample_df(spark), _rules())

    assert RULE_ID_COL not in valid.columns
    negative_row = quarantine.filter(col(RULE_ID_COL) == "NEG").first()
    assert negative_row[REASON_COL] == "amount negativo"
    assert negative_row[QUARANTINE_TS_COL] is not None


def test_sin_reglas_todo_es_valido(spark):
    sample = _sample_df(spark)
    valid, quarantine = split_by_rules(sample, [])

    assert valid.count() == sample.count()
    assert quarantine.count() == 0


def test_sin_reglas_la_quarantine_mantiene_su_esquema(spark):
    _, quarantine = split_by_rules(_sample_df(spark), [])

    assert {RULE_ID_COL, REASON_COL, RAW_COL, QUARANTINE_TS_COL} <= set(quarantine.columns)


def test_columnas_de_cuarentena_usan_los_nombres_del_contrato():
    contract = (RULE_ID_COL, REASON_COL, RAW_COL, QUARANTINE_TS_COL)

    assert contract == ("_rule_id", "_quarantine_reason", "_raw", "_quarantine_ts")


def test_raw_de_fila_que_parseo_es_el_json_de_sus_columnas(spark):
    _, quarantine = split_by_rules(_sample_df(spark), _rules())

    raw_by_amount = {row.amount: json.loads(row[RAW_COL]) for row in quarantine.collect()}
    # Las columnas nulas quedan explícitas en el JSON.
    assert raw_by_amount[3] == {"id": None, "amount": 3}
    assert raw_by_amount[-5] == {"id": "b", "amount": -5}


def test_raw_de_fila_que_no_parseo_es_el_registro_corrupto(spark):
    rows = spark.createDataFrame(
        [(None, None, "a;;roto"), (None, 7, None)],
        f"id string, amount int, {CORRUPT_RECORD_COL} string",
    )
    rules = [
        Rule("PARSE", "no parsea", col(CORRUPT_RECORD_COL).isNotNull()),
        Rule("NULL_ID", "id nulo", col("id").isNull()),
    ]

    _, quarantine = split_by_rules(rows, rules)

    raw_by_rule = {row[RULE_ID_COL]: row[RAW_COL] for row in quarantine.collect()}
    assert raw_by_rule["PARSE"] == "a;;roto"
    # Si la fila parseó, el registro corrupto (null) no se incluye en el JSON.
    assert json.loads(raw_by_rule["NULL_ID"]) == {"id": None, "amount": 7}


def test_any_null_sin_columnas_falla():
    with pytest.raises(ValueError, match="al menos una columna"):
        any_null(())


def test_any_null(spark):
    pairs = spark.createDataFrame(
        [("a", "b"), (None, "b"), ("a", None)], "first_value string, second_value string"
    )

    assert pairs.filter(any_null(("first_value", "second_value"))).count() == 2
    assert pairs.filter(any_null(["first_value"])).count() == 1
