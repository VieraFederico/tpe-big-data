import pytest
from pyspark.sql.functions import col

from src.quality.rules import QUARANTINE_TS_COL, REASON_COL, RULE_ID_COL, Rule, any_null, split_by_rules


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

    assert {RULE_ID_COL, REASON_COL, QUARANTINE_TS_COL} <= set(quarantine.columns)


def test_any_null_sin_columnas_falla():
    with pytest.raises(ValueError, match="al menos una columna"):
        any_null(())


def test_any_null(spark):
    pairs = spark.createDataFrame(
        [("a", "b"), (None, "b"), ("a", None)], "first_value string, second_value string"
    )

    assert pairs.filter(any_null(("first_value", "second_value"))).count() == 2
    assert pairs.filter(any_null(["first_value"])).count() == 1
