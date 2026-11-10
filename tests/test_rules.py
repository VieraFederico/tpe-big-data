from pyspark.sql.functions import col

from src.quality.rules import QUARANTINE_TS_COL, REASON_COL, RULE_ID_COL, Rule, any_null, split_by_rules


def _df(spark):
    return spark.createDataFrame(
        [("a", 1), ("b", -5), (None, 3), (None, -1), ("e", None)],
        "id string, n int",
    )


def _rules():
    # Las Column necesitan una sesión activa: se arman adentro de cada test.
    return [
        Rule("NULL_ID", "id nulo", col("id").isNull()),
        Rule("NEG", "n negativo", col("n") < 0),
    ]


def test_split_separa_validos_e_invalidos(spark):
    valid, quarantine = split_by_rules(_df(spark), _rules())

    assert sorted(r.id for r in valid.collect()) == ["a", "e"]
    assert quarantine.count() == 3


def test_primera_regla_que_falla_gana(spark):
    _, quarantine = split_by_rules(_df(spark), _rules())

    by_n = {r.n: r[RULE_ID_COL] for r in quarantine.collect()}
    assert by_n == {-5: "NEG", 3: "NULL_ID", -1: "NULL_ID"}


def test_violacion_nula_no_manda_a_quarantine(spark):
    # n nulo hace que "n < 0" sea null: no cuenta como violación.
    valid, _ = split_by_rules(_df(spark), _rules())

    assert "e" in [r.id for r in valid.collect()]


def test_quarantine_trae_motivo_y_timestamp(spark):
    valid, quarantine = split_by_rules(_df(spark), _rules())

    assert RULE_ID_COL not in valid.columns
    row = quarantine.filter(col(RULE_ID_COL) == "NEG").first()
    assert row[REASON_COL] == "n negativo"
    assert row[QUARANTINE_TS_COL] is not None


def test_sin_reglas_todo_es_valido(spark):
    df = _df(spark)
    valid, quarantine = split_by_rules(df, [])

    assert valid.count() == df.count()
    assert quarantine.count() == 0


def test_any_null(spark):
    df = spark.createDataFrame([("a", "b"), (None, "b"), ("a", None)], "x string, y string")

    assert df.filter(any_null(("x", "y"))).count() == 2
    assert df.filter(any_null(["x"])).count() == 1
