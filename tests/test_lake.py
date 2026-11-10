from src.common.lake import rebalance_by


def _analyzed_plan(df, capsys):
    df.explain(extended=True)
    plan = capsys.readouterr().out
    return plan.split("== Analyzed Logical Plan ==")[1].split("== Optimized Logical Plan ==")[0]


def _sample(spark):
    return spark.createDataFrame(
        [("2025-01-01", "a"), ("2025-01-02", "b")], "`ingest-date` string, id string"
    )


def test_rebalance_por_columnas_aplica_el_hint_con_esas_columnas(spark, capsys):
    rebalanced = rebalance_by(_sample(spark), ("ingest-date",))

    assert "RebalancePartitions [ingest-date#" in _analyzed_plan(rebalanced, capsys)


def test_rebalance_sin_columnas_aplica_el_hint_sin_clave(spark, capsys):
    rebalanced = rebalance_by(_sample(spark), ())

    plan = _analyzed_plan(rebalanced, capsys)
    assert "RebalancePartitions" in plan
    assert "RebalancePartitions [" not in plan


def test_rebalance_no_cambia_los_datos(spark):
    sample = _sample(spark)

    rebalanced = rebalance_by(sample, ("ingest-date",))

    assert rebalanced.columns == sample.columns
    assert sorted(rebalanced.collect()) == sorted(sample.collect())
