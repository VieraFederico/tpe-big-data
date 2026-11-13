from datetime import date, datetime, time

from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.schemas import CUSTOMERS_ORGS
from src.silver.master_data import conform_master, latest_snapshot
from src.silver.scd2 import ORGS_SCD2_COLUMNS, build_orgs_scd2

FIRST_SNAPSHOT = date(2025, 9, 1)
SECOND_SNAPSHOT = date(2025, 9, 2)
THIRD_SNAPSHOT = date(2025, 9, 3)

DEFAULT_ORG = {
    "org_id": "org_a",
    "org_name": "Nimbus Labs",
    "industry": "Education",
    "hq_region": "sa-east",
    "plan_tier": "standard",
    "is_enterprise": False,
    "signup_date": date(2025, 5, 26),
    "sales_rep": "rep_c",
    "lifecycle_stage": "active",
    "marketing_source": "partner",
    "nps_score": 10.0,
}

BRONZE_SCHEMA = StructType(
    [
        *CUSTOMERS_ORGS.schema.fields,
        StructField("ingest_ts", TimestampType()),
        StructField("ingest_date", DateType()),
        StructField("source_file", StringType()),
        StructField("run_id", StringType()),
    ]
)


def _snapshot_row(ingest_date, org_id, **overrides):
    fields = {**DEFAULT_ORG, "org_id": org_id, **overrides}
    assert set(fields) == set(DEFAULT_ORG)
    return (
        *fields.values(),
        datetime.combine(ingest_date, time(10)),
        ingest_date,
        f"file:/landing/{ingest_date}/customers_orgs.csv",
        "bronze-run",
    )


def _bronze(spark, *rows):
    return spark.createDataFrame(list(rows), BRONZE_SCHEMA)


def _versions(scd2, *columns):
    rows = scd2.orderBy("org_id", "valid_from").collect()
    versions = {}
    for row in rows:
        versions.setdefault(row.org_id, []).append(
            (row.valid_from, row.valid_to, row.is_current, *(row[name] for name in columns))
        )
    return versions


def test_cambio_de_plan_abre_una_version_y_silver_usa_el_ultimo_snapshot(spark):
    """Dado dos snapshots donde la organización A cambia plan_tier y B no.

    Cuando se arma Silver, entonces customers_orgs tiene el snapshot del 2025-09-02 y
    orgs_scd2 tiene dos versiones de A y una de B.
    """
    bronze = _bronze(
        spark,
        _snapshot_row(FIRST_SNAPSHOT, "A", plan_tier="standard"),
        _snapshot_row(FIRST_SNAPSHOT, "B", plan_tier="standard"),
        _snapshot_row(SECOND_SNAPSHOT, "A", plan_tier="enterprise"),
        _snapshot_row(SECOND_SNAPSHOT, "B", plan_tier="standard"),
    )

    current = conform_master("customers_orgs", latest_snapshot(bronze)).collect()
    versions = _versions(build_orgs_scd2(bronze), "plan_tier")

    assert {row.org_id: row.plan_tier for row in current} == {"A": "enterprise", "B": "standard"}
    assert {row.source_file for row in current} == {
        f"file:/landing/{SECOND_SNAPSHOT}/customers_orgs.csv"
    }
    assert versions == {
        "A": [
            (FIRST_SNAPSHOT, SECOND_SNAPSHOT, False, "standard"),
            (SECOND_SNAPSHOT, None, True, "enterprise"),
        ],
        "B": [(FIRST_SNAPSHOT, None, True, "standard")],
    }


def test_comparacion_tolera_nulos(spark):
    """Dado una organización con nps_score nulo en dos snapshots y otra que pasa de nulo a 5.

    Cuando se arma el historial, entonces la primera tiene una versión y la segunda dos.
    """
    bronze = _bronze(
        spark,
        _snapshot_row(FIRST_SNAPSHOT, "A", nps_score=None),
        _snapshot_row(SECOND_SNAPSHOT, "A", nps_score=None),
        _snapshot_row(FIRST_SNAPSHOT, "B", nps_score=None),
        _snapshot_row(SECOND_SNAPSHOT, "B", nps_score=5.0),
    )

    versions = _versions(build_orgs_scd2(bronze), "nps_score")

    assert versions == {
        "A": [(FIRST_SNAPSHOT, None, True, None)],
        "B": [
            (FIRST_SNAPSHOT, SECOND_SNAPSHOT, False, None),
            (SECOND_SNAPSHOT, None, True, 5.0),
        ],
    }


def test_volver_a_un_valor_anterior_abre_otra_version(spark):
    """Dado una organización que pasa de standard a enterprise y vuelve a standard.

    Cuando se arma el historial, entonces tiene tres versiones encadenadas.
    """
    bronze = _bronze(
        spark,
        _snapshot_row(FIRST_SNAPSHOT, "A", plan_tier="standard"),
        _snapshot_row(SECOND_SNAPSHOT, "A", plan_tier="enterprise"),
        _snapshot_row(THIRD_SNAPSHOT, "A", plan_tier="standard"),
    )

    versions = _versions(build_orgs_scd2(bronze), "plan_tier")

    assert versions == {
        "A": [
            (FIRST_SNAPSHOT, SECOND_SNAPSHOT, False, "standard"),
            (SECOND_SNAPSHOT, THIRD_SNAPSHOT, False, "enterprise"),
            (THIRD_SNAPSHOT, None, True, "standard"),
        ],
    }


def test_diferencias_que_la_conformacion_elimina_no_abren_version(spark):
    """Dado snapshots que solo difieren en espacios y mayúsculas de plan_tier o en un
    nps_score fuera de rango.

    Cuando se arma el historial, entonces hay una sola versión, con los valores conformados.
    """
    bronze = _bronze(
        spark,
        _snapshot_row(FIRST_SNAPSHOT, "A", plan_tier=" Standard ", nps_score=150.0),
        _snapshot_row(SECOND_SNAPSHOT, "A", plan_tier="standard", nps_score=200.0),
    )

    versions = _versions(build_orgs_scd2(bronze), "plan_tier", "nps_score")

    assert versions == {"A": [(FIRST_SNAPSHOT, None, True, "standard", None)]}


def test_organizacion_nueva_empieza_en_su_primer_snapshot(spark):
    """Dado una organización que aparece recién en el segundo snapshot, entonces su única
    versión empieza en esa fecha y es la vigente."""
    bronze = _bronze(
        spark,
        _snapshot_row(FIRST_SNAPSHOT, "A"),
        _snapshot_row(SECOND_SNAPSHOT, "A"),
        _snapshot_row(SECOND_SNAPSHOT, "B"),
    )

    versions = _versions(build_orgs_scd2(bronze))

    assert versions == {
        "A": [(FIRST_SNAPSHOT, None, True)],
        "B": [(SECOND_SNAPSHOT, None, True)],
    }


def test_historial_tiene_las_columnas_de_negocio_y_de_vigencia(spark):
    """Dado dos snapshots iguales de una organización.

    Cuando se arma el historial, entonces trae las columnas de negocio, valid_from, valid_to,
    is_current y la trazabilidad del primer snapshot de la versión.
    """
    bronze = _bronze(
        spark, _snapshot_row(FIRST_SNAPSHOT, "A"), _snapshot_row(SECOND_SNAPSHOT, "A")
    )

    scd2 = build_orgs_scd2(bronze)
    row = scd2.first()

    assert tuple(scd2.columns) == ORGS_SCD2_COLUMNS
    assert ORGS_SCD2_COLUMNS[: len(CUSTOMERS_ORGS.schema.fields)] == tuple(
        field.name for field in CUSTOMERS_ORGS.schema.fields
    )
    assert scd2.schema["valid_from"].dataType == DateType()
    assert scd2.schema["valid_to"].dataType == DateType()
    assert scd2.schema["is_current"].dataType == BooleanType()
    assert row.source_file == f"file:/landing/{FIRST_SNAPSHOT}/customers_orgs.csv"
    assert row.ingest_ts == datetime.combine(FIRST_SNAPSHOT, time(10))
