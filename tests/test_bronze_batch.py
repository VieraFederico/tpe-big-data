from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.bronze.batch import ingest
from src.bronze.batch.ingest import ROW_ORDER_COL, ingest_source
from src.common.columns import INGEST_DATE, INGEST_TS, RUN_ID, SOURCE_FILE
from src.common.lake import dataset_storage_stats, read_dataset
from src.common.schemas import CORRUPT_RECORD_COL, SOURCES, Source
from src.quality.rules import RULE_ID_COL

SOURCE = Source(
    "things",
    "things.csv",
    StructType([StructField("id", StringType()), StructField("qty", IntegerType())]),
    ("id",),
)

# a: válida · b: duplicada · c: tipo inválido · última: clave nula
CSV = """id,qty
a,1
b,2
b,2
c,no_es_numero
,4
"""


def _ingest(spark, settings):
    Path(settings.zone_path("landing"), SOURCE.path).write_text(CSV, encoding="utf-8")
    ts = lit(datetime(2026, 11, 8, tzinfo=timezone.utc)).cast("timestamp")
    return ingest_source(spark, settings, SOURCE, ts)


def test_conteos(spark, settings):
    counts = _ingest(spark, settings)

    assert counts == {
        "read": 5, "quarantine": 2, "written": 2,
        "exact_duplicates": 1, "key_conflicts": 0, "duplicates": 1,
    }


def test_bronze_deduplica_y_agrega_columnas_tecnicas(spark, settings):
    _ingest(spark, settings)
    bronze = read_dataset(spark, settings, "bronze", SOURCE.name)

    assert sorted(r.id for r in bronze.collect()) == ["a", "b"]
    assert {INGEST_TS, INGEST_DATE, SOURCE_FILE, RUN_ID} <= set(bronze.columns)
    assert {row[RUN_ID] for row in bronze.collect()} == {settings.run_id}
    assert CORRUPT_RECORD_COL not in bronze.columns
    assert bronze.filter(col(SOURCE_FILE).endswith(SOURCE.path)).count() == 2


def test_quarantine_por_tipo_invalido_y_clave_nula(spark, settings):
    _ingest(spark, settings)
    quarantine = read_dataset(spark, settings, "quarantine", SOURCE.name)

    assert sorted(r[RULE_ID_COL] for r in quarantine.collect()) == ["BRZ_NULL_KEY", "BRZ_PARSE"]
    corrupt = quarantine.filter(col(RULE_ID_COL) == "BRZ_PARSE").first()
    assert corrupt[CORRUPT_RECORD_COL] == "c,no_es_numero"


def test_conflicto_de_clave_gana_la_ultima_aparicion(spark, settings):
    # x aparece 3 veces con distinto qty: debe quedar la última (3)
    csv = """id,qty
x,1
y,5
x,2
x,3
"""
    Path(settings.zone_path("landing"), SOURCE.path).write_text(csv, encoding="utf-8")
    ts = lit(datetime(2026, 11, 8, tzinfo=timezone.utc)).cast("timestamp")
    counts = ingest_source(spark, settings, SOURCE, ts)

    bronze = {r.id: r.qty for r in read_dataset(spark, settings, "bronze", SOURCE.name).collect()}
    assert bronze == {"x": 3, "y": 5}
    assert (counts["exact_duplicates"], counts["key_conflicts"]) == (0, 2)
    assert ROW_ORDER_COL not in read_dataset(spark, settings, "bronze", SOURCE.name).columns


def test_reejecutar_no_duplica(spark, settings):
    _ingest(spark, settings)
    _ingest(spark, settings)

    assert read_dataset(spark, settings, "bronze", SOURCE.name).count() == 2


def test_storage_stats_reporta_ruta_archivos_y_tamano(spark, settings):
    _ingest(spark, settings)
    stats = dataset_storage_stats(settings, "bronze", SOURCE.name)

    assert stats.path.endswith(f"bronze/{SOURCE.name}")
    assert stats.partitions == 1  # una sola ingest_date
    assert stats.files >= 1 and stats.bytes > 0


def test_storage_stats_inexistente_da_ceros(settings):
    stats = dataset_storage_stats(settings, "bronze", "no_existe")

    assert (stats.partitions, stats.files, stats.bytes) == (0, 0, 0)


def test_run_ingesta_todas_las_fuentes_de_landing(spark, settings):
    for source in SOURCES.values():
        header = ",".join(field.name for field in source.schema.fields)
        Path(settings.zone_path("landing"), source.path).write_text(header + "\n", encoding="utf-8")

    results = ingest.run(spark, settings)

    assert set(results) == set(SOURCES)
    assert all(counts["read"] == 0 for counts in results.values())
