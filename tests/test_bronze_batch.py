import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.bronze.batch import ingest
from src.bronze.batch.ingest import ROW_ORDER_COL, ingest_source
from src.common.columns import INGEST_DATE, INGEST_TS, RUN_ID, SOURCE_FILE
from src.common.lake import dataset_storage_stats, quarantine_dataset, read_dataset
from src.common.manifests import read_manifest
from src.common.schemas import CORRUPT_RECORD_COL, SOURCES, Source
from src.config import Zone
from src.quality.rules import RAW_COL, REASON_COL, RULE_ID_COL

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


def _ingest(spark, settings, csv_content=CSV):
    Path(settings.zone_path(Zone.LANDING), SOURCE.path).write_text(csv_content, encoding="utf-8")
    ingest_ts = lit(datetime(2026, 11, 8, tzinfo=UTC)).cast("timestamp")
    return ingest_source(spark, settings, SOURCE, ingest_ts)


def test_conteos(spark, settings):
    counts = _ingest(spark, settings)

    assert counts == {
        "read": 5,
        "quarantine": 2,
        "written": 2,
        "exact_duplicates": 1,
        "key_conflicts": 0,
        "duplicates": 1,
    }


def test_bronze_deduplica_y_agrega_columnas_tecnicas(spark, settings):
    _ingest(spark, settings)
    bronze = read_dataset(spark, settings, Zone.BRONZE, SOURCE.name)

    assert sorted(row.id for row in bronze.collect()) == ["a", "b"]
    assert {INGEST_TS, INGEST_DATE, SOURCE_FILE, RUN_ID} <= set(bronze.columns)
    assert {row[RUN_ID] for row in bronze.collect()} == {settings.run_id}
    assert CORRUPT_RECORD_COL not in bronze.columns
    assert bronze.filter(col(SOURCE_FILE).endswith(SOURCE.path)).count() == 2


def _quarantine(spark, settings):
    return read_dataset(
        spark, settings, Zone.QUARANTINE, quarantine_dataset(Zone.BRONZE, SOURCE.name)
    )


def test_quarantine_por_tipo_invalido_y_clave_nula(spark, settings):
    _ingest(spark, settings)
    quarantine = _quarantine(spark, settings)

    assert sorted(row[RULE_ID_COL] for row in quarantine.collect()) == [
        "BRZ_NULL_KEY",
        "BRZ_PARSE",
    ]
    corrupt = quarantine.filter(col(RULE_ID_COL) == "BRZ_PARSE").first()
    assert corrupt[CORRUPT_RECORD_COL] == "c,no_es_numero"


def test_quarantine_guarda_motivo_y_registro_original(spark, settings):
    _ingest(spark, settings)
    row_by_rule = {row[RULE_ID_COL]: row for row in _quarantine(spark, settings).collect()}

    corrupt = row_by_rule["BRZ_PARSE"]
    assert corrupt[RAW_COL] == corrupt[CORRUPT_RECORD_COL] == "c,no_es_numero"
    assert corrupt[REASON_COL]

    # La fila parseó pero tiene clave nula: el original es el JSON de sus columnas.
    null_key = row_by_rule["BRZ_NULL_KEY"]
    original = json.loads(null_key[RAW_COL])
    assert original["id"] is None and original["qty"] == 4
    assert original[SOURCE_FILE].endswith(SOURCE.path)
    assert null_key[REASON_COL]


def test_quarantine_queda_bajo_la_carpeta_de_su_capa(spark, settings):
    _ingest(spark, settings)
    quarantine_root = Path(settings.zone_path(Zone.QUARANTINE))

    dataset_folder = quarantine_root / "bronze" / SOURCE.name
    partitions = [path.name for path in dataset_folder.iterdir() if path.is_dir()]
    assert partitions == ["ingest_date=2026-11-08"]
    assert not (quarantine_root / SOURCE.name).exists()


def test_conflicto_de_clave_gana_la_ultima_aparicion(spark, settings):
    # x aparece 3 veces con distinto qty: debe quedar la última (3)
    counts = _ingest(
        spark,
        settings,
        csv_content="""id,qty
x,1
y,5
x,2
x,3
""",
    )

    bronze = read_dataset(spark, settings, Zone.BRONZE, SOURCE.name)
    qty_by_id = {row.id: row.qty for row in bronze.collect()}
    assert qty_by_id == {"x": 3, "y": 5}
    assert (counts["exact_duplicates"], counts["key_conflicts"]) == (0, 2)
    assert ROW_ORDER_COL not in bronze.columns


def test_reejecutar_no_duplica(spark, settings):
    _ingest(spark, settings)
    _ingest(spark, settings)

    assert read_dataset(spark, settings, Zone.BRONZE, SOURCE.name).count() == 2


def test_storage_stats_reporta_ruta_archivos_y_tamano(spark, settings):
    _ingest(spark, settings)
    stats = dataset_storage_stats(settings, Zone.BRONZE, SOURCE.name)

    assert stats.path.endswith(f"bronze/{SOURCE.name}")
    assert stats.partitions == 1  # una sola ingest_date
    assert stats.files >= 1 and stats.bytes > 0


def test_un_archivo_por_particion_aunque_haya_varias_tareas(spark, settings):
    # Con 8 particiones de shuffle, el dedupe reparte las filas en varias tareas;
    # sin el repartition de write_dataset cada tarea escribiría su propio archivo.
    original_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    try:
        _ingest(spark, settings)
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", original_shuffle_partitions)

    stats = dataset_storage_stats(settings, Zone.BRONZE, SOURCE.name)
    assert (stats.partitions, stats.files) == (1, 1)


def test_storage_stats_inexistente_da_ceros(settings):
    stats = dataset_storage_stats(settings, Zone.BRONZE, "no_existe")

    assert (stats.partitions, stats.files, stats.bytes) == (0, 0, 0)


def test_run_ingesta_todas_las_fuentes_de_landing(spark, settings):
    for source in SOURCES.values():
        header = ",".join(field.name for field in source.schema.fields)
        Path(settings.zone_path(Zone.LANDING), source.path).write_text(
            header + "\n", encoding="utf-8"
        )

    results = ingest.run(spark, settings)

    assert set(results) == set(SOURCES)
    assert all(counts["read"] == 0 for counts in results.values())
    manifest = read_manifest(settings, "bronze_batch")
    assert manifest is not None
    assert manifest["datasets"] == results


def test_run_escribe_el_manifiesto_con_los_conteos_que_devuelve(spark, settings, monkeypatch):
    monkeypatch.setattr(ingest, "SOURCES", {SOURCE.name: SOURCE})
    Path(settings.zone_path(Zone.LANDING), SOURCE.path).write_text(CSV, encoding="utf-8")

    results = ingest.run(spark, settings)

    manifest = read_manifest(settings, "bronze_batch")
    assert manifest is not None
    assert manifest["run_id"] == settings.run_id
    assert manifest["datasets"] == results
    assert manifest["datasets"][SOURCE.name]["quarantine"] == 2
    bronze = read_dataset(spark, settings, Zone.BRONZE, SOURCE.name)
    assert {row[INGEST_DATE].isoformat() for row in bronze.collect()} == {manifest["ingest_date"]}
