import json
from datetime import date, datetime

import pytest
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, DoubleType, StringType

from src.quality.catalog import silver_event_flags, silver_event_rules
from src.quality.flags import DQ_FLAGS_COL, Flag, with_dq_flags
from src.quality.rules import RAW_COL, REASON_COL, RULE_ID_COL
from src.silver.events import CONFORMED_COLUMNS, conform_events

BRONZE_EVENTS_SCHEMA = (
    "event_id string, timestamp string, org_id string, resource_id string, service string, "
    "region string, metric string, value string, unit string, cost_usd_increment double, "
    "schema_version int, carbon_kg double, genai_tokens bigint, ingest_ts timestamp, "
    "ingest_date date, source_file string, run_id string, event_ts timestamp, event_date date"
)

BRONZE_EVENTS_SCHEMA_COLUMNS = [
    definition.split()[0] for definition in BRONZE_EVENTS_SCHEMA.split(", ")
]

FIRST_INGEST = datetime(2025, 8, 2, 10, 0, 0)
SECOND_INGEST = datetime(2025, 8, 3, 10, 0, 0)


def _event(event_id="e1", **overrides):
    fields = {
        "event_id": event_id,
        "timestamp": "2025-08-01T10:00:00Z",
        "org_id": "org_1",
        "resource_id": "res_1",
        "service": "compute",
        "region": "us-east",
        "metric": "cpu_hours",
        "value": "2.5",
        "unit": "hours",
        "cost_usd_increment": 0.5,
        "schema_version": 1,
        "carbon_kg": None,
        "genai_tokens": None,
        "ingest_ts": FIRST_INGEST,
        "ingest_date": FIRST_INGEST.date(),
        "source_file": "file:/landing/events_part_0000.jsonl",
        "run_id": "bronze-run",
        "event_ts": datetime(2025, 8, 1, 10, 0, 0),
        "event_date": date(2025, 8, 1),
        **overrides,
    }
    return tuple(fields.values())


def _v2_event(event_id, **overrides):
    return _event(event_id, schema_version=2, carbon_kg=0.3, **overrides)


def _bronze(spark, *events):
    return spark.createDataFrame(list(events), BRONZE_EVENTS_SCHEMA)


def _valid_by_id(result):
    return {row.event_id: row for row in result.valid.collect()}


def _rule_by_id(result):
    return {row.event_id: row[RULE_ID_COL] for row in result.quarantine.collect()}


# --- Deduplicación ---------------------------------------------------------------------


def test_dedupe_se_queda_con_el_menor_ingest_ts(spark):
    """Dado dos eventos con el mismo event_id y distinto ingest_ts.

    Cuando se conforman, entonces queda el de menor ingest_ts, el otro se cuenta como
    duplicado y no va a cuarentena.
    """
    bronze = _bronze(
        spark,
        _event("e1", ingest_ts=SECOND_INGEST, cost_usd_increment=1.0),
        _event("e1", ingest_ts=FIRST_INGEST, cost_usd_increment=2.0),
    )

    result = conform_events(bronze)

    # El costo hace que el JSON del descartado sea menor: gana igual el menor ingest_ts.
    assert [row.cost_usd_increment for row in result.valid.collect()] == [2.0]
    assert result.duplicates.count() == 1
    assert result.duplicates.first().cost_usd_increment == 1.0
    assert result.quarantine.count() == 0


def test_dedupe_desempata_por_source_file(spark):
    """Dado dos eventos con el mismo event_id e ingest_ts y distinto source_file.

    Cuando se conforman, entonces queda el de menor source_file.
    """
    bronze = _bronze(
        spark,
        _event("e1", source_file="b.jsonl", cost_usd_increment=1.0),
        _event("e1", source_file="a.jsonl", cost_usd_increment=2.0),
    )

    result = conform_events(bronze)

    assert [row.source_file for row in result.valid.collect()] == ["a.jsonl"]
    assert result.duplicates.count() == 1


def test_dedupe_desempata_por_json_del_registro_sin_mandar_a_cuarentena(spark):
    """Dado dos eventos iguales en event_id, ingest_ts y source_file: uno válido y otro
    con costo -5.

    Cuando se conforman dos veces y en distinto orden, entonces siempre queda el de menor
    JSON (el de costo -5), va a cuarentena por su costo y el descartado no aparece en
    cuarentena.
    """
    valid_event = _event("e1", cost_usd_increment=0.5)
    invalid_event = _event("e1", cost_usd_increment=-5.0)
    # En el JSON, "cost_usd_increment":-5.0 es menor que "cost_usd_increment":0.5.
    for events in ((valid_event, invalid_event), (invalid_event, valid_event)):
        result = conform_events(_bronze(spark, *events))

        assert result.valid.count() == 0
        assert [row.cost_usd_increment for row in result.quarantine.collect()] == [-5.0]
        assert [row.cost_usd_increment for row in result.duplicates.collect()] == [0.5]


def test_dedupe_no_agrupa_eventos_sin_event_id(spark):
    """Dado dos eventos con event_id nulo.

    Cuando se conforman, entonces no son duplicados: ambos van a cuarentena.
    """
    result = conform_events(_bronze(spark, _event(None), _event(None)))

    assert result.duplicates.count() == 0
    assert result.quarantine.count() == 2


# --- Reglas que mandan a cuarentena -----------------------------------------------------


def test_cada_regla_manda_a_cuarentena_con_su_rule_id(spark):
    """Dado un evento que viola cada regla de Silver.

    Cuando se conforman, entonces cada uno está en cuarentena con su rule_id y motivo, y
    ninguno queda entre los válidos.
    """
    bronze = _bronze(
        spark,
        _event(None),
        _event("dimension", org_id=None),
        _event("version", schema_version=3),
        _event("value", value="abc"),
        _event("unit", metric="gpu_hours", unit=None),
        _event("cost", cost_usd_increment=-0.02),
    )

    result = conform_events(bronze)

    assert _rule_by_id(result) == {
        None: "SLV_NULL_EVENT_ID",
        "dimension": "SLV_NULL_DIMENSION",
        "version": "SLV_SCHEMA_VERSION",
        "value": "SLV_VALUE_NOT_NUMERIC",
        "unit": "SLV_UNIT_UNKNOWN",
        "cost": "SLV_COST_INVALID",
    }
    assert all(row[REASON_COL] for row in result.quarantine.collect())
    assert result.valid.count() == 0


@pytest.mark.parametrize("column", ["org_id", "service", "metric"])
def test_dimension_nula_va_a_cuarentena(spark, column):
    """Dado un evento con una dimensión nula, entonces va a cuarentena por dimensión."""
    result = conform_events(_bronze(spark, _event("e1", **{column: None})))

    assert _rule_by_id(result) == {"e1": "SLV_NULL_DIMENSION"}


def test_la_primera_regla_violada_gana(spark):
    """Dado un evento con event_id nulo y costo -5.

    Cuando se conforma, entonces está una sola vez en cuarentena con SLV_NULL_EVENT_ID.
    """
    result = conform_events(_bronze(spark, _event(None, cost_usd_increment=-5.0)))

    assert [row[RULE_ID_COL] for row in result.quarantine.collect()] == ["SLV_NULL_EVENT_ID"]


def test_las_reglas_estan_en_el_orden_del_catalogo(spark):
    """Dado el catálogo de reglas de eventos, entonces sigue el orden de evaluación."""
    assert [rule.rule_id for rule in silver_event_rules()] == [
        "SLV_NULL_EVENT_ID",
        "SLV_NULL_DIMENSION",
        "SLV_SCHEMA_VERSION",
        "SLV_VALUE_NOT_NUMERIC",
        "SLV_UNIT_UNKNOWN",
        "SLV_COST_INVALID",
    ]
    assert [flag.flag_id for flag in silver_event_flags()] == [
        "VALUE_NULL",
        "UNIT_IMPUTED",
        "COST_NEGATIVE_SMALL",
    ]


def test_raw_de_la_cuarentena_es_el_registro_de_bronze_sin_normalizar(spark):
    """Dado un evento con service " Compute " y costo -5.

    Cuando se conforma, entonces la fila en cuarentena trae service normalizado y _raw
    vuelve a dar las columnas originales de Bronze.
    """
    result = conform_events(
        _bronze(spark, _event("e1", service=" Compute ", cost_usd_increment=-5.0))
    )

    row = result.quarantine.first()
    raw = json.loads(row[RAW_COL])
    assert row.service == "compute"
    assert raw["service"] == " Compute "
    assert raw["cost_usd_increment"] == -5.0
    assert set(raw) == set(BRONZE_EVENTS_SCHEMA_COLUMNS)


# --- value -----------------------------------------------------------------------------


def test_value_numerico_se_convierte_sin_marca(spark):
    """Dado eventos con value "120.0", "0" y "-3.5".

    Cuando se conforman, entonces value_numeric es 120.0, 0.0 y -3.5, sin marcas.
    """
    bronze = _bronze(
        spark, _event("a", value="120.0"), _event("b", value="0"), _event("c", value="-3.5")
    )

    valid = _valid_by_id(conform_events(bronze))

    assert {event_id: row.value_numeric for event_id, row in valid.items()} == {
        "a": 120.0,
        "b": 0.0,
        "c": -3.5,
    }
    assert all(row[DQ_FLAGS_COL] == [] for row in valid.values())


def test_value_no_numerico_va_a_cuarentena(spark):
    """Dado un evento con value "abc", entonces va a cuarentena con SLV_VALUE_NOT_NUMERIC."""
    result = conform_events(_bronze(spark, _event("e1", value="abc")))

    assert _rule_by_id(result) == {"e1": "SLV_VALUE_NOT_NUMERIC"}


def test_value_nulo_queda_con_marca(spark):
    """Dado un evento con value nulo.

    Cuando se conforma, entonces queda en Silver con value_numeric nulo y VALUE_NULL.
    """
    row = conform_events(_bronze(spark, _event("e1", value=None))).valid.first()

    assert row.value_numeric is None
    assert row[DQ_FLAGS_COL] == ["VALUE_NULL"]


# --- unit ------------------------------------------------------------------------------


def test_unit_nulo_se_imputa_desde_metric(spark):
    """Dado eventos con unit nulo, value presente y cada métrica conocida.

    Cuando se conforman, entonces unit toma la unidad de la métrica y tiene UNIT_IMPUTED.
    """
    bronze = _bronze(
        spark,
        _event("a", metric="requests", unit=None),
        _event("b", metric="cpu_hours", unit=None),
        _event("c", metric="storage_gb_hours", unit=None),
    )

    valid = _valid_by_id(conform_events(bronze))

    assert {event_id: row.unit for event_id, row in valid.items()} == {
        "a": "count",
        "b": "hours",
        "c": "gb_hours",
    }
    assert all(row[DQ_FLAGS_COL] == ["UNIT_IMPUTED"] for row in valid.values())


def test_unit_nulo_con_metrica_desconocida_va_a_cuarentena(spark):
    """Dado unit nulo, value presente y metric "gpu_hours", entonces va a cuarentena."""
    result = conform_events(_bronze(spark, _event("e1", metric="gpu_hours", unit=None)))

    assert _rule_by_id(result) == {"e1": "SLV_UNIT_UNKNOWN"}


def test_unit_nulo_con_value_nulo_no_se_imputa(spark):
    """Dado unit y value nulos, entonces queda sin imputar y solo con VALUE_NULL."""
    row = conform_events(_bronze(spark, _event("e1", value=None, unit=None))).valid.first()

    assert row.unit is None
    assert row[DQ_FLAGS_COL] == ["VALUE_NULL"]


# --- costo -----------------------------------------------------------------------------


def test_regla_hibrida_de_costo(spark):
    """Dado eventos con costo -0.02, -0.01, -0.005 y 0.

    Cuando se conforman, entonces -0.02 va a cuarentena con SLV_COST_INVALID, -0.01 y
    -0.005 quedan con su costo y COST_NEGATIVE_SMALL, y 0 queda sin marcas.
    """
    bronze = _bronze(
        spark,
        _event("a", cost_usd_increment=-0.02),
        _event("b", cost_usd_increment=-0.01),
        _event("c", cost_usd_increment=-0.005),
        _event("d", cost_usd_increment=0.0),
    )

    result = conform_events(bronze)
    valid = _valid_by_id(result)

    assert _rule_by_id(result) == {"a": "SLV_COST_INVALID"}
    flags_by_id = {
        event_id: (row.cost_usd_increment, row[DQ_FLAGS_COL]) for event_id, row in valid.items()
    }
    assert flags_by_id == {
        "b": (-0.01, ["COST_NEGATIVE_SMALL"]),
        "c": (-0.005, ["COST_NEGATIVE_SMALL"]),
        "d": (0.0, []),
    }


def test_costo_nulo_va_a_cuarentena(spark):
    """Dado un evento con costo nulo, entonces va a cuarentena con SLV_COST_INVALID."""
    result = conform_events(_bronze(spark, _event("e1", cost_usd_increment=None)))

    assert _rule_by_id(result) == {"e1": "SLV_COST_INVALID"}


# --- columnas por métrica y versiones ---------------------------------------------------


def test_columnas_por_metrica(spark):
    """Dado eventos cpu_hours con value "2.5" y con value nulo.

    Cuando se conforman, entonces cpu_hours es 2.5 (o null) y las otras dos son 0.0.
    """
    bronze = _bronze(spark, _event("a", value="2.5"), _event("b", value=None))

    valid = _valid_by_id(conform_events(bronze))

    metrics = {
        event_id: (row.requests, row.cpu_hours, row.storage_gb_hours)
        for event_id, row in valid.items()
    }
    assert metrics == {"a": (0.0, 2.5, 0.0), "b": (0.0, None, 0.0)}


def test_campos_v2_en_eventos_v1_quedan_nulos(spark):
    """Dado un evento v1 sin carbon_kg ni genai_tokens, entonces ambos quedan nulos."""
    row = conform_events(_bronze(spark, _event("e1"))).valid.first()

    assert row.carbon_kg is None
    assert row.genai_tokens is None


def test_eventos_v2_coherentes_son_validos(spark):
    """Dado eventos v2 con carbon_kg, uno de genai con tokens, entonces son válidos."""
    bronze = _bronze(
        spark,
        _v2_event("a"),
        _v2_event("b", service="genai", metric="requests", unit="count", genai_tokens=10),
    )

    valid = _valid_by_id(conform_events(bronze))

    assert valid["a"].carbon_kg == 0.3
    assert valid["b"].genai_tokens == 10


@pytest.mark.parametrize(
    "overrides",
    [
        {"carbon_kg": 0.3},
        {"genai_tokens": 10, "service": "genai"},
        {"schema_version": 3},
        {"schema_version": None},
        {"schema_version": 2},
        {"schema_version": 2, "carbon_kg": 0.3, "genai_tokens": 10},
    ],
    ids=[
        "v1_con_carbon",
        "v1_con_tokens",
        "version_3",
        "version_nula",
        "v2_sin_carbon",
        "tokens_fuera_de_genai",
    ],
)
def test_version_incoherente_va_a_cuarentena(spark, overrides):
    """Dado un evento con la versión o los campos v2 incoherentes, entonces va a
    cuarentena con SLV_SCHEMA_VERSION."""
    result = conform_events(_bronze(spark, _event("e1", **overrides)))

    assert _rule_by_id(result) == {"e1": "SLV_SCHEMA_VERSION"}


# --- normalización y forma del resultado ------------------------------------------------


def test_categoricas_se_normalizan(spark):
    """Dado service " Compute ", region " US-East", metric "CPU_Hours " y unit "Hours".

    Cuando se conforma, entonces quedan sin espacios y en minúsculas.
    """
    bronze = _bronze(
        spark,
        _event("e1", service=" Compute ", region=" US-East", metric="CPU_Hours ", unit="Hours"),
    )

    row = conform_events(bronze).valid.first()

    assert (row.service, row.region, row.metric, row.unit) == (
        "compute",
        "us-east",
        "cpu_hours",
        "hours",
    )
    assert row.cpu_hours == 2.5


def test_validos_tienen_las_columnas_conformadas_y_flags_vacios(spark):
    """Dado tres eventos v1 y tres v2 válidos.

    Cuando se conforman, entonces quedan los seis con las columnas conformadas y dq_flags
    vacío (no nulo).
    """
    bronze = _bronze(
        spark,
        *[_event(f"v1_{index}") for index in range(3)],
        *[_v2_event(f"v2_{index}") for index in range(3)],
    )

    result = conform_events(bronze)
    rows = result.valid.collect()

    assert tuple(result.valid.columns) == CONFORMED_COLUMNS
    assert result.valid.schema[DQ_FLAGS_COL].dataType == ArrayType(StringType())
    assert result.valid.schema["value_numeric"].dataType == DoubleType()
    assert len(rows) == 6
    assert all(row[DQ_FLAGS_COL] == [] for row in rows)
    assert result.quarantine.count() == 0
    assert result.duplicates.count() == 0


# --- with_dq_flags ---------------------------------------------------------------------


def test_dq_flags_ordenados_sin_repetidos_y_vacios(spark):
    """Dado marcas declaradas fuera de orden y una repetida.

    Cuando se aplican, entonces dq_flags queda ordenado, sin repetidos y vacío (no nulo)
    si no aplica ninguna; una condición nula no aplica.
    """
    rows = spark.createDataFrame([(1,), (-1,), (None,)], "amount int")
    flags = [
        Flag("Z_NEGATIVE", "negativo", col("amount") < 0),
        Flag("A_NEGATIVE", "negativo", col("amount") < 0),
        Flag("Z_NEGATIVE", "repetida", col("amount") < 0),
    ]

    flagged = {row.amount: row[DQ_FLAGS_COL] for row in with_dq_flags(rows, flags).collect()}

    assert flagged == {1: [], -1: ["A_NEGATIVE", "Z_NEGATIVE"], None: []}


def test_dq_flags_se_suman_a_los_existentes(spark):
    """Dado filas que ya tienen dq_flags, entonces las marcas nuevas se suman ordenadas."""
    rows = spark.createDataFrame(
        [(-1, ["M_OLD"]), (1, None)], "amount int, dq_flags array<string>"
    )
    flags = [Flag("A_NEGATIVE", "negativo", col("amount") < 0)]

    flagged = {row.amount: row[DQ_FLAGS_COL] for row in with_dq_flags(rows, flags).collect()}

    assert flagged == {-1: ["A_NEGATIVE", "M_OLD"], 1: []}


def test_dq_flags_sin_marcas_es_un_array_vacio(spark):
    """Dado una lista de marcas vacía, entonces dq_flags es un array<string> vacío."""
    flagged = with_dq_flags(spark.createDataFrame([(1,)], "amount int"), [])

    assert flagged.schema[DQ_FLAGS_COL].dataType == ArrayType(StringType())
    assert flagged.first()[DQ_FLAGS_COL] == []
