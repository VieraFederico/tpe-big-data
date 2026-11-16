"""Chequeo de idempotencia: cuenta filas del lake y del serving, vuelve a correr todo y compara.

Se corre con ``python -m src.evidence.idempotence``. Deja ``idempotencia.csv`` (``objeto``,
``antes``, ``despues``, ``igual``) e ``idempotencia.md`` en ``evidence/entrega-2/`` y sale con
código 1 si algún conteo cambió.

De los maestros de Bronze se cuenta solo el último ``ingest_date``: correr otro día agrega un
snapshot nuevo a propósito, y lo que tiene que dar igual es el contenido de cada snapshot.
"""

import csv
import logging
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cassandra.query import SimpleStatement
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max

from src.common.columns import INGEST_DATE
from src.common.logging_setup import setup_logging
from src.common.schemas import SOURCES
from src.common.spark import get_spark
from src.config import Settings, Zone, load_env_file, load_settings, new_run_id
from src.evidence.inventory import DELIVERY_DIR, LakeDataset, lake_datasets, read_if_present
from src.evidence.queries import QUERIES_DIR
from src.gold.marts import AS_OF_DATE, TOP_SERVICES_BY_ORG_14D
from src.pipeline import STEPS, run_steps
from src.serving.cassandra_client import read_cql_statements, session_scope
from src.serving.cassandra_writer import SCHEMA_DIR
from src.serving.evidence_params import ANCHOR_DATE

log = logging.getLogger("idempotence")

CSV_FILE = "idempotencia.csv"
REPORT_FILE = "idempotencia.md"
CSV_HEADER = ("objeto", "antes", "despues", "igual")
SERVING_PREFIX = "cassandra"
ORG_ID = "org_id"

# Columnas de la partición de cada tabla de Cassandra; por defecto, solo org_id.
SERVING_PARTITION_COLUMNS: dict[str, tuple[str, ...]] = {
    TOP_SERVICES_BY_ORG_14D: (ORG_ID, AS_OF_DATE),
}


@dataclass(frozen=True)
class CountComparison:
    """Conteo de un objeto antes y después de volver a correr el pipeline.

    Attributes:
        objeto: Dataset del lake (p. ej. ``bronze/users``) o tabla (``cassandra/<tabla>``).
        antes: Filas antes de la segunda corrida.
        despues: Filas después de la segunda corrida.
    """

    objeto: str
    antes: int
    despues: int

    @property
    def igual(self) -> bool:
        """True si la segunda corrida no cambió el conteo."""
        return self.antes == self.despues


def compare_counts(before: Mapping[str, int], after: Mapping[str, int]) -> list[CountComparison]:
    """Compara los conteos de las dos corridas; un objeto que falta de un lado cuenta 0.

    Returns:
        Una comparación por objeto, ordenadas por nombre.
    """
    return [
        CountComparison(name, before.get(name, 0), after.get(name, 0))
        for name in sorted(set(before) | set(after))
    ]


def exit_code(comparisons: list[CountComparison]) -> int:
    """0 si todos los conteos son iguales; 1 si alguno cambió."""
    return 0 if all(comparison.igual for comparison in comparisons) else 1


def count_lake(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Filas de cada dataset del lake; de los maestros de Bronze, solo el último snapshot.

    Returns:
        Filas por dataset, con la ruta relativa al lake como clave (``quarantine/bronze/users``).
    """
    return {item.name: _count_dataset(spark, settings, item) for item in lake_datasets(settings)}


def count_serving(settings: Settings, spark: SparkSession) -> dict[str, int]:
    """Filas de cada tabla del serving, sumando su ``count_<tabla>.cql`` por partición.

    Las particiones se toman del mart de Gold con el mismo nombre que la tabla; un mart sin
    archivos (p. ej. ningún día anómalo) da 0 sin consultar.

    Returns:
        Filas por tabla, con clave ``cassandra/<tabla>``.
    """
    tables = [cql_path.stem for cql_path in sorted(SCHEMA_DIR.glob("*.cql"))]
    counts: dict[str, int] = {}
    with session_scope(settings) as session:
        for table in tables:
            (cql,) = read_cql_statements(QUERIES_DIR / f"count_{table}.cql")
            statement = SimpleStatement(cql)
            total = 0
            for params in _serving_partitions(spark, settings, table):
                row = session.execute(statement, params).one()
                total += int(row[0]) if row is not None else 0
            counts[f"{SERVING_PREFIX}/{table}"] = total
            log.info("cassandra/%s: %d filas", table, total)
    return counts


def write_evidence(
    comparisons: list[CountComparison], settings: Settings, folder: Path
) -> tuple[Path, Path]:
    """Escribe ``idempotencia.csv`` e ``idempotencia.md`` en ``folder``.

    Returns:
        Rutas del .csv y del .md.
    """
    folder.mkdir(parents=True, exist_ok=True)
    csv_path = folder / CSV_FILE
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(CSV_HEADER)
        writer.writerows(
            (item.objeto, item.antes, item.despues, str(item.igual).lower())
            for item in comparisons
        )

    report_path = folder / REPORT_FILE
    report_path.write_text(_report(comparisons, settings), encoding="utf-8")
    log.info("Evidencia de idempotencia en %s y %s", csv_path, report_path)
    return csv_path, report_path


def main(folder: Path | None = None, run_id: str | None = None) -> int:
    """CLI: cuenta, corre el pipeline completo, vuelve a contar y escribe la evidencia.

    Args:
        folder: Carpeta de la evidencia (default: ``evidence/entrega-2``).
        run_id: Id de la segunda corrida; si no se pasa se genera uno.

    Returns:
        0 si todos los conteos dieron iguales; 1 si alguno cambió o si el pipeline falló.
    """
    load_env_file()
    run_id = run_id or new_run_id()
    settings = load_settings(run_id)
    output = folder or Path(settings.evidence_path) / DELIVERY_DIR

    before = _take_counts(settings)
    try:
        run_steps(list(STEPS), run_id)
    except Exception:
        log.exception("La segunda corrida del pipeline falló")
        return 1
    after = _take_counts(settings)

    comparisons = compare_counts(before, after)
    write_evidence(comparisons, settings, output)
    for item in comparisons:
        print(f"{item.objeto:<50} {item.antes:>8} {item.despues:>8} {str(item.igual).lower()}")
    code = exit_code(comparisons)
    if code:
        different = [item.objeto for item in comparisons if not item.igual]
        log.error("Hay conteos distintos: %s", different)
    return code


def _take_counts(settings: Settings) -> dict[str, int]:
    """Conteos del lake y del serving con una SparkSession propia (run_steps cierra la suya)."""
    spark = get_spark("idempotence")
    try:
        return {**count_lake(spark, settings), **count_serving(settings, spark)}
    finally:
        spark.stop()


def _count_dataset(spark: SparkSession, settings: Settings, item: LakeDataset) -> int:
    """Filas del dataset; 0 si no tiene archivos. Los maestros de Bronze, solo el último snapshot."""
    df = read_if_present(spark, settings, item)
    if df is None:
        return 0
    if _is_bronze_master(item) and INGEST_DATE in df.columns:
        df = _latest_snapshot(df)
    rows = df.count()
    log.info("%s: %d filas", item.name, rows)
    return rows


def _is_bronze_master(item: LakeDataset) -> bool:
    """True para los maestros (fuentes CSV) en ``bronze/`` y ``quarantine/bronze/``."""
    if item.zone == Zone.BRONZE:
        return item.dataset in SOURCES
    layer, _, dataset = item.dataset.partition("/")
    return item.zone == Zone.QUARANTINE and layer == Zone.BRONZE and dataset in SOURCES


def _latest_snapshot(df: DataFrame) -> DataFrame:
    """Filas de la partición con el ``ingest_date`` más alto."""
    latest = df.agg(spark_max(INGEST_DATE)).first()
    if latest is None or latest[0] is None:
        return df
    return df.filter(col(INGEST_DATE) == latest[0])


def _serving_partitions(
    spark: SparkSession, settings: Settings, table: str
) -> list[dict[str, Any]]:
    """Valores de la clave de partición de ``table`` presentes en su mart de Gold."""
    mart = read_if_present(spark, settings, LakeDataset(Zone.GOLD, table))
    if mart is None:
        return []
    columns = SERVING_PARTITION_COLUMNS.get(table, (ORG_ID,))
    if AS_OF_DATE in columns:
        mart = mart.filter(col(AS_OF_DATE) == ANCHOR_DATE)
    return [row.asDict() for row in mart.select(*columns).distinct().collect()]


def _report(comparisons: list[CountComparison], settings: Settings) -> str:
    """Texto de ``idempotencia.md``: destino, tabla de conteos y conclusión."""
    different = [item.objeto for item in comparisons if not item.igual]
    if different:
        listed = ", ".join(f"`{name}`" for name in different)
        conclusion = f"Hay conteos distintos entre las dos corridas: {listed}."
    else:
        conclusion = (
            "Todos los conteos son iguales: volver a correr el pipeline completo no duplica ni "
            "pierde filas en el lake ni en el serving."
        )
    lines = [
        "# Idempotencia",
        "",
        f"- Fecha: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M')} UTC",
        f"- `run_id` de la segunda corrida: `{settings.run_id}`",
        f"- Serving: `{settings.serving_target}` (keyspace `{settings.keyspace}`)",
        "",
        "Se contaron las filas de cada dataset del lake y de cada tabla del serving, se volvió a "
        "correr el pipeline completo y se contaron de nuevo. De los maestros de `bronze/` y "
        "`quarantine/bronze/` se cuenta solo el snapshot con el `ingest_date` más alto, porque "
        "correr otro día agrega un snapshot nuevo. En Cassandra se suma `count_<tabla>.cql` "
        "por cada partición presente en el mart de Gold (en `top_services_by_org_14d`, solo "
        f"`as_of_date = {ANCHOR_DATE.isoformat()}`).",
        "",
        "| Objeto | Antes | Después | Igual |",
        "| :--- | ---: | ---: | :---: |",
        *(
            f"| `{item.objeto}` | {item.antes} | {item.despues} | {str(item.igual).lower()} |"
            for item in comparisons
        ),
        "",
        f"**Conclusión:** {conclusion}",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    second_run_id = new_run_id()
    setup_logging("idempotence", run_id=second_run_id)
    sys.exit(main(run_id=second_run_id))
