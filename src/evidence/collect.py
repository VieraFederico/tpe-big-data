"""Junta en ``evidence/entrega-2/`` la evidencia de la última corrida del pipeline.

Se corre con ``python -m src.evidence.collect`` (es el último paso de ``./infra/run.sh
evidence``): conteos de los manifiestos, tamaños y esquemas del lake, la evidencia de calidad y
analítica de la corrida y sus logs, sin tokens ni rutas con usuarios.
"""

import csv
import json
import logging
import re
import shutil
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.common.lake import BYTES_PER_MB, dataset_storage_stats
from src.common.logging_setup import setup_logging
from src.common.manifests import MANIFESTS_DIR, read_manifest
from src.common.spark import get_spark
from src.config import Settings, load_env_file, load_settings
from src.evidence.inventory import DELIVERY_DIR, lake_datasets, read_if_present
from src.gold.anomalies import ANALYTICS_DIR, ANOMALY_SUMMARY_FILE
from src.gold.build import STEP_NAME as GOLD_STEP
from src.gold.mapreduce import MAPREDUCE_CHECK_FILE
from src.quality.report import QUALITY_DIR

log = logging.getLogger("evidence_collect")

COUNTS_FILE = "conteos.csv"
COUNTS_HEADER = ("run_id", "paso", "dataset", "metrica", "valor")
STORAGE_FILE = "storage.csv"
STORAGE_HEADER = ("zona", "dataset", "ruta", "particiones", "archivos", "mb")
SCHEMAS_FILE = "schemas.json"
LOGS_DIR = "logs"
# Comandos de la evidencia cuyo último log se copia (prefijo del archivo en evidence/logs/).
LOG_PREFIXES = ("pipeline", "mapreduce_check", "evidence_queries", "idempotence")
# Métrica con la que se registran los marts de gold.json, que guardan solo la cantidad de filas.
ROWS_METRIC = "filas"

# Lo que nunca puede quedar en la evidencia: tokens, bundles y rutas con usuarios del sistema.
_SENSITIVE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"AstraCS:\S+"), "<token>"),
    (re.compile(r"secure-connect-[\w.-]+"), "<bundle>"),
    (re.compile(r"(/home/|/Users/|[A-Za-z]:\\Users\\)[^/\\\s]+"), "<home>"),
)


def manifest_counts(settings: Settings) -> list[tuple[str, str, str, str, int]]:
    """Filas de ``conteos.csv``: ``(run_id, paso, dataset, metrica, valor)`` de cada manifiesto.

    Toma los conteos por dataset (``datasets``) y las filas de cada mart (``marts``).
    """
    rows: list[tuple[str, str, str, str, int]] = []
    manifests_dir = Path(settings.datalake_path) / MANIFESTS_DIR
    for path in sorted(manifests_dir.glob("*.json")):
        step = path.stem
        manifest = read_manifest(settings, step) or {}
        run_id = str(manifest.get("run_id", ""))
        datasets: dict[str, Any] = manifest.get("datasets") or {}
        for dataset, metrics in datasets.items():
            rows.extend(
                (run_id, step, dataset, metric, value) for metric, value in metrics.items()
            )
        marts: dict[str, Any] = manifest.get("marts") or {}
        rows.extend((run_id, step, mart, ROWS_METRIC, value) for mart, value in marts.items())
    return rows


def storage_rows(settings: Settings) -> list[tuple[str, str, str, int, int, float]]:
    """Filas de ``storage.csv``; la ruta queda relativa a la raíz del lake."""
    lake_root = Path(settings.datalake_path)
    rows: list[tuple[str, str, str, int, int, float]] = []
    for item in lake_datasets(settings):
        stats = dataset_storage_stats(settings, item.zone, item.dataset)
        relative_path = Path(stats.path).relative_to(lake_root).as_posix()
        rows.append(
            (
                str(item.zone),
                item.dataset,
                relative_path,
                stats.partitions,
                stats.files,
                round(stats.bytes / BYTES_PER_MB, 3),
            )
        )
    return rows


def dataset_schemas(spark: SparkSession, settings: Settings) -> dict[str, list[dict[str, str]]]:
    """Columnas y tipos de cada dataset del lake (incluye las columnas de partición).

    Un dataset sin archivos parquet (p. ej. una cuarentena vacía) queda con la lista vacía.
    """
    schemas: dict[str, list[dict[str, str]]] = {}
    for item in lake_datasets(settings):
        df = read_if_present(spark, settings, item)
        schemas[item.name] = (
            []
            if df is None
            else [
                {"columna": field.name, "tipo": field.dataType.simpleString()}
                for field in df.schema.fields
            ]
        )
    return schemas


def sanitize(text: str) -> str:
    """Reemplaza tokens, nombres de bundle y rutas con usuarios del sistema."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def write_csv(path: Path, header: tuple[str, ...], rows: Iterable[tuple[Any, ...]]) -> Path:
    """Escribe un CSV con encabezado, creando la carpeta si hace falta."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def collect(spark: SparkSession, settings: Settings, output: Path) -> str:
    """Deja en ``output`` toda la evidencia de la última corrida.

    Args:
        spark: Sesión de Spark (para leer los esquemas).
        settings: Configuración (define el lake y la carpeta de evidencia).
        output: Carpeta destino, p. ej. ``evidence/entrega-2``.

    Returns:
        El ``run_id`` de la corrida cuya evidencia se copió.

    Raises:
        FileNotFoundError: Si no hay ``_manifests/gold.json`` (el pipeline no llegó a Gold).
    """
    gold_manifest = read_manifest(settings, GOLD_STEP)
    if gold_manifest is None:
        raise FileNotFoundError(f"Falta {MANIFESTS_DIR}/{GOLD_STEP}.json: correr el pipeline")
    run_id = str(gold_manifest["run_id"])
    evidence_root = Path(settings.evidence_path)

    write_csv(output / COUNTS_FILE, COUNTS_HEADER, manifest_counts(settings))
    write_csv(output / STORAGE_FILE, STORAGE_HEADER, storage_rows(settings))
    (output / SCHEMAS_FILE).write_text(
        json.dumps(dataset_schemas(spark, settings), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    _replace_folder(evidence_root / QUALITY_DIR / run_id, output / QUALITY_DIR)
    analytics = output / ANALYTICS_DIR
    _replace_folder(evidence_root / ANALYTICS_DIR / run_id, analytics)
    latest_check = _latest(evidence_root / ANALYTICS_DIR, f"*/{MAPREDUCE_CHECK_FILE}")
    if latest_check is not None:
        shutil.copy2(latest_check, analytics / MAPREDUCE_CHECK_FILE)
    if not (analytics / ANOMALY_SUMMARY_FILE).is_file():
        log.warning("La corrida %s no dejó %s", run_id, ANOMALY_SUMMARY_FILE)

    logs = output / LOGS_DIR
    if logs.is_dir():
        shutil.rmtree(logs)
    logs.mkdir(parents=True)
    for prefix in LOG_PREFIXES:
        latest_log = _latest(evidence_root / LOGS_DIR, f"{prefix}_*.log")
        if latest_log is not None:
            (logs / latest_log.name).write_text(
                sanitize(latest_log.read_text(encoding="utf-8")), encoding="utf-8"
            )
    log.info("Evidencia de la corrida %s en %s", run_id, output)
    return run_id


def main() -> int:
    """CLI: junta la evidencia en ``evidence/entrega-2``; 1 si el pipeline no llegó a Gold."""
    load_env_file()
    settings = load_settings()
    spark = get_spark("evidence-collect")
    try:
        collect(spark, settings, Path(settings.evidence_path) / DELIVERY_DIR)
    except FileNotFoundError as error:
        print(error, file=sys.stderr)
        return 1
    finally:
        spark.stop()
    return 0


def _replace_folder(source: Path, target: Path) -> None:
    """Reemplaza ``target`` por una copia de ``source`` (lo deja vacío si ``source`` no existe)."""
    if target.is_dir():
        shutil.rmtree(target)
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        log.warning("No existe %s", source)
        target.mkdir(parents=True)


def _latest(folder: Path, pattern: str) -> Path | None:
    """Archivo más reciente que cumple ``pattern``: los nombres llevan un timestamp ordenable."""
    matches = sorted(folder.glob(pattern), key=lambda path: path.as_posix())
    return matches[-1] if matches else None


if __name__ == "__main__":
    setup_logging("evidence_collect")
    sys.exit(main())
