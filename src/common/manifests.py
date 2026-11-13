"""Manifiestos de conteos por paso, guardados como JSON en ``<lake>/_manifests/``."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config import Settings

MANIFESTS_DIR = "_manifests"

# Claves que agrega write_manifest; el contenido del paso no puede usarlas.
RESERVED_KEYS = frozenset({"run_id", "written_at"})

# ISO 8601 en UTC con segundos, p. ej. ``2026-11-14T12:03:10Z``.
WRITTEN_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def manifest_path(settings: Settings, step: str) -> Path:
    """Ruta del manifiesto de un paso, p. ej. ``<lake>/_manifests/bronze_batch.json``."""
    return Path(settings.datalake_path) / MANIFESTS_DIR / f"{step}.json"


def write_manifest(settings: Settings, step: str, payload: Mapping[str, object]) -> Path:
    """Escribe (o reemplaza) el manifiesto de un paso.

    El archivo se escribe primero con otro nombre y después se renombra, para que nunca
    quede un manifiesto a medio escribir.

    Args:
        settings: Configuración de la corrida (define el lake y el ``run_id``).
        step: Nombre del paso, p. ej. ``bronze_batch``.
        payload: Contenido del paso; debe poder serializarse a JSON.

    Returns:
        Ruta del manifiesto escrito.

    Raises:
        ValueError: Si ``payload`` usa una clave reservada (``run_id``, ``written_at``).
    """
    reserved_in_payload = RESERVED_KEYS & set(payload)
    if reserved_in_payload:
        raise ValueError(f"Claves reservadas en el manifiesto: {sorted(reserved_in_payload)}")

    content = {
        "run_id": settings.run_id,
        "written_at": datetime.now(UTC).strftime(WRITTEN_AT_FORMAT),
        **payload,
    }
    path = manifest_path(settings, step)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f"{path.name}.tmp")
    temporary_path.write_text(
        json.dumps(content, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary_path.replace(path)
    return path


def read_manifest(settings: Settings, step: str) -> dict[str, Any] | None:
    """Lee el manifiesto de un paso.

    Args:
        settings: Configuración de la corrida (define el lake).
        step: Nombre del paso, p. ej. ``bronze_batch``.

    Returns:
        El contenido del manifiesto, o None si el paso todavía no lo escribió.

    Raises:
        ValueError: Si el archivo no contiene un objeto JSON.
    """
    path = manifest_path(settings, step)
    if not path.is_file():
        return None
    content = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise ValueError(f"El manifiesto {path} no contiene un objeto JSON")
    return content
