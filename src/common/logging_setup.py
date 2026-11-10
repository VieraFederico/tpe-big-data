"""Configuración de logging de los puntos de entrada (consola + archivo por corrida)."""

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from src.config import REPO_ROOT, RUN_TIMESTAMP_FORMAT

FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def setup_logging(
    run_name: str = "pipeline", log_dir: Path | None = None, run_id: str | None = None
) -> Path:
    """Configura el logging raíz para escribir a consola y a un archivo de la corrida.

    Se llama una sola vez, desde el punto de entrada; los módulos solo piden su logger.

    Args:
        run_name: Prefijo del archivo de log.
        log_dir: Carpeta de logs; por defecto LOG_DIR o ``evidence/logs``.
        run_id: Sufijo del archivo; por defecto, el timestamp actual.

    Returns:
        Ruta del archivo de log creado.
    """
    directory = log_dir or Path(os.getenv("LOG_DIR", REPO_ROOT / "evidence" / "logs"))
    directory.mkdir(parents=True, exist_ok=True)
    suffix = run_id or datetime.now(UTC).strftime(RUN_TIMESTAMP_FORMAT)
    log_file = directory / f"{run_name}_{suffix}.log"

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=FORMAT,
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )
    for noisy_logger_name in ("cassandra", "py4j"):
        logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)
    return log_file
