"""Punto de entrada del pipeline Landing -> Serving: ejecuta los pasos en orden."""
import argparse
import importlib
import logging
import sys
import time
from collections.abc import Callable

from src.common.logging_setup import setup_logging
from src.config import load_settings, new_run_id

log = logging.getLogger("pipeline")

STEPS: dict[str, str] = {
    "bronze_batch": "src.bronze.batch.ingest",
    "bronze_stream": "src.bronze.streaming.ingest",
    "silver": "src.silver.build",
    "gold": "src.gold.build",
    "serving": "src.serving.load",
}


def _resolve(step: str) -> Callable[..., object]:
    """Importa el módulo del paso y devuelve su función ``run(spark, settings)``.

    Raises:
        NotImplementedError: Si el módulo del paso no existe o no define ``run``.
    """
    module_name = STEPS[step]
    missing_message = f"Paso {step!r} sin implementar: falta {module_name}.run"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as error:
        if error.name == module_name:
            raise NotImplementedError(missing_message) from error
        raise
    step_runner = getattr(module, "run", None)
    if not callable(step_runner):
        raise NotImplementedError(missing_message)
    return step_runner


def run_steps(steps: list[str], run_id: str | None = None) -> None:
    """Ejecuta los pasos indicados, en orden, con una misma SparkSession y Settings.

    Args:
        steps: Nombres de pasos (claves de STEPS).
        run_id: Id de la corrida; si no se pasa se genera uno.

    Raises:
        ValueError: Si algún paso no existe.
        NotImplementedError: Si algún paso todavía no tiene ``run``.
    """
    unknown = [step for step in steps if step not in STEPS]
    if unknown:
        raise ValueError(f"Pasos desconocidos: {unknown}. Disponibles: {list(STEPS)}")

    from src.common.spark import get_spark

    settings = load_settings(run_id)
    log.info("Pipeline: run_id=%s, pasos=%s, lake=%s, serving=%s",
             settings.run_id, steps, settings.datalake_path, settings.serving_target)
    spark = get_spark()
    try:
        for step in steps:
            started = time.monotonic()
            log.info("== %s: inicio", step)
            _resolve(step)(spark, settings)
            log.info("== %s: ok en %.1fs", step, time.monotonic() - started)
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    """CLI del pipeline. Devuelve el código de salida (0 ok, 1 si falló algún paso)."""
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline Landing -> Serving.")
    parser.add_argument("--steps", help=f"Pasos separados por coma (default: todos). Opciones: {','.join(STEPS)}")
    parser.add_argument("--list", action="store_true", help="Lista los pasos y si están implementados")
    arguments = parser.parse_args(argv)

    if arguments.list:
        for step in STEPS:
            try:
                _resolve(step)
                status = "ok"
            except NotImplementedError:
                status = "sin implementar"
            print(f"{step:<14} {STEPS[step]:<32} {status}")
        return 0

    run_id = new_run_id()
    log_file = setup_logging("pipeline", run_id=run_id)
    log.info("Log de la corrida: %s", log_file)
    steps = arguments.steps.split(",") if arguments.steps else list(STEPS)
    try:
        run_steps([step.strip() for step in steps], run_id)
    except Exception:
        log.exception("El pipeline falló")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
