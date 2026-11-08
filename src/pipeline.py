import argparse
import importlib
import logging
import sys
import time

from src.common.logging_setup import setup_logging
from src.config import load_settings

log = logging.getLogger("pipeline")

STEPS: dict[str, str] = {
    "bronze_batch": "src.bronze.batch.ingest",
    "bronze_stream": "src.bronze.streaming.ingest",
    "silver": "src.silver.build",
    "gold": "src.gold.build",
    "serving": "src.serving.load",
}


def _resolve(step: str):
    module_name = STEPS[step]
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name == module_name:
            raise NotImplementedError(f"Paso {step!r} sin implementar: falta {module_name}.run") from exc
        raise
    return module.run


def run_steps(steps: list[str]) -> None:
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise ValueError(f"Pasos desconocidos: {unknown}. Disponibles: {list(STEPS)}")

    from src.common.spark import get_spark

    settings = load_settings()
    log.info("Pipeline: pasos=%s, lake=%s, serving=%s", steps, settings.datalake_path, settings.serving_target)
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
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline Landing -> Serving.")
    parser.add_argument("--steps", help=f"Pasos separados por coma (default: todos). Opciones: {','.join(STEPS)}")
    parser.add_argument("--list", action="store_true", help="Lista los pasos y si están implementados")
    args = parser.parse_args(argv)

    if args.list:
        for step in STEPS:
            try:
                _resolve(step)
                status = "ok"
            except NotImplementedError:
                status = "sin implementar"
            print(f"{step:<14} {STEPS[step]:<32} {status}")
        return 0

    log_file = setup_logging("pipeline")
    log.info("Log de la corrida: %s", log_file)
    steps = args.steps.split(",") if args.steps else list(STEPS)
    try:
        run_steps([s.strip() for s in steps])
    except Exception:
        log.exception("El pipeline falló")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
