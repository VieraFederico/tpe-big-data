import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src.config import REPO_ROOT

FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def setup_logging(run_name: str = "pipeline", log_dir: Path | None = None) -> Path:
    directory = log_dir or Path(os.getenv("LOG_DIR", REPO_ROOT / "evidence" / "logs"))
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = directory / f"{run_name}_{stamp}.log"

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=FORMAT,
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")],
        force=True,
    )
    for noisy in ("cassandra", "py4j"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return log_file
