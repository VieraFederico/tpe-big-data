import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    
    env_file = Path(os.getenv("ENV_FILE", REPO_ROOT / "config" / ".env"))
    load_dotenv(env_file, override=False)
except ImportError:
    pass

VALID_TARGETS = {"local", "astra"}


def new_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:6]}"


@dataclass(frozen=True)
class Settings:
    datalake_path: str
    serving_target: str
    keyspace: str
    cassandra_host: str
    cassandra_port: int
    astra_token: str | None
    astra_api_endpoint: str | None
    astra_bundle_path: str | None
    run_id: str = field(default_factory=new_run_id)

    def zone_path(self, zone: str) -> str:
        """Ruta de una zona del lake, p. ej. zone_path('bronze')."""
        return f"{self.datalake_path.rstrip('/')}/{zone}"


def load_settings(run_id: str | None = None) -> Settings:
    target = os.getenv("SERVING_TARGET", "local").strip().lower()
    if target not in VALID_TARGETS:
        raise ValueError(f"SERVING_TARGET={target!r} inválido; usar uno de {sorted(VALID_TARGETS)}")

    settings = Settings(
        datalake_path=os.getenv("DATALAKE_PATH", "./data/datalake"),
        serving_target=target,
        keyspace=os.getenv("CASSANDRA_KEYSPACE", "cloud_analytics"),
        cassandra_host=os.getenv("CASSANDRA_HOST", "localhost"),
        cassandra_port=int(os.getenv("CASSANDRA_PORT", "9042")),
        astra_token=os.getenv("ASTRA_DB_APPLICATION_TOKEN") or None,
        astra_api_endpoint=os.getenv("ASTRA_DB_API_ENDPOINT") or None,
        astra_bundle_path=os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH") or None,
        run_id=run_id or new_run_id(),
    )

    if target == "astra":
        if not settings.astra_token:
            raise ValueError("SERVING_TARGET=astra requiere ASTRA_DB_APPLICATION_TOKEN")
        has_bundle = settings.astra_bundle_path and Path(settings.astra_bundle_path).is_file()
        if not has_bundle and not settings.astra_api_endpoint:
            raise ValueError(
                "SERVING_TARGET=astra requiere ASTRA_DB_API_ENDPOINT (para descargar el "
                "secure connect bundle) o un bundle existente en ASTRA_DB_SECURE_BUNDLE_PATH"
            )
    return settings
