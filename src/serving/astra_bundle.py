import json
import logging
import os
import re
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

from src.config import REPO_ROOT, Settings

log = logging.getLogger(__name__)

DEVOPS_API = "https://api.astra.datastax.com/v2/databases/{db_id}/secureBundleURL"
# https://<db_id (uuid)>-<region>.apps.astra.datastax.com
ENDPOINT_RE = re.compile(r"^https://([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-")


def database_id_from_endpoint(endpoint: str) -> str:
    match = ENDPOINT_RE.match(endpoint.strip())
    if not match:
        raise ValueError(
            "ASTRA_DB_API_ENDPOINT no tiene el formato esperado "
            "https://<db_id>-<region>.apps.astra.datastax.com"
        )
    return match.group(1)

# Descarga y garantiza que este el secure bundle necesario para astra cloud
# Esto trae un certificado y etc necesarios para poder correr CQL contra astra
# La idea es que lo descargue automaticamente si no existe o si no se usa la env var
# De forma que el proceso sea transparente y simple
def ensure_bundle(settings: Settings) -> str:
    if settings.astra_bundle_path and Path(settings.astra_bundle_path).is_file():
        return settings.astra_bundle_path

    db_id = database_id_from_endpoint(settings.astra_api_endpoint or "")
    target = Path(settings.astra_bundle_path or REPO_ROOT / "secrets" / f"secure-connect-{db_id}.zip")
    if target.is_file():
        return str(target)

    log.info("Descargando secure connect bundle de la base %s", db_id)
    download_url = _request_download_url(db_id, settings.astra_token or "")
    _download(download_url, target)
    log.info("Bundle guardado en %s", target)
    return str(target)


def _request_download_url(db_id: str, token: str) -> str:
    req = urllib.request.Request(
        DEVOPS_API.format(db_id=db_id),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.load(resp)
    except urllib.error.HTTPError as exception:
        hint = " (token sin permisos sobre la base o inválido)" if exception.code in (401, 403) else ""
        raise RuntimeError(f"La DevOps API de Astra respondió {exception.code}{hint}") from exception
    return body["downloadURL"]


def _download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=target.parent, suffix=".part")
    try:
        with os.fdopen(fd, "wb") as out, urllib.request.urlopen(url, timeout=60) as resp:
            out.write(resp.read())
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
