"""Obtención del secure connect bundle de AstraDB (descarga automática si falta)."""
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
DEVOPS_API_TIMEOUT_SECONDS = 30
BUNDLE_DOWNLOAD_TIMEOUT_SECONDS = 60
AUTH_ERROR_STATUS_CODES = (401, 403)


def database_id_from_endpoint(endpoint: str) -> str:
    """Extrae el id (uuid) de la base del endpoint de Astra.

    Raises:
        ValueError: Si el endpoint no tiene el formato ``https://<db_id>-<region>...``.
    """
    match = ENDPOINT_RE.match(endpoint.strip())
    if not match:
        raise ValueError(
            "ASTRA_DB_API_ENDPOINT no tiene el formato esperado "
            "https://<db_id>-<region>.apps.astra.datastax.com"
        )
    return match.group(1)


def ensure_bundle(settings: Settings) -> str:
    """Devuelve la ruta del secure connect bundle de Astra, descargándolo si hace falta.

    El bundle trae los certificados necesarios para conectarse por CQL. Si
    ASTRA_DB_SECURE_BUNDLE_PATH apunta a un archivo existente se usa ese; si no, se
    descarga con la DevOps API, así el proceso es transparente para quien corre el pipeline.

    Raises:
        ValueError: Si no hay bundle y falta el endpoint o el token para descargarlo.
        RuntimeError: Si la DevOps API de Astra responde con error.
    """
    if settings.astra_bundle_path and Path(settings.astra_bundle_path).is_file():
        return settings.astra_bundle_path

    if not settings.astra_api_endpoint:
        raise ValueError(
            "No hay secure connect bundle en ASTRA_DB_SECURE_BUNDLE_PATH y falta "
            "ASTRA_DB_API_ENDPOINT para descargarlo"
        )
    db_id = database_id_from_endpoint(settings.astra_api_endpoint)
    target = Path(settings.astra_bundle_path or REPO_ROOT / "secrets" / f"secure-connect-{db_id}.zip")
    if target.is_file():
        return str(target)

    if not settings.astra_token:
        raise ValueError("Falta ASTRA_DB_APPLICATION_TOKEN para descargar el secure connect bundle")
    log.info("Descargando secure connect bundle de la base %s", db_id)
    download_url = _request_download_url(db_id, settings.astra_token)
    _download(download_url, target)
    log.info("Bundle guardado en %s", target)
    return str(target)


def _request_download_url(db_id: str, token: str) -> str:
    """Pide a la DevOps API de Astra la URL temporal de descarga del bundle."""
    request = urllib.request.Request(
        DEVOPS_API.format(db_id=db_id),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=DEVOPS_API_TIMEOUT_SECONDS) as response:
            body = json.load(response)
    except urllib.error.HTTPError as error:
        is_auth_error = error.code in AUTH_ERROR_STATUS_CODES
        hint = " (token sin permisos sobre la base o inválido)" if is_auth_error else ""
        raise RuntimeError(f"La DevOps API de Astra respondió {error.code}{hint}") from error
    return body["downloadURL"]


def _download(url: str, target: Path) -> None:
    """Descarga a un archivo temporal y lo renombra al final, para no dejar un zip a medias."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_file_descriptor, temp_path = tempfile.mkstemp(dir=target.parent, suffix=".part")
    try:
        with (
            os.fdopen(temp_file_descriptor, "wb") as temp_file,
            urllib.request.urlopen(url, timeout=BUNDLE_DOWNLOAD_TIMEOUT_SECONDS) as response,
        ):
            temp_file.write(response.read())
        os.replace(temp_path, target)
    except BaseException:
        Path(temp_path).unlink(missing_ok=True)
        raise
