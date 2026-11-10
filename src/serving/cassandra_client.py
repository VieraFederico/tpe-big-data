"""Sesiones CQL contra la Cassandra local o AstraDB."""
import atexit
import logging
from contextlib import contextmanager
from typing import Generator

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import InvalidRequest

from src.config import Settings, load_settings
from src.serving.astra_bundle import ensure_bundle

log = logging.getLogger(__name__)

_shared_sessions: dict[Settings, Session] = {}


def _default_profile(local_dc: str | None) -> dict:
    """Perfil de ejecución por defecto: token-aware, LOCAL_QUORUM y timeout de 30 s."""
    return {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=local_dc)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=30,
        )
    }


def get_session(settings: Settings | None = None) -> Session:
    """Abre una sesión CQL contra Astra o la Cassandra local, ya posicionada en el keyspace.

    Si algo falla después de crear el ``Cluster`` (conexión, creación o selección del
    keyspace), el cluster se cierra antes de propagar el error para no dejar hilos vivos.

    Raises:
        RuntimeError: Si el keyspace no existe (en Astra se crea desde la UI).
    """
    resolved_settings = settings or load_settings()
    is_astra = resolved_settings.serving_target == "astra"

    if is_astra:
        cluster = Cluster(
            cloud={"secure_connect_bundle": ensure_bundle(resolved_settings)},
            auth_provider=PlainTextAuthProvider("token", resolved_settings.astra_token),
            protocol_version=4,
            execution_profiles=_default_profile(local_dc=None),
        )
    else:
        cluster = Cluster(
            [resolved_settings.cassandra_host],
            port=resolved_settings.cassandra_port,
            protocol_version=5,
            execution_profiles=_default_profile(local_dc="datacenter1"),
        )

    try:
        session = cluster.connect()
        if not is_astra:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {resolved_settings.keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
        session.set_keyspace(resolved_settings.keyspace)
    except InvalidRequest as error:
        cluster.shutdown()
        raise RuntimeError(
            f"El keyspace {resolved_settings.keyspace!r} no existe en "
            f"{resolved_settings.serving_target}. "
            "En Astra hay que crearlo desde la UI (ver infra/README.md)."
        ) from error
    except BaseException:
        cluster.shutdown()
        raise

    log.info("Sesión CQL abierta (target=%s, keyspace=%s)",
             resolved_settings.serving_target, resolved_settings.keyspace)
    return session


@contextmanager
def session_scope(settings: Settings | None = None) -> Generator[Session, None, None]:
    """Abre una sesión CQL y cierra su cluster al salir del bloque ``with``."""
    session = get_session(settings)
    try:
        yield session
    finally:
        session.cluster.shutdown()


def get_shared_session(settings: Settings) -> Session:
    """Sesión reutilizable por proceso (una por Settings), pensada para los workers de Spark.

    Abrir un cluster por partición es caro; así cada worker abre uno solo y se cierra
    al terminar el proceso.
    """
    session = _shared_sessions.get(settings)
    if session is None or session.is_shutdown:
        session = get_session(settings)
        _shared_sessions[settings] = session
    return session


@atexit.register
def _shutdown_shared_sessions() -> None:
    for session in _shared_sessions.values():
        session.cluster.shutdown()
    _shared_sessions.clear()
