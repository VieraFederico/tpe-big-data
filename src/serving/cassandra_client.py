"""Sesiones CQL contra la Cassandra local o AstraDB."""
import atexit
import logging
from collections.abc import Iterator
from contextlib import contextmanager

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import InvalidRequest

from src.config import Settings, load_settings
from src.serving.astra_bundle import ensure_bundle
from src.serving.errors import KeyspaceNotFoundError

log = logging.getLogger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
# Astra acepta hasta la v4 del protocolo nativo; la Cassandra local soporta la v5.
ASTRA_PROTOCOL_VERSION = 4
LOCAL_PROTOCOL_VERSION = 5
# Nombre del datacenter por defecto en la imagen oficial de Cassandra.
LOCAL_DATACENTER = "datacenter1"
LOCAL_REPLICATION_FACTOR = 1

_shared_sessions: dict[Settings, Session] = {}


def _default_profile(local_dc: str | None) -> dict[object, ExecutionProfile]:
    """Perfil de ejecución por defecto: token-aware, LOCAL_QUORUM y REQUEST_TIMEOUT_SECONDS."""
    return {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=local_dc)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=REQUEST_TIMEOUT_SECONDS,
        )
    }


def get_session(settings: Settings | None = None) -> Session:
    """Abre una sesión CQL contra Astra o la Cassandra local, ya posicionada en el keyspace.

    Si algo falla después de crear el ``Cluster`` (conexión, creación o selección del
    keyspace), el cluster se cierra antes de propagar el error para no dejar hilos vivos.

    Raises:
        KeyspaceNotFoundError: Si el keyspace no existe (en Astra se crea desde la UI).
    """
    resolved_settings = settings or load_settings()
    is_astra = resolved_settings.serving_target == "astra"

    if is_astra:
        cluster = Cluster(
            cloud={"secure_connect_bundle": ensure_bundle(resolved_settings)},
            auth_provider=PlainTextAuthProvider("token", resolved_settings.astra_token),
            protocol_version=ASTRA_PROTOCOL_VERSION,
            execution_profiles=_default_profile(local_dc=None),
        )
    else:
        cluster = Cluster(
            [resolved_settings.cassandra_host],
            port=resolved_settings.cassandra_port,
            protocol_version=LOCAL_PROTOCOL_VERSION,
            execution_profiles=_default_profile(local_dc=LOCAL_DATACENTER),
        )

    try:
        session = cluster.connect()
        if not is_astra:
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {resolved_settings.keyspace} "
                "WITH replication = {'class': 'SimpleStrategy', "
                f"'replication_factor': {LOCAL_REPLICATION_FACTOR}}}"
            )
        _use_keyspace(session, resolved_settings)
    except BaseException:
        cluster.shutdown()
        raise

    log.info("Sesión CQL abierta (target=%s, keyspace=%s)",
             resolved_settings.serving_target, resolved_settings.keyspace)
    return session


def _use_keyspace(session: Session, settings: Settings) -> None:
    """Posiciona la sesión en el keyspace configurado.

    Raises:
        KeyspaceNotFoundError: Si el keyspace no existe.
    """
    try:
        session.set_keyspace(settings.keyspace)
    except InvalidRequest as error:
        raise KeyspaceNotFoundError(
            settings.keyspace,
            f"El keyspace {settings.keyspace!r} no existe en {settings.serving_target}. "
            "En Astra hay que crearlo desde la UI (ver infra/README.md).",
        ) from error


@contextmanager
def session_scope(settings: Settings | None = None) -> Iterator[Session]:
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
