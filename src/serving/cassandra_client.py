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
    return {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=local_dc)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=30,
        )
    }


def get_session(settings: Settings | None = None) -> Session:
    s = settings or load_settings()

    if s.serving_target == "astra":
        cluster = Cluster(
            cloud={"secure_connect_bundle": ensure_bundle(s)},
            auth_provider=PlainTextAuthProvider("token", s.astra_token),
            protocol_version=4,
            execution_profiles=_default_profile(local_dc=None),
        )
        session = cluster.connect()
    else:
        cluster = Cluster(
            [s.cassandra_host],
            port=s.cassandra_port,
            protocol_version=5,
            execution_profiles=_default_profile(local_dc="datacenter1"),
        )
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {s.keyspace} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )

    try:
        session.set_keyspace(s.keyspace)
    except InvalidRequest as exc:
        cluster.shutdown()
        raise RuntimeError(
            f"El keyspace {s.keyspace!r} no existe en {s.serving_target}. "
            "En Astra hay que crearlo desde la UI (ver infra/README.md)."
        ) from exc

    log.info("Sesión CQL abierta (target=%s, keyspace=%s)", s.serving_target, s.keyspace)
    return session


@contextmanager
def session_scope(settings: Settings | None = None) -> Generator[Session, None, None]:
    session = get_session(settings)
    try:
        yield session
    finally:
        session.cluster.shutdown()


def get_shared_session(settings: Settings) -> Session:
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
