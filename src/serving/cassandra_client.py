import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.protocol import InvalidRequest

from src.config import Settings, load_settings
from src.serving.astra_bundle import ensure_bundle

log = logging.getLogger(__name__)


def get_session(settings: Settings | None = None) -> Session:
    s = settings or load_settings()

    if s.serving_target == "astra":
        cluster = Cluster(
            cloud={"secure_connect_bundle": ensure_bundle(s)},
            auth_provider=PlainTextAuthProvider("token", s.astra_token),
            protocol_version=4,
        )
        session = cluster.connect()
    else:
        cluster = Cluster(
            [s.cassandra_host],
            port=s.cassandra_port,
            protocol_version=5,
            execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(
                    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
                )
            },
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
            "En Astra hay que crearlo desde la UI (Database -> Add Keyspace)."
        ) from exc

    log.info("Sesión CQL abierta (target=%s, keyspace=%s)", s.serving_target, s.keyspace)
    return session
