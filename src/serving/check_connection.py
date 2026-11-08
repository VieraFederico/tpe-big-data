import logging
import sys

from src.config import load_settings
from src.serving.cassandra_client import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("check_connection")


def main() -> int:
    settings = load_settings()
    session = get_session(settings)
    try:
        version = session.execute("SELECT release_version FROM system.local").one().release_version
        tables = [
            r.table_name
            for r in session.execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
                (settings.keyspace,),
            )
        ]
        log.info("Conectado a %s (Cassandra %s)", settings.serving_target, version)
        log.info("Tablas en %s: %s", settings.keyspace, tables or "(ninguna todavía)")
    finally:
        session.cluster.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
