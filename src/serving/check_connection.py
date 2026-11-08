import logging
import sys

from src.common.logging_setup import setup_logging
from src.config import load_settings
from src.serving.cassandra_client import session_scope

log = logging.getLogger("check_connection")


def main() -> int:
    settings = load_settings()
    with session_scope(settings) as session:
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
    return 0


if __name__ == "__main__":
    setup_logging("check_connection")
    sys.exit(main())
