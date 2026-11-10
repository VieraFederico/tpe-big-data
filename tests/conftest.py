import pytest

from src.common.spark import get_spark
from src.config import Settings, Zone


@pytest.fixture(scope="session")
def spark():
    """SparkSession chica para tests, con la misma configuración semántica que el pipeline."""
    session = get_spark(
        "tests",
        master="local[1]",
        config_overrides={
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "1",
            "spark.ui.enabled": "false",
        },
        log_level="ERROR",
    )
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Lake temporario por test, con landing vacío."""
    (tmp_path / Zone.LANDING).mkdir()
    return Settings(
        datalake_path=str(tmp_path),
        serving_target="local",
        keyspace="test",
        cassandra_host="localhost",
        cassandra_port=9042,
        astra_token=None,
        astra_api_endpoint=None,
        astra_bundle_path=None,
    )
