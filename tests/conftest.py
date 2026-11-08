import pytest
from pyspark.sql import SparkSession

from src.config import Settings


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Lake temporario por test, con landing vacío."""
    (tmp_path / "landing").mkdir()
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
