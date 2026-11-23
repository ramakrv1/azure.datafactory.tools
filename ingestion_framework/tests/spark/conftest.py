"""Local Spark session (plain Spark, no Delta) and an order-independent DataFrame assertion."""
import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("ingest-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _canonical(rows: list[dict]) -> list[str]:
    return sorted(repr(sorted(r.items())) for r in rows)


def assert_rows(df: DataFrame, expected: list[dict]) -> None:
    """Assert `df` holds exactly the `expected` rows (as dicts), ignoring row and column order."""
    actual = [r.asDict(recursive=True) for r in df.collect()]
    assert _canonical(actual) == _canonical(expected)
