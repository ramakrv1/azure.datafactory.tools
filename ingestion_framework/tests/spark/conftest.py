"""Order-independent DataFrame assertion (the Spark session fixture lives in tests/conftest.py)."""
from pyspark.sql import DataFrame


def _canonical(rows: list[dict]) -> list[str]:
    return sorted(repr(sorted(r.items())) for r in rows)


def assert_rows(df: DataFrame, expected: list[dict]) -> None:
    """Assert `df` holds exactly the `expected` rows (as dicts), ignoring row and column order."""
    actual = [r.asDict(recursive=True) for r in df.collect()]
    assert _canonical(actual) == _canonical(expected)
