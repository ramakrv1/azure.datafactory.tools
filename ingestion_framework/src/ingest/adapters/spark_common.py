"""Spark pieces shared by every Spark-based adapter (Fabric, Databricks, local)."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingest.core.model import Source, TableRef


class SparkReader:
    """Reads `files` and `jdbc` sources and existing tables with plain Spark."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read(self, source: Source) -> DataFrame:
        """Read a `files` or `jdbc` source; `table` sources are resolved by the pipeline via `read_table`."""
        if source.type == "files":
            return self._spark.read.format(source.format).options(**source.options).load(source.path)
        if source.type == "jdbc":
            return self._spark.read.format("jdbc").options(**source.options).load()
        raise ValueError(f"cannot read a '{source.type}' source directly; use read_table")

    def read_table(self, ref: TableRef) -> DataFrame:
        """Read an existing table by its fully qualified name."""
        return self._spark.table(ref.fqn())


def assert_unique_keys(df: DataFrame, keys: tuple[str, ...]) -> None:
    """Raise ValueError if `df` has two rows with the same key (NULL counts as a value), in one aggregation."""
    if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"merge source has duplicate keys: more than one row per ({', '.join(keys)})")
