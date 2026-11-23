"""Delta Lake writer and state store, shared by Fabric and Databricks.

Merge semantics (identical to the memory adapter, pinned by the contract suite):
- the source must have at most one row per key, otherwise `ValueError("merge source has duplicate keys ...")`;
- keys match null-safely (`<=>`): a NULL key is a value, so repeated loads update that row instead of piling up copies;
- the schema evolves: columns new in the source are added.
"""
from __future__ import annotations

import json

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingest.core.model import TableRef
from ingest.ports import WriteMode

from .spark_common import assert_unique_keys


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _ensure_schema(spark: SparkSession, ref: TableRef) -> None:
    namespace = ".".join(_quote(p) for p in (ref.catalog, ref.schema) if p)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")


class DeltaWriter:
    """Writes Delta tables by name (managed tables), creating the schema and the table when absent."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def write(self, df: DataFrame, ref: TableRef, mode: WriteMode, keys: tuple[str, ...] = ()) -> None:
        """Overwrite, append, or upsert on `keys`; see the module docstring for merge semantics."""
        _ensure_schema(self._spark, ref)
        name = ref.fqn()
        if mode is WriteMode.merge and self._spark.catalog.tableExists(name):
            self._merge(df, name, keys)
        elif mode is WriteMode.append:
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(name)
        else:  # overwrite, or the first merge into a table that does not exist yet
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)

    def _merge(self, df: DataFrame, name: str, keys: tuple[str, ...]) -> None:
        assert_unique_keys(df, keys)
        condition = " AND ".join(f"t.{_quote(k)} <=> s.{_quote(k)}" for k in keys)
        (DeltaTable.forName(self._spark, name).alias("t").merge(df.alias("s"), condition)
         .withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


class DeltaStateStore:
    """Run state in a Delta table `(entity string, state string /* JSON */, updated_at timestamp)`."""

    def __init__(self, spark: SparkSession, ref: TableRef):
        self._spark, self._ref = spark, ref
        _ensure_schema(spark, ref)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {ref.fqn()} (entity STRING, state STRING, updated_at TIMESTAMP) USING DELTA")

    def get(self, entity_name: str) -> dict | None:
        """Return the stored state, or None if the entity never ran."""
        row = self._spark.table(self._ref.fqn()).filter(F.col("entity") == entity_name).select("state").first()
        return json.loads(row["state"]) if row else None

    def put(self, entity_name: str, state: dict) -> None:
        """Replace the stored state (one row per entity)."""
        row = self._spark.range(1).select(F.lit(entity_name).alias("entity"), F.lit(json.dumps(state)).alias("state"),
                                          F.current_timestamp().alias("updated_at"))
        (DeltaTable.forName(self._spark, self._ref.fqn()).alias("t").merge(row.alias("s"), "t.entity = s.entity")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
