"""In-memory adapter: reference implementation of the engine ports, for tests and local runs."""
from __future__ import annotations

from functools import reduce
from typing import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingest.core.model import Source, TableRef
from ingest.ports import Engine, WriteMode

from .spark_common import assert_unique_keys


class MemoryTables(dict):
    """Tables by fully qualified name (`catalog.schema.name`)."""


class MemoryReader:
    """Serves `files` sources by path, `jdbc` sources by dbtable/query, and tables from `tables`."""

    def __init__(self, sources: Mapping[str, DataFrame], tables: MemoryTables):
        self._sources = sources
        self._tables = tables

    def read(self, source: Source) -> DataFrame:
        """Return the DataFrame registered for the source; unknown sources raise KeyError."""
        key = source.path if source.type == "files" else source.options.get("dbtable") or source.options.get("query")
        if key not in self._sources:
            raise KeyError(f"no data registered for {source.type} source '{key}'")
        return self._sources[key]

    def read_table(self, ref: TableRef) -> DataFrame:
        """Return the table; a missing table raises KeyError."""
        if ref.fqn() not in self._tables:
            raise KeyError(f"table '{ref.fqn()}' does not exist")
        return self._tables[ref.fqn()]


class MemoryWriter:
    """Writes into `tables`; append and merge evolve the schema by column name."""

    def __init__(self, tables: MemoryTables):
        self._tables = tables

    def write(self, df: DataFrame, ref: TableRef, mode: WriteMode, keys: tuple[str, ...] = ()) -> None:
        """Overwrite, append, or upsert on `keys` (NULL matches NULL; duplicate source keys raise ValueError)."""
        name, existing = ref.fqn(), self._tables.get(ref.fqn())
        if mode is WriteMode.overwrite or existing is None:
            self._tables[name] = df
        elif mode is WriteMode.append:
            self._tables[name] = existing.unionByName(df, allowMissingColumns=True)
        else:
            assert_unique_keys(df, keys)
            same_key = reduce(lambda a, b: a & b, (F.col(f"t.`{k}`").eqNullSafe(F.col(f"s.`{k}`")) for k in keys))
            kept = existing.alias("t").join(df.select(*keys).alias("s"), same_key, "left_anti")
            self._tables[name] = kept.unionByName(df, allowMissingColumns=True)


class MemoryState:
    """Dict-backed state store."""

    def __init__(self) -> None:
        self._states: dict[str, dict] = {}

    def get(self, entity_name: str) -> dict | None:
        """Return the stored state or None."""
        return self._states.get(entity_name)

    def put(self, entity_name: str, state: dict) -> None:
        """Replace the stored state."""
        self._states[entity_name] = dict(state)


class DictSecrets:
    """Secrets from a `{"scope/key": value}` mapping."""

    def __init__(self, mapping: Mapping[str, str]):
        self._mapping = mapping

    def get(self, scope: str, key: str) -> str:
        """Return the secret; a missing one raises KeyError naming scope/key (never a value)."""
        if f"{scope}/{key}" not in self._mapping:
            raise KeyError(f"secret '{scope}/{key}' not found")
        return self._mapping[f"{scope}/{key}"]


def memory_engine(
    sources: Mapping[str, DataFrame] | None = None, secrets: Mapping[str, str] | None = None
) -> tuple[Engine, MemoryTables]:
    """Build an all-in-memory engine and return it with its tables."""
    tables = MemoryTables()
    engine = Engine(MemoryReader({} if sources is None else sources, tables), MemoryWriter(tables), MemoryState(), DictSecrets(secrets or {}))
    return engine, tables
