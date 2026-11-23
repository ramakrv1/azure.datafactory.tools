"""Engine ports: the only surface the portable core uses to touch storage, state and secrets."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from pyspark.sql import DataFrame

from ingest.core.model import Source, TableRef


class WriteMode(str, Enum):
    """How a DataFrame is written to a table."""

    overwrite = "overwrite"
    append = "append"
    merge = "merge"


class SourceReader(Protocol):
    """Reads source data; `source.options` are already secret-resolved."""

    def read(self, source: Source) -> DataFrame:
        """Read a `files` or `jdbc` source."""

    def read_table(self, ref: TableRef) -> DataFrame:
        """Read an existing table."""


class TableWriter(Protocol):
    """Writes a DataFrame to a table."""

    def write(self, df: DataFrame, ref: TableRef, mode: WriteMode, keys: tuple[str, ...] = ()) -> None:
        """Write `df`; `merge` upserts on `keys` (update matched, insert unmatched)."""


class StateStore(Protocol):
    """Persists per-entity run state (watermark, last run)."""

    def get(self, entity_name: str) -> dict | None:
        """Return the stored state, or None if the entity never ran."""

    def put(self, entity_name: str, state: dict) -> None:
        """Replace the stored state."""


class SecretProvider(Protocol):
    """Resolves secrets by scope and key."""

    def get(self, scope: str, key: str) -> str:
        """Return the secret value."""


@dataclass(frozen=True)
class Engine:
    """The ports bundled for one run."""

    reader: SourceReader
    writer: TableWriter
    state: StateStore
    secrets: SecretProvider
