"""Microsoft Fabric adapter: the shared Spark/Delta adapters plus Key Vault secrets via `notebookutils`.

`notebookutils` exists only inside Fabric, so it is passed in (never imported here); tests pass a fake.

Naming: in a schema-enabled lakehouse a table is `lakehouse.schema.table`, so set `TableRef.catalog` to the
lakehouse name (for example with an env overlay on `target.catalog`). Lakehouse tables are managed, which is what
`DeltaWriter` creates. In secret references `${secret:<vault>/<name>}`, the scope is the Key Vault name.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from ingest.adapters.delta_common import DeltaStateStore, DeltaWriter
from ingest.adapters.spark_common import SparkReader
from ingest.core.model import TableRef
from ingest.ports import Engine
from ingest.runner import run_metadata


class FabricSecretProvider:
    """Reads Key Vault secrets as the identity running the notebook."""

    def __init__(self, notebookutils):
        self._credentials = notebookutils.credentials

    def get(self, scope: str, key: str) -> str:
        """Return secret `key` from the vault named `scope` (a full `https://` vault URL is used as given)."""
        vault_url = scope if scope.startswith("https://") else f"https://{scope}.vault.azure.net/"
        return self._credentials.getSecret(vault_url, key)


def fabric_engine(spark, notebookutils, state_table: TableRef) -> Engine:
    """Wire the ports for Fabric; run state lives in the Delta table `state_table`."""
    return Engine(SparkReader(spark), DeltaWriter(spark), DeltaStateStore(spark, state_table),
                  FabricSecretProvider(notebookutils))


def run_from_metadata(
    spark, notebookutils, metadata_dir: str | Path, env: str | None, run_id: str,
    includes: Iterable[str] = (), excludes: Iterable[str] = (), *, state_table: TableRef,
    variables: Mapping[str, str] | None = None,
) -> dict:
    """Run a metadata project on Fabric and return the JSON-serialisable summary (see `ingest.runner`)."""
    return run_metadata(fabric_engine(spark, notebookutils, state_table), metadata_dir, env, run_id, includes, excludes,
                        variables)


def _csv(text: str) -> list[str]:
    return [part.strip() for part in text.split(",") if part.strip()]


def run_notebook(
    spark, notebookutils, *, env: str, metadata_dir: str, run_id: str, includes: str, excludes: str, state_table: str,
) -> dict:
    """Entry point for the runner notebook: its parameters are plain strings (comma-separated selection patterns,
    `lakehouse.schema.table` state table, empty `run_id` = UTC timestamp)."""
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return run_from_metadata(spark, notebookutils, metadata_dir, env, run_id, _csv(includes), _csv(excludes),
                             state_table=TableRef.parse(state_table))
