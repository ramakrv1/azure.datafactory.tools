"""Engine-neutral run entry point: metadata directory in, JSON-serialisable summary out."""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Mapping

from ingest.build import build_id
from ingest.core.config import load_project
from ingest.core.pipeline import run_project
from ingest.core.selection import select
from ingest.ports import Engine


def run_metadata(
    engine: Engine, metadata_dir: str | Path, env: str | None, run_id: str,
    includes: Iterable[str] = (), excludes: Iterable[str] = (), variables: Mapping[str, str] | None = None,
) -> dict:
    """Load the project, run the selected entities and summarise.

    `succeeded` counts `succeeded` and `no_new_data` results (both are healthy); `results` has every detail. `variables` override the overlay's token values.
    """
    entities = load_project(metadata_dir, env, variables=variables)
    results = run_project(entities, engine, run_id, select(entities, includes, excludes))
    count = lambda *statuses: sum(r.status in statuses for r in results)  # noqa: E731
    return {"run_id": run_id, "env": env, "build": build_id(), "succeeded": count("succeeded", "no_new_data"),
            "failed": count("failed"), "skipped": count("skipped"), "results": [asdict(r) for r in results]}
