"""Run entities: read, filter by watermark, transform, check quality, write, save state."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Iterable, Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingest.ports import Engine, WriteMode

from .config import resolve_secrets
from .dag import topological_order
from .model import Entity, LoadMode, TableRef
from .quality import apply_quality
from .transforms import TransformContext, apply_transforms, validate_steps

_WRITE_MODES = {LoadMode.full: WriteMode.overwrite, LoadMode.incremental: WriteMode.append,
                LoadMode.merge: WriteMode.merge}


@dataclass(frozen=True)
class RunResult:
    """Outcome of one entity run. `rows_written` is `rows_read - rows_quarantined` (transforms that drop or
    multiply rows are not recounted)."""

    entity: str
    status: str  # succeeded | no_new_data | failed | skipped
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    watermark: str | None = None
    error: str | None = None


class _RecordingSecrets:
    """Wraps a SecretProvider and remembers the values it handed out, so errors can be redacted."""

    def __init__(self, inner):
        self._inner = inner
        self._values: set[str] = set()

    def get(self, scope: str, key: str) -> str:
        value = self._inner.get(scope, key)
        self._values.add(value)
        return value

    def redact(self, text: str) -> str:
        for value in self._values:
            text = text.replace(value, "***")
        return text


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_source(entity: Entity, project: Mapping[str, Entity], engine: Engine) -> DataFrame:
    source = entity.source
    if source.type == "table":
        return engine.reader.read_table(project[source.table].target)
    return engine.reader.read(replace(source, options=resolve_secrets(source.options, engine.secrets)))


def _run(entity: Entity, project: Mapping[str, Entity], engine: Engine, run_id: str, old: dict) -> RunResult:
    validate_steps(entity.transforms, entity.name)
    load, old_wm = entity.load, old.get("watermark")
    df = _read_source(entity, project, engine)
    wm_col = load.watermark_column  # a column of the source, not of the transformed output
    if wm_col and old_wm is not None:
        df = df.filter(F.col(wm_col) > F.lit(old_wm).cast(df.schema[wm_col].dataType))
    stats = df.agg(F.count(F.lit(1)).alias("n"),
                   *([F.max(wm_col).cast("string").alias("wm")] if wm_col else [])).first()
    rows_read, new_wm = stats["n"], (stats["wm"] if wm_col else None) or old_wm
    if rows_read == 0 and load.mode is not LoadMode.full:
        engine.state.put(entity.name, {**old, "last_run_id": run_id, "last_status": "no_new_data", "updated_at": _now()})
        return RunResult(entity.name, "no_new_data", watermark=old_wm)

    ctx = TransformContext(lambda name: engine.reader.read_table(project[name].target))
    out = apply_transforms(df, entity.transforms, ctx)
    out = (out.withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_id", F.lit(run_id))
           .withColumn("_source_entity", F.lit(entity.name)))
    quality = apply_quality(out, entity.quality)
    engine.writer.write(quality.valid, entity.target, _WRITE_MODES[load.mode], load.keys)
    if quality.quarantined:
        target = entity.target
        engine.writer.write(quality.quarantine, TableRef(target.schema, target.name + "_quarantine", target.catalog),
                            WriteMode.append)
    written = rows_read - quality.quarantined
    engine.state.put(entity.name, {"watermark": new_wm, "last_run_id": run_id, "last_status": "succeeded",
                                   "rows_written": written, "updated_at": _now()})
    return RunResult(entity.name, "succeeded", rows_read, written, quality.quarantined, new_wm)


def run_entity(entity: Entity, project: Mapping[str, Entity], engine: Engine, run_id: str) -> RunResult:
    """Run one entity and record its state; any exception becomes a `failed` result (watermark unchanged).

    The stored watermark is Spark's string cast of `max(watermark_column)`, advanced only after a successful write.
    """
    secrets = _RecordingSecrets(engine.secrets)
    old = engine.state.get(entity.name) or {}
    try:
        return _run(entity, project, replace(engine, secrets=secrets), run_id, old)
    except Exception as exc:  # noqa: BLE001 - every failure is reported per entity
        engine.state.put(entity.name, {**old, "last_run_id": run_id, "last_status": "failed", "updated_at": _now()})
        return RunResult(entity.name, "failed", watermark=old.get("watermark"), error=secrets.redact(str(exc)))


def run_project(
    entities: Iterable[Entity], engine: Engine, run_id: str, selected: Iterable[Entity] | None = None,
    fail_fast: bool = True,
) -> list[RunResult]:
    """Run entities dependencies-first; dependents of a failed or skipped entity are skipped, and with
    `fail_fast` so is everything after the first failure."""
    entities = tuple(entities)
    project = {e.name: e for e in entities}
    broken: set[str] = set()
    first_failure: str | None = None
    results: list[RunResult] = []
    for entity in topological_order(entities, selected):
        upstream = sorted(broken & set(entity.references()))
        if upstream or (fail_fast and first_failure):
            reason = f"upstream '{upstream[0]}' did not succeed" if upstream else f"fail_fast after '{first_failure}'"
            results.append(RunResult(entity.name, "skipped", error=reason))
            broken.add(entity.name)
            continue
        result = run_entity(entity, project, engine, run_id)
        results.append(result)
        if result.status == "failed":
            broken.add(entity.name)
            first_failure = first_failure or entity.name
    return results
