"""Data-quality rules: one-pass failure counts, then fail / quarantine / warn handling."""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Iterable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .model import QualityRule

QUARANTINE_COLUMN = "_dq_failed_rules"


class DataQualityError(Exception):
    """Raised when rows violate one or more `fail` rules."""


@dataclass(frozen=True)
class QualityResult:
    """Rows to keep, rows set aside (None without quarantine rules), per-rule failure counts, row totals."""

    valid: DataFrame
    quarantine: DataFrame | None
    failures: dict[str, int]
    quarantined: int = 0
    rows: int = 0  # rows evaluated, i.e. after transforms


def _failed(rule: QualityRule) -> Column:
    """True when the row violates the rule; a NULL result counts as a violation."""
    return ~F.coalesce(F.expr(rule.expr), F.lit(False))


def apply_quality(df: DataFrame, rules: Iterable[QualityRule]) -> QualityResult:
    """Evaluate the rules in a single aggregation, then raise, quarantine or just count per rule action."""
    rules = tuple(rules)
    if not rules:
        return QualityResult(df, None, {}, rows=df.count())
    quarantine_rules = [r for r in rules if r.action == "quarantine"]
    any_quarantined = reduce(lambda a, b: a | b, (_failed(r) for r in quarantine_rules), F.lit(False))
    row = df.agg(
        F.count(F.lit(1)).alias("_rows"),
        F.count(F.when(any_quarantined, 1)).alias("_quarantined"),
        *[F.count(F.when(_failed(r), 1)).alias(f"_r{i}") for i, r in enumerate(rules)],
    ).first()
    failures = {r.name: row[f"_r{i}"] for i, r in enumerate(rules)}
    broken = [f"{r.name} ({failures[r.name]} rows)" for r in rules if r.action == "fail" and failures[r.name]]
    if broken:
        raise DataQualityError("fail rule(s) violated: " + ", ".join(broken))
    if not quarantine_rules:
        return QualityResult(df, None, failures, rows=row["_rows"])
    tags = F.array_compact(F.array(*[F.when(_failed(r), F.lit(r.name)) for r in quarantine_rules]))
    return QualityResult(
        valid=df.filter(~any_quarantined),
        quarantine=df.withColumn(QUARANTINE_COLUMN, tags).filter(F.size(QUARANTINE_COLUMN) > 0),
        failures=failures,
        quarantined=row["_quarantined"],
        rows=row["_rows"],
    )
