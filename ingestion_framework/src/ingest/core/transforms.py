"""Registry of pure transform steps `fn(df, ctx, **args) -> DataFrame`, declared by their signature."""
from __future__ import annotations

import importlib
import inspect
import keyword
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .model import MetadataError, TransformStep


@dataclass(frozen=True)
class TransformContext:
    """What a step may use besides the DataFrame: a reader for other entities' targets."""

    read_entity: Callable[[str], DataFrame]


@dataclass(frozen=True)
class _Op:
    fn: Callable[..., DataFrame]
    required: frozenset[str]
    allowed: frozenset[str]


_REGISTRY: dict[str, _Op] = {}


def _arg_name(param: str) -> str:
    """Metadata arg for a parameter; a trailing underscore escapes Python keywords (`with_` -> `with`)."""
    base = param.removesuffix("_")
    return base if keyword.iskeyword(base) else param


def register(op: str) -> Callable:
    """Register `fn(df, ctx, **args)` as op `op`; required args are the parameters without a default."""

    def deco(fn: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
        params = list(inspect.signature(fn).parameters.values())[2:]
        _REGISTRY[op] = _Op(
            fn,
            frozenset(_arg_name(p.name) for p in params if p.default is p.empty),
            frozenset(_arg_name(p.name) for p in params),
        )
        return fn

    return deco


def validate_steps(steps: Iterable[TransformStep], entity: str) -> None:
    """Check ops and args of every step without Spark; errors name the entity, step index and op."""
    for i, step in enumerate(steps):
        where = f"entity '{entity}': transforms[{i}] op '{step.op}'"
        if step.op not in _REGISTRY:
            raise MetadataError(f"{where}: unknown op, expected one of {sorted(_REGISTRY)}")
        spec = _REGISTRY[step.op]
        if missing := sorted(spec.required - set(step.args)):
            raise MetadataError(f"{where}: missing arg(s) {', '.join(missing)}")
        if extra := sorted(set(step.args) - spec.allowed):
            raise MetadataError(f"{where}: unexpected arg(s) {', '.join(extra)}")


def apply_transforms(df: DataFrame, steps: Iterable[TransformStep], ctx: TransformContext) -> DataFrame:
    """Apply the steps in order."""
    for step in steps:
        kwargs = {(k + "_" if keyword.iskeyword(k) else k): v for k, v in step.args.items()}
        df = _REGISTRY[step.op].fn(df, ctx, **kwargs)
    return df


def _sort_column(spec: str):
    """Parse `"col"` / `"col desc"` into a sort column."""
    name, _, direction = spec.partition(" ")
    col = F.col(name)
    return col.desc() if direction.strip().lower() == "desc" else col.asc()


@register("select")
def _select(df, ctx, columns):
    return df.select(*columns)


@register("drop")
def _drop(df, ctx, columns):
    return df.drop(*columns)


@register("rename")
def _rename(df, ctx, columns):
    for old, new in columns.items():
        df = df.withColumnRenamed(old, new)
    return df


@register("cast")
def _cast(df, ctx, columns):
    return df.withColumns({c: F.col(c).cast(t) for c, t in columns.items()})


@register("filter")
def _filter(df, ctx, expr):
    return df.filter(expr)


@register("with_columns")
def _with_columns(df, ctx, columns):
    return df.withColumns({name: F.expr(e) for name, e in columns.items()})


@register("trim_strings")
def _trim_strings(df, ctx):
    return df.withColumns({f.name: F.trim(F.col(f.name)) for f in df.schema.fields
                           if isinstance(f.dataType, StringType)})


@register("drop_duplicates")
def _drop_duplicates(df, ctx, keys, order_by=None):
    if not order_by:
        return df.dropDuplicates(list(keys))
    window = Window.partitionBy(*keys).orderBy(*[_sort_column(o) for o in order_by])
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


@register("hash_key")
def _hash_key(df, ctx, columns, name):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return df.withColumn(name, F.sha2(F.concat_ws("||", *parts), 256))


@register("join")
def _join(df, ctx, with_, on, how="inner", select=None):
    right = ctx.read_entity(with_)
    if select is not None:
        right = right.select(*on, *select)
    return df.join(right, on=list(on), how=how)


@register("aggregate")
def _aggregate(df, ctx, group_by, aggs):
    keys = []
    for g in group_by:
        keys += [F.col(g)] if isinstance(g, str) else [F.expr(e).alias(n) for n, e in g.items()]
    return df.groupBy(*keys).agg(*[F.expr(e).alias(n) for n, e in aggs.items()])


@register("custom")
def _custom(df, ctx, function, args=None):
    module, _, name = function.partition(":")
    try:
        fn: Any = getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError, ValueError) as e:
        raise MetadataError(f"custom transform '{function}': cannot import ({e})") from None
    return fn(df, **(args or {}))
