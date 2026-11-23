"""Metadata model: frozen dataclasses plus parsing and validation of entity definitions."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class MetadataError(ValueError):
    """Raised for any invalid or inconsistent metadata."""


class LoadMode(str, Enum):
    """How an entity is loaded into its target."""

    full = "full"
    incremental = "incremental"
    merge = "merge"


LAYERS = ("bronze", "silver", "gold")
QUALITY_ACTIONS = ("fail", "quarantine", "warn")
SOURCE_TYPES = ("files", "jdbc", "table")
_ENTITY_KEYS = {"name", "layer", "group", "source", "load", "target", "transforms", "quality", "depends_on"}


def _mapping(value: Any, where: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise MetadataError(f"{where}: expected a mapping, got {type(value).__name__}")
    return value


def _check_keys(d: Mapping, allowed: set, where: str) -> None:
    unknown = sorted(set(d) - allowed)
    if unknown:
        raise MetadataError(f"{where}: unknown field(s) {', '.join(map(str, unknown))}")


def _list(value: Any, where: str) -> list:
    """Return `value` as a list; None means empty. Scalars and mappings are errors (a str would split into chars)."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise MetadataError(f"{where}: expected a list, got {type(value).__name__}")
    return value


def _str_tuple(value: Any, where: str) -> tuple[str, ...]:
    """Return a list of strings as a tuple; anything else is an error."""
    items = _list(value, where)
    if not all(isinstance(i, str) and i for i in items):
        raise MetadataError(f"{where}: expected a list of non-empty strings")
    return tuple(items)


def _require(d: Mapping, field_name: str, where: str) -> Any:
    if d.get(field_name) in (None, ""):
        raise MetadataError(f"{where}: missing required field '{field_name}'")
    return d[field_name]


@dataclass(frozen=True)
class TableRef:
    """A (optionally catalog-qualified) table reference."""

    schema: str
    name: str
    catalog: str | None = None

    def fqn(self) -> str:
        """Return the fully qualified name, `catalog.schema.name` or `schema.name`."""
        return ".".join(p for p in (self.catalog, self.schema, self.name) if p)

    @classmethod
    def parse(cls, text: str) -> "TableRef":
        """Parse `schema.name` or `catalog.schema.name`."""
        parts = text.split(".") if isinstance(text, str) else []
        if len(parts) not in (2, 3) or not all(parts):
            raise MetadataError(f"table '{text}': expected 'schema.name' or 'catalog.schema.name'")
        return cls(*parts[-2:], catalog=parts[0] if len(parts) == 3 else None)


@dataclass(frozen=True)
class Source:
    """Where an entity reads from; `table` names another entity (a dependency)."""

    type: str
    options: dict = field(default_factory=dict)
    format: str | None = None
    path: str | None = None
    table: str | None = None

    @classmethod
    def from_dict(cls, d: Any, where: str) -> "Source":
        """Parse and validate a source mapping."""
        d = _mapping(d, f"{where}.source")
        _check_keys(d, {"type", "options", "format", "path", "table"}, f"{where}.source")
        typ = _require(d, "type", f"{where}.source")
        if typ not in SOURCE_TYPES:
            raise MetadataError(f"{where}.source: invalid type '{typ}', expected one of {SOURCE_TYPES}")
        options = dict(_mapping(d.get("options", {}), f"{where}.source.options"))
        if typ == "files":
            _require(d, "format", f"{where}.source")
            _require(d, "path", f"{where}.source")
        elif typ == "jdbc":
            if not options:
                raise MetadataError(f"{where}.source: jdbc requires 'options'")
            _require(options, "url", f"{where}.source.options")
            if "dbtable" not in options and "query" not in options:
                raise MetadataError(f"{where}.source.options: jdbc requires 'dbtable' or 'query'")
        else:
            _require(d, "table", f"{where}.source")
        return cls(typ, options, d.get("format"), d.get("path"), d.get("table"))


@dataclass(frozen=True)
class Load:
    """Load strategy of an entity."""

    mode: LoadMode = LoadMode.full
    keys: tuple[str, ...] = ()
    watermark_column: str | None = None

    @classmethod
    def from_dict(cls, d: Any, where: str) -> "Load":
        """Parse and validate a load mapping."""
        d = _mapping(d, f"{where}.load")
        _check_keys(d, {"mode", "keys", "watermark_column"}, f"{where}.load")
        try:
            mode = LoadMode(d.get("mode", "full"))
        except ValueError:
            raise MetadataError(f"{where}.load: invalid mode '{d.get('mode')}', expected one of "
                                f"{[m.value for m in LoadMode]}") from None
        keys, wm = _str_tuple(d.get("keys"), f"{where}.load.keys"), d.get("watermark_column")
        if mode is LoadMode.merge and not keys:
            raise MetadataError(f"{where}.load: merge requires 'keys'")
        if mode is LoadMode.incremental and not wm:
            raise MetadataError(f"{where}.load: incremental requires 'watermark_column'")
        return cls(mode, keys, wm)


@dataclass(frozen=True)
class TransformStep:
    """One transform step; ops are validated by the transform registry, not here."""

    op: str
    args: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Any, where: str) -> "TransformStep":
        """Parse a transform step; a join must name its `with` entity."""
        d = _mapping(d, where)
        _check_keys(d, {"op", "args"}, where)
        op = _require(d, "op", where)
        args = dict(_mapping(d.get("args", {}), f"{where}.args"))
        if op == "join":
            _require(args, "with", f"{where}.args")
        return cls(op, args)


@dataclass(frozen=True)
class QualityRule:
    """A data-quality expression and what to do with rows that violate it."""

    name: str
    expr: str
    action: str = "quarantine"

    @classmethod
    def from_dict(cls, d: Any, where: str) -> "QualityRule":
        """Parse and validate a quality rule."""
        d = _mapping(d, where)
        _check_keys(d, {"name", "expr", "action"}, where)
        action = d.get("action", "quarantine")
        if action not in QUALITY_ACTIONS:
            raise MetadataError(f"{where}: invalid action '{action}', expected one of {QUALITY_ACTIONS}")
        return cls(_require(d, "name", where), _require(d, "expr", where), action)


@dataclass(frozen=True)
class Entity:
    """A table to ingest: source, load strategy, transforms, quality rules and target."""

    name: str
    layer: str
    group: str
    source: Source
    target: TableRef
    load: Load = field(default_factory=Load)
    transforms: tuple[TransformStep, ...] = ()
    quality: tuple[QualityRule, ...] = ()
    depends_on: tuple[str, ...] = ()

    @property
    def full_name(self) -> str:
        """Return `<layer>.<name>`."""
        return f"{self.layer}.{self.name}"

    def references(self) -> tuple[str, ...]:
        """Return the sorted, de-duplicated names of entities this one depends on."""
        refs = set(self.depends_on)
        if self.source.type == "table" and self.source.table:
            refs.add(self.source.table)
        refs.update(t.args["with"] for t in self.transforms if t.op == "join")
        return tuple(sorted(refs))

    @classmethod
    def from_dict(cls, d: Any) -> "Entity":
        """Build a validated entity from a metadata mapping."""
        d = _mapping(d, "entity")
        name = _require(d, "name", "entity")
        where = f"entity '{name}'"
        _check_keys(d, _ENTITY_KEYS, where)
        layer = _require(d, "layer", where)
        if layer not in LAYERS:
            raise MetadataError(f"{where}: invalid layer '{layer}', expected one of {LAYERS}")
        source = Source.from_dict(_require(d, "source", where), where)
        return cls(
            name=name, layer=layer, group=d.get("group") or "", source=source,
            target=_parse_target(_require(d, "target", where), where),
            load=Load.from_dict(d.get("load", {}), where),
            transforms=tuple(TransformStep.from_dict(t, f"{where}.transforms[{i}]")
                             for i, t in enumerate(_list(d.get("transforms"), f"{where}.transforms"))),
            quality=tuple(QualityRule.from_dict(q, f"{where}.quality[{i}]")
                          for i, q in enumerate(_list(d.get("quality"), f"{where}.quality"))),
            depends_on=_str_tuple(d.get("depends_on"), f"{where}.depends_on"),
        )


def _parse_target(d: Any, where: str) -> TableRef:
    d = _mapping(d, f"{where}.target")
    _check_keys(d, {"table", "catalog"}, f"{where}.target")
    try:
        ref = TableRef.parse(_require(d, "table", f"{where}.target"))
    except MetadataError as e:
        raise MetadataError(f"{where}.target: {e}") from None
    catalog = d.get("catalog")
    if catalog and ref.catalog and catalog != ref.catalog:
        raise MetadataError(f"{where}.target: catalog '{catalog}' conflicts with table '{d['table']}'")
    return TableRef(ref.schema, ref.name, ref.catalog or catalog)


def parse_entities(dicts: list[dict]) -> tuple[Entity, ...]:
    """Parse entity mappings, rejecting duplicate names."""
    entities = tuple(Entity.from_dict(d) for d in dicts)
    seen: set[str] = set()
    for e in entities:
        if e.name in seen:
            raise MetadataError(f"duplicate entity name '{e.name}'")
        seen.add(e.name)
    return entities
