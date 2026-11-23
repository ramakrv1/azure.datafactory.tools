"""Include/exclude selection of entities by wildcard (port of ApplyExclusionOptions / IsNameMatch)."""
from __future__ import annotations

from fnmatch import fnmatchcase
from pathlib import Path
from typing import Iterable

from .model import Entity


def _like(text: str, pattern: str) -> bool:
    return fnmatchcase(text.lower(), pattern.lower())


def matches(entity: Entity, pattern: str) -> bool:
    """Return True if `<layer>.<name>[@<groupPattern>]` matches the entity (case-insensitive wildcards)."""
    name_pattern, _, group_pattern = pattern.partition("@")
    if not _like(entity.full_name, name_pattern):
        return False
    return not group_pattern or _like(entity.group, group_pattern)


def select(entities: Iterable[Entity], includes: Iterable[str] = (), excludes: Iterable[str] = ()) -> tuple[Entity, ...]:
    """Keep entities matching any include; when no includes are given, drop those matching any exclude."""
    includes, excludes = tuple(includes), tuple(excludes)
    if includes:
        return tuple(e for e in entities if any(matches(e, p) for p in includes))
    return tuple(e for e in entities if not any(matches(e, p) for p in excludes))


def load_rules_file(path: str | Path) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Read a rules file (`+include`, `-exclude`, bare = include, `#` comments) into (includes, excludes)."""
    includes: list[str] = []
    excludes: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            excludes.append(line[1:].strip())
        else:
            includes.append(line.lstrip("+").strip())
    return tuple(includes), tuple(excludes)
