"""Load entity metadata, apply a per-environment overlay and expand ${TOKEN}s (port of Update-PropertiesFromFile)."""
from __future__ import annotations

import os
import re
import warnings
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Callable, Mapping

import yaml

from ingest.ports import SecretProvider

from .model import Entity, MetadataError, parse_entities

_ACTIONS = ("update", "add", "remove")
_TOKEN = re.compile(r"\$\$\{(?P<esc>[^}]*)\}|\$\{(?P<name>[^}]*)\}")


@dataclass(frozen=True)
class OverlayReport:
    """Counts of overlay operations that took effect."""

    updated: int = 0
    added: int = 0
    removed: int = 0


def _read_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_entity_dicts(metadata_dir: str | Path) -> list[dict]:
    """Read `entities/**/*.yaml|yml` (sorted); each file holds one mapping or a list of mappings."""
    root = Path(metadata_dir) / "entities"
    files = sorted(p for p in root.glob("**/*") if p.suffix in (".yaml", ".yml"))
    out: list[dict] = []
    for f in files:
        content = _read_yaml(f)
        items = content if isinstance(content, list) else [content]
        if not all(isinstance(i, dict) for i in items):
            raise MetadataError(f"{f}: expected a mapping or a list of mappings")
        out.extend(items)
    return out


def _split_path(path: str, action: str) -> tuple[list[str], str]:
    """Strip `$.`/`$.properties.` and a `+`/`-` prefix; return (keys, action)."""
    path = path.strip()
    if path[:1] in ("+", "-"):
        action = "add" if path[0] == "+" else "remove"
        path = path[1:]
    for prefix in ("$.properties.", "$."):
        if path.startswith(prefix):
            path = path[len(prefix):]
            break
    keys = [k for k in path.split(".") if k]
    if not keys:
        raise MetadataError("override: empty 'path'")
    return keys, action


def _parent(d: dict, keys: list[str]) -> dict | None:
    """Return the dict holding the leaf key, or None if an intermediate step is missing."""
    for k in keys[:-1]:
        d = d.get(k)
        if not isinstance(d, dict):
            return None
    return d


def _path_not_found(entity: str, path: str, fail: bool) -> None:
    msg = f"entity '{entity}': path '{path}' not found"
    if fail:
        raise MetadataError(msg)
    warnings.warn(msg, stacklevel=3)


def apply_overlay(
    entity_dicts: list[dict],
    overrides: list[Mapping],
    *,
    fail_when_entity_not_found: bool = True,
    fail_when_path_not_found: bool = True,
) -> OverlayReport:
    """Apply update/add/remove overrides, in order, to the matching entity dicts (mutated in place)."""
    counts = {"update": 0, "add": 0, "remove": 0}
    for i, o in enumerate(overrides):
        where = f"override[{i}]"
        extra = sorted(set(o) - {"entity", "path", "value", "action"})
        if extra:
            raise MetadataError(f"{where}: unknown field(s) {', '.join(extra)}")
        for req in ("entity", "path"):
            if not o.get(req):
                raise MetadataError(f"{where}: missing required field '{req}'")
        action = o.get("action", "update")
        if action not in _ACTIONS:
            raise MetadataError(f"{where}: invalid action '{action}', expected one of {_ACTIONS}")
        keys, action = _split_path(o["path"], action)
        pattern = o["entity"].lower()
        hits = [d for d in entity_dicts if fnmatchcase(str(d.get("name", "")).lower(), pattern)]
        if not hits:
            msg = f"{where}: no entity matches '{o['entity']}'"
            if fail_when_entity_not_found:
                raise MetadataError(msg)
            warnings.warn(msg, stacklevel=2)
            continue
        leaf = keys[-1]
        for d in hits:
            if action == "add":
                parent = d
                for depth, k in enumerate(keys[:-1], 1):
                    parent = parent.setdefault(k, {})
                    if not isinstance(parent, dict):
                        raise MetadataError(
                            f"{where}: entity '{d['name']}': '{'.'.join(keys[:depth])}' is not a mapping"
                        )
                parent[leaf] = o.get("value")
            else:
                parent = _parent(d, keys)
                if parent is None or leaf not in parent:
                    _path_not_found(d["name"], ".".join(keys), fail_when_path_not_found)
                    continue
                if action == "update":
                    parent[leaf] = o.get("value")
                else:
                    del parent[leaf]
            counts[action] += 1
    return OverlayReport(counts["update"], counts["add"], counts["remove"])


def _walk_strings(obj: Any, fn: Callable[[str], str]) -> Any:
    """Return a copy of a nested dict/list structure with `fn` applied to every string."""
    if isinstance(obj, str):
        return fn(obj)
    if isinstance(obj, dict):
        return {k: _walk_strings(v, fn) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_strings(v, fn) for v in obj]
    return obj


def expand_tokens(obj: Any, variables: Mapping[str, str], environ: Mapping[str, str] | None = None) -> Any:
    """Replace `${NAME}` in all strings (variables, then environ); keep `${secret:..}`, unescape `$${X}`."""
    environ = os.environ if environ is None else environ
    missing: set[str] = set()

    def sub(m: re.Match) -> str:
        if m["esc"] is not None:
            return "${" + m["esc"] + "}"
        name = m["name"]
        if name.startswith("secret:"):
            return m.group(0)
        if name in variables:
            return str(variables[name])
        if name in environ:
            return environ[name]
        missing.add(name)
        return m.group(0)

    result = _walk_strings(obj, lambda text: _TOKEN.sub(sub, text))
    if missing:
        raise MetadataError(f"unresolved token(s): {', '.join(sorted(missing))}")
    return result


def resolve_secrets(obj: Any, provider: SecretProvider) -> Any:
    """Replace `${secret:scope/key}` in all strings via `provider`; call only at run time, never log the result."""

    def sub(m: re.Match) -> str:
        name = m["name"]
        if m["esc"] is not None or not name.startswith("secret:"):
            return m.group(0)
        scope, sep, key = name[len("secret:"):].partition("/")
        if not (sep and scope and key):
            raise MetadataError(f"malformed secret token '${{{name}}}': expected ${{secret:scope/key}}")
        return provider.get(scope, key)

    return _walk_strings(obj, lambda text: _TOKEN.sub(sub, text))


def _load_overlay(metadata_dir: Path, env: str) -> dict:
    for ext in (".yaml", ".yml"):
        path = metadata_dir / "env" / f"{env}{ext}"
        if path.exists():
            overlay = _read_yaml(path) or {}
            extra = sorted(set(overlay) - {"variables", "overrides", "options"})
            if extra:
                raise MetadataError(f"{path}: unknown field(s) {', '.join(extra)}")
            return overlay
    raise MetadataError(f"environment '{env}': no overlay file env/{env}.yaml in {metadata_dir}")


def load_project(
    metadata_dir: str | Path, env: str | None, environ: Mapping[str, str] | None = None,
    variables: Mapping[str, str] | None = None,
) -> tuple[Entity, ...]:
    """Load entities, apply the `env` overlay (if any), expand tokens and parse into entities.

    Token precedence: explicit `variables`, then the overlay's `variables`, then `environ`.
    """
    metadata_dir = Path(metadata_dir)
    dicts = load_entity_dicts(metadata_dir)
    merged: dict[str, str] = {}
    if env:
        overlay = _load_overlay(metadata_dir, env)
        merged.update(overlay.get("variables") or {})
        apply_overlay(dicts, overlay.get("overrides") or [], **(overlay.get("options") or {}))
    merged.update(variables or {})
    return parse_entities(expand_tokens(dicts, merged, environ))
