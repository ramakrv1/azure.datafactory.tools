"""Dependency graph of entities: validation, deterministic ordering, parallel levels, Mermaid diagram."""
from __future__ import annotations

import re
from typing import Iterable

from .model import Entity, MetadataError


class CycleError(MetadataError):
    """Raised when entity references form a cycle."""


def build_graph(entities: Iterable[Entity]) -> dict[str, frozenset[str]]:
    """Map each entity name to the names it references; unknown references are errors."""
    entities = tuple(entities)
    known = {e.name for e in entities}
    graph: dict[str, frozenset[str]] = {}
    for e in entities:
        for ref in e.references():
            if ref not in known:
                raise MetadataError(f"entity '{e.name}' references unknown entity '{ref}'")
        graph[e.name] = frozenset(e.references())
    return graph


def _find_cycle(graph: dict[str, frozenset[str]], nodes: set[str]) -> list[str]:
    """Return one cycle (first node repeated at the end) among `nodes`."""
    path: list[str] = []
    done: set[str] = set()

    def visit(n: str) -> list[str] | None:
        if n in path:
            return path[path.index(n):] + [n]
        if n in done:
            return None
        path.append(n)
        for m in sorted(graph[n] & nodes):
            found = visit(m)
            if found:
                return found
        path.pop()
        done.add(n)
        return None

    for n in sorted(nodes):
        found = visit(n)
        if found:
            return found
    return []


def _blocked(graph: dict[str, frozenset[str]], remaining: dict[str, frozenset[str]]) -> CycleError:
    cycle = _find_cycle(graph, set(remaining))
    return CycleError("dependency cycle: " + " -> ".join(cycle))


def levels(entities: Iterable[Entity]) -> list[list[str]]:
    """Group entity names into waves that can run in parallel (each sorted by name)."""
    graph = build_graph(entities)
    remaining = dict(graph)
    waves: list[list[str]] = []
    while remaining:
        ready = sorted(n for n, deps in remaining.items() if not deps & remaining.keys())
        if not ready:
            raise _blocked(graph, remaining)
        waves.append(ready)
        for n in ready:
            del remaining[n]
    return waves


def topological_order(entities: Iterable[Entity], selected: Iterable[Entity] | None = None) -> list[Entity]:
    """Order entities dependencies-first (Kahn, ties by name); optionally restrict to `selected`."""
    entities = tuple(entities)
    by_name = {e.name: e for e in entities}
    graph = build_graph(entities)
    remaining = dict(graph)
    order: list[str] = []
    while remaining:
        ready = [n for n, deps in remaining.items() if not deps & remaining.keys()]
        if not ready:
            raise _blocked(graph, remaining)
        order.append(min(ready))
        del remaining[order[-1]]
    wanted = None if selected is None else {e.name for e in selected}
    return [by_name[n] for n in order if wanted is None or n in wanted]


def to_mermaid(entities: Iterable[Entity]) -> str:
    """Render the dependency graph as a Mermaid `graph LR` (edge = dependency --> dependent)."""
    entities = sorted(entities, key=lambda e: e.name)
    node = lambda name: re.sub(r"\W", "_", name)  # noqa: E731
    lines = ["graph LR"]
    lines += [f'  {node(e.name)}["{e.full_name}"]' for e in entities]
    lines += [f"  {node(ref)} --> {node(e.name)}" for e in entities for ref in e.references()]
    return "\n".join(lines) + "\n"
