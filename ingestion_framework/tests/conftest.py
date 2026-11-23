"""Shared fixtures: YAML writers and entity-dict builders."""
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def write_yaml():
    """Return a helper that dumps an object as YAML at a path (creating parents)."""
    def _write(path: Path, obj) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(obj, sort_keys=False), encoding="utf-8")
        return path
    return _write


@pytest.fixture
def entity_dict():
    """Return a builder for a minimal valid entity dict (files source, full load)."""
    def _build(name="sales.orders", layer="bronze", **over) -> dict:
        d = {
            "name": name,
            "layer": layer,
            "source": {"type": "files", "format": "csv", "path": f"/data/{name}"},
            "target": {"table": f"{layer}.{name.split('.')[-1]}"},
        }
        d.update(over)
        return d
    return _build


@pytest.fixture
def make_entities(entity_dict):
    """Return a builder: {name: [references]} -> parsed entities (depends_on carries the edges)."""
    from ingest.core.model import parse_entities

    def _build(graph: dict, layer="bronze"):
        return parse_entities([entity_dict(n, layer, depends_on=list(r)) for n, r in graph.items()])
    return _build
