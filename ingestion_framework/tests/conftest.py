"""Shared fixtures: one Delta-enabled local SparkSession, YAML writers and entity-dict builders."""
from pathlib import Path

import pytest
import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """The one SparkSession per process: local, with Delta enabled (harmless for tests that ignore it)."""
    builder = (
        SparkSession.builder.master("local[1]").appName("ingest-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


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
