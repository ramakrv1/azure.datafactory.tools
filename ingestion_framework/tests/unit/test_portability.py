import ast
from pathlib import Path

CORE = Path(__file__).parents[2] / "src" / "ingest" / "core"
FORBIDDEN = ("dbutils", "pyspark.dbutils", "notebookutils", "snowflake", "delta", "databricks", "ingest.adapters")


def _imports(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            yield from (a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            yield node.module or ""


def test_core_has_no_engine_specific_imports():
    files = sorted(CORE.glob("**/*.py"))
    assert files
    bad = [
        f"{f.name}: {m}"
        for f in files
        for m in _imports(ast.parse(f.read_text(encoding="utf-8")))
        if any(m == p or m.startswith(p + ".") for p in FORBIDDEN)
    ]
    assert not bad, bad


ADAPTERS = Path(__file__).parents[2] / "src" / "ingest" / "adapters"
SHARED_FORBIDDEN = ("dbutils", "pyspark.dbutils", "notebookutils", "databricks")


def test_shared_spark_and_delta_adapters_are_engine_neutral():
    """spark_common and delta_common serve Fabric and Databricks alike."""
    files = [ADAPTERS / "spark_common.py", ADAPTERS / "delta_common.py"]
    bad = [
        f"{f.name}: {m}"
        for f in files
        for m in _imports(ast.parse(f.read_text(encoding="utf-8")))
        if any(m == p or m.startswith(p + ".") for p in SHARED_FORBIDDEN)
    ]
    assert not bad, bad
