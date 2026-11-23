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
