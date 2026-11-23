import json

import pytest

from ingest.adapters.memory import memory_engine
from ingest.runner import run_metadata

pytestmark = pytest.mark.spark


def test_runs_selected_entities_and_returns_json_summary(spark, tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("s.a"))
    write_yaml(tmp_path / "entities" / "b.yaml", entity_dict("s.b", source={"type": "files", "format": "csv",
                                                                           "path": "/missing"}))
    write_yaml(tmp_path / "entities" / "c.yaml", entity_dict("s.c", depends_on=["s.b"]))
    engine, tables = memory_engine({"/data/s.a": spark.createDataFrame([(1,)], "id int")})
    summary = run_metadata(engine, tmp_path, None, "r1", excludes=["*.c"])
    assert json.loads(json.dumps(summary)) == summary
    assert summary["build"] == "dev"  # identifies the deployed wheel; "dev" when unstamped
    assert (summary["run_id"], summary["env"], summary["succeeded"], summary["failed"], summary["skipped"]) == \
        ("r1", None, 1, 1, 0)
    assert [r["entity"] for r in summary["results"]] == ["s.a", "s.b"]
    assert summary["results"][1]["status"] == "failed" and "bronze.a" in tables


def test_variables_are_passed_to_the_project_loader(spark, tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml",
               entity_dict("s.a", source={"type": "files", "format": "csv", "path": "${ROOT}/a"}))
    engine, _ = memory_engine({"/data/a": spark.createDataFrame([(1,)], "id int")})
    summary = run_metadata(engine, tmp_path, None, "r1", variables={"ROOT": "/data"})
    assert summary["succeeded"] == 1
