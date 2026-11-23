"""Static checks of the fabric-cicd repository directory (`fabric/`): item layout the deploy relies on."""
import json
import re
import uuid
from pathlib import Path

import pytest
import yaml

FABRIC = Path(__file__).parents[2] / "fabric"
ITEMS = {"ingest_runner.Notebook": "Notebook", "ingest_env.Environment": "Environment"}


@pytest.mark.parametrize("folder,kind", ITEMS.items())
def test_platform_files_declare_type_name_and_a_real_logical_id(folder, kind):
    platform = json.loads((FABRIC / folder / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == kind
    assert platform["metadata"]["displayName"] == folder.split(".")[0]
    assert uuid.UUID(platform["config"]["logicalId"]).int != 0  # fabric-cicd rejects the all-zero default


def test_logical_ids_are_unique():
    ids = [json.loads((FABRIC / f / ".platform").read_text())["config"]["logicalId"] for f in ITEMS]
    assert len(set(ids)) == len(ids)


def test_environment_is_runtime_2_with_complete_compute_settings():
    settings = yaml.safe_load((FABRIC / "ingest_env.Environment/Setting/Sparkcompute.yml").read_text())
    assert str(settings["runtime_version"]) == "2.0"
    required = {"enable_native_execution_engine", "instance_pool_id", "driver_cores", "driver_memory",
                "executor_cores", "executor_memory", "dynamic_executor_allocation", "runtime_version"}
    assert required <= set(settings)


def test_environment_folder_holds_only_definition_files():
    """fabric-cicd uploads every file of an item folder, so no placeholders (.gitkeep) may live there."""
    files = {p.relative_to(FABRIC / "ingest_env.Environment").as_posix()
             for p in (FABRIC / "ingest_env.Environment").rglob("*") if p.is_file()}
    assert {f for f in files if not f.endswith(".whl")} == {".platform", "Setting/Sparkcompute.yml"}


def test_runner_notebook_has_parameters_cell_one_call_and_an_unwrapped_exit():
    text = (FABRIC / "ingest_runner.Notebook/notebook-content.py").read_text(encoding="utf-8")
    assert text.startswith("# Fabric notebook source\n")
    assert text.count("# PARAMETERS CELL ****") == 1
    _, params, run, final = re.split(r"^# (?:PARAMETERS )?CELL \*+$", text, flags=re.M)
    for name in ("env", "metadata_dir", "run_id", "includes", "excludes", "state_table"):
        assert re.search(rf"^{name} = ", params, flags=re.M)
    assert "run_notebook(" in run
    assert "notebookutils.notebook.exit(json.dumps(summary))" in final
    assert not re.search(r"^\s*(try|except)\b", text, flags=re.M)
    assert "default_lakehouse" not in text  # bound at run time by the deploy script, no GUIDs in the repo
