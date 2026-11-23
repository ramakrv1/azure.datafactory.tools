import json
from types import SimpleNamespace

import pytest

from ingest.adapters.fabric import FabricSecretProvider, fabric_engine, run_from_metadata
from ingest.core.model import Entity, TableRef
from ingest.core.pipeline import run_entity
from tests.spark.conftest import assert_rows


class FakeNotebookUtils:
    """Records `credentials.getSecret` calls and returns `secret:<key>`."""

    def __init__(self):
        self.calls = []
        self.credentials = SimpleNamespace(getSecret=self._get_secret)

    def _get_secret(self, vault_url, key):
        self.calls.append((vault_url, key))
        return f"secret:{key}"


def test_secret_provider_builds_the_vault_url_from_the_scope():
    nbu = FakeNotebookUtils()
    assert FabricSecretProvider(nbu).get("kv", "pwd") == "secret:pwd"
    assert nbu.calls == [("https://kv.vault.azure.net/", "pwd")]


def test_secret_provider_passes_a_full_url_through():
    nbu = FakeNotebookUtils()
    FabricSecretProvider(nbu).get("https://other.vault.azure.net/", "k")
    assert nbu.calls == [("https://other.vault.azure.net/", "k")]


@pytest.mark.spark
def test_fabric_engine_runs_an_entity_end_to_end(spark, tmp_path):
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id,v\n1,a\n", encoding="utf-8")
    nbu = FakeNotebookUtils()
    engine = fabric_engine(spark, nbu, TableRef("fab_state", "state"))
    entity = Entity.from_dict({
        "name": "fab", "layer": "bronze",
        "source": {"type": "files", "format": "csv", "path": str(tmp_path / "in"),
                   "options": {"header": "true", "note": "${secret:vault1/token}"}},
        "target": {"table": "fab_bronze.t"}})
    result = run_entity(entity, {"fab": entity}, engine, "r1")
    assert result.status == "succeeded" and result.rows_written == 1
    assert nbu.calls == [("https://vault1.vault.azure.net/", "token")]
    assert_rows(spark.table("fab_bronze.t").select("id", "v"), [{"id": "1", "v": "a"}])


@pytest.mark.spark
def test_run_from_metadata_returns_a_json_summary(spark, tmp_path, write_yaml, entity_dict):
    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.csv").write_text("id\n1\n", encoding="utf-8")
    write_yaml(tmp_path / "meta" / "entities" / "a.yaml", entity_dict(
        "fabrun.a", source={"type": "files", "format": "csv", "path": str(tmp_path / "in"),
                            "options": {"header": "true"}}, target={"table": "fab_run.a"}))
    summary = run_from_metadata(spark, FakeNotebookUtils(), tmp_path / "meta", None, "r9",
                                state_table=TableRef("fab_state", "run_state"))
    assert json.loads(json.dumps(summary))["succeeded"] == 1 and summary["run_id"] == "r9"


# ---- run_notebook: the notebook's one call (string parameters in, summary out)
def test_run_notebook_parses_notebook_parameters(monkeypatch):
    from ingest.adapters import fabric

    seen = {}
    monkeypatch.setattr(fabric, "run_from_metadata", lambda *a, **kw: seen.update(args=a, kw=kw) or {"ok": 1})
    result = fabric.run_notebook("spark", "nbu", env="fabric-dev", metadata_dir="/m", run_id="42",
                                 includes=" sales.* , x ", excludes="", state_table="lh.ops.st")
    assert result == {"ok": 1}
    assert seen["args"] == ("spark", "nbu", "/m", "fabric-dev", "42", ["sales.*", "x"], [])
    assert seen["kw"] == {"state_table": TableRef("ops", "st", "lh")}


def test_run_notebook_defaults_the_run_id_to_a_utc_timestamp(monkeypatch):
    import re
    from ingest.adapters import fabric

    seen = {}
    monkeypatch.setattr(fabric, "run_from_metadata", lambda *a, **kw: seen.update(args=a) or {})
    fabric.run_notebook("s", "n", env="e", metadata_dir="/m", run_id="", includes="", excludes="",
                        state_table="lh.ops.st")
    assert re.fullmatch(r"\d{8}T\d{6}Z", seen["args"][4])
