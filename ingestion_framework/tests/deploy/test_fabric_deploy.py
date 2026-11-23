"""Fabric deploy script, tested with fakes only: no network, no Azure, no fabric-cicd calls."""
import json
from types import SimpleNamespace

import pytest

from deploy import fabric_deploy as fd

WS, LH, NB, ENV = "ws-id", "lh-id", "nb-id", "env-id"


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}
        self.text = json.dumps(body) if body is not None else ""

    def json(self):
        return self._body


class FakeSession:
    """Scripted `requests.Session` stand-in: hands out queued responses in order and records every call."""

    def __init__(self, *responses):
        self.queue, self.calls = list(responses), []

    def request(self, method, url, **kwargs):
        self.calls.append(SimpleNamespace(method=method, url=url, **kwargs))
        return self.queue.pop(0)


def client(*responses, clock=None):
    session = FakeSession(*responses)
    sleeps = []
    ticks = iter(range(0, 10_000, 1)) if clock is None else clock
    return fd.FabricClient(session, lambda: "TOKEN", sleep=sleeps.append, clock=lambda: next(ticks)), session, sleeps


# ---- payload builders
def test_lakehouse_body_is_schema_enabled():
    assert fd.lakehouse_body("ingest_lh") == {"displayName": "ingest_lh", "creationPayload": {"enableSchemas": True}}


def test_notebook_run_body_binds_lakehouse_environment_and_typed_parameters():
    body = fd.notebook_run_body(WS, LH, ENV, {"env": "fabric-dev", "run_id": "7"})
    assert body == {
        "executionData": {"compute": "Spark", "computeConfiguration": {
            "defaultLakehouse": {"referenceType": "ById", "itemId": LH, "workspaceId": WS},
            "attachedEnvironment": {"referenceType": "ById", "itemId": ENV, "workspaceId": WS}}},
        "parameters": [{"name": "env", "value": "fabric-dev", "type": "Text"},
                       {"name": "run_id", "value": "7", "type": "Text"}]}


# ---- lakehouse find / create
def test_ensure_lakehouse_returns_an_existing_one_without_creating():
    c, s, _ = client(FakeResponse(body={"value": [{"id": "x", "displayName": "other"},
                                                  {"id": LH, "displayName": "ingest_lh"}]}))
    assert c.ensure_lakehouse(WS, "ingest_lh") == LH
    assert [(x.method, x.url) for x in s.calls] == [("GET", f"{fd.FABRIC_API}/workspaces/{WS}/items?type=Lakehouse")]
    assert s.calls[0].headers == {"Authorization": "Bearer TOKEN"}


def test_list_items_follows_continuation_tokens():
    c, s, _ = client(FakeResponse(body={"value": [{"id": "1", "displayName": "a"}], "continuationToken": "t 1"}),
                     FakeResponse(body={"value": [{"id": LH, "displayName": "ingest_lh"}]}))
    assert c.find_item(WS, "Lakehouse", "ingest_lh")["id"] == LH
    assert s.calls[1].url.endswith("?type=Lakehouse&continuationToken=t%201")


def test_ensure_lakehouse_creates_when_missing_201():
    c, s, _ = client(FakeResponse(body={"value": []}), FakeResponse(201, {"id": LH, "displayName": "ingest_lh"}))
    assert c.ensure_lakehouse(WS, "ingest_lh") == LH
    assert (s.calls[1].method, s.calls[1].url) == ("POST", f"{fd.FABRIC_API}/workspaces/{WS}/lakehouses")
    assert s.calls[1].json == fd.lakehouse_body("ingest_lh")


def test_ensure_lakehouse_polls_the_long_running_operation_on_202():
    op = "https://api.fabric.microsoft.com/v1/operations/op1"
    c, s, sleeps = client(
        FakeResponse(body={"value": []}),
        FakeResponse(202, headers={"Location": op, "Retry-After": "5"}),
        FakeResponse(body={"status": "Running"}, headers={"Retry-After": "2"}),
        FakeResponse(body={"status": "Succeeded"}),
        FakeResponse(body={"value": [{"id": LH, "displayName": "ingest_lh"}]}))
    assert c.ensure_lakehouse(WS, "ingest_lh") == LH
    assert [x.url for x in s.calls[2:4]] == [op, op] and sleeps == [2]


def test_failed_operation_raises():
    c, _, _ = client(FakeResponse(body={"value": []}), FakeResponse(202, headers={"Location": "u"}),
                     FakeResponse(body={"status": "Failed", "error": {"message": "boom"}}))
    with pytest.raises(fd.DeployError, match="boom"):
        c.ensure_lakehouse(WS, "ingest_lh")


def test_http_errors_raise_without_leaking_the_token():
    c, _, _ = client(FakeResponse(403, {"errorCode": "Forbidden"}))
    with pytest.raises(fd.DeployError, match="403") as err:
        c.list_items(WS, "Lakehouse")
    assert "TOKEN" not in str(err.value)


def test_throttled_requests_are_retried_after_retry_after():
    c, _, sleeps = client(FakeResponse(429, headers={"Retry-After": "3"}), FakeResponse(body={"value": []}))
    assert c.list_items(WS, "Notebook") == []
    assert sleeps == [3]


# ---- notebook job
def test_start_notebook_run_posts_to_the_release_endpoint_and_returns_the_job_id():
    loc = f"{fd.FABRIC_API}/workspaces/{WS}/items/{NB}/jobs/instances/job-9"
    c, s, _ = client(FakeResponse(202, headers={"Location": loc}))
    assert c.start_notebook_run(WS, NB, {"parameters": []}) == "job-9"
    assert (s.calls[0].method, s.calls[0].url) == (
        "POST", f"{fd.FABRIC_API}/workspaces/{WS}/notebooks/{NB}/jobs/execute/instances?beta=false")


def test_wait_for_job_polls_the_beta_endpoint_until_terminal():
    done = {"status": "Completed", "exitValue": "{}"}
    c, s, sleeps = client(FakeResponse(body={"status": "NotStarted"}), FakeResponse(body={"status": "InProgress"}),
                          FakeResponse(body=done))
    assert c.wait_for_job(WS, NB, "j", timeout_s=600, poll_s=7) == done
    assert s.calls[0].url == f"{fd.FABRIC_API}/workspaces/{WS}/notebooks/{NB}/jobs/execute/instances/j?beta=true"
    assert sleeps == [7, 7]


def test_wait_for_job_times_out():
    c, _, _ = client(*[FakeResponse(body={"status": "InProgress"})] * 50)
    with pytest.raises(fd.DeployError, match="timed out"):
        c.wait_for_job(WS, NB, "j", timeout_s=5, poll_s=1)


# ---- exit-value evaluation
def summary(**over):
    base = {"run_id": "1", "env": "fabric-dev", "succeeded": 3, "failed": 0, "skipped": 0, "results": [
        {"entity": "a", "status": "succeeded", "rows_read": 9, "rows_written": 8, "rows_quarantined": 1,
         "watermark": None, "error": None}]}
    return {**base, **over}


def job(status="Completed", exit_value=None, **over):
    return {"status": status, "exitValue": json.dumps(summary()) if exit_value is None else exit_value, **over}


def test_evaluate_accepts_a_clean_run():
    outcome = fd.evaluate_run(job())
    assert outcome.ok and outcome.problems == [] and outcome.summary["succeeded"] == 3


@pytest.mark.parametrize("j,expected", [
    (job(exit_value=json.dumps(summary(failed=1))), "1 entity failed"),
    (job(exit_value=json.dumps(summary(skipped=2))), "2 entities skipped"),
    (job(exit_value="not json"), "not JSON"),
    (job(exit_value="[1]"), "not a run summary"),
    ({"status": "Completed"}, "no exit value"),
    (job("Failed", exit_value="", failureReason={"message": "OOM"}), "Failed"),
    (job("Cancelled", exit_value=""), "Cancelled"),
])
def test_evaluate_rejects_bad_runs(j, expected):
    outcome = fd.evaluate_run(j)
    assert not outcome.ok and any(expected in p for p in outcome.problems), outcome.problems


def test_evaluate_rejects_a_stale_build():
    outcome = fd.evaluate_run(job(exit_value=json.dumps(summary(build="old"))), expected_build="new")
    assert not outcome.ok and any("stale" in p and "old" in p and "new" in p for p in outcome.problems)


def test_evaluate_accepts_the_expected_build():
    assert fd.evaluate_run(job(exit_value=json.dumps(summary(build="new"))), expected_build="new").ok


def test_failed_job_reports_the_failure_reason():
    outcome = fd.evaluate_run(job("Failed", exit_value="", failureReason={"message": "OOM"}))
    assert any("OOM" in p for p in outcome.problems)


def test_format_table_lists_every_entity():
    text = fd.format_table(summary())
    assert text.splitlines()[0].split() == ["entity", "status", "read", "written", "quarantined", "error"]
    assert "a" in text.splitlines()[1] and "succeeded" in text.splitlines()[1]


# ---- OneLake upload
def test_plan_uploads_maps_local_files_under_the_lakehouse_files_area(tmp_path):
    (tmp_path / "entities").mkdir()
    (tmp_path / "entities" / "a.yaml").write_text("x")
    (tmp_path / "top.csv").write_text("y")
    plan = fd.plan_uploads(tmp_path, LH, "Files/ingest/metadata")
    assert sorted((p.relative_to(tmp_path).as_posix(), remote) for p, remote in plan) == [
        ("entities/a.yaml", f"{LH}/Files/ingest/metadata/entities/a.yaml"),
        ("top.csv", f"{LH}/Files/ingest/metadata/top.csv")]


def test_upload_files_overwrites_each_remote_file(tmp_path):
    (tmp_path / "a.csv").write_bytes(b"1,2")
    uploaded = {}

    class FileClient:
        def __init__(self, path):
            self.path = path

        def upload_data(self, data, overwrite):
            uploaded[self.path] = (data.read() if hasattr(data, "read") else data, overwrite)

    fs = SimpleNamespace(get_file_client=FileClient)
    fd.upload_files(fs, fd.plan_uploads(tmp_path, LH, "Files/ingest/data"))
    assert uploaded == {f"{LH}/Files/ingest/data/a.csv": (b"1,2", True)}


def test_plan_uploads_requires_an_existing_directory(tmp_path):
    with pytest.raises(fd.DeployError, match="missing"):
        fd.plan_uploads(tmp_path / "missing", LH, "Files/x")


# ---- wheel staging
def test_stage_wheel_copies_the_single_wheel_and_drops_stale_ones(tmp_path):
    dist, repo = tmp_path / "dist", tmp_path / "fabric"
    libs = repo / "ingest_env.Environment" / "Libraries" / "CustomLibraries"
    libs.mkdir(parents=True), dist.mkdir()
    (libs / "ingest-0.0.1-py3-none-any.whl").write_bytes(b"old")
    (dist / "ingest-0.1.0-py3-none-any.whl").write_bytes(b"new")
    assert fd.stage_wheel(dist, repo).name == "ingest-0.1.0-py3-none-any.whl"
    assert [p.name for p in libs.iterdir()] == ["ingest-0.1.0-py3-none-any.whl"]
    assert (libs / "ingest-0.1.0-py3-none-any.whl").read_bytes() == b"new"


@pytest.mark.parametrize("names", [[], ["a-1-py3-none-any.whl", "b-1-py3-none-any.whl"]])
def test_stage_wheel_needs_exactly_one_wheel(tmp_path, names):
    dist = tmp_path / "dist"
    dist.mkdir()
    for n in names:
        (dist / n).write_bytes(b"")
    with pytest.raises(fd.DeployError, match="wheel"):
        fd.stage_wheel(dist, tmp_path / "fabric")


# ---- fabric-cicd
def test_publish_items_scopes_to_notebook_and_environment_and_keeps_orphans():
    seen = {}

    class Workspace:
        def __init__(self, **kw):
            seen["init"] = kw

    fd.publish_items(WS, "/repo", "CRED", workspace_cls=Workspace,
                     publish=lambda ws, **kw: seen.update(ws=ws, publish_kw=kw))
    assert seen["init"] == {"workspace_id": WS, "repository_directory": "/repo",
                            "item_type_in_scope": ["Notebook", "Environment"], "token_credential": "CRED"}
    assert isinstance(seen["ws"], Workspace) and seen["publish_kw"] == {}  # no unpublish of other items


def test_fabric_cicd_api_matches_what_the_script_calls():
    inspect = pytest.importorskip("inspect")
    fabric_cicd = pytest.importorskip("fabric_cicd")
    params = inspect.signature(fabric_cicd.FabricWorkspace.__init__).parameters
    assert {"workspace_id", "repository_directory", "item_type_in_scope", "token_credential"} <= set(params)
    assert callable(fabric_cicd.publish_all_items)


# ---- orchestration
class FakeSteps:
    """Stands in for credential-backed collaborators of `deploy`."""

    def __init__(self, job_result):
        self.log, self.job_result = [], job_result

    def publish(self):
        self.log.append("publish")

    def upload(self, lakehouse_id, metadata_dir, sample_dir):
        self.log.append(("upload", lakehouse_id, str(metadata_dir), str(sample_dir)))


def config(**over):
    base = dict(workspace_id=WS, repo_dir="fabric", lakehouse="ingest_lh", env="fabric-dev", metadata_dir="meta",
                sample_data_dir="data", smoke=True, timeout_min=1, run_id="55", wheel_dir=None, expect_build=None)
    return SimpleNamespace(**{**base, **over})


def scripted_client(job_result):
    return client(
        FakeResponse(body={"value": [{"id": LH, "displayName": "ingest_lh"}]}),       # ensure_lakehouse
        FakeResponse(body={"value": [{"id": NB, "displayName": "ingest_runner"}]}),    # notebook id
        FakeResponse(body={"value": [{"id": ENV, "displayName": "ingest_env"}]}),      # environment id
        FakeResponse(202, headers={"Location": f"{fd.FABRIC_API}/x/jobs/instances/j1"}),
        FakeResponse(body=job_result))


def test_deploy_runs_the_steps_in_order_and_returns_zero_on_a_clean_smoke_run(capsys):
    c, s, _ = scripted_client(job())
    steps = FakeSteps(None)
    assert fd.deploy(config(), c, steps.publish, steps.upload) == 0
    assert steps.log == ["publish", ("upload", LH, "meta", "data")]
    body = s.calls[3].json
    assert {"name": "run_id", "value": "55", "type": "Text"} in body["parameters"]
    assert "succeeded" in capsys.readouterr().out


def test_deploy_returns_one_when_an_entity_failed(capsys):
    c, _, _ = scripted_client(job(exit_value=json.dumps(summary(failed=1))))
    assert fd.deploy(config(), c, lambda: None, lambda *a: None) == 1
    assert "1 entity failed" in capsys.readouterr().err


def test_deploy_without_smoke_stops_after_the_upload():
    c, s, _ = client(FakeResponse(body={"value": [{"id": LH, "displayName": "ingest_lh"}]}))
    steps = FakeSteps(None)
    assert fd.deploy(config(smoke=False), c, steps.publish, steps.upload) == 0
    assert len(s.calls) == 1 and steps.log[0] == "publish"


def test_parse_args_defaults_and_smoke_flag():
    args = fd.parse_args(["--workspace-id", WS, "--repo-dir", "fabric", "--metadata-dir", "m",
                          "--sample-data-dir", "d"])
    assert (args.lakehouse, args.env, args.smoke, args.timeout_min) == ("ingest_lh", "fabric-dev", True, 30)
    assert fd.parse_args(["--workspace-id", WS, "--repo-dir", "r", "--metadata-dir", "m", "--sample-data-dir", "d",
                          "--no-smoke", "--wheel-dir", "dist"]).smoke is False


def test_default_run_id_prefers_the_github_run_id(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    assert fd.default_run_id() == "123"
    monkeypatch.delenv("GITHUB_RUN_ID")
    assert fd.default_run_id().endswith("Z")


def test_deploy_stages_the_wheel_before_publishing(tmp_path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "ingest-0.1.0-py3-none-any.whl").write_bytes(b"w")
    c, _, _ = client(FakeResponse(body={"value": [{"id": LH, "displayName": "ingest_lh"}]}))
    seen = []
    cfg = config(smoke=False, wheel_dir=str(dist), repo_dir=str(tmp_path / "fabric"))
    fd.deploy(cfg, c, lambda: seen.append(list((tmp_path / "fabric").rglob("*.whl"))), lambda *a: None)
    assert [p.name for p in seen[0]] == ["ingest-0.1.0-py3-none-any.whl"]
