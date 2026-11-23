"""Deploy the ingestion framework to a Microsoft Fabric workspace and smoke-test it.

Steps: ensure the schema-enabled lakehouse, publish the runner notebook and the environment (fabric-cicd), upload the
metadata and sample data to OneLake, run the notebook once and check its JSON exit value.

This is a deploy-time script, not part of the `ingest` wheel. Needs the `deploy` extra
(`pip install "./ingestion_framework[deploy]"`); the Azure/fabric-cicd imports are lazy so the pure parts test without it.
Nothing here prints tokens or secrets.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import quote

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"  # OneLake accepts only storage-audience tokens
NOTEBOOK_NAME, ENVIRONMENT_NAME = "ingest_runner", "ingest_env"
JOB_TERMINAL = {"Completed", "Failed", "Cancelled", "Deduped"}
MAX_THROTTLE_RETRIES = 3


class DeployError(RuntimeError):
    """A deploy step failed; the message never contains credentials."""


# ---- payload builders (pure) -------------------------------------------------------------------------------------
def lakehouse_body(name: str) -> dict:
    """Create Lakehouse request; `enableSchemas` is required for `lakehouse.schema.table` names."""
    return {"displayName": name, "creationPayload": {"enableSchemas": True}}


def notebook_run_body(workspace_id: str, lakehouse_id: str, environment_id: str, parameters: Mapping[str, str]) -> dict:
    """Run-on-demand-notebook body: binds the default lakehouse and environment per run (no GUIDs in the repo)."""
    ref = lambda item_id: {"referenceType": "ById", "itemId": item_id, "workspaceId": workspace_id}  # noqa: E731
    return {
        "executionData": {"compute": "Spark", "computeConfiguration": {
            "defaultLakehouse": ref(lakehouse_id), "attachedEnvironment": ref(environment_id)}},
        "parameters": [{"name": k, "value": v, "type": "Text"} for k, v in parameters.items()],
    }


# ---- Fabric REST client ------------------------------------------------------------------------------------------
class FabricClient:
    """Thin wrapper over a `requests.Session`-like object (`.request(method, url, **kw)`); bearer token per call."""

    def __init__(self, session, get_token: Callable[[], str], *, sleep=time.sleep, clock=time.monotonic):
        self._session, self._get_token, self._sleep, self._clock = session, get_token, sleep, clock

    def _request(self, method: str, url: str, **kwargs):
        for attempt in range(MAX_THROTTLE_RETRIES + 1):
            response = self._session.request(method, url, headers={"Authorization": f"Bearer {self._get_token()}"},
                                             **kwargs)
            if response.status_code == 429 and attempt < MAX_THROTTLE_RETRIES:
                self._sleep(int(response.headers.get("Retry-After", 5)))
                continue
            if response.status_code >= 400:
                raise DeployError(f"{method} {url.split('?')[0]} failed with HTTP {response.status_code}: "
                                  f"{response.text[:500]}")
            return response

    def list_items(self, workspace_id: str, item_type: str) -> list[dict]:
        """All items of one type in the workspace (follows continuation tokens)."""
        url = f"{FABRIC_API}/workspaces/{workspace_id}/items?type={item_type}"
        items, token = [], None
        while True:
            body = self._request("GET", url + (f"&continuationToken={quote(token)}" if token else "")).json()
            items += body.get("value", [])
            token = body.get("continuationToken")
            if not token:
                return items

    def find_item(self, workspace_id: str, item_type: str, name: str) -> dict | None:
        """The item with this display name, or None."""
        return next((i for i in self.list_items(workspace_id, item_type) if i["displayName"] == name), None)

    def require_item(self, workspace_id: str, item_type: str, name: str) -> str:
        """Id of an item that must exist (for example after publishing)."""
        item = self.find_item(workspace_id, item_type, name)
        if item is None:
            raise DeployError(f"{item_type} '{name}' not found in workspace after publishing")
        return item["id"]

    def ensure_lakehouse(self, workspace_id: str, name: str, timeout_s: float = 600) -> str:
        """Id of the lakehouse `name`, creating a schema-enabled one if missing (handles the 202 long-running case)."""
        existing = self.find_item(workspace_id, "Lakehouse", name)
        if existing:
            return existing["id"]
        response = self._request("POST", f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses",
                                 json=lakehouse_body(name))
        if response.status_code == 202:
            self._wait_operation(response.headers["Location"], timeout_s)
            return self.require_item(workspace_id, "Lakehouse", name)
        return response.json()["id"]

    def _wait_operation(self, location: str, timeout_s: float) -> None:
        deadline = self._clock() + timeout_s
        while True:
            response = self._request("GET", location)
            body = response.json()
            status = body.get("status")
            if status == "Succeeded":
                return
            if status in ("Failed", "Cancelled"):
                raise DeployError(f"operation {status}: {(body.get('error') or {}).get('message', '')}")
            if self._clock() >= deadline:
                raise DeployError("operation timed out")
            self._sleep(int(response.headers.get("Retry-After", 5)))

    def start_notebook_run(self, workspace_id: str, notebook_id: str, body: dict) -> str:
        """Submit the notebook (release API, `beta=false`) and return the job instance id."""
        url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/execute/instances?beta=false"
        location = self._request("POST", url, json=body).headers["Location"]
        return location.rstrip("/").rsplit("/", 1)[-1].split("?")[0]

    def wait_for_job(self, workspace_id: str, notebook_id: str, job_id: str, timeout_s: float, poll_s: float = 30) -> dict:
        """Poll the job instance until it is terminal. `beta=true` is what returns `exitValue`."""
        url = (f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{notebook_id}/jobs/execute/instances/{job_id}"
               "?beta=true")
        deadline = self._clock() + timeout_s
        while True:
            job = self._request("GET", url).json()
            if job.get("status") in JOB_TERMINAL:
                return job
            if self._clock() >= deadline:
                raise DeployError(f"timed out waiting for notebook job {job_id} (last status {job.get('status')})")
            self._sleep(poll_s)


# ---- run evaluation (pure) ---------------------------------------------------------------------------------------
@dataclass
class RunOutcome:
    ok: bool
    problems: list[str] = field(default_factory=list)
    summary: dict | None = None


def evaluate_run(job: Mapping[str, Any], expected_build: str | None = None) -> RunOutcome:
    """Healthy means: job Completed, exit value is the JSON run summary, no failed or skipped entities, and
    (when `expected_build` is given) the run used that build, so a stale wheel cannot pass the smoke test."""
    problems: list[str] = []
    if job.get("status") != "Completed":
        reason = (job.get("failureReason") or {}).get("message", "")
        problems.append(f"job status {job.get('status')}" + (f": {reason}" if reason else ""))
    exit_value, summary = job.get("exitValue"), None
    if not exit_value:
        problems.append("job returned no exit value")
    else:
        try:
            summary = json.loads(exit_value)
        except ValueError:
            problems.append("exit value is not JSON")
        else:
            if not isinstance(summary, dict) or not {"failed", "skipped", "results"} <= set(summary):
                problems.append("exit value is not a run summary")
                summary = None
    if summary:
        for key, verb in (("failed", "failed"), ("skipped", "skipped")):
            if summary[key]:
                noun = "entity" if summary[key] == 1 else "entities"
                problems.append(f"{summary[key]} {noun} {verb}")
        if expected_build and summary.get("build") != expected_build:
            problems.append(f"stale build: the run used {summary.get('build')!r}, expected {expected_build!r}")
    return RunOutcome(not problems, problems, summary)


def format_table(summary: Mapping[str, Any]) -> str:
    """Compact per-entity table for the workflow log."""
    rows = [("entity", "status", "read", "written", "quarantined", "error")] + [
        (r["entity"], r["status"], str(r["rows_read"]), str(r["rows_written"]), str(r["rows_quarantined"]),
         (r.get("error") or "")[:120]) for r in summary["results"]]
    widths = [max(len(row[i]) for row in rows) for i in range(6)]
    return "\n".join("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip() for row in rows)


# ---- OneLake upload ----------------------------------------------------------------------------------------------
def plan_uploads(local_dir: str | Path, lakehouse_id: str, remote_dir: str) -> list[tuple[Path, str]]:
    """(local file, path inside the workspace file system) pairs; OneLake paths start with the lakehouse GUID."""
    root = Path(local_dir)
    if not root.is_dir():
        raise DeployError(f"upload source directory missing: {root}")
    return [(p, f"{lakehouse_id}/{remote_dir}/{p.relative_to(root).as_posix()}")
            for p in sorted(root.rglob("*")) if p.is_file()]


def upload_files(file_system, plan: list[tuple[Path, str]]) -> None:
    """Upload every planned file (overwriting) through a `FileSystemClient`."""
    for local, remote in plan:
        with local.open("rb") as handle:
            file_system.get_file_client(remote).upload_data(handle, overwrite=True)


def stage_wheel(wheel_dir: str | Path, repo_dir: str | Path) -> Path:
    """Place the one built wheel in the Environment's custom-library folder, replacing stale wheels."""
    wheels = sorted(Path(wheel_dir).glob("*.whl"))
    if len(wheels) != 1:
        raise DeployError(f"expected exactly one wheel in {wheel_dir}, found {len(wheels)}")
    libs = Path(repo_dir) / f"{ENVIRONMENT_NAME}.Environment" / "Libraries" / "CustomLibraries"
    libs.mkdir(parents=True, exist_ok=True)
    for stale in libs.glob("*.whl"):
        stale.unlink()
    return Path(shutil.copy2(wheels[0], libs / wheels[0].name))


# ---- fabric-cicd -------------------------------------------------------------------------------------------------
def publish_items(workspace_id: str, repo_dir: str, credential, *, workspace_cls=None, publish=None) -> None:
    """Publish the Notebook and Environment items. Orphans are not unpublished: the workspace has other workloads."""
    if workspace_cls is None or publish is None:
        from fabric_cicd import FabricWorkspace, publish_all_items
        workspace_cls, publish = workspace_cls or FabricWorkspace, publish or publish_all_items
    workspace = workspace_cls(workspace_id=workspace_id, repository_directory=str(repo_dir),
                              item_type_in_scope=["Notebook", "Environment"], token_credential=credential)
    publish(workspace)


# ---- orchestration -----------------------------------------------------------------------------------------------
def default_run_id() -> str:
    """GitHub's run id when running in Actions, else a UTC timestamp."""
    return os.environ.get("GITHUB_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def deploy(cfg, client: FabricClient, publish: Callable[[], None],
           upload: Callable[[str, str | Path, str | Path], None]) -> int:
    """Run all steps; returns the process exit code (0 healthy, 1 smoke test unhealthy)."""
    if cfg.wheel_dir:
        print(f"staged wheel {stage_wheel(cfg.wheel_dir, cfg.repo_dir).name}")
    lakehouse_id = client.ensure_lakehouse(cfg.workspace_id, cfg.lakehouse)
    print(f"lakehouse {cfg.lakehouse} ready")
    publish()
    print("published notebook and environment")
    upload(lakehouse_id, cfg.metadata_dir, cfg.sample_data_dir)
    print("uploaded metadata and sample data")
    if not cfg.smoke:
        return 0

    notebook_id = client.require_item(cfg.workspace_id, "Notebook", NOTEBOOK_NAME)
    environment_id = client.require_item(cfg.workspace_id, "Environment", ENVIRONMENT_NAME)
    body = notebook_run_body(cfg.workspace_id, lakehouse_id, environment_id,
                             {"env": cfg.env, "run_id": cfg.run_id})
    job_id = client.start_notebook_run(cfg.workspace_id, notebook_id, body)
    print(f"smoke run started (job {job_id}); waiting up to {cfg.timeout_min} min")
    outcome = evaluate_run(client.wait_for_job(cfg.workspace_id, notebook_id, job_id, cfg.timeout_min * 60),
                           cfg.expect_build)
    if outcome.summary:
        print(format_table(outcome.summary))
    for problem in outcome.problems:
        print(f"SMOKE TEST PROBLEM: {problem}", file=sys.stderr)
    print("smoke test " + ("passed" if outcome.ok else "FAILED"))
    return 0 if outcome.ok else 1


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="fabric_deploy", description=__doc__.split("\n")[0])
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--repo-dir", required=True, help="fabric-cicd repository directory (the `fabric/` folder)")
    parser.add_argument("--lakehouse", default="ingest_lh")
    parser.add_argument("--env", default="fabric-dev", help="metadata overlay the smoke run uses")
    parser.add_argument("--metadata-dir", required=True)
    parser.add_argument("--sample-data-dir", required=True)
    parser.add_argument("--wheel-dir", help="directory with the built wheel to stage into the Environment item")
    parser.add_argument("--smoke", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--timeout-min", type=int, default=30)
    parser.add_argument("--expect-build", help="fail the smoke test unless the run reports this build id (commit SHA)")
    args = parser.parse_args(argv)
    args.run_id = default_run_id()
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    import requests
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    credential = DefaultAzureCredential()  # after azure/login OIDC this resolves to the Azure CLI login
    client = FabricClient(requests.Session(), lambda: credential.get_token(FABRIC_SCOPE).token)

    def upload(lakehouse_id, metadata_dir, sample_dir):
        # The SDK requests the storage-audience token (https://storage.azure.com/.default) itself.
        file_system = DataLakeServiceClient(ONELAKE_URL, credential=credential).get_file_system_client(
            args.workspace_id)
        upload_files(file_system, plan_uploads(metadata_dir, lakehouse_id, "Files/ingest/metadata"))
        upload_files(file_system, plan_uploads(sample_dir, lakehouse_id, "Files/ingest/data"))

    try:
        return deploy(args, client, lambda: publish_items(args.workspace_id, args.repo_dir, credential), upload)
    except DeployError as exc:
        print(f"DEPLOY ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
