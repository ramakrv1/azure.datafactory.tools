"""The shipped sample project, run with the `fabric-dev` overlay on local Delta (no catalog, local sample files)."""
import json
import shutil
from pathlib import Path

import pytest

from ingest.adapters.delta_common import DeltaStateStore, DeltaWriter
from ingest.adapters.memory import DictSecrets
from ingest.adapters.spark_common import SparkReader
from ingest.core.model import TableRef
from ingest.ports import Engine
from ingest.runner import run_metadata

pytestmark = pytest.mark.spark

ROOT = Path(__file__).parents[2]
TABLES = ("bronze.orders", "bronze.customers", "silver.orders_clean", "silver.orders_clean_quarantine")


@pytest.fixture
def run(spark, tmp_path):
    shutil.copytree(ROOT / "sample_data", tmp_path / "data")
    state = TableRef("smoke_ops", "ingest_state")
    for table in (*TABLES, state.fqn()):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    engine = Engine(SparkReader(spark), DeltaWriter(spark), DeltaStateStore(spark, state), DictSecrets({}))

    def _run(run_id):
        return run_metadata(engine, ROOT / "metadata", "fabric-dev", run_id,
                            variables={"DATA_ROOT": str(tmp_path / "data"), "CATALOG": ""})

    yield _run
    for table in (*TABLES, state.fqn()):
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_sample_data_loads_end_to_end_and_the_second_run_finds_nothing_new(spark, run):
    first = run("r1")
    assert json.loads(json.dumps(first)) == first
    assert (first["failed"], first["skipped"], first["succeeded"]) == (0, 0, 3)
    assert {r["entity"]: r["status"] for r in first["results"]} == {
        "sales.customers": "succeeded", "sales.orders": "succeeded", "sales.orders_clean": "succeeded"}

    assert spark.table("bronze.orders").count() == 9
    silver = {r["order_id"]: r for r in spark.table("silver.orders_clean").collect()}
    assert sorted(silver) == [1001, 1002, 1003, 1004, 1005, 1006, 1007]
    assert silver[1003]["amount"] == 24.99  # the later duplicate wins
    assert silver[1001]["country"] == "UK" and silver[1001]["name"] == "Ada Lovelace"
    (quarantined,) = spark.table("silver.orders_clean_quarantine").collect()
    assert quarantined["order_id"] == 1008 and quarantined["customer_id"] is None

    second = run("r2")
    status = {r["entity"]: r["status"] for r in second["results"]}
    assert status["sales.orders"] == "no_new_data" and status["sales.orders_clean"] == "no_new_data"
    assert second["failed"] == 0 and second["skipped"] == 0
