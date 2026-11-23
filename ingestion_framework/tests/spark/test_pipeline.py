from dataclasses import replace

import pytest
from pyspark.sql import functions as F

from ingest.adapters.memory import MemoryReader, memory_engine
from ingest.core.model import Entity, TableRef
from ingest.core.pipeline import run_entity, run_project
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


def make(name, **over):
    d = {"name": name, "layer": "bronze", "source": {"type": "files", "format": "csv", "path": f"/in/{name}"},
         "target": {"table": f"bronze.{name}", "catalog": "cat"}}
    d.update(over)
    return Entity.from_dict(d)


def orders(spark, rows):
    """Rows of (id, amount, modified_at as 'YYYY-MM-DD HH:MM:SS')."""
    return (spark.createDataFrame(rows, "id int, amount int, m string")
            .withColumn("modified_at", F.col("m").cast("timestamp")).drop("m"))


def table(tables, name):
    return tables[f"cat.bronze.{name}"].drop("_ingested_at", "_run_id", "_source_entity")


INCR = {"load": {"mode": "incremental", "watermark_column": "modified_at"}}


def test_full_load_writes_target_with_audit_columns_and_state(spark):
    src = spark.createDataFrame([(1, "a"), (2, "b")], "ID int, v string")
    engine, tables = memory_engine({"/in/t": src})
    e = make("t", transforms=[{"op": "rename", "args": {"columns": {"ID": "id"}}}])
    res = run_entity(e, {"t": e}, engine, "run-1")
    assert (res.status, res.rows_read, res.rows_written, res.rows_quarantined, res.error) == ("succeeded", 2, 2, 0, None)
    out = tables["cat.bronze.t"]
    assert_rows(out.select("id", "v", "_run_id", "_source_entity"),
                [{"id": 1, "v": "a", "_run_id": "run-1", "_source_entity": "t"},
                 {"id": 2, "v": "b", "_run_id": "run-1", "_source_entity": "t"}])
    assert out.filter("_ingested_at IS NULL").count() == 0
    state = engine.state.get("t")
    assert state["last_run_id"] == "run-1" and state["last_status"] == "succeeded" and state["rows_written"] == 2


def test_incremental_picks_up_only_newer_rows_and_keeps_watermark_when_idle(spark):
    sources = {"/in/t": orders(spark, [(1, 10, "2024-01-01 10:00:00"), (2, 20, "2024-01-02 10:00:00")])}
    engine, tables = memory_engine(sources)
    e = make("t", **INCR)
    first = run_entity(e, {"t": e}, engine, "r1")
    assert first.status == "succeeded" and first.watermark == "2024-01-02 10:00:00"

    sources["/in/t"] = orders(spark, [(2, 20, "2024-01-02 10:00:00"), (3, 30, "2024-01-03 10:00:00")])
    second = run_entity(e, {"t": e}, engine, "r2")
    assert (second.rows_read, second.rows_written, second.watermark) == (1, 1, "2024-01-03 10:00:00")
    assert table(tables, "t").count() == 3

    idle = run_entity(e, {"t": e}, engine, "r3")
    assert (idle.status, idle.rows_read, idle.watermark) == ("no_new_data", 0, "2024-01-03 10:00:00")
    assert table(tables, "t").count() == 3
    state = engine.state.get("t")
    assert state["watermark"] == "2024-01-03 10:00:00" and state["last_run_id"] == "r3"
    assert state["last_status"] == "no_new_data" and state["rows_written"] == 1


def test_merge_upserts_on_keys(spark):
    sources = {"/in/t": orders(spark, [(1, 10, "2024-01-01 10:00:00"), (2, 20, "2024-01-01 10:00:00")])}
    engine, tables = memory_engine(sources)
    e = make("t", load={"mode": "merge", "keys": ["id"]})
    run_entity(e, {"t": e}, engine, "r1")
    sources["/in/t"] = orders(spark, [(2, 99, "2024-01-02 10:00:00"), (3, 30, "2024-01-02 10:00:00")])
    run_entity(e, {"t": e}, engine, "r2")
    assert_rows(table(tables, "t").select("id", "amount"),
                [{"id": 1, "amount": 10}, {"id": 2, "amount": 99}, {"id": 3, "amount": 30}])


def test_quarantine_rows_go_to_quarantine_table(spark):
    src = spark.createDataFrame([(1,), (None,), (None,)], "id int")
    engine, tables = memory_engine({"/in/t": src})
    e = make("t", quality=[{"name": "id_set", "expr": "id IS NOT NULL", "action": "quarantine"}])
    res = run_entity(e, {"t": e}, engine, "r1")
    assert (res.status, res.rows_read, res.rows_written, res.rows_quarantined) == ("succeeded", 3, 1, 2)
    assert_rows(table(tables, "t"), [{"id": 1}])
    q = tables["cat.bronze.t_quarantine"]
    assert q.count() == 2 and {"_dq_failed_rules", "_run_id"} <= set(q.columns)


def test_fail_rule_fails_run_and_does_not_advance_watermark(spark):
    sources = {"/in/t": orders(spark, [(1, 10, "2024-01-01 10:00:00")])}
    engine, tables = memory_engine(sources)
    e = make("t", quality=[{"name": "positive", "expr": "amount >= 0", "action": "fail"}], **INCR)
    run_entity(e, {"t": e}, engine, "r1")
    sources["/in/t"] = orders(spark, [(2, -5, "2024-01-02 10:00:00")])
    res = run_entity(e, {"t": e}, engine, "r2")
    assert res.status == "failed" and "positive (1 rows)" in res.error and res.watermark == "2024-01-01 10:00:00"
    state = engine.state.get("t")
    assert state["watermark"] == "2024-01-01 10:00:00" and state["last_status"] == "failed" and state["last_run_id"] == "r2"
    assert table(tables, "t").count() == 1


def test_invalid_transform_fails_the_run_without_reading(spark):
    engine, _ = memory_engine()
    e = make("t", transforms=[{"op": "nope"}])
    res = run_entity(e, {"t": e}, engine, "r1")
    assert res.status == "failed" and "unknown op" in res.error and engine.state.get("t")["last_status"] == "failed"


class _SpyReader(MemoryReader):
    def read(self, source):
        self.seen = source.options
        return super().read(source)


def test_secret_reaches_reader_but_never_state(spark):
    engine, tables = memory_engine(secrets={"kv/pwd": "s3cret"})
    reader = _SpyReader({"dbo.T": spark.createDataFrame([(1,)], "id int")}, tables)
    engine = replace(engine, reader=reader)
    e = make("t", source={"type": "jdbc", "options": {"url": "jdbc:x", "dbtable": "dbo.T", "password": "${secret:kv/pwd}"}})
    res = run_entity(e, {"t": e}, engine, "r1")
    assert res.status == "succeeded" and reader.seen["password"] == "s3cret"
    assert "s3cret" not in repr(engine.state.get("t")) and "s3cret" not in repr(res)


def test_secret_is_redacted_from_error(spark):
    class Leaky(MemoryReader):
        def read(self, source):
            raise RuntimeError(f"login failed for {source.options['password']}")

    engine, tables = memory_engine(secrets={"kv/pwd": "s3cret"})
    engine = replace(engine, reader=Leaky({}, tables))
    e = make("t", source={"type": "jdbc", "options": {"url": "jdbc:x", "dbtable": "dbo.T", "password": "${secret:kv/pwd}"}})
    res = run_entity(e, {"t": e}, engine, "r1")
    assert res.status == "failed" and "s3cret" not in res.error and "login failed" in res.error


def test_table_source_reads_other_entity_target_and_join_step(spark):
    customers = spark.createDataFrame([(1, "Ann")], "customer_id int, cname string")
    engine, tables = memory_engine({"/in/orders": spark.createDataFrame([(10, 1), (11, 2)], "id int, customer_id int"),
                                    "/in/customers": customers})
    o, c = make("orders"), make("customers")
    s = make("enriched", source={"type": "table", "table": "orders"},
             transforms=[{"op": "join", "args": {"with": "customers", "on": ["customer_id"], "how": "left"}}])
    project = {e.name: e for e in (o, c, s)}
    results = run_project(list(project.values()), engine, "r1")
    assert [r.status for r in results] == ["succeeded"] * 3
    assert_rows(table(tables, "enriched").select("id", "cname"), [{"id": 10, "cname": "Ann"}, {"id": 11, "cname": None}])


def _chain(spark):
    """a -> b -> c plus independent d; 'a' has a failing source (not registered)."""
    engine, _ = memory_engine({"/in/d": spark.createDataFrame([(1,)], "id int")})
    a = make("a")
    b = make("b", source={"type": "table", "table": "a"})
    c = make("c", source={"type": "table", "table": "b"})
    d = make("d")
    return engine, [c, d, b, a]


def test_run_project_orders_dependencies_first(spark):
    engine, _ = memory_engine({"/in/a": spark.createDataFrame([(1,)], "id int")})
    a = make("a")
    b = make("b", source={"type": "table", "table": "a"})
    results = run_project([b, a], engine, "r1")
    assert [(r.entity, r.status) for r in results] == [("a", "succeeded"), ("b", "succeeded")]


def test_failure_skips_transitive_dependents_but_not_independents(spark):
    engine, entities = _chain(spark)
    results = {r.entity: r for r in run_project(entities, engine, "r1", fail_fast=False)}
    assert {n: r.status for n, r in results.items()} == {"a": "failed", "b": "skipped", "c": "skipped", "d": "succeeded"}
    assert "'a'" in results["b"].error and engine.state.get("b") is None


def test_fail_fast_skips_everything_after_first_failure(spark):
    engine, entities = _chain(spark)
    results = run_project(entities, engine, "r1")
    assert [(r.entity, r.status) for r in results] == [("a", "failed"), ("d", "skipped"), ("b", "skipped"), ("c", "skipped")]


def test_run_project_honours_selection(spark):
    engine, entities = _chain(spark)
    selected = [e for e in entities if e.name == "d"]
    assert [(r.entity, r.status) for r in run_project(entities, engine, "r1", selected)] == [("d", "succeeded")]
