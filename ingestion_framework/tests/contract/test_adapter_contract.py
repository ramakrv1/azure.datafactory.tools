"""Behaviour every engine adapter must have. Add a new engine by adding a harness to `ENGINES`."""
import uuid

import pytest

from ingest.adapters.delta_common import DeltaStateStore, DeltaWriter
from ingest.adapters.memory import DictSecrets, memory_engine
from ingest.adapters.spark_common import SparkReader
from ingest.core.model import Entity, TableRef
from ingest.core.pipeline import run_entity
from ingest.ports import Engine, WriteMode
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


class MemoryHarness:
    def __init__(self, spark, tmp_path):
        self.spark, self._sources = spark, {}
        self.engine, _ = memory_engine(self._sources)

    def lay_out(self, name, rows, ddl):
        """Make `rows` readable as a parquet source; returns its path."""
        path = f"/in/{name}"
        self._sources[path] = self.spark.createDataFrame(rows, ddl)
        return path


class DeltaHarness:
    def __init__(self, spark, tmp_path):
        self.spark, self._root = spark, tmp_path
        state = TableRef(f"state_{uuid.uuid4().hex[:8]}", "state")
        self.engine = Engine(SparkReader(spark), DeltaWriter(spark), DeltaStateStore(spark, state), DictSecrets({}))

    def lay_out(self, name, rows, ddl):
        path = str(self._root / name)
        self.spark.createDataFrame(rows, ddl).write.mode("overwrite").parquet(path)
        return path


ENGINES = {"memory": MemoryHarness, "delta": DeltaHarness}


@pytest.fixture(params=ENGINES)
def harness(request, spark, tmp_path):
    return ENGINES[request.param](spark, tmp_path)


@pytest.fixture
def ref():
    return TableRef(f"contract_{uuid.uuid4().hex[:8]}", "t")


def df(h, rows, ddl):
    return h.spark.createDataFrame(rows, ddl)


def write(h, ref, rows, ddl, mode, keys=()):
    h.engine.writer.write(df(h, rows, ddl), ref, mode, keys)


def read(h, ref):
    return h.engine.reader.read_table(ref)


def test_overwrite_replaces_rows_and_schema(harness, ref):
    write(harness, ref, [(1, "a")], "id int, v string", WriteMode.overwrite)
    write(harness, ref, [(2, 5)], "id int, n int", WriteMode.overwrite)
    assert_rows(read(harness, ref), [{"id": 2, "n": 5}])


def test_append_adds_rows_and_evolves_schema(harness, ref):
    write(harness, ref, [(1, "a")], "id int, v string", WriteMode.append)
    write(harness, ref, [(2, "b", 9)], "id int, v string, n int", WriteMode.append)
    assert_rows(read(harness, ref), [{"id": 1, "v": "a", "n": None}, {"id": 2, "v": "b", "n": 9}])


def test_merge_creates_missing_table(harness, ref):
    write(harness, ref, [(1, "a")], "id int, v string", WriteMode.merge, ("id",))
    assert_rows(read(harness, ref), [{"id": 1, "v": "a"}])


def test_merge_updates_matched_inserts_unmatched_and_evolves_schema(harness, ref):
    write(harness, ref, [(1, "a"), (2, "b")], "id int, v string", WriteMode.overwrite)
    write(harness, ref, [(2, "B", 7), (3, "c", 8)], "id int, v string, n int", WriteMode.merge, ("id",))
    assert_rows(read(harness, ref), [{"id": 1, "v": "a", "n": None}, {"id": 2, "v": "B", "n": 7},
                                     {"id": 3, "v": "c", "n": 8}])


def test_merge_with_composite_keys(harness, ref):
    write(harness, ref, [(1, "x", "a"), (1, "y", "b")], "id int, k string, v string", WriteMode.overwrite)
    write(harness, ref, [(1, "y", "B")], "id int, k string, v string", WriteMode.merge, ("id", "k"))
    assert_rows(read(harness, ref), [{"id": 1, "k": "x", "v": "a"}, {"id": 1, "k": "y", "v": "B"}])


def test_merge_rejects_duplicate_source_keys_and_leaves_table_untouched(harness, ref):
    write(harness, ref, [(1, "a")], "id int, v string", WriteMode.overwrite)
    with pytest.raises(ValueError, match=r"merge source has duplicate keys.*id"):
        write(harness, ref, [(1, "x"), (1, "y")], "id int, v string", WriteMode.merge, ("id",))
    assert_rows(read(harness, ref), [{"id": 1, "v": "a"}])


def test_merge_treats_null_key_as_a_value(harness, ref):
    write(harness, ref, [(None, "a"), (1, "b")], "id int, v string", WriteMode.overwrite)
    write(harness, ref, [(None, "A")], "id int, v string", WriteMode.merge, ("id",))
    write(harness, ref, [(None, "AA")], "id int, v string", WriteMode.merge, ("id",))
    assert_rows(read(harness, ref), [{"id": None, "v": "AA"}, {"id": 1, "v": "b"}])


def test_missing_table_cannot_be_read(harness, ref):
    with pytest.raises(Exception):
        read(harness, ref).count()


def test_state_round_trip(harness):
    state = harness.engine.state
    assert state.get("e") is None
    state.put("e", {"watermark": "2024-01-01", "rows_written": 3})
    state.put("other", {"watermark": "x"})
    assert state.get("e") == {"watermark": "2024-01-01", "rows_written": 3}
    state.put("e", {"watermark": "2024-02-01"})
    assert state.get("e") == {"watermark": "2024-02-01"} and state.get("other") == {"watermark": "x"}


def test_incremental_run_twice_appends_only_new_rows(harness, ref):
    path = harness.lay_out("orders", [(1, "2024-01-01 00:00:00"), (2, "2024-01-02 00:00:00")], "id int, m string")
    entity = Entity.from_dict({
        "name": "orders", "layer": "bronze", "source": {"type": "files", "format": "parquet", "path": path},
        "load": {"mode": "incremental", "watermark_column": "m"}, "target": {"table": f"{ref.schema}.{ref.name}"}})
    first = run_entity(entity, {"orders": entity}, harness.engine, "r1")
    path = harness.lay_out("orders", [(1, "2024-01-01 00:00:00"), (2, "2024-01-02 00:00:00"),
                                      (3, "2024-01-03 00:00:00")], "id int, m string")
    second = run_entity(entity, {"orders": entity}, harness.engine, "r2")
    assert (first.status, first.rows_written, first.watermark) == ("succeeded", 2, "2024-01-02 00:00:00")
    assert (second.status, second.rows_written, second.watermark) == ("succeeded", 1, "2024-01-03 00:00:00")
    out = read(harness, ref).select("id", "_run_id")
    assert_rows(out, [{"id": 1, "_run_id": "r1"}, {"id": 2, "_run_id": "r1"}, {"id": 3, "_run_id": "r2"}])
    assert harness.engine.state.get("orders")["watermark"] == "2024-01-03 00:00:00"
