import pytest

from ingest.adapters.memory import MemoryReader, MemoryTables, MemoryWriter
from ingest.core.model import Source, TableRef
from ingest.ports import WriteMode
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark

REF = TableRef("silver", "t", "cat")


@pytest.fixture
def writer():
    tables = MemoryTables()
    return MemoryWriter(tables), tables


def frame(spark, rows, schema="id int, v string"):
    return spark.createDataFrame(rows, schema)


def test_overwrite_replaces(spark, writer):
    w, tables = writer
    w.write(frame(spark, [(1, "a")]), REF, WriteMode.overwrite)
    w.write(frame(spark, [(2, "b")]), REF, WriteMode.overwrite)
    assert_rows(tables["cat.silver.t"], [{"id": 2, "v": "b"}])


def test_append_creates_then_unions_with_schema_evolution(spark, writer):
    w, tables = writer
    w.write(frame(spark, [(1, "a")]), REF, WriteMode.append)
    w.write(frame(spark, [(2, 5)], "id int, extra int"), REF, WriteMode.append)
    assert_rows(tables["cat.silver.t"], [{"id": 1, "v": "a", "extra": None}, {"id": 2, "v": None, "extra": 5}])


def test_merge_creates_then_upserts_on_keys(spark, writer):
    w, tables = writer
    w.write(frame(spark, [(1, "a"), (2, "b")]), REF, WriteMode.merge, ("id",))
    w.write(frame(spark, [(2, "B"), (3, "c")]), REF, WriteMode.merge, ("id",))
    assert_rows(tables["cat.silver.t"], [{"id": 1, "v": "a"}, {"id": 2, "v": "B"}, {"id": 3, "v": "c"}])


def test_merge_evolves_schema(spark, writer):
    w, tables = writer
    w.write(frame(spark, [(1, "a")]), REF, WriteMode.merge, ("id",))
    w.write(frame(spark, [(1, 9)], "id int, extra int"), REF, WriteMode.merge, ("id",))
    assert_rows(tables["cat.silver.t"], [{"id": 1, "v": None, "extra": 9}])


def test_reader_resolves_files_jdbc_and_tables(spark):
    a, b, c = frame(spark, [(1, "a")]), frame(spark, [(2, "b")]), frame(spark, [(3, "c")])
    tables = MemoryTables({"cat.silver.t": c})
    reader = MemoryReader({"/in/a": a, "dbo.B": b}, tables)
    assert reader.read(Source("files", format="csv", path="/in/a")) is a
    assert reader.read(Source("jdbc", {"url": "u", "dbtable": "dbo.B"})) is b
    assert reader.read(Source("jdbc", {"url": "u", "query": "dbo.B"})) is b
    assert reader.read_table(REF) is c


def test_reader_unknown_key_raises_clear_error(spark):
    reader = MemoryReader({}, MemoryTables())
    with pytest.raises(KeyError, match="no data registered for files source '/x'"):
        reader.read(Source("files", format="csv", path="/x"))
    with pytest.raises(KeyError, match="table 'cat.silver.t' does not exist"):
        reader.read_table(REF)
