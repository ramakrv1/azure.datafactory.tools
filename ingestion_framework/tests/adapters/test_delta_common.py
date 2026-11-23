import pytest

from ingest.adapters.delta_common import DeltaStateStore, DeltaWriter
from ingest.core.model import TableRef
from ingest.ports import WriteMode
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


def test_writer_creates_missing_schema(spark):
    ref = TableRef("brand_new_schema", "t")
    DeltaWriter(spark).write(spark.createDataFrame([(1,)], "id int"), ref, WriteMode.append)
    assert_rows(spark.table(ref.fqn()), [{"id": 1}])


def test_state_store_creates_its_table_and_keeps_one_row_per_entity(spark):
    ref = TableRef("state_schema", "st")
    store = DeltaStateStore(spark, ref)
    store.put("a", {"n": 1})
    store.put("a", {"n": 2})
    assert DeltaStateStore(spark, ref).get("a") == {"n": 2}
    rows = spark.table(ref.fqn()).collect()
    assert len(rows) == 1 and rows[0]["updated_at"] is not None
