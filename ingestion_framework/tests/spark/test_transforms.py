import hashlib

import pytest

from ingest.core.model import TransformStep
from ingest.core.transforms import TransformContext, apply_transforms
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


def run(df, op, ctx=None, **args):
    return apply_transforms(df, [TransformStep(op, args)], ctx or TransformContext(lambda n: None))


def double_amount(df, factor):
    return df.withColumn("amount", df.amount * factor)


@pytest.fixture
def orders(spark):
    return spark.createDataFrame([(1, "a ", 10, "x"), (2, " b", 20, "x"), (2, "c", 30, "y")],
                                 "id int, name string, amount int, grp string")


def test_select_drop_rename(orders):
    assert run(orders, "select", columns=["id"]).columns == ["id"]
    assert run(orders, "drop", columns=["name", "grp"]).columns == ["id", "amount"]
    assert run(orders, "rename", columns={"id": "order_id"}).columns[0] == "order_id"


def test_cast_filter_with_columns(orders):
    assert dict(run(orders, "cast", columns={"id": "string"}).dtypes)["id"] == "string"
    assert_rows(run(orders, "filter", expr="amount > 10").select("id", "amount"),
                [{"id": 2, "amount": 20}, {"id": 2, "amount": 30}])
    out = run(orders, "with_columns", columns={"double": "amount * 2"})
    assert_rows(out.select("id", "double").filter("id = 1"), [{"id": 1, "double": 20}])


def test_trim_strings_only_touches_string_columns(orders):
    out = run(orders, "trim_strings")
    assert_rows(out.select("name"), [{"name": "a"}, {"name": "b"}, {"name": "c"}])
    assert out.schema == orders.schema


def test_drop_duplicates_plain_and_ordered(orders):
    assert run(orders, "drop_duplicates", keys=["id"]).count() == 2
    kept = run(orders, "drop_duplicates", keys=["id"], order_by=["amount desc"])
    assert_rows(kept.select("id", "amount"), [{"id": 1, "amount": 10}, {"id": 2, "amount": 30}])
    assert "_rn" not in kept.columns
    kept = run(orders, "drop_duplicates", keys=["id"], order_by=["amount"])
    assert_rows(kept.select("id", "amount"), [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}])


def test_hash_key_is_null_safe_sha256(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], "a int, b string")
    out = run(df, "hash_key", columns=["a", "b"], name="k")
    sha = lambda s: hashlib.sha256(s.encode()).hexdigest()  # noqa: E731
    assert_rows(out.select("a", "k"), [{"a": 1, "k": sha("1||")}, {"a": 2, "k": sha("2||x")}])


def test_join_reads_entity_and_projects_columns(spark, orders):
    customers = spark.createDataFrame([(1, "Ann", "junk"), (3, "Cy", "junk")], "id int, cname string, other string")
    ctx = TransformContext(lambda n: customers)
    out = run(orders, "join", ctx, **{"with": "sales.customers", "on": ["id"], "how": "left", "select": ["cname"]})
    assert "other" not in out.columns
    assert_rows(out.select("id", "cname").distinct(), [{"id": 1, "cname": "Ann"}, {"id": 2, "cname": None}])


def test_aggregate_with_plain_and_computed_keys(orders):
    out = run(orders, "aggregate", group_by=["grp", {"odd": "id % 2"}], aggs={"total": "sum(amount)", "n": "count(*)"})
    assert_rows(out, [{"grp": "x", "odd": 1, "total": 10, "n": 1}, {"grp": "x", "odd": 0, "total": 20, "n": 1},
                      {"grp": "y", "odd": 0, "total": 30, "n": 1}])


def test_custom_calls_function_with_args(orders):
    out = run(orders, "custom", function="tests.spark.test_transforms:double_amount", args={"factor": 3})
    assert_rows(out.select("amount"), [{"amount": 30}, {"amount": 60}, {"amount": 90}])


def test_steps_apply_in_order(orders):
    steps = [TransformStep("filter", {"expr": "id = 2"}), TransformStep("select", {"columns": ["amount"]})]
    assert_rows(apply_transforms(orders, steps, TransformContext(None)), [{"amount": 20}, {"amount": 30}])
