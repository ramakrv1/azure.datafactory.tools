import pytest

from ingest.core.model import QualityRule
from ingest.core.quality import DataQualityError, apply_quality
from tests.spark.conftest import assert_rows

pytestmark = pytest.mark.spark


@pytest.fixture
def df(spark):
    return spark.createDataFrame([(1, 10), (2, -5), (None, 7), (None, -1)], "id int, amount int")


def rule(name, expr, action):
    return QualityRule(name, expr, action)


def test_no_rules_returns_input_unchanged(df):
    res = apply_quality(df, ())
    assert res.valid is df and res.quarantine is None and res.failures == {} and res.quarantined == 0


def test_warn_counts_but_keeps_rows(df):
    res = apply_quality(df, [rule("pos", "amount >= 0", "warn")])
    assert res.failures == {"pos": 2} and res.valid.count() == 4 and res.quarantine is None


def test_quarantine_splits_rows_and_tags_failed_rules(df):
    res = apply_quality(df, [rule("id_set", "id IS NOT NULL", "quarantine"), rule("pos", "amount >= 0", "quarantine")])
    assert res.failures == {"id_set": 2, "pos": 2} and res.quarantined == 3
    assert_rows(res.valid, [{"id": 1, "amount": 10}])
    assert_rows(res.quarantine, [{"id": 2, "amount": -5, "_dq_failed_rules": ["pos"]},
                                 {"id": None, "amount": 7, "_dq_failed_rules": ["id_set"]},
                                 {"id": None, "amount": -1, "_dq_failed_rules": ["id_set", "pos"]}])


def test_fail_rules_raise_listing_every_failure(df):
    with pytest.raises(DataQualityError) as err:
        apply_quality(df, [rule("pos", "amount >= 0", "fail"), rule("id_set", "id IS NOT NULL", "fail"),
                           rule("ok", "amount > -100", "fail")])
    assert "pos (2 rows)" in str(err.value) and "id_set (2 rows)" in str(err.value) and "ok" not in str(err.value)


def test_null_expression_result_counts_as_failure(df):
    res = apply_quality(df, [rule("gt", "id > 0", "warn")])
    assert res.failures == {"gt": 2}


def test_quarantine_rule_with_no_failures_yields_empty_quarantine(df):
    res = apply_quality(df, [rule("any", "amount > -100", "quarantine")])
    assert res.quarantined == 0 and res.quarantine.count() == 0 and res.valid.count() == 4


def test_job_count_does_not_grow_with_rule_count(df):
    sc = df.sparkSession.sparkContext

    def jobs_for(rules, group):
        sc.setJobGroup(group, group)
        apply_quality(df, rules)
        return len(sc.statusTracker().getJobIdsForGroup(group))

    one = [rule("a", "id IS NOT NULL", "warn")]
    many = one + [rule("b", "amount >= 0", "warn"), rule("c", "amount < 100", "quarantine")]
    assert jobs_for(many, "dq-many") == jobs_for(one, "dq-one")


def test_helper_columns_do_not_leak(df):
    res = apply_quality(df, [rule("pos", "amount >= 0", "quarantine"), rule("w", "id > 0", "warn")])
    assert res.valid.columns == ["id", "amount"]
    assert res.quarantine.columns == ["id", "amount", "_dq_failed_rules"]
