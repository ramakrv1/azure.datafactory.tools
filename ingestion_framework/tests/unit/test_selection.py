import pytest

from ingest.core.selection import load_rules_file, matches, select


@pytest.fixture
def es(make_entities, entity_dict):
    from ingest.core.model import parse_entities
    return parse_entities([
        entity_dict("sales.orders", "bronze", group="sales"),
        entity_dict("sales.customers", "bronze", group="sales"),
        entity_dict("hr.people", "bronze", group="hr"),
        entity_dict("sales.orders_clean", "silver", group="sales"),
    ])


@pytest.mark.parametrize("pattern,expected", [
    ("bronze.sales.*", ["sales.orders", "sales.customers"]),
    ("*.orders@sales", ["sales.orders"]),
    ("*.orders*", ["sales.orders", "sales.orders_clean"]),
    ("SILVER.*", ["sales.orders_clean"]),
    ("*@HR", ["hr.people"]),
    ("*.orders@hr", []),
    ("gold.*", []),
])
def test_matches(es, pattern, expected):
    assert [e.name for e in es if matches(e, pattern)] == expected


def test_select_no_rules_keeps_all(es):
    assert select(es) == es


def test_select_includes_only(es):
    assert [e.name for e in select(es, includes=["silver.*", "*.customers"])] == ["sales.customers", "sales.orders_clean"]


def test_select_excludes_only_preserves_order(es):
    assert [e.name for e in select(es, excludes=["bronze.sales.*"])] == ["hr.people", "sales.orders_clean"]


def test_includes_override_excludes(es):
    out = select(es, includes=["bronze.*"], excludes=["bronze.*"])
    assert len(out) == 3


def test_load_rules_file(tmp_path):
    f = tmp_path / "r.txt"
    f.write_text("# comment\n\n+bronze.*\n-silver.*\ngold.*\n  +x@y  \n")
    assert load_rules_file(f) == (("bronze.*", "gold.*", "x@y"), ("silver.*",))
