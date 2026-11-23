import pytest

from ingest.core.model import (
    Entity, LoadMode, MetadataError, QualityRule, Source, TableRef, parse_entities,
)

JDBC = {"type": "jdbc", "options": {"url": "jdbc:x", "dbtable": "dbo.T"}}


# ---- TableRef
def test_tableref_parse_two_and_three_parts():
    assert TableRef.parse("a.b") == TableRef(schema="a", name="b")
    t = TableRef.parse("c.a.b")
    assert (t.catalog, t.schema, t.name) == ("c", "a", "b")
    assert t.fqn() == "c.a.b" and TableRef("a", "b").fqn() == "a.b"


@pytest.mark.parametrize("bad", ["a", "a.b.c.d", "", "a..b"])
def test_tableref_parse_rejects_bad(bad):
    with pytest.raises(MetadataError):
        TableRef.parse(bad)


# ---- Entity happy paths
def test_minimal_entity_defaults(entity_dict):
    e = Entity.from_dict(entity_dict())
    assert e.group == "" and e.load.mode is LoadMode.full
    assert e.full_name == "bronze.sales.orders"
    assert e.target == TableRef("bronze", "orders")
    assert e.transforms == () and e.quality == () and e.depends_on == ()


def test_full_entity(entity_dict):
    e = Entity.from_dict(entity_dict(
        group="sales", source=JDBC,
        load={"mode": "incremental", "watermark_column": "m"},
        target={"table": "bronze.orders", "catalog": "dev"},
        transforms=[{"op": "rename", "args": {"columns": {"a": "b"}}}],
        quality=[{"name": "r", "expr": "a IS NOT NULL"}, {"name": "w", "expr": "b > 0", "action": "warn"}],
    ))
    assert e.target.fqn() == "dev.bronze.orders"
    assert e.load.watermark_column == "m"
    assert e.transforms[0].op == "rename" and e.transforms[0].args["columns"] == {"a": "b"}
    assert e.quality[0] == QualityRule("r", "a IS NOT NULL", "quarantine")
    assert e.quality[1].action == "warn"


def test_references_dedup_sorted(entity_dict):
    e = Entity.from_dict(entity_dict(
        "silver.x", "silver",
        source={"type": "table", "table": "sales.orders"},
        transforms=[{"op": "join", "args": {"with": "sales.customers"}},
                    {"op": "join", "args": {"with": "sales.orders"}}],
        depends_on=["a.z", "sales.customers"],
    ))
    assert e.references() == ("a.z", "sales.customers", "sales.orders")


def test_references_empty(entity_dict):
    assert Entity.from_dict(entity_dict()).references() == ()


# ---- Entity errors
def test_unknown_top_level_key(entity_dict):
    with pytest.raises(MetadataError, match="sales.orders.*unknown.*laod"):
        Entity.from_dict(entity_dict(laod={}))


@pytest.mark.parametrize("field", ["name", "layer", "source", "target"])
def test_missing_required(entity_dict, field):
    d = entity_dict()
    del d[field]
    with pytest.raises(MetadataError, match=field):
        Entity.from_dict(d)


def test_bad_layer(entity_dict):
    with pytest.raises(MetadataError, match="layer"):
        Entity.from_dict(entity_dict(layer="platinum"))


@pytest.mark.parametrize("source,msg", [
    ({"type": "files", "path": "/x"}, "format"),
    ({"type": "files", "format": "csv"}, "path"),
    ({"type": "jdbc"}, "options"),
    ({"type": "jdbc", "options": {"dbtable": "t"}}, "url"),
    ({"type": "jdbc", "options": {"url": "u"}}, "dbtable"),
    ({"type": "table"}, "table"),
    ({"type": "kafka"}, "type"),
    ({}, "type"),
])
def test_bad_source(entity_dict, source, msg):
    with pytest.raises(MetadataError, match=msg):
        Entity.from_dict(entity_dict(source=source))


def test_jdbc_query_ok(entity_dict):
    e = Entity.from_dict(entity_dict(source={"type": "jdbc", "options": {"url": "u", "query": "select 1"}}))
    assert isinstance(e.source, Source) and e.source.type == "jdbc"


@pytest.mark.parametrize("load,msg", [
    ({"mode": "merge"}, "keys"),
    ({"mode": "incremental"}, "watermark_column"),
    ({"mode": "snapshot"}, "mode"),
])
def test_bad_load(entity_dict, load, msg):
    with pytest.raises(MetadataError, match=msg):
        Entity.from_dict(entity_dict(load=load))


def test_merge_with_keys_and_watermark(entity_dict):
    e = Entity.from_dict(entity_dict(load={"mode": "merge", "keys": ["id"], "watermark_column": "m"}))
    assert e.load.keys == ("id",) and e.load.watermark_column == "m"


def test_join_requires_with(entity_dict):
    with pytest.raises(MetadataError, match="with"):
        Entity.from_dict(entity_dict(transforms=[{"op": "join", "args": {}}]))


def test_transform_requires_op(entity_dict):
    with pytest.raises(MetadataError, match="op"):
        Entity.from_dict(entity_dict(transforms=[{"args": {}}]))


def test_bad_quality_action_and_missing_expr(entity_dict):
    with pytest.raises(MetadataError, match="action"):
        Entity.from_dict(entity_dict(quality=[{"name": "r", "expr": "1=1", "action": "explode"}]))
    with pytest.raises(MetadataError, match="expr"):
        Entity.from_dict(entity_dict(quality=[{"name": "r"}]))


def test_bad_target(entity_dict):
    with pytest.raises(MetadataError, match="target"):
        Entity.from_dict(entity_dict(target={"table": "onlyname"}))
    with pytest.raises(MetadataError, match="target"):
        Entity.from_dict(entity_dict(target={"table": "c.s.t", "catalog": "other"}))


# ---- parse_entities
def test_parse_entities_and_duplicates(entity_dict):
    es = parse_entities([entity_dict("a.x"), entity_dict("a.y")])
    assert [e.name for e in es] == ["a.x", "a.y"]
    with pytest.raises(MetadataError, match="duplicate.*a.x"):
        parse_entities([entity_dict("a.x"), entity_dict("a.x")])
