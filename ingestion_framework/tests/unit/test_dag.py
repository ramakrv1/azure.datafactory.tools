import pytest

from ingest.core.dag import CycleError, build_graph, levels, to_mermaid, topological_order
from ingest.core.model import MetadataError


def names(es):
    return [e.name for e in es]


def test_build_graph(make_entities):
    es = make_entities({"b": ["a"], "a": []})
    assert build_graph(es) == {"a": frozenset(), "b": frozenset({"a"})}


def test_unknown_reference(make_entities):
    with pytest.raises(MetadataError, match="b.*ghost|ghost.*b"):
        build_graph(make_entities({"b": ["ghost"]}))


def test_order_deterministic_ties_by_name(make_entities):
    es = make_entities({"d": ["b", "c"], "c": ["a"], "b": ["a"], "a": [], "z": [], "y": []})
    # order follows the parallel waves, so `plan` output matches the generated job graph
    assert names(topological_order(es)) == ["a", "y", "z", "b", "c", "d"]
    assert names(topological_order(es)) == [n for wave in levels(es) for n in wave]


def test_cycle_message(make_entities):
    with pytest.raises(CycleError, match="a -> b -> a"):
        topological_order(make_entities({"a": ["b"], "b": ["a"]}))
    assert issubclass(CycleError, MetadataError)


def test_self_cycle(make_entities):
    with pytest.raises(CycleError, match="a -> a"):
        topological_order(make_entities({"a": ["a"]}))


def test_selected_subset_ordered_and_outside_deps_ignored(make_entities):
    es = make_entities({"c": ["b"], "b": ["a"], "a": []})
    sel = [e for e in es if e.name in ("c", "b")]
    assert names(topological_order(es, sel)) == ["b", "c"]


def test_selected_still_validates_unknown(make_entities):
    es = make_entities({"c": ["ghost"], "a": []})
    with pytest.raises(MetadataError):
        topological_order(es, [es[1]])


def test_levels(make_entities):
    es = make_entities({"d": ["b", "c"], "c": ["a"], "b": ["a"], "a": [], "z": []})
    assert levels(es) == [["a", "z"], ["b", "c"], ["d"]]


def test_mermaid(make_entities):
    out = to_mermaid(make_entities({"sales.b": ["sales.a"], "sales.a": []}))
    lines = out.splitlines()
    assert lines[0] == "graph LR"
    assert any("sales_a" in l and "-->" in l and "sales_b" in l for l in lines)
    assert sum('["bronze.sales.a"]' in l for l in lines) == 1
