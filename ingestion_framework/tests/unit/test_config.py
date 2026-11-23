import warnings

import pytest

from ingest.core.config import (
    OverlayReport, apply_overlay, expand_tokens, load_entity_dicts, load_project,
)
from ingest.core.model import MetadataError


def ents(entity_dict, *names):
    return [entity_dict(n) for n in names]


def ov(entity="sales.*", path="target.catalog", value="c", **kw):
    return {"entity": entity, "path": path, "value": value, **kw}


# ---- load_entity_dicts
def test_load_entity_dicts_single_and_list_sorted(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "b.yaml", entity_dict("b.one"))
    write_yaml(tmp_path / "entities" / "sub" / "a.yml", [entity_dict("a.one"), entity_dict("a.two")])
    assert [d["name"] for d in load_entity_dicts(tmp_path)] == ["b.one", "a.one", "a.two"]


def test_load_entity_dicts_bad_content(tmp_path):
    (tmp_path / "entities").mkdir()
    (tmp_path / "entities" / "x.yaml").write_text("- 1\n- 2\n")
    with pytest.raises(MetadataError, match="x.yaml"):
        load_entity_dicts(tmp_path)


# ---- overlay: update / add / remove
def test_update_existing_path(entity_dict):
    es = ents(entity_dict, "sales.a")
    es[0]["target"]["catalog"] = "old"
    rep = apply_overlay(es, [ov()])
    assert es[0]["target"]["catalog"] == "c" and rep == OverlayReport(updated=1, added=0, removed=0)


def test_update_missing_path_fails(entity_dict):
    with pytest.raises(MetadataError, match="sales.a.*target.catalog"):
        apply_overlay(ents(entity_dict, "sales.a"), [ov()])


def test_update_missing_path_warns_when_not_failing(entity_dict):
    es = ents(entity_dict, "sales.a")
    with pytest.warns(UserWarning, match="target.catalog"):
        rep = apply_overlay(es, [ov()], fail_when_path_not_found=False)
    assert "catalog" not in es[0]["target"] and rep.updated == 0


def test_add_creates_intermediate_dicts(entity_dict):
    es = ents(entity_dict, "sales.a")
    rep = apply_overlay(es, [ov(path="source.options.fetchsize", value=100, action="add")])
    assert es[0]["source"]["options"]["fetchsize"] == 100 and rep.added == 1


def test_add_through_non_mapping_is_metadata_error(entity_dict):
    es = ents(entity_dict, "sales.a")
    es[0]["source"]["options"] = "not-a-dict"
    with pytest.raises(MetadataError, match="sales.a.*source.options"):
        apply_overlay(es, [ov(path="source.options.fetchsize", value=1, action="add")])


def test_add_overwrites_existing(entity_dict):
    es = ents(entity_dict, "sales.a")
    apply_overlay(es, [ov(path="target.table", value="x.y", action="add")])
    assert es[0]["target"]["table"] == "x.y"


def test_remove_key_and_missing(entity_dict):
    es = ents(entity_dict, "sales.a")
    rep = apply_overlay(es, [ov(path="source.format", action="remove")])
    assert "format" not in es[0]["source"] and rep.removed == 1
    with pytest.raises(MetadataError, match="source.format"):
        apply_overlay(es, [ov(path="source.format", action="remove")])
    with pytest.warns(UserWarning):
        apply_overlay(es, [ov(path="source.format", action="remove")], fail_when_path_not_found=False)


def test_plus_minus_prefix_shorthand(entity_dict):
    es = ents(entity_dict, "sales.a")
    rep = apply_overlay(es, [ov(path="+target.catalog"), ov(path="-source.format", value=None)])
    assert es[0]["target"]["catalog"] == "c" and "format" not in es[0]["source"]
    assert (rep.added, rep.removed) == (1, 1)


def test_path_prefix_stripping(entity_dict):
    es = ents(entity_dict, "sales.a")
    apply_overlay(es, [ov(path="$.target.table", value="p.q"), ov(path="$.properties.target.table", value="r.s")])
    assert es[0]["target"]["table"] == "r.s"


def test_wildcard_hits_many_and_case_insensitive(entity_dict):
    es = ents(entity_dict, "sales.a", "Sales.b", "hr.c")
    rep = apply_overlay(es, [ov(entity="SALES.*", path="target.catalog", action="add")])
    assert [("catalog" in e["target"]) for e in es] == [True, True, False] and rep.added == 2


def test_pattern_matching_nothing(entity_dict):
    es = ents(entity_dict, "sales.a")
    with pytest.raises(MetadataError, match="nope"):
        apply_overlay(es, [ov(entity="nope.*")])
    with pytest.warns(UserWarning, match="nope"):
        apply_overlay(es, [ov(entity="nope.*")], fail_when_entity_not_found=False)


def test_overrides_applied_in_order(entity_dict):
    es = ents(entity_dict, "sales.a")
    apply_overlay(es, [ov(action="add", value="one"), ov(value="two")])
    assert es[0]["target"]["catalog"] == "two"


def test_bad_action_and_missing_keys(entity_dict):
    with pytest.raises(MetadataError, match="action"):
        apply_overlay(ents(entity_dict, "sales.a"), [ov(action="upsert")])
    with pytest.raises(MetadataError, match="path"):
        apply_overlay(ents(entity_dict, "sales.a"), [{"entity": "sales.*", "value": 1}])


# ---- tokens
def test_expand_tokens_variables_then_environ():
    out = expand_tokens({"a": ["${X}-${Y}", 5], "b": {"c": "${X}"}}, {"X": "vx"}, {"X": "env", "Y": "ey"})
    assert out == {"a": ["vx-ey", 5], "b": {"c": "vx"}}


def test_expand_tokens_missing_lists_all():
    with pytest.raises(MetadataError) as ei:
        expand_tokens({"a": "${ONE}", "b": ["${TWO}", "${ONE}"]}, {}, {})
    assert "ONE" in str(ei.value) and "TWO" in str(ei.value)


def test_secret_tokens_untouched_and_escape():
    out = expand_tokens({"p": "${secret:kv/pwd}", "e": "$${X}"}, {}, {})
    assert out == {"p": "${secret:kv/pwd}", "e": "${X}"}


def test_expand_tokens_default_environ(monkeypatch):
    monkeypatch.setenv("INGEST_T", "hello")
    assert expand_tokens("${INGEST_T}", {}) == "hello"


# ---- load_project
def test_load_project_with_env(tmp_path, write_yaml, entity_dict):
    d = entity_dict("sales.a", source={"type": "jdbc", "options": {"url": "${SQL_URL}", "dbtable": "t",
                                                                   "password": "${secret:kv/p}"}})
    write_yaml(tmp_path / "entities" / "a.yaml", d)
    write_yaml(tmp_path / "env" / "dev.yaml", {
        "variables": {"CATALOG": "dev_lake", "SQL_URL": "jdbc:dev"},
        "overrides": [{"entity": "sales.*", "path": "target.catalog", "value": "${CATALOG}", "action": "add"}],
    })
    (e,) = load_project(tmp_path, "dev", environ={})
    assert e.target.fqn() == "dev_lake.bronze.a"
    assert e.source.options["url"] == "jdbc:dev" and e.source.options["password"] == "${secret:kv/p}"


def test_load_project_missing_env_file(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a"))
    with pytest.raises(MetadataError, match="qa"):
        load_project(tmp_path, "qa", environ={})


def test_load_project_without_env_skips_overlay(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a"))
    (e,) = load_project(tmp_path, None, environ={})
    assert e.target.catalog is None


def test_overlay_unknown_keys(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a"))
    write_yaml(tmp_path / "env" / "dev.yaml", {"varibles": {}})
    with pytest.raises(MetadataError, match="varibles"):
        load_project(tmp_path, "dev", environ={})


def test_options_from_overlay_file(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a"))
    write_yaml(tmp_path / "env" / "dev.yaml", {
        "overrides": [{"entity": "nope", "path": "target.catalog", "value": 1}],
        "options": {"fail_when_entity_not_found": False}})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert len(load_project(tmp_path, "dev", environ={})) == 1


class _Secrets:
    def get(self, scope, key):
        return f"<{scope}:{key}>"


def test_resolve_secrets_walks_nested_structures():
    from ingest.core.config import resolve_secrets

    obj = {"a": "${secret:kv/pwd}", "b": ["x-${secret:s/k}-y", 1], "c": {"d": "${OTHER}"}}
    assert resolve_secrets(obj, _Secrets()) == {"a": "<kv:pwd>", "b": ["x-<s:k>-y", 1], "c": {"d": "${OTHER}"}}


@pytest.mark.parametrize("token", ["${secret:nokey}", "${secret:/k}", "${secret:s/}"])
def test_resolve_secrets_rejects_malformed_token(token):
    from ingest.core.config import resolve_secrets

    with pytest.raises(MetadataError, match="malformed secret token"):
        resolve_secrets({"p": token}, _Secrets())


# ---- explicit variables (precedence: explicit > overlay > environ)
def _var_project(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml",
               entity_dict("sales.a", source={"type": "files", "format": "csv", "path": "${ROOT}/a"}))
    write_yaml(tmp_path / "env" / "dev.yaml", {"variables": {"ROOT": "overlay"}})


def test_explicit_variables_beat_overlay_variables(tmp_path, write_yaml, entity_dict):
    _var_project(tmp_path, write_yaml, entity_dict)
    (e,) = load_project(tmp_path, "dev", environ={"ROOT": "environ"}, variables={"ROOT": "explicit"})
    assert e.source.path == "explicit/a"


def test_overlay_variables_beat_environ_and_explicit_works_without_env(tmp_path, write_yaml, entity_dict):
    _var_project(tmp_path, write_yaml, entity_dict)
    (e,) = load_project(tmp_path, "dev", environ={"ROOT": "environ"})
    assert e.source.path == "overlay/a"
    (e,) = load_project(tmp_path, None, environ={}, variables={"ROOT": "x"})
    assert e.source.path == "x/a"


def test_empty_catalog_variable_means_no_catalog(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a"))
    write_yaml(tmp_path / "env" / "dev.yaml", {
        "variables": {"CATALOG": "lh"},
        "overrides": [{"entity": "*", "path": "target.catalog", "value": "${CATALOG}", "action": "add"}]})
    (e,) = load_project(tmp_path, "dev", environ={}, variables={"CATALOG": ""})
    assert e.target.catalog is None and e.target.fqn() == "bronze.a"


def test_overlay_can_replace_a_whole_source_mapping(tmp_path, write_yaml, entity_dict):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("sales.a", source={"type": "jdbc", "options": {"url": "u"}}))
    write_yaml(tmp_path / "env" / "dev.yaml", {"overrides": [{
        "entity": "sales.a", "path": "source",
        "value": {"type": "files", "format": "csv", "path": "${P}", "options": {"header": "true"}}}],
        "variables": {"P": "/x"}})
    (e,) = load_project(tmp_path, "dev", environ={})
    assert (e.source.type, e.source.path, e.source.options) == ("files", "/x", {"header": "true"})
