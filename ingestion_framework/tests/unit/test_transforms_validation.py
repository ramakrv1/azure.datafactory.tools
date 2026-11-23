import pytest

from ingest.core.model import MetadataError, TransformStep
from ingest.core.transforms import apply_transforms, validate_steps, TransformContext


def steps(*pairs):
    return [TransformStep(op, args) for op, args in pairs]


def test_valid_steps_pass():
    validate_steps(steps(("select", {"columns": ["a"]}), ("join", {"with": "x", "on": ["id"]}),
                         ("drop_duplicates", {"keys": ["id"], "order_by": ["a desc"]}),
                         ("trim_strings", {})), "e")


@pytest.mark.parametrize("step,fragment", [
    (("nope", {}), "unknown op"),
    (("select", {}), "missing arg(s) columns"),
    (("hash_key", {"columns": ["a"]}), "missing arg(s) name"),
    (("select", {"columns": ["a"], "extra": 1}), "unexpected arg(s) extra"),
    (("join", {"on": ["id"]}), "missing arg(s) with"),
])
def test_invalid_step_names_entity_index_and_op(step, fragment):
    with pytest.raises(MetadataError) as err:
        validate_steps(steps(("trim_strings", {}), step), "sales.orders")
    msg = str(err.value)
    assert "entity 'sales.orders'" in msg and "transforms[1]" in msg and f"op '{step[0]}'" in msg
    assert fragment in msg


def test_custom_import_failure_is_metadata_error():
    with pytest.raises(MetadataError, match="cannot import"):
        apply_transforms(None, steps(("custom", {"function": "no.such.module:f"})), TransformContext(None))
