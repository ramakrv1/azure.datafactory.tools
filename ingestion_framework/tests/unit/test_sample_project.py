from pathlib import Path

import pytest

from ingest.core.dag import topological_order
from ingest.core.config import load_project

METADATA = Path(__file__).parents[2] / "metadata"


@pytest.mark.parametrize("env,catalog", [("dev", "dev_lake"), ("prod", "prod_lake")])
def test_sample_metadata_loads_and_orders(env, catalog):
    es = load_project(METADATA, env=env, environ={})
    assert {e.target.catalog for e in es} == {catalog}
    order = [e.name for e in topological_order(es)]
    assert order.index("sales.orders") < order.index("sales.orders_clean")
    assert order.index("sales.customers") < order.index("sales.orders_clean")
    by_name = {e.name: e for e in es}
    assert by_name["sales.orders"].source.options["password"] == "${secret:kv/sql-pwd}"
    assert by_name["sales.orders"].source.options["url"].startswith("jdbc:")


def test_sample_transforms_are_valid():
    from ingest.core.transforms import validate_steps

    for e in load_project(METADATA, env="dev", environ={}):
        validate_steps(e.transforms, e.name)
