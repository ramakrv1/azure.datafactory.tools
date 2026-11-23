from pathlib import Path

from ingest.cli import main

METADATA = str(Path(__file__).parents[2] / "metadata")


def test_validate_ok(capsys):
    assert main(["validate", "--metadata", METADATA, "--env", "dev"]) == 0
    assert capsys.readouterr().out.strip() == "OK: 3 entities"


def test_validate_reports_metadata_errors_with_exit_2(capsys):
    assert main(["validate", "--metadata", METADATA, "--env", "nope"]) == 2
    assert "nope" in capsys.readouterr().err


def test_validate_reports_bad_transform_steps(tmp_path, write_yaml, entity_dict, capsys):
    write_yaml(tmp_path / "entities" / "a.yaml", entity_dict("a.x", transforms=[{"op": "no_such_op"}]))
    assert main(["validate", "--metadata", str(tmp_path), "--env", ""]) == 2
    assert "no_such_op" in capsys.readouterr().err


def test_plan_prints_waves_and_honours_selection(capsys):
    assert main(["plan", "--metadata", METADATA, "--env", "dev"]) == 0
    assert capsys.readouterr().out.splitlines() == [
        "1: sales.customers, sales.orders", "2: sales.orders_clean"]
    assert main(["plan", "--metadata", METADATA, "--env", "dev", "--exclude", "*clean*"]) == 0
    assert capsys.readouterr().out.splitlines() == ["1: sales.customers, sales.orders"]


def test_docs_prints_mermaid(capsys):
    assert main(["docs", "--metadata", METADATA, "--env", "dev"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("graph LR") and "-->" in out
