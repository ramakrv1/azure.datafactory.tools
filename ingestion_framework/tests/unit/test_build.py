import sys
import types

from ingest.build import build_id


def test_build_id_defaults_to_dev_without_a_stamp(monkeypatch):
    monkeypatch.setitem(sys.modules, "ingest._build", None)  # import of the stamp module fails
    assert build_id() == "dev"


def test_build_id_reads_the_stamp_written_at_build_time(monkeypatch):
    monkeypatch.setitem(sys.modules, "ingest._build", types.SimpleNamespace(BUILD="abc123"))
    assert build_id() == "abc123"
