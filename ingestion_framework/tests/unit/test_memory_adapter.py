import pytest

from ingest.adapters.memory import DictSecrets, MemoryState


def test_state_round_trip_and_missing():
    state = MemoryState()
    assert state.get("e") is None
    state.put("e", {"watermark": "1"})
    assert state.get("e") == {"watermark": "1"}


def test_secrets_lookup_and_clear_error():
    secrets = DictSecrets({"kv/pwd": "s3cret"})
    assert secrets.get("kv", "pwd") == "s3cret"
    with pytest.raises(KeyError, match="secret 'kv/missing' not found"):
        secrets.get("kv", "missing")
