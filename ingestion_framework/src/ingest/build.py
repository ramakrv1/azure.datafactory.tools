"""Identity of the installed build, so a deployment can prove which code actually ran."""
from __future__ import annotations


def build_id() -> str:
    """Return the stamp CI writes to `ingest/_build.py` before building the wheel (the commit SHA), else "dev"."""
    try:
        from ingest._build import BUILD
    except ImportError:
        return "dev"
    return BUILD
