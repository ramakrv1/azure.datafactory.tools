"""`ingest` command line: validate, plan and document a metadata project (running happens inside the engine)."""
from __future__ import annotations

import argparse
import sys
from typing import Sequence

from ingest.core import dag
from ingest.core.config import load_project
from ingest.core.model import MetadataError
from ingest.core.selection import select
from ingest.core.transforms import validate_steps


def _validate(entities, args) -> None:
    for entity in entities:
        validate_steps(entity.transforms, entity.name)
    dag.levels(entities)  # raises on cycles
    print(f"OK: {len(entities)} entities")


def _plan(entities, args) -> None:
    chosen = select(entities, args.include, args.exclude)
    names = {e.name for e in chosen}
    for number, wave in enumerate(dag.levels(entities), 1):
        wave = [n for n in wave if n in names]
        if wave:
            print(f"{number}: {', '.join(wave)}")


def _docs(entities, args) -> None:
    print(dag.to_mermaid(entities), end="")


def _key_value(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not (sep and key):
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got '{text}'")
    return key, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest")
    commands = parser.add_subparsers(dest="command", required=True)
    for name, handler, help_ in (("validate", _validate, "check metadata, transform steps and the DAG"),
                                 ("plan", _plan, "print the parallel waves of the selected entities"),
                                 ("docs", _docs, "print the dependency graph as Mermaid")):
        sub = commands.add_parser(name, help=help_)
        sub.add_argument("--metadata", required=True, help="metadata directory")
        sub.add_argument("--env", required=True, help="environment overlay name ('' for none)")
        sub.add_argument("--var", action="append", default=[], type=_key_value, metavar="KEY=VALUE",
                         help="token value, overrides the overlay's variables (repeatable)")
        if name == "plan":
            sub.add_argument("--include", action="append", default=[], metavar="PATTERN")
            sub.add_argument("--exclude", action="append", default=[], metavar="PATTERN")
        sub.set_defaults(handler=handler)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI; returns the exit code (0 ok, 2 invalid metadata)."""
    args = _parser().parse_args(argv)
    try:
        args.handler(load_project(args.metadata, args.env or None, variables=dict(args.var)), args)
    except MetadataError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0

