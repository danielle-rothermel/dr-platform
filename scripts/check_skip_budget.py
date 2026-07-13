"""Enforce the hosted-CI pytest skip budget from a junit XML report.

Hosted CI must exercise the real PostgreSQL behavior suite; the only test
allowed to skip is the explicitly credential-gated MotherDuck capability
test. Any other skip means the run silently lost its evidence.
"""
# The XML being parsed is pytest's own junit report, not untrusted input.
# ruff: noqa: ICN001, S314, TC003

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated
from xml.etree import ElementTree

import typer

ALLOWED_SKIPS = frozenset(
    {"test_motherduck_fence_rejects_stale_stage_and_uses_returning"}
)

app = typer.Typer(add_completion=False)


@app.command()
def check(
    junit_xml: Annotated[
        Path, typer.Argument(help="Path to the pytest junit XML report.")
    ],
) -> None:
    root = ElementTree.parse(junit_xml).getroot()
    skipped = [
        str(case.get("name"))
        for case in root.iter("testcase")
        if case.find("skipped") is not None
    ]
    unexpected = [name for name in skipped if name not in ALLOWED_SKIPS]
    total = sum(
        int(suite.get("tests", "0")) for suite in root.iter("testsuite")
    )
    print(f"{total} collected, {len(skipped)} skipped: {skipped}")
    if total == 0:
        print("no tests ran; the suite lost its evidence", file=sys.stderr)
        raise typer.Exit(code=1)
    if unexpected or len(skipped) > len(ALLOWED_SKIPS):
        print(
            f"skip budget exceeded: {unexpected or skipped}", file=sys.stderr
        )
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
