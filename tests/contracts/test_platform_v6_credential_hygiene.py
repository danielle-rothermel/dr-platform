"""Source contract: rebuilt DSNs must never pass through password masking.

Stringifying a SQLAlchemy URL — or rendering it without an explicit
``hide_password`` — replaces the password with the literal ``***``. A DSN
rebuilt that way still authenticates against
trust-auth local sockets, so the suite passes locally, but every fresh
connection fails with ``password authentication failed`` against a
password-authenticated server such as the hosted CI service container.
Every render of a URL that will be reconnected must state its masking
explicitly.
"""

from __future__ import annotations

import re
from pathlib import Path

from sqlalchemy import create_engine

from tests.conftest import engine_dsn

REPO_ROOT = Path(__file__).resolve().parents[2]

_STR_OF_URL = re.compile(r"\bstr\(\s*[\w.]+\.url\s*\)")
_BARE_RENDER_AS_STRING = re.compile(r"\.render_as_string\(\s*\)")


def test_no_source_rebuilds_a_dsn_through_password_masking() -> None:
    offenders = [
        f"{path.relative_to(REPO_ROOT)}:{number}: {line.strip()}"
        for tree in ("src", "tests", "scripts")
        for path in sorted((REPO_ROOT / tree).rglob("*.py"))
        for number, line in enumerate(
            path.read_text().splitlines(), start=1
        )
        if _STR_OF_URL.search(line)
        or _BARE_RENDER_AS_STRING.search(line)
    ]
    assert not offenders, (
        "password-masking DSN rebuild; use engine_dsn(...) or "
        "render_as_string(hide_password=False) instead:\n"
        + "\n".join(offenders)
    )


def test_engine_dsn_round_trips_credentials() -> None:
    url = "postgresql+psycopg://alice:s3cret@db.example:5432/app"
    dsn = engine_dsn(create_engine(url))
    assert dsn == url
    assert "***" not in dsn
