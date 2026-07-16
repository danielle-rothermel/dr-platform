"""Credential-preserving database URL behavior.

Stringifying a SQLAlchemy URL — or rendering it without an explicit
``hide_password`` — replaces the password with the literal ``***``. A DSN
rebuilt that way still authenticates against
trust-auth local sockets, so the suite passes locally, but every fresh
connection fails with ``password authentication failed`` against a
password-authenticated server such as the hosted CI service container.
Every render of a URL that will be reconnected must state its masking
explicitly. The round-trip behavior below covers that connection boundary.
"""

from __future__ import annotations

from sqlalchemy import create_engine

from tests.conftest import engine_dsn


def test_engine_dsn_round_trips_credentials() -> None:
    url = "postgresql+psycopg://alice:s3cret@db.example:5432/app"
    dsn = engine_dsn(create_engine(url))
    assert dsn == url
