from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine, make_url, text

TEST_DATABASE_URL = os.environ.get(
    "DR_PLATFORM_TEST_DATABASE_URL",
    "postgresql+psycopg:///dr_platform_test",
)


def engine_dsn(engine: Engine) -> str:
    """The engine's DSN with credentials intact.

    ``str(URL)`` masks any password as the literal ``***``; a DSN
    rebuilt that way still authenticates against trust-auth local sockets
    but fails against password-authenticated servers such as the hosted CI
    service container.
    """
    return engine.url.render_as_string(hide_password=False)


def _validate_test_database_url(database_url: str) -> None:
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        raise ValueError("DR_PLATFORM_TEST_DATABASE_URL must use PostgreSQL")
    database_identity_overrides = {
        key.lower()
        for key in url.query
        if key.lower() in {"dbname", "service", "servicefile"}
    }
    if database_identity_overrides:
        raise ValueError(
            "DR_PLATFORM_TEST_DATABASE_URL must not override database "
            "identity through query parameters"
        )
    database_name = url.database
    if database_name is None or not database_name.endswith("_test"):
        raise ValueError(
            "DR_PLATFORM_TEST_DATABASE_URL must name a database ending in "
            "'_test'"
        )


def _verify_postgres_available(database_url: str) -> None:
    _validate_test_database_url(database_url)
    engine = create_engine(database_url)
    try:
        with engine.connect():
            pass
    except Exception:
        if os.environ.get("CI", "").lower() == "true":
            raise
        pytest.skip(
            "postgres unavailable (set DR_PLATFORM_TEST_DATABASE_URL "
            "or create dr_platform_test)"
        )
    finally:
        engine.dispose()


def _reset_test_database(database_url: str) -> None:
    _validate_test_database_url(database_url)
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            # pgcrypto installs its functions in public. Dropping public
            # without removing the extension leaves a catalog entry whose
            # functions no longer exist, so recreate the extension after the
            # schema reset.
            connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
            connection.execute(text("DROP SCHEMA public CASCADE"))
            connection.execute(text("CREATE SCHEMA public"))
            connection.execute(text("CREATE EXTENSION pgcrypto"))
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def pg_url() -> str:
    _verify_postgres_available(TEST_DATABASE_URL)
    return TEST_DATABASE_URL


@pytest.fixture
def clean_pg(pg_url: str) -> str:
    """A scratch database with pgcrypto restored after each schema reset."""
    _reset_test_database(pg_url)
    return pg_url


@pytest.fixture
def pg_engine(clean_pg: str) -> Iterator[Engine]:
    engine = create_engine(clean_pg)
    yield engine
    engine.dispose()
