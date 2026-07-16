from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, create_engine, text

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


@pytest.fixture(scope="session")
def pg_url() -> str:
    try:
        engine = create_engine(TEST_DATABASE_URL)
        with engine.connect():
            pass
        engine.dispose()
    except Exception:  # noqa: BLE001 -- any connect failure means skip
        pytest.skip(
            "postgres unavailable (set DR_PLATFORM_TEST_DATABASE_URL "
            "or create dr_platform_test)"
        )
    return TEST_DATABASE_URL


@pytest.fixture
def clean_pg(pg_url: str) -> str:
    """A scratch database with pgcrypto restored after each schema reset."""
    engine = create_engine(pg_url)
    with engine.begin() as connection:
        # pgcrypto installs its functions in public. Dropping public without
        # removing the extension leaves a catalog entry whose functions no
        # longer exist, so recreate the extension after the schema reset.
        connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("CREATE EXTENSION pgcrypto"))
    engine.dispose()
    return pg_url


@pytest.fixture
def pg_engine(clean_pg: str) -> Iterator[Engine]:
    engine = create_engine(clean_pg)
    yield engine
    engine.dispose()
