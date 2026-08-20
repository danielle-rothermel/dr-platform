from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine

from dr_platform.runtime.database.migrate import upgrade_platform_schema
from dr_platform.testing._dsn import validate_test_database_url

DEFAULT_TEST_DATABASE_URL = os.environ.get(
    "DR_PLATFORM_TEST_DATABASE_URL",
    "postgresql+psycopg:///dr_platform_test",
)


def _engine_dsn(engine: Engine) -> str:
    return engine.url.render_as_string(hide_password=False)


@contextmanager
def migrated_engine(dsn: str | None = None) -> Iterator[Engine]:
    """Create an engine on a validated test DSN and run migrations."""
    selected_dsn = dsn or DEFAULT_TEST_DATABASE_URL
    validate_test_database_url(selected_dsn)
    engine = create_engine(selected_dsn)
    try:
        upgrade_platform_schema(_engine_dsn(engine))
        yield engine
    finally:
        engine.dispose()
