from __future__ import annotations

import pytest
from sqlalchemy import text

from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform.testing import migrated_engine, validate_test_database_url
from dr_platform.testing.engine import DEFAULT_TEST_DATABASE_URL
from tests.conftest import TEST_DATABASE_URL, _reset_test_database


def test_validate_test_database_url_rejects_non_postgres() -> None:
    with pytest.raises(ValueError, match="must use PostgreSQL"):
        validate_test_database_url("sqlite:///dr_platform_test")


def test_validate_test_database_url_rejects_identity_override() -> None:
    with pytest.raises(ValueError, match="identity"):
        validate_test_database_url(
            "postgresql+psycopg:///dr_platform_test?dbname=production"
        )


def test_validate_test_database_url_rejects_non_test_database_name() -> None:
    with pytest.raises(ValueError, match="_test"):
        validate_test_database_url("postgresql+psycopg:///dr_platform_prod")


def test_validate_test_database_url_accepts_default_env_dsn() -> None:
    validate_test_database_url(TEST_DATABASE_URL)
    validate_test_database_url(DEFAULT_TEST_DATABASE_URL)


def test_migrated_engine_yields_usable_schema(clean_pg: str) -> None:
    _reset_test_database(clean_pg)
    with migrated_engine(clean_pg) as engine, engine.connect() as connection:
        schema = LedgerSchema()
        tables = connection.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        ).scalars()
        assert schema.work_items.name in set(tables)


def test_migrated_engine_disposes_on_exit(clean_pg: str) -> None:
    _reset_test_database(clean_pg)
    with migrated_engine(clean_pg) as engine:
        pool = engine.pool
        assert pool is not None
        disposed = pool.status()
        assert disposed

    with migrated_engine(clean_pg) as engine, engine.connect() as connection:
        connection.execute(text("SELECT 1"))
