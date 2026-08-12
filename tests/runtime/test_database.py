from __future__ import annotations

from uuid import uuid4

import pytest
from dr_store.storage_backends.postgresql import (
    POSTGRES_METADATA,
    POSTGRES_SCHEMA_FORMAT,
    PostgresBackend,
)
from psycopg import sql
from sqlalchemy import Connection, Engine, create_engine, text

from dr_platform.inspection.campaigns import list_campaigns
from dr_platform.runtime.database import upgrade_platform_schema
from dr_platform.runtime.database.migrate import (
    PLATFORM_BASELINE_REVISION,
    PLATFORM_HEAD_REVISION,
)
from tests import conftest
from tests.conftest import engine_dsn


class _UnavailableEngine:
    def __init__(self) -> None:
        self.disposed = False

    def connect(self) -> None:
        raise ConnectionError("postgres unavailable")

    def dispose(self) -> None:
        self.disposed = True


def _quote_identifier(connection: Connection, value: str) -> str:
    return connection.dialect.identifier_preparer.quote(value)


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg:///dr_platform_test",
        "postgresql+psycopg://user:secret@db:5432/team_agent_test",
    ],
)
def test_test_database_identity_accepts_explicit_test_names(
    database_url: str,
) -> None:
    conftest._validate_test_database_url(database_url)


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg:///postgres",
        "postgresql+psycopg:///dr_platform",
        "postgresql+psycopg://db.example.invalid",
    ],
)
def test_test_database_identity_rejects_other_names(
    database_url: str,
) -> None:
    with pytest.raises(ValueError, match="ending in '_test'"):
        conftest._validate_test_database_url(database_url)


def test_test_database_identity_rejects_other_backends() -> None:
    with pytest.raises(ValueError, match="must use PostgreSQL"):
        conftest._validate_test_database_url("sqlite:///dr_platform_test")


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg:///dr_platform",
        "postgresql+psycopg:///safe_test?dbname=dr_platform",
        "postgresql+psycopg:///safe_test?service=production",
        "postgresql+psycopg:///safe_test?servicefile=/tmp/pg_service.conf",
    ],
)
def test_unsafe_database_url_is_rejected_before_destructive_setup(
    monkeypatch: pytest.MonkeyPatch,
    database_url: str,
) -> None:
    def fail_if_called(_database_url: str) -> None:
        pytest.fail("unsafe database URL reached engine creation")

    monkeypatch.setattr(conftest, "create_engine", fail_if_called)

    with pytest.raises(ValueError, match="DR_PLATFORM_TEST_DATABASE_URL"):
        conftest._reset_test_database(database_url)


@pytest.mark.parametrize("ci_value", ["true", "TRUE", "1", "yes"])
def test_ci_connection_failure_fails_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
    ci_value: str,
) -> None:
    engine = _UnavailableEngine()
    monkeypatch.setenv("CI", ci_value)
    monkeypatch.setattr(
        conftest,
        "create_engine",
        lambda _database_url: engine,
    )

    with pytest.raises(ConnectionError, match="postgres unavailable"):
        conftest._verify_postgres_available(
            "postgresql+psycopg:///dr_platform_test"
        )

    assert engine.disposed


@pytest.mark.parametrize("ci_value", [None, "", "false", "FALSE", "0"])
def test_local_connection_failure_skips_and_disposes_engine(
    monkeypatch: pytest.MonkeyPatch,
    ci_value: str | None,
) -> None:
    engine = _UnavailableEngine()
    if ci_value is None:
        monkeypatch.delenv("CI", raising=False)
    else:
        monkeypatch.setenv("CI", ci_value)
    monkeypatch.setattr(
        conftest,
        "create_engine",
        lambda _database_url: engine,
    )

    with pytest.raises(pytest.skip.Exception, match="postgres unavailable"):
        conftest._verify_postgres_available(
            "postgresql+psycopg:///dr_platform_test"
        )

    assert engine.disposed


def test_password_dsn_survives_rendering_reconnection_and_migration(
    pg_engine: Engine,
) -> None:
    role = f"dr_platform_password_{uuid4().hex}"
    password = f"credential-{uuid4().hex}"
    database = pg_engine.url.database
    assert database is not None

    with pg_engine.begin() as connection:
        quoted_role = _quote_identifier(connection, role)
        quoted_database = _quote_identifier(connection, database)
        connection.connection.cursor().execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role),
                sql.Literal(password),
            )
        )
        connection.exec_driver_sql(
            f"GRANT CONNECT ON DATABASE {quoted_database} TO {quoted_role}"
        )
        connection.exec_driver_sql(
            f"GRANT CREATE ON DATABASE {quoted_database} TO {quoted_role}"
        )
        connection.exec_driver_sql(
            f"GRANT USAGE, CREATE ON SCHEMA public TO {quoted_role}"
        )

    credential_url = pg_engine.url.set(
        username=role,
        password=password,
        host=pg_engine.url.host or "127.0.0.1",
    )
    rendered_engine = create_engine(credential_url)
    rendered = engine_dsn(rendered_engine)
    rendered_engine.dispose()
    credential_engine = create_engine(rendered)
    try:
        assert "***" not in rendered
        assert password in rendered
        with credential_engine.connect():
            pass
        upgrade_platform_schema(rendered)
        assert list_campaigns(engine=credential_engine) == ()
    finally:
        credential_engine.dispose()
        with pg_engine.begin() as connection:
            quoted_role = _quote_identifier(connection, role)
            connection.exec_driver_sql(f"DROP OWNED BY {quoted_role} CASCADE")
            connection.exec_driver_sql(f"DROP ROLE {quoted_role}")


def _alembic_revision(engine: Engine) -> str | None:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT version_num FROM platform_platform_alembic_version")
        ).scalar_one_or_none()


def test_dr_store_migration_converges_empty_preexisting_namespace(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(
        engine_dsn(pg_engine),
        revision=PLATFORM_BASELINE_REVISION,
    )
    with pg_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS dr_store CASCADE"))
        connection.execute(text("CREATE SCHEMA dr_store"))

    upgrade_platform_schema(
        engine_dsn(pg_engine),
        revision=PLATFORM_HEAD_REVISION,
    )

    assert _alembic_revision(pg_engine) == PLATFORM_HEAD_REVISION
    with pg_engine.connect() as connection:
        formats = (
            connection.execute(
                text("SELECT format FROM dr_store.schema_format")
            )
            .scalars()
            .all()
        )
        assert formats == [POSTGRES_SCHEMA_FORMAT]
        assert connection.execute(
            text(
                """
                SELECT to_regclass('dr_store.objects') IS NOT NULL
                    AND to_regclass('dr_store.bindings') IS NOT NULL
                """
            )
        ).scalar_one()
    PostgresBackend.open_sync(pg_engine)


def test_dr_store_migration_rejects_incompatible_format_marker(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(
        engine_dsn(pg_engine),
        revision=PLATFORM_BASELINE_REVISION,
    )
    with pg_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS dr_store CASCADE"))
        connection.execute(text("CREATE SCHEMA dr_store"))
        POSTGRES_METADATA.create_all(connection, checkfirst=True)
        connection.execute(
            text(
                """
                INSERT INTO dr_store.schema_format (singleton, format)
                VALUES (TRUE, :format)
                """
            ),
            {"format": "dr-store-postgresql-v0"},
        )

    with pytest.raises(RuntimeError, match="incompatible format"):
        upgrade_platform_schema(
            engine_dsn(pg_engine),
            revision=PLATFORM_HEAD_REVISION,
        )

    assert _alembic_revision(pg_engine) == PLATFORM_BASELINE_REVISION
