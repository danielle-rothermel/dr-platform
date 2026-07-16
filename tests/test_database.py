"""High-leverage PostgreSQL migration and connection guarantees."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from psycopg import sql
from sqlalchemy import Connection, Engine, create_engine, text

from dr_platform import (
    FailureClass,
    PlatformSchema,
    list_operations,
    upgrade_platform_schema,
)
from dr_platform.backoff import (
    list_throttle_states,
    record_throttle_failure,
)
from dr_platform.db.migrate import (
    PLATFORM_BASELINE_REVISION,
    PLATFORM_HEAD_REVISION,
)
from tests.conftest import engine_dsn


def test_public_migration_creates_a_usable_schema(pg_engine: Engine) -> None:
    upgrade_platform_schema(engine_dsn(pg_engine))
    schema = PlatformSchema()
    now = datetime(2026, 1, 1, tzinfo=UTC)

    with pg_engine.begin() as connection:
        recorded = record_throttle_failure(
            connection,
            throttle_key="provider:model",
            failure_class=FailureClass.TRANSIENT,
            error_type="RateLimited",
            now=now,
            schema=schema,
        )
    with pg_engine.connect() as connection:
        persisted = list_throttle_states(connection, schema=schema)

    assert recorded is not None
    assert persisted == (recorded,)


def test_upgrade_from_published_baseline_preserves_registration_progress(
    pg_engine: Engine,
) -> None:
    dsn = engine_dsn(pg_engine)
    upgrade_platform_schema(dsn, revision=PLATFORM_BASELINE_REVISION)
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_operations (
                    operation_key, group_key, workflow_role, status,
                    requested_count, manifest_version, manifest_digest,
                    manifest_page_size, manifest_page_count,
                    operation_execution_recipe_digest,
                    target_key, target_version, target_contract_digest,
                    platform_cut_version, registration_cursor, retry_policy,
                    inserted_count, already_present_count, enqueued_count,
                    workflow_already_present_count, enqueue_failed_count,
                    active_count, succeeded_count, terminal_failed_count,
                    cancelled_count, spec, metadata, created_at, updated_at
                ) VALUES (
                    'existing', 'group', 'role', 'registering',
                    3, 3, 'manifest-digest', 2, 2, 'recipe-digest',
                    'target', 1, 'target-contract',
                    1, 1, '{"max_attempts": 3, "max_enqueue_tries": 3}',
                    1, 1, 0, 0, 0, 0, 0, 0, 0,
                    '{}', '{}', now(), now()
                )
                """
            )
        )

    upgrade_platform_schema(dsn)

    with pg_engine.connect() as connection:
        assert connection.execute(
            text(
                """
                SELECT registration_page_size, registration_page_count,
                       registration_cursor, inserted_count
                FROM platform_operations
                WHERE operation_key = 'existing'
                """
            )
        ).one() == (2, 2, 1, 2)
        assert (
            connection.execute(
                text(
                    "SELECT version_num FROM platform_platform_alembic_version"
                )
            ).scalar_one()
            == PLATFORM_HEAD_REVISION
        )


def _quote_identifier(connection: Connection, value: str) -> str:
    return connection.dialect.identifier_preparer.quote(value)


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
        assert list_operations(engine=credential_engine) == ()
    finally:
        credential_engine.dispose()
        with pg_engine.begin() as connection:
            quoted_role = _quote_identifier(connection, role)
            connection.exec_driver_sql(f"DROP OWNED BY {quoted_role} CASCADE")
            connection.exec_driver_sql(f"DROP ROLE {quoted_role}")
