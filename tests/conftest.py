from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from dbos import DBOS, DBOSConfig
from sqlalchemy import Engine, create_engine, make_url, text

from dr_platform._core.ledger.schema import StagingSchema
from dr_platform.runtime.database.migrate import upgrade_platform_schema

if TYPE_CHECKING:
    from dbos import DBOSClient

    from dr_platform.admission.runner import AdmissionPayload

TEST_DATABASE_URL = os.environ.get(
    "DR_PLATFORM_TEST_DATABASE_URL",
    "postgresql+psycopg:///dr_platform_test",
)

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def engine_dsn(engine: Engine) -> str:
    """Render credentials; ``str(URL)`` masks them and breaks password auth."""
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
        # Only explicit false values disable CI detection; CI=1 fails loudly.
        ci_value = os.environ.get("CI", "").lower()
        if ci_value and ci_value not in {"false", "0"}:
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
            # Dropping public strands pgcrypto's catalog entry.
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
    _reset_test_database(pg_url)
    return pg_url


@pytest.fixture
def pg_engine(clean_pg: str) -> Iterator[Engine]:
    engine = create_engine(clean_pg)
    yield engine
    engine.dispose()


def _migrate(engine: Engine) -> StagingSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return StagingSchema()


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_reference,)


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


class _RecordingCanceller:
    def __init__(self) -> None:
        self.cancelled: list[tuple[str, bool]] = []

    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        cancel_children: bool = False,
    ) -> None:
        self.cancelled.append((workflow_id, cancel_children))


def dbos_config(
    *,
    name: str,
    system_database_url: str,
    application_version: str,
    application_database_url: str | None = None,
    notification_listener_polling_interval_sec: float | None = None,
) -> DBOSConfig:
    """Disable admin ports and background LISTEN/NOTIFY for isolation."""
    config: DBOSConfig = {
        "name": name,
        "system_database_url": system_database_url,
        "application_version": application_version,
        "run_admin_server": False,
        "use_listen_notify": False,
    }
    if application_database_url is not None:
        config["application_database_url"] = application_database_url
    if notification_listener_polling_interval_sec is not None:
        config["notification_listener_polling_interval_sec"] = (
            notification_listener_polling_interval_sec
        )
    return config


def initialize_dbos_schema(config: DBOSConfig) -> None:
    try:
        DBOS(config=config)
        DBOS.launch()
    finally:
        DBOS.destroy(destroy_registry=True)
