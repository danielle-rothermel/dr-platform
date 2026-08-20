from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator, Sized
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from typing import TYPE_CHECKING, cast

import pytest
from dbos import DBOS, DBOSConfig
from sqlalchemy import Engine, create_engine, text

from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.pipeline.definitions import PipelineDefinition
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.live_identity import (
    LOCAL_EXECUTOR_SENTINEL,
    LiveDbosIdentity,
)
from dr_platform.runtime.database.migrate import upgrade_platform_schema
from dr_platform.runtime.dbos import DEFAULT_POOL_SIZE, PlatformDbosConfig
from dr_platform.runtime.dispatcher import (
    DispatcherRegistration,
)
from dr_platform.submission.stream import (
    RunMemberInput,
    RunRegistrationDeclaration,
    SubmissionReceipt,
    WorkInput,
    submit,
)
from dr_platform.testing._dsn import validate_test_database_url

if TYPE_CHECKING:
    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy import Connection

    from dr_platform.admission.runner import AdmissionPayload
    from dr_platform.pipeline.definitions import PipelineDefinition

TEST_DATABASE_URL = os.environ.get(
    "DR_PLATFORM_TEST_DATABASE_URL",
    "postgresql+psycopg:///dr_platform_test",
)

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)
DEFAULT_MAX_RECOVERY_ATTEMPTS = 1


def default_live_dbos_identity() -> LiveDbosIdentity:
    return LiveDbosIdentity(
        executor_ids=frozenset({LOCAL_EXECUTOR_SENTINEL}),
    )


def set_live_dbos_identity(
    monkeypatch: pytest.MonkeyPatch,
    *,
    app_version: str,
    executor_id: str = LOCAL_EXECUTOR_SENTINEL,
) -> None:
    from dbos._utils import GlobalParams

    monkeypatch.setattr(GlobalParams, "app_version", app_version)
    monkeypatch.setattr(GlobalParams, "executor_id", executor_id)


def default_platform_dbos_config(
    database_url: str,
    *,
    system_database_url: str | None,
    max_recovery_attempts: int,
) -> PlatformDbosConfig:
    resolved_system = system_database_url or database_url
    return PlatformDbosConfig(
        database_url=database_url,
        system_database_url=resolved_system,
        max_recovery_attempts=max_recovery_attempts,
        pool_size=DEFAULT_POOL_SIZE,
    )


def engine_dsn(engine: Engine) -> str:
    """Render credentials; ``str(URL)`` masks them and breaks password auth."""
    return engine.url.render_as_string(hide_password=False)


def _verify_postgres_available(database_url: str) -> None:
    validate_test_database_url(database_url)
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
    validate_test_database_url(database_url)
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            # Dropping public strands pgcrypto's catalog entry.
            connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
            connection.execute(text("DROP SCHEMA IF EXISTS dr_store CASCADE"))
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


def _migrate(engine: Engine) -> LedgerSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return LedgerSchema()


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_reference,)


def submit_items(
    *,
    items: Iterable[WorkInput],
    expected_member_count: int | None = None,
    **kwargs: object,
) -> SubmissionReceipt:
    """Test helper adapting item collections to ordered run registration."""
    if expected_member_count is None:
        if not isinstance(items, Sized):
            items = tuple(items)
        expected_member_count = len(items)
    return submit(
        **kwargs,  # ty: ignore[invalid-argument-type]
        declaration=RunRegistrationDeclaration(expected_member_count),
        members=(
            RunMemberInput(ordinal=ordinal, work=item)
            for ordinal, item in enumerate(items)
        ),
    )


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


@dataclass(frozen=True, slots=True)
class _WorkflowStatus:
    """The DBOS workflow-status attributes the sweeps read."""

    workflow_id: str
    status: str
    error: Exception | None = None
    app_version: str | None = None
    executor_id: str | None = None


def _payload_of(args: tuple[object, ...]) -> dict[str, object]:
    payload = args[0]
    assert isinstance(payload, dict)
    return cast("dict[str, object]", payload)


class _RecordingClient:
    """Records enqueue options; args and payloads are recorded alongside.

    ``fail_runs`` rejects enqueues whose first positional payload carries a
    matching ``run_key``, standing in for a client that cannot reach DBOS.
    """

    def __init__(self, *, fail_runs: frozenset[str] = frozenset()) -> None:
        self.fail_runs = fail_runs
        self.enqueued: list[EnqueueOptions] = []
        self.enqueued_args: list[tuple[object, ...]] = []
        self._lock = Lock()

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        *args: object,
        **_kwargs: object,
    ) -> object:
        if self.fail_runs and args:
            run_key = _payload_of(args).get("run_key")
            if run_key in self.fail_runs:
                raise RuntimeError(f"cannot enqueue {run_key}")
        with self._lock:
            self.enqueued.append(cast("EnqueueOptions", dict(options)))
            self.enqueued_args.append(args)
        return object()

    @property
    def enqueued_payloads(self) -> list[dict[str, object]]:
        """Read the first positional argument of each enqueue as a mapping."""
        return [_payload_of(args) for args in self.enqueued_args]


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
        "db_engine_kwargs": {
            "pool_size": DEFAULT_POOL_SIZE,
            "max_overflow": 0,
        },
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


def handoff_utc_now() -> datetime:
    return datetime.now(UTC)


def configure_stage_controls(
    engine: Engine,
    pipeline: PipelineDefinition,
    *,
    capacity: int,
    clock: Callable[[], datetime] | None = None,
) -> None:
    from dr_platform.admission.controls import upsert_stage_control

    selected_clock = clock or handoff_utc_now
    with engine.begin() as connection:
        for stage in pipeline.stages:
            upsert_stage_control(
                connection,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key,
                selector={},
                capacity=capacity,
                paused=False,
                updated_at=selected_clock(),
            )


def stage_state_count(
    engine: Engine,
    schema: LedgerSchema,
    *,
    stage_index: int,
    state: StageExecutionState,
) -> int:
    from sqlalchemy import func, select

    with engine.connect() as connection:
        return connection.execute(
            select(func.count())
            .select_from(schema.stage_executions)
            .where(
                schema.stage_executions.c.stage_index == stage_index,
                schema.stage_executions.c.state == state.value,
            )
        ).scalar_one()


def wait_for_handoff(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 15,
) -> None:
    import time

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for DBOS stage workflow")


def launch_handoff_dbos(
    database_url: str,
    *,
    suffix: str,
    engine: Engine,
    registry: PipelineRegistry,
    app_version_prefix: str = "handoff-v2",
) -> DispatcherRegistration:
    from dr_platform.runtime.dispatcher import register_scheduled_dispatcher

    DBOS(
        config=dbos_config(
            name=f"drp-{app_version_prefix}-{suffix}",
            system_database_url=database_url,
            application_database_url=database_url,
            application_version=f"{app_version_prefix}-{suffix}",
            notification_listener_polling_interval_sec=0.01,
        )
    )
    registration = register_scheduled_dispatcher(
        live_dbos_identity=default_live_dbos_identity(),
        config=PlatformDbosConfig(
            database_url=database_url,
            system_database_url=database_url,
            max_recovery_attempts=1,
        ),
        engine=engine,
        registry=registry,
        sweep_cron=None,
    )
    try:
        DBOS.launch()
    except Exception:
        registration.close()
        raise
    return registration


def recorded_workflow_id(options: EnqueueOptions) -> str:
    workflow_id = options.get("workflow_id")
    assert workflow_id is not None
    return workflow_id
