from __future__ import annotations

import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dbos import DBOS, DBOSClient, EnqueueOptions, Queue
from sqlalchemy import Engine, func, select

import dr_platform.recovery.cancellation as cancellation_module
import dr_platform.recovery.sweep as sweep_module
from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import upsert_stage_control
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import (
    StageHandoffMismatchError,
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.cancellation import (
    CancellationDisposition,
    WorkCancellationResult,
    cancel_work,
)
from dr_platform.recovery.sweep import sweep_abandoned_stages
from dr_platform.runtime.dbos import PlatformDbosConfig
from dr_platform.runtime.dispatcher import (
    DispatcherRegistration,
    register_scheduled_dispatcher,
)
from dr_platform.submission.stream import WorkInput
from dr_platform.submission.work_items import stable_random_rank
from tests.conftest import (
    _args_for,
    _as_dbos_client,
    _migrate,
    _RecordingCanceller,
    dbos_config,
    submit_items,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import StagingSchema


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _pipeline(
    *,
    key: str,
    stage_logic: tuple[tuple[str, Callable[[str], object]], ...],
) -> PipelineDefinition:
    def as_async(logic: Callable[[str], object]):
        async def run(input_reference: str) -> str | None:
            result = logic(input_reference)
            if inspect.isawaitable(result):
                result = await result
            return cast("str | None", result)

        return run

    return PipelineDefinition(
        key=PipelineKey(key),
        version=1,
        stages=tuple(
            StageDefinition(
                key=StageKey(stage_key),
                queue_name=f"{key}-{stage_key}-queue",
                workflow=as_async(logic),
                args_for=_args_for,
            )
            for stage_key, logic in stage_logic
        ),
    )


def _configure_controls(
    engine: Engine,
    pipeline: PipelineDefinition,
    *,
    capacity: int,
) -> None:
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
                updated_at=_utc_now(),
            )


def _submit_items(  # noqa: PLR0913 -- explicit submission facts
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    campaign_key: str,
    run_key: str,
    items: tuple[WorkInput, ...],
    clock: Callable[[], datetime] = _utc_now,
) -> None:
    submit_items(
        campaign_key=campaign_key,
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference="config:smoke",
        items=items,
        registry=registry,
        engine=engine,
        clock=clock,
    )


def _launch_dbos(
    database_url: str,
    *,
    suffix: str,
    engine: Engine,
    registry: PipelineRegistry,
) -> DispatcherRegistration:
    DBOS(
        config=dbos_config(
            name=f"drp-handoff-{suffix}",
            system_database_url=database_url,
            application_database_url=database_url,
            application_version=f"handoff-{suffix}",
            notification_listener_polling_interval_sec=0.01,
        )
    )
    registration = register_scheduled_dispatcher(
        config=PlatformDbosConfig(
            database_url=database_url,
            system_database_url=database_url,
        ),
        engine=engine,
        registry=registry,
    )
    try:
        DBOS.launch()
    except Exception:
        registration.close()
        raise
    return registration


def _wait_for(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 5,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for DBOS stage workflow")


def _wait_for_workflow_statuses(
    client: DBOSClient,
    workflow_ids: list[str],
    *,
    expected_status: str,
) -> None:
    _wait_for(
        lambda: (
            len(
                client.list_workflows(
                    workflow_ids=workflow_ids,
                    status=expected_status,
                    load_input=False,
                    load_output=False,
                )
            )
            == len(workflow_ids)
        )
    )


def _stage_state_count(
    engine: Engine,
    schema: StagingSchema,
    *,
    stage_index: int,
    state: StageExecutionState,
) -> int:
    with engine.connect() as connection:
        return connection.execute(
            select(func.count())
            .select_from(schema.stage_executions)
            .where(
                schema.stage_executions.c.stage_index == stage_index,
                schema.stage_executions.c.state == state.value,
            )
        ).scalar_one()


class _RecordingClient:
    def __init__(self) -> None:
        self.enqueued: list[tuple[EnqueueOptions, tuple[object, ...]]] = []

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        *args: object,
        **_kwargs: object,
    ) -> object:
        self.enqueued.append((cast("EnqueueOptions", dict(options)), args))
        return object()


def _recorded_workflow_id(options: EnqueueOptions) -> str:
    workflow_id = options.get("workflow_id")
    assert workflow_id is not None
    return workflow_id


def _submit_and_admit_one(
    engine: Engine,
    schema: StagingSchema,
    pipeline: PipelineDefinition,
    *,
    campaign_key: str,
    run_key: str,
) -> tuple[str, int, int]:
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(engine, pipeline, capacity=1)
    _submit_items(
        engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=run_key,
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
            ),
        ),
    )
    client = _RecordingClient()
    admitted = run_admission_pass(
        engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    workflow_id = _recorded_workflow_id(client.enqueued[0][0])
    with engine.connect() as connection:
        stage_execution_id, work_item_id = connection.execute(
            select(
                schema.stage_executions.c.stage_execution_id,
                schema.stage_executions.c.work_item_id,
            ).where(
                schema.stage_executions.c.state
                == StageExecutionState.ADMITTED.value
            )
        ).one()
    return workflow_id, stage_execution_id, work_item_id


def _handoff_snapshot(
    engine: Engine,
    schema: StagingSchema,
) -> tuple[tuple[tuple[object, ...], ...], tuple[tuple[object, ...], ...]]:
    with engine.connect() as connection:
        executions = tuple(
            connection.execute(
                select(
                    schema.stage_executions.c.stage_execution_id,
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.current_attempt,
                    schema.stage_executions.c.output_reference,
                    schema.stage_executions.c.updated_at,
                ).order_by(schema.stage_executions.c.stage_execution_id)
            ).tuples()
        )
        attempts = tuple(
            connection.execute(
                select(
                    schema.stage_attempts.c.stage_attempt_id,
                    schema.stage_attempts.c.attempt_number,
                    schema.stage_attempts.c.workflow_id,
                    schema.stage_attempts.c.terminal_at,
                    schema.stage_attempts.c.terminal_summary,
                    schema.stage_attempts.c.terminal_reference,
                ).order_by(schema.stage_attempts.c.stage_attempt_id)
            ).tuples()
        )
    return executions, attempts


def test_three_stage_pipeline_streams_end_to_end_through_wrapped_workflows(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def prepare(input_reference: str) -> str:
        return f"prepared:{input_reference}"

    def execute(input_reference: str) -> str:
        return f"executed:{input_reference}"

    def score(input_reference: str) -> str:
        return f"scored:{input_reference}"

    declared = _pipeline(
        key=f"evaluation-{suffix}",
        stage_logic=(
            ("prepare", prepare),
            ("execute", execute),
            ("score", score),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        items=(
            WorkInput(work_key="work-a", input_reference="input:a", labels={}),
            WorkInput(work_key="work-b", input_reference="input:b", labels={}),
        ),
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        for stage_index in range(3):
            summary = run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            )
            assert summary.admitted_total == 2
            _wait_for(
                lambda stage_index=stage_index: (
                    _stage_state_count(
                        pg_engine,
                        schema,
                        stage_index=stage_index,
                        state=StageExecutionState.SUCCEEDED,
                    )
                    == 2
                )
            )

        with pg_engine.connect() as connection:
            rows = (
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.state,
                        schema.stage_executions.c.output_reference,
                    ).order_by(
                        schema.stage_executions.c.work_item_id,
                        schema.stage_executions.c.stage_index,
                    )
                )
                .tuples()
                .all()
            )
            workflow_ids = (
                connection.execute(select(schema.stage_attempts.c.workflow_id))
                .scalars()
                .all()
            )

        assert len(rows) == 6
        assert {row[0] for row in rows} == {0, 1, 2}
        assert {row[1] for row in rows} == {"prepare", "execute", "score"}
        assert all(
            row[2] == StageExecutionState.SUCCEEDED.value for row in rows
        )
        assert all(row[3] for row in rows)
        assert len(workflow_ids) == 6
        _wait_for_workflow_statuses(
            client,
            list(workflow_ids),
            expected_status="SUCCESS",
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_completion_and_next_ready_insert_roll_back_together(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="atomic-handoff",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-atomic",
        run_key="run-atomic",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0][0])

    def fail_before_successor() -> None:
        raise RuntimeError("injected between completion and handoff")

    with (
        pytest.raises(RuntimeError, match="injected between"),
        pg_engine.begin() as connection,
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            next_stage_key="execute",
            next_stage_index=1,
            completed_at=_utc_now(),
            before_next_stage=fail_before_successor,
        )

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                )
            )
            .tuples()
            .all()
        )
        terminal_at = connection.execute(
            select(schema.stage_attempts.c.terminal_at)
        ).scalar_one()

    assert execution_rows == [("prepare", "admitted", None)]
    assert terminal_at is None


@pytest.mark.parametrize(
    "mismatch",
    [
        "workflow_id",
        "pipeline_version",
        "stage_key",
        "stage_index",
    ],
)
def test_completion_identity_mismatch_does_not_mutate_state(
    pg_engine: Engine,
    mismatch: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="identity-mismatch",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-identity-mismatch",
        run_key="run-identity-mismatch",
    )
    before = _handoff_snapshot(pg_engine, schema)
    expected_error = (
        LookupError if mismatch == "workflow_id" else StageHandoffMismatchError
    )

    with (
        pytest.raises(expected_error),
        pg_engine.begin() as connection,
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=(
                "missing-workflow"
                if mismatch == "workflow_id"
                else workflow_id
            ),
            pipeline_key=pipeline.key.value,
            pipeline_version=(
                pipeline.version + 1
                if mismatch == "pipeline_version"
                else pipeline.version
            ),
            stage_key=(
                "wrong-stage" if mismatch == "stage_key" else "prepare"
            ),
            stage_index=1 if mismatch == "stage_index" else 0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            next_stage_key="execute",
            next_stage_index=1,
            completed_at=_utc_now(),
        )

    assert _handoff_snapshot(pg_engine, schema) == before


_TERMINAL_OBJECT_REFERENCE = (
    "objref://terminal-result/v3"
    "?schema=terminal_result"
    "&schema_version=7"
    "&content_hash="
    "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3"
)


def test_output_reference_is_transported_opaquely_without_parsing(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="opaque-output",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-opaque-output",
        run_key="run-opaque-output",
    )

    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="execute",
            stage_index=0,
            succeeded=True,
            output_reference=_TERMINAL_OBJECT_REFERENCE,
            terminal_summary={"outcome": "succeeded"},
            terminal_reference=_TERMINAL_OBJECT_REFERENCE,
            next_stage_key=None,
            next_stage_index=None,
            completed_at=_utc_now(),
        )

    with pg_engine.connect() as connection:
        stored_output = connection.execute(
            select(schema.stage_executions.c.output_reference)
        ).scalar_one()
        stored_terminal = connection.execute(
            select(schema.stage_attempts.c.terminal_reference)
        ).scalar_one()

    assert stored_output == _TERMINAL_OBJECT_REFERENCE
    assert stored_terminal == _TERMINAL_OBJECT_REFERENCE


def test_application_failure_is_recorded_in_band_and_releases_capacity(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    campaign_key = f"campaign-failure-{suffix}"
    work_keys = ("work-a", "work-b")
    first_work_key = min(
        work_keys,
        key=lambda work_key: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                CampaignKey(campaign_key), WorkKey(work_key)
            )
        ),
    )

    def sometimes_fails(input_reference: str) -> str:
        if input_reference == "input:fail":
            raise RuntimeError("application stage failed")
        return f"output:{input_reference}"

    declared = _pipeline(
        key=f"failure-{suffix}",
        stage_logic=(("execute", sometimes_fails),),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=f"run-failure-{suffix}",
        items=tuple(
            WorkInput(
                work_key=work_key,
                input_reference=(
                    "input:fail" if work_key == first_work_key else "input:ok"
                ),
                labels={},
            )
            for work_key in work_keys
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        first = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert first.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            failed_workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
        _wait_for_workflow_statuses(
            client,
            [failed_workflow_id],
            expected_status="SUCCESS",
        )

        second = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert second.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


@pytest.mark.parametrize(
    "invalid_output",
    [
        pytest.param("", id="empty-string"),
        pytest.param(7, id="non-string"),
    ],
)
def test_invalid_application_output_lands_failed_without_a_successor(
    clean_pg: str,
    pg_engine: Engine,
    invalid_output: object,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def returns_invalid_output(_input_reference: str) -> object:
        return invalid_output

    declared = _pipeline(
        key=f"invalid-output-{suffix}",
        stage_logic=(
            ("execute", returns_invalid_output),
            ("score", lambda input_reference: f"score:{input_reference}"),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-invalid-output-{suffix}",
        run_key=f"run-invalid-output-{suffix}",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            execution_rows = (
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.state,
                        schema.stage_executions.c.output_reference,
                    ).order_by(schema.stage_executions.c.stage_index)
                )
                .tuples()
                .all()
            )
            attempt = connection.execute(
                select(
                    schema.stage_attempts.c.workflow_id,
                    schema.stage_attempts.c.terminal_at,
                    schema.stage_attempts.c.terminal_summary,
                    schema.stage_attempts.c.terminal_reference,
                )
            ).one()
        _wait_for_workflow_statuses(
            client,
            [attempt.workflow_id],
            expected_status="SUCCESS",
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)

    assert execution_rows == [(0, "execute", "failed", None)]
    assert attempt.terminal_at is not None
    assert attempt.terminal_summary == {
        "outcome": "failed",
        "error_type": "builtins.ValueError",
        "message": (
            "stage application logic must return a non-empty "
            "output-reference string"
        ),
    }
    assert attempt.terminal_reference is None
    assert attempt.terminal_summary["error_type"] == "builtins.ValueError"


class _UnprintableError(RuntimeError):
    def __str__(self) -> str:
        raise ValueError("this error message cannot be rendered")


def test_application_failure_with_unprintable_error_lands_failed(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def raises_unprintable(_input_reference: str) -> str:
        raise _UnprintableError

    declared = _pipeline(
        key=f"unprintable-{suffix}",
        stage_logic=(("execute", raises_unprintable),),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-unprintable-{suffix}",
        run_key=f"run-unprintable-{suffix}",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            terminal_summary = connection.execute(
                select(schema.stage_attempts.c.terminal_summary)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)

    expected_type = (
        f"{_UnprintableError.__module__}.{_UnprintableError.__qualname__}"
    )
    assert terminal_summary["error_type"] == expected_type
    assert terminal_summary["message"] == (
        f"<unprintable {expected_type} message>"
    )


@dataclass(frozen=True, slots=True)
class _Status:
    workflow_id: str
    status: str
    error: Exception | None = None


class _StatusClient:
    def __init__(self, statuses: tuple[_Status, ...]) -> None:
        self._statuses = statuses

    def list_workflows(self, **_kwargs: object) -> list[_Status]:
        return list(self._statuses)


class _BarrierStatusClient:
    def __init__(self, status: _Status, barrier: Barrier) -> None:
        self._status = status
        self._barrier = barrier

    def list_workflows(self, **_kwargs: object) -> list[_Status]:
        self._barrier.wait(timeout=10)
        return [self._status]


class _PagingStatusClient:
    def __init__(self, statuses: tuple[_Status, ...]) -> None:
        self._by_id = {status.workflow_id: status for status in statuses}
        self.requested_ids: list[tuple[str, ...]] = []

    def list_workflows(
        self, *, workflow_ids: list[str], **_kwargs: object
    ) -> list[_Status]:
        self.requested_ids.append(tuple(workflow_ids))
        return [
            self._by_id[workflow_id]
            for workflow_id in workflow_ids
            if workflow_id in self._by_id
        ]


def _commit_successful_handoff(
    engine: Engine,
    *,
    workflow_id: str,
    pipeline: PipelineDefinition,
    completed_at: datetime,
    before_next_stage: Callable[[], None] | None = None,
) -> None:
    with engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key=pipeline.stages[0].key.value,
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            next_stage_key=pipeline.stages[1].key.value,
            next_stage_index=1,
            completed_at=completed_at,
            before_next_stage=before_next_stage,
        )


def _release_after_projection(
    monkeypatch: pytest.MonkeyPatch,
    barrier: Barrier,
) -> None:
    """Release the contender after sweep projection fixes write order."""
    project = cast(
        "Callable[..., bool]",
        sweep_module._project_terminal_status,
    )

    def project_then_release(*args: object, **kwargs: object) -> bool:
        applied = project(*args, **kwargs)
        assert applied
        barrier.wait(timeout=10)
        return applied

    monkeypatch.setattr(
        sweep_module,
        "_project_terminal_status",
        project_then_release,
    )


@pytest.mark.parametrize("winner", ["handoff", "sweep"])
def test_sweep_race_with_successful_handoff_has_one_terminal_outcome(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-handoff-race",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-sweep-handoff-race",
        run_key="run-sweep-handoff-race",
    )
    barrier = Barrier(2)
    race_time = _utc_now() + timedelta(seconds=1)
    abandoned = _Status(
        workflow_id,
        "ERROR",
        RuntimeError("reported abandoned"),
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        if winner == "handoff":

            def release_sweep_after_handoff() -> None:
                barrier.wait(timeout=10)

            handoff = executor.submit(
                _commit_successful_handoff,
                pg_engine,
                workflow_id=workflow_id,
                pipeline=pipeline,
                completed_at=race_time,
                before_next_stage=release_sweep_after_handoff,
            )
            summary = sweep_abandoned_stages(
                pg_engine,
                client=_as_dbos_client(
                    _BarrierStatusClient(abandoned, barrier)
                ),
                clock=lambda: race_time + timedelta(seconds=1),
            )
            handoff.result()
        else:
            _release_after_projection(monkeypatch, barrier)

            def handoff_after_projection() -> None:
                barrier.wait(timeout=10)
                _commit_successful_handoff(
                    pg_engine,
                    workflow_id=workflow_id,
                    pipeline=pipeline,
                    completed_at=race_time + timedelta(seconds=1),
                )

            handoff = executor.submit(handoff_after_projection)
            summary = sweep_abandoned_stages(
                pg_engine,
                client=_as_dbos_client(_StatusClient((abandoned,))),
                clock=lambda: race_time,
            )
            with pytest.raises(StageHandoffMismatchError):
                handoff.result()

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .tuples()
            .all()
        )
        attempts = connection.execute(
            select(
                schema.stage_attempts.c.terminal_at,
                schema.stage_attempts.c.terminal_summary,
            )
        ).all()

    assert len(attempts) == 1
    assert attempts[0].terminal_at is not None
    if winner == "handoff":
        assert summary.projected_count == 0
        assert execution_rows == [
            (0, "succeeded", "output:prepare"),
            (1, "ready", None),
        ]
        assert attempts[0].terminal_summary == {"outcome": "succeeded"}
    else:
        assert summary.projected_count == 1
        assert execution_rows == [(0, "failed", None)]
        assert attempts[0].terminal_summary == {
            "outcome": "failed",
            "dbos_status": "ERROR",
            "message": "reported abandoned",
        }


@pytest.mark.parametrize("winner", ["cancellation", "sweep"])
def test_sweep_race_with_operator_cancellation_has_one_terminal_outcome(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-cancellation-race",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-sweep-cancellation-race",
        run_key="run-sweep-cancellation-race",
    )
    barrier = Barrier(2)
    race_time = _utc_now() + timedelta(seconds=1)
    abandoned = _Status(
        workflow_id,
        "ERROR",
        RuntimeError("reported abandoned"),
    )
    canceller = _RecordingCanceller()

    with ThreadPoolExecutor(max_workers=1) as executor:
        if winner == "cancellation":
            cancel_current = cast(
                "Callable[..., WorkCancellationResult]",
                cancellation_module._cancel_current_stage,
            )

            def cancel_then_release(
                *args: object,
                **kwargs: object,
            ) -> WorkCancellationResult:
                result = cancel_current(*args, **kwargs)
                barrier.wait(timeout=10)
                return result

            monkeypatch.setattr(
                cancellation_module,
                "_cancel_current_stage",
                cancel_then_release,
            )
            cancellation = executor.submit(
                cancel_work,
                engine=pg_engine,
                client=canceller,
                work_item_id=work_item_id,
                clock=lambda: race_time,
            )
            summary = sweep_abandoned_stages(
                pg_engine,
                client=_as_dbos_client(
                    _BarrierStatusClient(abandoned, barrier)
                ),
                clock=lambda: race_time + timedelta(seconds=1),
            )
            result = cancellation.result()
        else:
            _release_after_projection(monkeypatch, barrier)

            def cancel_after_projection() -> WorkCancellationResult:
                barrier.wait(timeout=10)
                return cancel_work(
                    engine=pg_engine,
                    client=canceller,
                    work_item_id=work_item_id,
                    clock=lambda: race_time + timedelta(seconds=1),
                )

            cancellation = executor.submit(cancel_after_projection)
            summary = sweep_abandoned_stages(
                pg_engine,
                client=_as_dbos_client(_StatusClient((abandoned,))),
                clock=lambda: race_time,
            )
            result = cancellation.result()

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .tuples()
            .all()
        )
        attempts = connection.execute(
            select(
                schema.stage_attempts.c.terminal_at,
                schema.stage_attempts.c.terminal_summary,
            )
        ).all()

    assert execution_rows == [(0, "cancelled", None)]
    assert len(attempts) == 1
    assert attempts[0].terminal_at is not None
    if winner == "cancellation":
        assert summary.projected_count == 0
        assert result.disposition is CancellationDisposition.CANCELLED_ADMITTED
        assert attempts[0].terminal_summary == {
            "outcome": "cancelled",
            "reason": "operator_requested",
        }
        assert canceller.cancelled == [(workflow_id, False)]
    else:
        assert summary.projected_count == 1
        assert result.disposition is CancellationDisposition.CANCELLED_FAILED
        assert attempts[0].terminal_summary == {
            "outcome": "failed",
            "dbos_status": "ERROR",
            "message": "reported abandoned",
        }
        assert canceller.cancelled == []


def test_sweep_projects_only_cancelled_or_abandoned_admitted_attempts(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-pipeline",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=3)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep",
        run_key="run-sweep",
        items=tuple(
            WorkInput(
                work_key=f"work-{index}",
                input_reference=f"input:{index}",
                labels={},
            )
            for index in range(4)
        ),
    )
    admission_client = _RecordingClient()
    first = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert first.admitted_total == 3
    workflow_ids = tuple(
        _recorded_workflow_id(options)
        for options, _args in admission_client.enqueued
    )
    status_client = _StatusClient(
        (
            _Status(workflow_ids[0], "CANCELLED"),
            _Status(
                workflow_ids[1],
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
            _Status(workflow_ids[2], "PENDING"),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        clock=_utc_now,
    )

    with pg_engine.connect() as connection:
        state_counts = {
            row[0]: row[1]
            for row in connection.execute(
                select(
                    schema.stage_executions.c.state,
                    func.count(),
                ).group_by(schema.stage_executions.c.state)
            ).all()
        }
        terminal_count = connection.execute(
            select(func.count())
            .select_from(schema.stage_attempts)
            .where(schema.stage_attempts.c.terminal_at.is_not(None))
        ).scalar_one()

    assert summary.inspected_count == 3
    assert summary.projected_count == 2
    assert {item.state for item in summary.projections} == {
        StageExecutionState.CANCELLED,
        StageExecutionState.FAILED,
    }
    assert state_counts == {
        StageExecutionState.ADMITTED.value: 1,
        StageExecutionState.CANCELLED.value: 1,
        StageExecutionState.FAILED.value: 1,
        StageExecutionState.READY.value: 1,
    }
    assert terminal_count == 2

    second = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=_utc_now,
    )
    assert second.admitted_total == 1


def test_sweep_projects_an_abandoned_attempt_with_an_unprintable_error(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-unprintable",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep-unprintable",
        run_key="run-sweep-unprintable",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    admission_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0][0])
    status_client = _StatusClient(
        (
            _Status(
                workflow_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                _UnprintableError(),
            ),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        clock=_utc_now,
    )

    with pg_engine.connect() as connection:
        terminal_summary = connection.execute(
            select(schema.stage_attempts.c.terminal_summary).where(
                schema.stage_attempts.c.terminal_at.is_not(None)
            )
        ).scalar_one()

    assert summary.projected_count == 1
    assert terminal_summary["message"] == "<unprintable error message>"


def test_sweep_paginates_to_reach_abandoned_attempt_in_later_page(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-page-pipeline",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    admitted_total = 5
    _configure_controls(pg_engine, pipeline, capacity=admitted_total)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep-page",
        run_key="run-sweep-page",
        items=tuple(
            WorkInput(
                work_key=f"work-{index}",
                input_reference=f"input:{index}",
                labels={},
            )
            for index in range(admitted_total)
        ),
    )
    admission_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == admitted_total

    with pg_engine.connect() as connection:
        ordered_ids = [
            row[0]
            for row in connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .order_by(schema.stage_executions.c.stage_execution_id)
            ).all()
        ]
    assert len(ordered_ids) == admitted_total
    abandoned_id = ordered_ids[-1]
    status_client = _PagingStatusClient(
        (
            _Status(
                abandoned_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
        )
    )

    batch_size = 2
    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        batch_size=batch_size,
        clock=_utc_now,
    )

    assert summary.inspected_count == admitted_total
    assert summary.projected_count == 1
    assert summary.projections[0].workflow_id == abandoned_id
    assert summary.projections[0].state == StageExecutionState.FAILED
    assert len(status_client.requested_ids) > 1
    assert any(abandoned_id in page for page in status_client.requested_ids)
