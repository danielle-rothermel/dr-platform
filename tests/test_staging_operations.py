"""PostgreSQL behavior proofs for staging operator surfaces."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from dbos import DBOSClient, EnqueueOptions
from sqlalchemy import Engine, func, select

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging import (
    PipelineDefinition,
    PipelineRegistry,
    StageDefinition,
    StageExecutionState,
)
from dr_platform.staging.admission import AdmissionPayload, run_admission_pass
from dr_platform.staging.handoff import _complete_stage_in_transaction
from dr_platform.staging.operations import (
    CancellationDisposition,
    cancel_work,
    pause,
    resume,
    retry_stage,
    set_selector_capacity,
    set_stage_capacity,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import (
    get_stage_attempt,
    list_stage_attempts,
    record_stage_attempt_terminal,
)
from dr_platform.staging.stage_executions import transition_stage_execution
from dr_platform.staging.submission import WorkInput, submit
from tests.conftest import engine_dsn

if TYPE_CHECKING:
    from sqlalchemy import Connection

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _workflow(input_ref: str) -> str:
    return f"output:{input_ref}"


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_ref,)


def _registry(*, two_stages: bool = False) -> PipelineRegistry:
    stages = [
        StageDefinition(
            key="execute",
            queue_name="execute-queue",
            workflow=_workflow,
            args_for=_args_for,
        )
    ]
    if two_stages:
        stages.append(
            StageDefinition(
                key="score",
                queue_name="score-queue",
                workflow=_workflow,
                args_for=_args_for,
            )
        )
    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key="evaluation",
            version=1,
            stages=tuple(stages),
        )
    )
    return registry


def _migrate(engine: Engine) -> StagingSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return StagingSchema()


def _submit(
    engine: Engine,
    registry: PipelineRegistry,
    *,
    run_key: str,
    work_keys: tuple[str, ...],
) -> None:
    submit(
        campaign_key="campaign-1",
        run_key=run_key,
        pipeline=("evaluation", 1),
        config_ref="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_ref=f"input:{work_key}",
                labels={"cohort": "blue"},
            )
            for work_key in work_keys
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


class _RecordingClient:
    def __init__(self) -> None:
        self.enqueued: list[EnqueueOptions] = []

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        *_args: object,
        **_kwargs: object,
    ) -> object:
        self.enqueued.append(cast("EnqueueOptions", dict(options)))
        return object()


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


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


def _execution_rows(
    engine: Engine,
    schema: StagingSchema,
) -> list[tuple[int, int, str, int]]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                select(
                    schema.stage_executions.c.stage_execution_id,
                    schema.stage_executions.c.work_item_id,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.current_attempt,
                ).order_by(schema.stage_executions.c.stage_execution_id)
            ).tuples()
        )


def test_lowering_capacity_drains_without_preempting_admitted_work(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        run_key="run-1",
        work_keys=("work-0", "work-1", "work-2"),
    )
    set_stage_capacity(
        pipeline=("evaluation", 1),
        stage_key="execute",
        capacity=3,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    assert run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    ).admitted_total == 3

    set_stage_capacity(
        pipeline=("evaluation", 1),
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    _submit(
        pg_engine,
        registry,
        run_key="run-2",
        work_keys=("work-3",),
    )

    assert run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=2),
    ).admitted_total == 0
    rows = _execution_rows(pg_engine, schema)
    assert [row[2] for row in rows].count("admitted") == 3
    assert [row[2] for row in rows].count("ready") == 1

    with pg_engine.begin() as connection:
        for stage_execution_id, _work_item_id, state, _attempt in rows[:3]:
            assert state == StageExecutionState.ADMITTED.value
            transition_stage_execution(
                connection,
                stage_execution_id=stage_execution_id,
                new_state=StageExecutionState.SUCCEEDED,
                output_reference=f"output:{stage_execution_id}",
                updated_at=NOW + timedelta(seconds=3),
            )
    assert run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=4),
    ).admitted_total == 1


def test_retry_preserves_lineage_and_readmits_prepared_attempt(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        run_key="run-retry",
        work_keys=("work-retry",),
    )
    set_stage_capacity(
        pipeline=("evaluation", 1),
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )
    stage_execution_id = _execution_rows(pg_engine, schema)[0][0]
    with pg_engine.begin() as connection:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=NOW + timedelta(seconds=1),
        )
        first = record_stage_attempt_terminal(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
            terminal_at=NOW + timedelta(seconds=1),
            terminal_summary={"outcome": "failed"},
        )

    retried = retry_stage(
        stage_execution_id,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    assert retried.stage_execution.state is StageExecutionState.READY
    assert retried.stage_execution.current_attempt == 2
    assert retried.new_attempt.attempt_number == 2
    assert retried.new_attempt.workflow_id.endswith("-a2")
    assert retried.new_attempt.admitted_at is None

    assert run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    ).admitted_total == 1
    with pg_engine.connect() as connection:
        attempts = list_stage_attempts(
            connection,
            stage_execution_id=stage_execution_id,
        )
    assert len(attempts) == 2
    assert attempts[0] == first
    assert attempts[0].terminal_at is not None
    assert attempts[1].admitted_at == NOW + timedelta(seconds=3)
    assert client.enqueued[-1].get("workflow_id") == attempts[1].workflow_id
    assert _execution_rows(pg_engine, schema)[0][2:] == ("admitted", 2)


def test_cancellation_delegates_only_an_admitted_exact_attempt(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry(two_stages=True)
    _submit(
        pg_engine,
        registry,
        run_key="run-cancel",
        work_keys=("work-a", "work-b"),
    )
    set_stage_capacity(
        pipeline=("evaluation", 1),
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=lambda: NOW,
    )
    rows = _execution_rows(pg_engine, schema)
    admitted = next(row for row in rows if row[2] == "admitted")
    ready = next(row for row in rows if row[2] == "ready")
    canceller = _RecordingCanceller()

    ready_result = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=ready[1],
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert ready_result.disposition is (
        CancellationDisposition.CANCELLED_READY
    )
    assert ready_result.delegated_workflow_id is None
    assert canceller.cancelled == []

    admitted_result = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=admitted[1],
        clock=lambda: NOW + timedelta(seconds=2),
    )
    workflow_id = admitted_result.delegated_workflow_id
    assert admitted_result.disposition is (
        CancellationDisposition.CANCELLED_ADMITTED
    )
    assert workflow_id is not None
    assert canceller.cancelled == [(workflow_id, False)]
    with pg_engine.connect() as connection:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=admitted[0],
            attempt_number=1,
        )
    assert attempt is not None
    assert attempt.terminal_summary == {
        "outcome": "cancelled",
        "reason": "operator_requested",
    }

    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            stage_index=0,
            succeeded=True,
            output_reference="late-output",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="late-output",
            next_stage_key="score",
            next_stage_index=1,
            completed_at=NOW + timedelta(seconds=3),
        )
    with pg_engine.connect() as connection:
        assert connection.execute(
            select(func.count()).select_from(schema.stage_executions)
        ).scalar_one() == 2


def test_exact_label_pause_resume_preserves_capacity(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    control = set_selector_capacity(
        pipeline=("evaluation", 1),
        stage_key="execute",
        labels={"cohort": "blue"},
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    paused = pause(
        pipeline=("evaluation", 1),
        stage_key="execute",
        labels={"cohort": "blue"},
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    resumed = resume(
        pipeline=("evaluation", 1),
        stage_key="execute",
        labels={"cohort": "blue"},
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert paused.stage_control_id == control.stage_control_id
    assert paused.capacity == 4
    assert paused.paused is True
    assert resumed.capacity == 4
    assert resumed.paused is False
