"""PostgreSQL behavior proofs for staged-work recovery actions."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dbos import DBOSClient, EnqueueOptions
from psycopg.errors import LockNotAvailable
from sqlalchemy import Engine, create_engine, func, select, text
from sqlalchemy.exc import OperationalError

from dr_platform._core.identities import PipelineKey, StageKey
from dr_platform._core.ledger.attempts import (
    get_stage_attempt,
    list_stage_attempts,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import (
    pause,
    resume,
    set_selector_capacity,
    set_stage_capacity,
)
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import _complete_stage_in_transaction
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.cancellation import (
    CancellationDisposition,
    cancel_work,
)
from dr_platform.recovery.retry import retry_stage
from dr_platform.submission.stream import WorkInput, submit
from tests.conftest import (
    NOW,
    _args_for,
    _as_dbos_client,
    _migrate,
    _RecordingCanceller,
    dbos_config,
    engine_dsn,
    initialize_dbos_schema,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection


def _workflow(input_reference: str) -> str:
    return f"output:{input_reference}"


def _registry(*, two_stages: bool = False) -> PipelineRegistry:
    stages = [
        StageDefinition(
            key=StageKey("execute"),
            queue_name="execute-queue",
            workflow=_workflow,
            args_for=_args_for,
        )
    ]
    if two_stages:
        stages.append(
            StageDefinition(
                key=StageKey("score"),
                queue_name="score-queue",
                workflow=_workflow,
                args_for=_args_for,
            )
        )
    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=tuple(stages),
        )
    )
    return registry


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
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_reference=f"input:{work_key}",
                labels={"cohort": "blue"},
            )
            for work_key in work_keys
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


# This variant records only the enqueue options; the args-tracking
# _RecordingClient in the admission/handoff suites is a distinct shape.
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


class _RaisingCanceller:
    def __init__(self) -> None:
        self.attempts: list[tuple[str, bool]] = []

    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        cancel_children: bool = False,
    ) -> None:
        self.attempts.append((workflow_id, cancel_children))
        raise RuntimeError("delegation exploded")


def _wait_until_row_locked(engine: Engine, stage_execution_id: int) -> None:
    """Block until the source stage-execution row is FOR UPDATE locked."""
    schema = StagingSchema()
    table = schema.stage_executions
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            with engine.connect() as probe:
                probe.execute(
                    select(table.c.stage_execution_id)
                    .where(table.c.stage_execution_id == stage_execution_id)
                    .with_for_update(nowait=True)
                )
        except OperationalError as error:
            if not isinstance(error.orig, LockNotAvailable):
                raise
            return
        time.sleep(0.01)
    raise AssertionError("source row was never locked by the handoff holder")


def _wait_until_blocked_on_lock(
    engine: Engine,
    *,
    application_name: str,
) -> None:
    """Block until the intended cancellation backend is waiting on a lock."""
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        with engine.connect() as probe:
            waiting = probe.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' "
                    "AND datname = current_database() "
                    "AND application_name = :application_name"
                ),
                {"application_name": application_name},
            ).scalar_one()
        if waiting == 1:
            return
        time.sleep(0.01)
    raise AssertionError(
        "the intended cancellation backend never blocked on the source "
        "row lock"
    )


def _launch_dbos_schema(database_url: str, *, suffix: str) -> None:
    initialize_dbos_schema(
        dbos_config(
            name=f"drp-operations-{suffix[:12]}",
            system_database_url=database_url,
            application_version=f"operations-{suffix}",
        )
    )


def _admit_one(
    engine: Engine,
    registry: PipelineRegistry,
    schema: StagingSchema,
    *,
    run_key: str,
    work_key: str,
) -> tuple[int, int]:
    """Submit and admit a single work item, returning its stage/work ids."""
    _submit(engine, registry, run_key=run_key, work_keys=(work_key,))
    set_stage_capacity(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=1,
        engine=engine,
        clock=lambda: NOW,
    )
    run_admission_pass(
        engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW,
    )
    row = _execution_rows(engine, schema)[0]
    return row[0], row[1]


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
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=3,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=lambda: NOW,
        ).admitted_total
        == 3
    )

    set_stage_capacity(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
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

    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=lambda: NOW + timedelta(seconds=2),
        ).admitted_total
        == 0
    )
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
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=lambda: NOW + timedelta(seconds=4),
        ).admitted_total
        == 1
    )


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
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
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

    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=lambda: NOW + timedelta(seconds=3),
        ).admitted_total
        == 1
    )
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


def test_cancel_terminalizes_retry_prepared_attempt_without_delegation(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-retry-cancel",
        work_key="work-retry-cancel",
    )
    with pg_engine.connect() as connection:
        first_attempt = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
        )
    assert first_attempt is not None
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=first_attempt.workflow_id,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            stage_index=0,
            succeeded=False,
            output_reference=None,
            terminal_summary={"outcome": "failed"},
            terminal_reference=None,
            next_stage_key=None,
            next_stage_index=None,
            completed_at=NOW + timedelta(seconds=1),
        )

    retried = retry_stage(
        stage_execution_id,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    canceller = _RecordingCanceller()
    cancelled_at = NOW + timedelta(seconds=3)

    result = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=work_item_id,
        clock=lambda: cancelled_at,
    )

    assert retried.stage_execution.state is StageExecutionState.READY
    assert result.stage_execution.state is StageExecutionState.CANCELLED
    assert result.disposition is CancellationDisposition.CANCELLED_READY
    assert result.delegated_workflow_id is None
    assert canceller.cancelled == []
    with pg_engine.connect() as connection:
        second_attempt = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=2,
        )
    assert second_attempt is not None
    assert second_attempt.admitted_at is None
    assert second_attempt.terminal_at == cancelled_at
    assert second_attempt.terminal_summary == {
        "outcome": "cancelled",
        "reason": "operator_requested",
    }
    assert second_attempt.terminal_reference is None


def test_concurrent_retry_prepares_exactly_one_new_attempt(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    stage_execution_id, _work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-concurrent-retry",
        work_key="work-concurrent-retry",
    )
    with pg_engine.begin() as connection:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=NOW + timedelta(seconds=1),
        )
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
            terminal_at=NOW + timedelta(seconds=1),
            terminal_summary={"outcome": "failed"},
        )

    start = Barrier(2)

    def retry_concurrently() -> object:
        start.wait(timeout=10)
        return retry_stage(
            stage_execution_id,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=2),
        )

    successes: list[object] = []
    failures: list[ValueError] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(retry_concurrently) for _ in range(2)]
        for future in futures:
            try:
                successes.append(future.result())
            except ValueError as error:
                failures.append(error)

    assert len(successes) == 1
    assert len(failures) == 1
    assert "only a FAILED stage execution can be retried" in str(failures[0])
    assert _execution_rows(pg_engine, schema)[0][2:] == ("ready", 2)
    with pg_engine.connect() as connection:
        attempts = list_stage_attempts(
            connection,
            stage_execution_id=stage_execution_id,
        )
    assert [attempt.attempt_number for attempt in attempts] == [1, 2]
    assert attempts[1].admitted_at is None


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
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
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
        assert (
            connection.execute(
                select(func.count()).select_from(schema.stage_executions)
            ).scalar_one()
            == 2
        )


def test_live_dbos_cancellation_targets_only_the_admitted_workflow(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    suffix = uuid4().hex
    stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-live-cancel",
        work_key=f"work-live-cancel-{suffix}",
    )
    with pg_engine.connect() as connection:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
        )
    assert attempt is not None

    child_workflow_id = f"operations-child-{suffix}"
    _launch_dbos_schema(clean_pg, suffix=suffix)
    client = DBOSClient(system_database_url=clean_pg)
    try:
        with pg_engine.begin() as connection:
            for workflow_id in (attempt.workflow_id, child_workflow_id):
                options: EnqueueOptions = {
                    "workflow_name": "operations_cancellation_probe",
                    "queue_name": "operations-cancellation-probe",
                    "workflow_id": workflow_id,
                }
                client.enqueue_in_transaction(connection, options)
            # This writes into DBOS-internal schema (dbos.workflow_status) to
            # forge a parent/child link the public API cannot express. It is
            # tolerable only because dbos==2.27.0 is pinned exactly, so the
            # column names cannot drift underneath us.
            connection.execute(
                text(
                    "UPDATE dbos.workflow_status "
                    "SET parent_workflow_id = :parent_workflow_id "
                    "WHERE workflow_uuid = :child_workflow_id"
                ),
                {
                    "parent_workflow_id": attempt.workflow_id,
                    "child_workflow_id": child_workflow_id,
                },
            )

        result = cancel_work(
            engine=pg_engine,
            client=client,
            work_item_id=work_item_id,
            clock=lambda: NOW + timedelta(seconds=1),
        )

        statuses = {
            status.workflow_id: status.status
            for status in client.list_workflows(
                workflow_ids=[attempt.workflow_id, child_workflow_id],
                load_input=False,
                load_output=False,
            )
        }
        children = client.list_workflows(
            parent_workflow_id=attempt.workflow_id,
            load_input=False,
            load_output=False,
        )
        assert result.delegated_workflow_id == attempt.workflow_id
        assert statuses == {
            attempt.workflow_id: "CANCELLED",
            child_workflow_id: "ENQUEUED",
        }
        assert [child.workflow_id for child in children] == [child_workflow_id]
    finally:
        client.destroy()


def test_exact_label_pause_resume_preserves_capacity(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    control = set_selector_capacity(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        labels={"cohort": "blue"},
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    paused = pause(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        labels={"cohort": "blue"},
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    resumed = resume(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
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


@pytest.mark.parametrize("operation", [pause, resume])
def test_pause_and_resume_reject_a_missing_exact_selector(
    operation: Callable[..., object],
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="stage control does not exist"):
        operation(
            pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
            stage_key="execute",
            labels={"cohort": "missing"},
            engine=pg_engine,
            clock=lambda: NOW,
        )


def test_cancel_resolves_work_by_campaign_and_work_keys(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        run_key="run-logical-cancel",
        work_keys=("work-logical-cancel",),
    )
    work_item_id = _execution_rows(pg_engine, schema)[0][1]
    canceller = _RecordingCanceller()

    result = cancel_work(
        engine=pg_engine,
        client=canceller,
        campaign_key="campaign-1",
        work_key="work-logical-cancel",
        clock=lambda: NOW + timedelta(seconds=1),
    )

    assert result.work_item_id == work_item_id
    assert result.disposition is CancellationDisposition.CANCELLED_READY
    assert result.stage_execution.state is StageExecutionState.CANCELLED
    assert canceller.cancelled == []


def test_cancel_of_succeeded_work_is_idempotent(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-succeeded",
        work_key="work-succeeded",
    )
    with pg_engine.begin() as connection:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference="output:succeeded",
            updated_at=NOW + timedelta(seconds=1),
        )
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
            terminal_at=NOW + timedelta(seconds=1),
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:succeeded",
        )

    canceller = _RecordingCanceller()
    first = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=work_item_id,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    second = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=work_item_id,
        clock=lambda: NOW + timedelta(seconds=3),
    )

    assert first.disposition is CancellationDisposition.ALREADY_TERMINAL
    assert second.disposition is CancellationDisposition.ALREADY_TERMINAL
    assert first.stage_execution.state is StageExecutionState.SUCCEEDED
    assert second.stage_execution == first.stage_execution
    assert first.delegated_workflow_id is None
    assert second.delegated_workflow_id is None
    assert canceller.cancelled == []


def test_cancel_fences_failed_work_against_a_later_retry(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-failed",
        work_key="work-failed",
    )
    with pg_engine.begin() as connection:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=NOW + timedelta(seconds=1),
        )
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
            terminal_at=NOW + timedelta(seconds=1),
            terminal_summary={"outcome": "failed"},
        )

    canceller = _RecordingCanceller()
    result = cancel_work(
        engine=pg_engine,
        client=canceller,
        work_item_id=work_item_id,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    assert result.disposition is CancellationDisposition.CANCELLED_FAILED
    assert result.delegated_workflow_id is None
    assert result.stage_execution.state is StageExecutionState.CANCELLED
    assert canceller.cancelled == []

    with pytest.raises(ValueError, match="only a FAILED stage execution"):
        retry_stage(
            stage_execution_id,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=3),
        )


def test_repeated_cancel_self_heals_a_lost_admitted_delegation(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-lost",
        work_key="work-lost",
    )

    raising = _RaisingCanceller()
    with pytest.raises(RuntimeError, match="delegation exploded"):
        cancel_work(
            engine=pg_engine,
            client=raising,
            work_item_id=work_item_id,
            clock=lambda: NOW + timedelta(seconds=1),
        )
    # Platform state committed before the failing post-commit delegation.
    assert len(raising.attempts) == 1
    assert _execution_rows(pg_engine, schema)[0][2] == (
        StageExecutionState.CANCELLED.value
    )

    healing = _RecordingCanceller()
    result = cancel_work(
        engine=pg_engine,
        client=healing,
        work_item_id=work_item_id,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    lost_workflow_id = raising.attempts[0][0]
    assert result.disposition is CancellationDisposition.ALREADY_TERMINAL
    assert result.delegated_workflow_id == lost_workflow_id
    assert healing.cancelled == [(lost_workflow_id, False)]


def test_cancel_after_committed_handoff_cancels_the_successor(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry(two_stages=True)
    stage_execution_id, work_item_id = _admit_one(
        pg_engine,
        registry,
        schema,
        run_key="run-handoff",
        work_key="work-handoff",
    )
    with pg_engine.connect() as connection:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=1,
        )
    assert attempt is not None
    workflow_id = attempt.workflow_id

    # A handoff commits the successor READY stage while cancel is blocked on
    # the source row lock.  The background thread locks the source and inserts
    # the successor, then commits only once the main connection is observed
    # waiting on that lock; that forces the EvalPlanQual window the fix
    # survives.
    dsn = engine_dsn(pg_engine)
    holder = create_engine(dsn)
    cancellation_application_name = f"operations-cancel-{uuid4().hex}"
    cancellation_engine = create_engine(
        dsn,
        connect_args={"application_name": cancellation_application_name},
    )

    def commit_handoff() -> None:
        with holder.begin() as connection:
            _complete_stage_in_transaction(
                connection,
                workflow_id=workflow_id,
                pipeline_key="evaluation",
                pipeline_version=1,
                stage_key="execute",
                stage_index=0,
                succeeded=True,
                output_reference="handoff-output",
                terminal_summary={"outcome": "succeeded"},
                terminal_reference="handoff-output",
                next_stage_key="score",
                next_stage_index=1,
                completed_at=NOW + timedelta(seconds=1),
            )
            _wait_until_blocked_on_lock(
                pg_engine,
                application_name=cancellation_application_name,
            )

    canceller = _RecordingCanceller()
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(commit_handoff)
            # Give the holder time to take the source lock before cancel reads
            # the pre-handoff max stage and blocks acquiring it.
            _wait_until_row_locked(pg_engine, stage_execution_id)
            result = cancel_work(
                engine=cancellation_engine,
                client=canceller,
                work_item_id=work_item_id,
                clock=lambda: NOW + timedelta(seconds=2),
            )
            future.result()
    finally:
        cancellation_engine.dispose()
        holder.dispose()

    assert result.disposition is not CancellationDisposition.ALREADY_TERMINAL
    assert result.disposition is CancellationDisposition.CANCELLED_READY
    assert result.stage_execution.stage_index == 1
    assert result.stage_execution.stage_key == StageKey("score")
    assert result.stage_execution.state is StageExecutionState.CANCELLED
    assert canceller.cancelled == []


def test_set_stage_capacity_rejects_a_raw_pipeline_tuple(
    pg_engine: Engine,
) -> None:
    with pytest.raises(TypeError, match="pipeline must be a PipelineIdentity"):
        set_stage_capacity(
            pipeline=(  # ty: ignore[invalid-argument-type]
                PipelineKey("evaluation"),
                1,
            ),
            stage_key="execute",
            capacity=1,
            engine=pg_engine,
        )
