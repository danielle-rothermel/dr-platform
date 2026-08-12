from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Engine

from dr_platform._core.ledger.completion_attempts import (
    get_run_completion_attempt,
)
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)
from dr_platform.completion.barrier import run_barrier_pass
from dr_platform.completion.execution import (
    RunCompletionOutcomeError,
    record_run_completion_outcome,
)
from dr_platform.recovery.run_completion_retry import retry_run_completion
from dr_platform.recovery.sweep import sweep_abandoned_run_completions
from tests.completion.test_run_barrier import (
    _members,
    _RecordingClient,
    _registry,
    _set_states,
    _submit_run,
)
from tests.conftest import NOW, _as_dbos_client, _migrate

if TYPE_CHECKING:
    from dr_platform.pipeline.registry import PipelineRegistry


@dataclass(frozen=True, slots=True)
class _WorkflowStatus:
    workflow_id: str
    status: str
    error: Exception | None = None


class _StatusClient:
    def __init__(self, statuses: tuple[_WorkflowStatus, ...]) -> None:
        self._statuses = {status.workflow_id: status for status in statuses}

    def list_workflows(
        self, *, workflow_ids: list[str], **_kwargs: object
    ) -> list[_WorkflowStatus]:
        return [
            self._statuses[workflow_id]
            for workflow_id in workflow_ids
            if workflow_id in self._statuses
        ]


def _enqueue_completion(
    pg_engine: Engine,
    *,
    key: str = "retry",
) -> tuple[PipelineRegistry, str]:
    registry, pipeline = _registry(key)
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-retry",
        members=_members(0),
    )
    _set_states(pg_engine, {"work-0": StageExecutionState.SUCCEEDED})
    client = _RecordingClient()
    summary = run_barrier_pass(
        pg_engine,
        client=client,  # ty: ignore[invalid-argument-type]
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    return registry, summary.releases[0].workflow_id


def test_retry_run_completion_appends_attempt_and_reenqueues(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, workflow_id = _enqueue_completion(pg_engine)
    failed_at = NOW + timedelta(seconds=2)
    with pg_engine.begin() as connection:
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={"error_type": "ValueError", "message": "broken"},
            terminal_at=failed_at,
        )

    client = _RecordingClient()
    result = retry_run_completion(
        "run-retry",
        engine=pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    )

    assert result.execution.state is RunCompletionExecutionState.ENQUEUED
    assert result.execution.current_attempt == 2
    assert result.execution.workflow_id == result.new_attempt.workflow_id
    assert result.new_attempt.attempt_number == 2
    assert client.enqueued
    assert (
        client.enqueued[-1][0]["workflow_id"] == result.new_attempt.workflow_id
    )

    with pg_engine.connect() as connection:
        first = get_run_completion_attempt(
            connection,
            run_completion_execution_id=result.execution.run_completion_execution_id,
            attempt_number=1,
            schema=schema,
        )
        second = get_run_completion_attempt(
            connection,
            run_completion_execution_id=result.execution.run_completion_execution_id,
            attempt_number=2,
            schema=schema,
        )
    assert first is not None
    assert first.terminal_at is not None
    assert second is not None
    assert second.enqueued_at is not None


def test_stale_run_completion_workflow_id_cannot_terminalize_retry(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _enqueue_completion(pg_engine)
    failed_at = NOW + timedelta(seconds=2)
    with pg_engine.begin() as connection:
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={"error_type": "ValueError", "message": "broken"},
            terminal_at=failed_at,
        )

    retry_run_completion(
        "run-retry",
        engine=pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    )

    with (
        pytest.raises(
            RunCompletionOutcomeError,
            match="does not match the current attempt",
        ),
        pg_engine.begin() as connection,
    ):
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={"error_type": "ValueError", "message": "late"},
            terminal_at=NOW + timedelta(seconds=4),
        )


def test_retry_run_completion_rejects_non_failed_execution(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _enqueue_completion(pg_engine, key="not-failed")
    with pg_engine.begin() as connection:
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=True,
            output_reference="aggregate:1",
            error_summary=None,
            terminal_at=NOW + timedelta(seconds=2),
        )

    with pytest.raises(ValueError, match="FAILED"):
        retry_run_completion(
            "run-retry",
            engine=pg_engine,
            client=_as_dbos_client(_RecordingClient()),
            registry=registry,
        )


def test_sweep_projects_exhausted_run_completion_for_operator_retry(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, workflow_id = _enqueue_completion(pg_engine, key="sweep-retry")
    status_client = _StatusClient(
        (
            _WorkflowStatus(
                workflow_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
        )
    )
    sweep = sweep_abandoned_run_completions(
        pg_engine,
        client=_as_dbos_client(status_client),
        clock=lambda: NOW + timedelta(seconds=2),
    )
    assert sweep.inspected_count == 1
    assert sweep.projected_count == 1
    assert sweep.projections[0].state is RunCompletionExecutionState.FAILED

    client = _RecordingClient()
    result = retry_run_completion(
        "run-retry",
        engine=pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    )
    assert result.execution.state is RunCompletionExecutionState.ENQUEUED
    assert result.execution.current_attempt == 2
    assert client.enqueued

    with pg_engine.connect() as connection:
        first = get_run_completion_attempt(
            connection,
            run_completion_execution_id=result.execution.run_completion_execution_id,
            attempt_number=1,
            schema=schema,
        )
    assert first is not None
    assert first.terminal_at is not None
    assert first.terminal_summary is not None
    assert first.terminal_summary["outcome"] == (
        RunCompletionExecutionState.FAILED.value
    )
