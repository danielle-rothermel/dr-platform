from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import Engine

from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
)
from dr_platform._core.ledger.completion_attempts import (
    get_run_completion_attempt,
)
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
    StateCount,
)
from dr_platform.completion.barrier import run_barrier_pass
from dr_platform.completion.execution import (
    RunCompletionOutcomeError,
    RunCompletionPayload,
    inspect_run_completion,
    record_run_completion_outcome,
)
from tests.completion.test_run_barrier import (
    _members,
    _RecordingClient,
    _registry,
    _set_states,
    _submit_run,
)
from tests.conftest import NOW, _migrate


def _enqueue_completion(pg_engine: Engine, *, key: str = "outcome") -> str:
    _migrate(pg_engine)
    registry, pipeline = _registry(key)
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
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
    return summary.releases[0].workflow_id


def test_run_completion_records_one_success_idempotently(
    pg_engine: Engine,
) -> None:
    workflow_id = _enqueue_completion(pg_engine)
    terminal_at = NOW + timedelta(seconds=2)
    with pg_engine.begin() as connection:
        first = record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=True,
            output_reference="aggregate:1",
            error_summary=None,
            terminal_at=terminal_at,
        )
        replay = record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=True,
            output_reference="aggregate:1",
            error_summary=None,
            terminal_at=terminal_at + timedelta(seconds=1),
        )
    assert replay == first
    assert first.state is RunCompletionExecutionState.SUCCEEDED
    assert first.output_reference == "aggregate:1"
    assert first.terminal_at == terminal_at
    assert inspect_run_completion("run-1", engine=pg_engine) == first


def test_run_completion_records_application_failure(pg_engine: Engine) -> None:
    workflow_id = _enqueue_completion(pg_engine, key="failure")
    with pg_engine.begin() as connection:
        failed = record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={"error_type": "ValueError", "message": "broken"},
            terminal_at=NOW + timedelta(seconds=2),
        )
    assert failed.state is RunCompletionExecutionState.FAILED
    assert failed.output_reference is None
    assert failed.error_summary == {
        "error_type": "ValueError",
        "message": "broken",
    }


def test_run_completion_attempt_outcome_wins_over_error_summary(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    workflow_id = _enqueue_completion(pg_engine, key="outcome-pin")
    with pg_engine.begin() as connection:
        failed = record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={
                "outcome": "succeeded",
                "error_type": "ValueError",
                "message": "broken",
            },
            terminal_at=NOW + timedelta(seconds=2),
        )
        attempt = get_run_completion_attempt(
            connection,
            run_completion_execution_id=failed.run_completion_execution_id,
            attempt_number=failed.current_attempt,
            schema=schema,
        )
    assert failed.state is RunCompletionExecutionState.FAILED
    assert attempt is not None
    assert attempt.terminal_summary is not None
    assert attempt.terminal_summary["outcome"] == (
        RunCompletionExecutionState.FAILED.value
    )


def test_run_completion_rejects_a_different_second_outcome(
    pg_engine: Engine,
) -> None:
    workflow_id = _enqueue_completion(pg_engine, key="conflict")
    with pg_engine.begin() as connection:
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=True,
            output_reference="aggregate:1",
            error_summary=None,
            terminal_at=NOW + timedelta(seconds=2),
        )
    with (
        pytest.raises(RunCompletionOutcomeError, match="different"),
        pg_engine.begin() as connection,
    ):
        record_run_completion_outcome(
            connection,
            workflow_id=workflow_id,
            succeeded=False,
            output_reference=None,
            error_summary={"error_type": "ValueError"},
            terminal_at=NOW + timedelta(seconds=3),
        )


def test_completion_payload_validates_compact_release_facts() -> None:
    payload = RunCompletionPayload(
        campaign_key=CampaignKey("campaign-1"),
        run_key=RunKey("run-1"),
        pipeline_key=PipelineKey("pipeline-1"),
        pipeline_version=1,
        execution_config_reference="config:1",
        manifest_reference="manifest:1",
        membership_digest="digest:1",
        member_count=2,
        released_at=NOW,
        release_terminal_state_counts=(
            StateCount(state=StageExecutionState.SUCCEEDED, count=1),
            StateCount(state=StageExecutionState.FAILED, count=1),
            StateCount(state=StageExecutionState.CANCELLED, count=0),
        ),
    )
    assert (
        RunCompletionPayload.model_validate(payload.model_dump(mode="json"))
        == payload
    )

    with pytest.raises(ValidationError, match="sum to members"):
        payload.model_copy(update={"member_count": 3}).model_validate(
            payload.model_copy(update={"member_count": 3}).model_dump()
        )
