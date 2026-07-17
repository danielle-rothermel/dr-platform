"""Internal operator controls for staged pipeline work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import select

from dr_platform.staging.controls import (
    set_stage_control_capacity,
    set_stage_control_paused,
)
from dr_platform.staging.definitions import (
    PipelineIdentity,
    validate_positive_integer,
)
from dr_platform.staging.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    StageKey,
    WorkKey,
    validate_key_value,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import (
    append_stage_attempt,
    get_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform.staging.stage_executions import (
    get_stage_execution,
    transition_stage_execution,
)
from dr_platform.staging.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Connection, Engine

    from dr_platform.staging.records import (
        StageAttemptRecord,
        StageControlRecord,
        StageExecutionRecord,
    )

PIPELINE_IDENTITY_PARTS = 2


def _utc_now() -> datetime:
    return datetime.now(UTC)


class WorkflowCanceller(Protocol):
    """The public DBOS cancellation operation used by this boundary."""

    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        cancel_children: bool = False,
    ) -> None: ...


class CancellationDisposition(StrEnum):
    CANCELLED_READY = "cancelled_ready"
    CANCELLED_ADMITTED = "cancelled_admitted"
    ALREADY_TERMINAL = "already_terminal"


@dataclass(frozen=True, slots=True)
class WorkCancellationResult:
    work_item_id: int
    stage_execution: StageExecutionRecord
    disposition: CancellationDisposition
    delegated_workflow_id: str | None


@dataclass(frozen=True, slots=True)
class StageRetryResult:
    stage_execution: StageExecutionRecord
    new_attempt: StageAttemptRecord


def set_stage_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> StageControlRecord:
    """Set the empty-selector capacity without changing pause state."""
    return _set_capacity(
        pipeline=pipeline,
        stage_key=stage_key,
        labels={},
        capacity=capacity,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def set_selector_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str],
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> StageControlRecord:
    """Set one exact-label selector capacity without changing pause state."""
    return _set_capacity(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        capacity=capacity,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def pause(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    engine: Engine,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> StageControlRecord:
    """Pause an existing exact-selector control; running work is untouched."""
    return _set_paused(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        paused=True,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def resume(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    engine: Engine,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> StageControlRecord:
    """Resume an existing exact-selector control."""
    return _set_paused(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        paused=False,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def cancel_work(  # noqa: PLR0913 -- two explicit identity forms
    *,
    engine: Engine,
    client: WorkflowCanceller,
    work_item_id: int | None = None,
    campaign_key: CampaignKey | str | None = None,
    work_key: WorkKey | str | None = None,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> WorkCancellationResult:
    """Cancel the current logical stage and delegate admitted cancellation.

    READY work has no physical workflow and is only marked CANCELLED.  For an
    ADMITTED attempt, platform state and its terminal record are committed
    first, then DBOS is asked to cancel that exact workflow with child
    cancellation disabled.  Already-terminal work is an idempotent no-op.
    """
    selected_schema = schema or StagingSchema()
    cancelled_at = clock()
    with engine.begin() as connection:
        resolved_work_item_id = _resolve_work_item_id(
            connection,
            work_item_id=work_item_id,
            campaign_key=campaign_key,
            work_key=work_key,
            schema=selected_schema,
        )
        current = _lock_current_stage(
            connection,
            work_item_id=resolved_work_item_id,
            schema=selected_schema,
        )
        if current.state not in {
            StageExecutionState.READY,
            StageExecutionState.ADMITTED,
        }:
            result = WorkCancellationResult(
                work_item_id=resolved_work_item_id,
                stage_execution=current,
                disposition=CancellationDisposition.ALREADY_TERMINAL,
                delegated_workflow_id=None,
            )
        else:
            result = _cancel_current_stage(
                connection,
                current=current,
                cancelled_at=cancelled_at,
                schema=selected_schema,
            )

    if result.delegated_workflow_id is not None:
        client.cancel_workflow(
            result.delegated_workflow_id,
            cancel_children=False,
        )
    return result


def retry_stage(
    stage_execution_id: int,
    *,
    engine: Engine,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> StageRetryResult:
    """Prepare exactly one new attempt for a terminal FAILED stage.

    CANCELLED and all other states are intentionally ineligible.  Admission
    later marks this prepared attempt admitted and enqueues its workflow.
    """
    selected_schema = schema or StagingSchema()
    retried_at = clock()
    with engine.begin() as connection:
        table = selected_schema.stage_executions
        row = (
            connection.execute(
                select(table.c.state, table.c.current_attempt)
                .where(table.c.stage_execution_id == stage_execution_id)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise LookupError(
                f"stage execution does not exist: {stage_execution_id}"
            )
        if row["state"] != StageExecutionState.FAILED.value:
            raise ValueError("only a FAILED stage execution can be retried")
        previous = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=row["current_attempt"],
            schema=selected_schema,
        )
        if previous is None or previous.terminal_at is None:
            raise RuntimeError(
                "FAILED stage execution has no terminal current attempt"
            )
        new_attempt = append_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            created_at=retried_at,
            schema=selected_schema,
        )
        execution = transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.READY,
            updated_at=retried_at,
            schema=selected_schema,
        )
    return StageRetryResult(
        stage_execution=execution,
        new_attempt=new_attempt,
    )


def _set_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str],
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime],
    schema: StagingSchema | None,
) -> StageControlRecord:
    pipeline_key, pipeline_version = _validate_pipeline(pipeline)
    selected_schema = schema or StagingSchema()
    with engine.begin() as connection:
        return set_stage_control_capacity(
            connection,
            pipeline_key=pipeline_key,
            pipeline_version=pipeline_version,
            stage_key=stage_key,
            selector=labels,
            capacity=capacity,
            updated_at=clock(),
            schema=selected_schema,
        )


def _set_paused(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None,
    paused: bool,
    engine: Engine,
    clock: Callable[[], datetime],
    schema: StagingSchema | None,
) -> StageControlRecord:
    pipeline_key, pipeline_version = _validate_pipeline(pipeline)
    selected_schema = schema or StagingSchema()
    with engine.begin() as connection:
        return set_stage_control_paused(
            connection,
            pipeline_key=pipeline_key,
            pipeline_version=pipeline_version,
            stage_key=stage_key,
            selector=labels,
            paused=paused,
            updated_at=clock(),
            schema=selected_schema,
        )


def _validate_pipeline(pipeline: PipelineIdentity) -> PipelineIdentity:
    if (
        not isinstance(pipeline, tuple)
        or len(pipeline) != PIPELINE_IDENTITY_PARTS
        or not isinstance(pipeline[0], str)
        or not isinstance(pipeline[1], int)
    ):
        raise TypeError("pipeline must be a (key, version) tuple")
    validate_key_value(pipeline[0], label="pipeline key")
    validate_positive_integer(pipeline[1], label="pipeline version")
    return pipeline


def _resolve_work_item_id(
    connection: Connection,
    *,
    work_item_id: int | None,
    campaign_key: CampaignKey | str | None,
    work_key: WorkKey | str | None,
    schema: StagingSchema,
) -> int:
    has_logical_identity = campaign_key is not None or work_key is not None
    if work_item_id is not None and has_logical_identity:
        raise ValueError(
            "supply either work_item_id or campaign_key and work_key"
        )
    table = schema.work_items
    if work_item_id is not None:
        if (
            isinstance(work_item_id, bool)
            or not isinstance(work_item_id, int)
            or work_item_id <= 0
        ):
            raise ValueError("work item id must be a positive integer")
        statement = select(table.c.work_item_id).where(
            table.c.work_item_id == work_item_id
        )
    elif campaign_key is not None and work_key is not None:
        identity = CampaignWorkIdentity(campaign_key, work_key)
        statement = select(table.c.work_item_id).where(
            table.c.campaign_key == identity.campaign_key.value,
            table.c.work_key == identity.work_key.value,
        )
    else:
        raise ValueError(
            "campaign_key and work_key must be supplied together"
        )
    resolved = connection.execute(statement).scalar_one_or_none()
    if resolved is None:
        raise LookupError("work item does not exist")
    return resolved


def _lock_current_stage(
    connection: Connection,
    *,
    work_item_id: int,
    schema: StagingSchema,
) -> StageExecutionRecord:
    table = schema.stage_executions
    stage_execution_id = connection.execute(
        select(table.c.stage_execution_id)
        .where(table.c.work_item_id == work_item_id)
        .order_by(
            table.c.stage_index.desc(),
            table.c.stage_execution_id.desc(),
        )
        .limit(1)
        .with_for_update()
    ).scalar_one_or_none()
    if stage_execution_id is None:
        raise LookupError(f"work item has no stage execution: {work_item_id}")
    current = get_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        schema=schema,
    )
    assert current is not None
    return current


def _cancel_current_stage(
    connection: Connection,
    *,
    current: StageExecutionRecord,
    cancelled_at: datetime,
    schema: StagingSchema,
) -> WorkCancellationResult:
    workflow_id: str | None = None
    disposition = CancellationDisposition.CANCELLED_READY
    if current.state is StageExecutionState.ADMITTED:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=current.stage_execution_id,
            attempt_number=current.current_attempt,
            schema=schema,
        )
        if attempt is None or attempt.terminal_at is not None:
            raise RuntimeError(
                "ADMITTED stage has no active current attempt"
            )
        workflow_id = attempt.workflow_id
        disposition = CancellationDisposition.CANCELLED_ADMITTED

    execution = transition_stage_execution(
        connection,
        stage_execution_id=current.stage_execution_id,
        new_state=StageExecutionState.CANCELLED,
        updated_at=cancelled_at,
        schema=schema,
    )
    if workflow_id is not None:
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=current.stage_execution_id,
            attempt_number=current.current_attempt,
            terminal_at=cancelled_at,
            terminal_summary={
                "outcome": StageExecutionState.CANCELLED.value,
                "reason": "operator_requested",
            },
            terminal_reference=workflow_id,
            schema=schema,
        )
    return WorkCancellationResult(
        work_item_id=current.work_item_id,
        stage_execution=execution,
        disposition=disposition,
        delegated_workflow_id=workflow_id,
    )
