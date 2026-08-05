"""Operator cancellation for staged pipeline work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import func, select

from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    WorkKey,
)
from dr_platform._core.ledger.attempts import (
    get_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    get_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection, Engine

    from dr_platform._core.ledger.executions import StageExecutionRecord


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
    CANCELLED_FAILED = "cancelled_failed"
    ALREADY_TERMINAL = "already_terminal"


# A linear pipeline is a finite chain, so re-selecting the current stage after
# a concurrent handoff commits can only advance a bounded number of times.
_MAX_CURRENT_STAGE_RESELECTS = 64


@dataclass(frozen=True, slots=True)
class WorkCancellationResult:
    work_item_id: int
    stage_execution: StageExecutionRecord
    disposition: CancellationDisposition
    delegated_workflow_id: str | None


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
    cancellation disabled.  A FAILED attempt is already terminal, so cancelling
    it only fences the stage against a later ``retry_stage`` and delegates
    nothing.

    Already-CANCELLED work self-heals a lost post-commit delegation: platform
    state commits before the canceller runs, so a crashed or raising delegation
    would otherwise leak a live DBOS workflow.  If the current stage is already
    CANCELLED with a recorded admitted attempt that was never superseded, its
    workflow cancellation is re-issued idempotently before returning the
    ALREADY_TERMINAL disposition.  Other terminal work is an idempotent no-op.
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
        if current.state in {
            StageExecutionState.READY,
            StageExecutionState.ADMITTED,
            StageExecutionState.FAILED,
        }:
            result = _cancel_current_stage(
                connection,
                current=current,
                cancelled_at=cancelled_at,
                schema=selected_schema,
            )
        else:
            result = WorkCancellationResult(
                work_item_id=resolved_work_item_id,
                stage_execution=current,
                disposition=CancellationDisposition.ALREADY_TERMINAL,
                delegated_workflow_id=_redelegable_workflow_id(
                    connection,
                    current=current,
                    schema=selected_schema,
                ),
            )

    if result.delegated_workflow_id is not None:
        client.cancel_workflow(
            result.delegated_workflow_id,
            cancel_children=False,
        )
    return result


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
        identity = CampaignWorkIdentity(
            campaign_key
            if isinstance(campaign_key, CampaignKey)
            else CampaignKey(campaign_key),
            work_key if isinstance(work_key, WorkKey) else WorkKey(work_key),
        )
        statement = select(table.c.work_item_id).where(
            table.c.campaign_key == identity.campaign_key.value,
            table.c.work_key == identity.work_key.value,
        )
    else:
        raise ValueError("campaign_key and work_key must be supplied together")
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
    # Under READ COMMITTED the max-stage_index SELECT takes its snapshot
    # before blocking on any concurrent handoff's row lock; when that handoff
    # commits a freshly inserted successor stage, Postgres re-evaluates only
    # the locked row and never sees it.  So after acquiring the lock, re-select
    # the max stage index: if a successor now exists the locked row is stale,
    # and we loop to lock the newer row.  The pipeline is a finite linear
    # chain, so each iteration advances by at least one stage and is bounded.
    table = schema.stage_executions
    for _ in range(_MAX_CURRENT_STAGE_RESELECTS):
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
            raise LookupError(
                f"work item has no stage execution: {work_item_id}"
            )
        current = get_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            schema=schema,
        )
        assert current is not None
        latest_index = connection.execute(
            select(func.max(table.c.stage_index)).where(
                table.c.work_item_id == work_item_id
            )
        ).scalar_one()
        if latest_index == current.stage_index:
            return current
    raise RuntimeError(
        f"stage index kept advancing while locking work item: {work_item_id}"
    )


def _cancel_current_stage(
    connection: Connection,
    *,
    current: StageExecutionRecord,
    cancelled_at: datetime,
    schema: StagingSchema,
) -> WorkCancellationResult:
    workflow_id: str | None = None
    if current.state is StageExecutionState.FAILED:
        # The attempt is already terminal; cancelling only fences the stage
        # against a later retry, so nothing is delegated to DBOS.
        disposition = CancellationDisposition.CANCELLED_FAILED
    else:
        disposition = CancellationDisposition.CANCELLED_READY
    if current.state is StageExecutionState.ADMITTED:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=current.stage_execution_id,
            attempt_number=current.current_attempt,
            schema=schema,
        )
        if attempt is None or attempt.terminal_at is not None:
            raise RuntimeError("ADMITTED stage has no active current attempt")
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


def _redelegable_workflow_id(
    connection: Connection,
    *,
    current: StageExecutionRecord,
    schema: StagingSchema,
) -> str | None:
    """The workflow to re-cancel when a prior delegation may have been lost.

    Platform CANCELLED commits before the post-commit canceller runs, so a
    crashed or raising delegation leaves the DBOS workflow alive.  When the
    already-CANCELLED current stage carries an admitted, non-superseded
    attempt, re-issuing its cancellation lets repeated ``cancel_work``
    self-heal.

    Re-issuing is safe even when the workflow already finished only because
    DBOS's ``cancel_workflows`` UPDATE is guarded with ``status NOT IN
    (SUCCESS, ERROR)``; a DBOS upgrade that drops that guard would let this
    path rewrite completed workflows' statuses.
    """
    if current.state is not StageExecutionState.CANCELLED:
        return None
    attempt = get_stage_attempt(
        connection,
        stage_execution_id=current.stage_execution_id,
        attempt_number=current.current_attempt,
        schema=schema,
    )
    if attempt is None or attempt.admitted_at is None:
        return None
    return attempt.workflow_id
