from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import literal, select

from dr_platform._core.clock import utc_now
from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    WorkKey,
    normalize_key,
)
from dr_platform._core.ledger.attempts import (
    get_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    get_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryProducer,
    build_terminal_summary,
)
from dr_platform._core.ledger.work_item_status import work_item_status_rows

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from sqlalchemy import Connection, Engine

    from dr_platform._core.ledger.executions import StageExecutionRecord


class WorkflowCanceller(Protocol):
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


_NONTERMINAL_STATES = frozenset(
    {
        StageExecutionState.READY,
        StageExecutionState.ADMITTED,
        StageExecutionState.FAILED,
    }
)

# Watchdog against relock livelock while resolving nonterminal executions.
_MAX_CURRENT_STAGE_RESELECTS = 4_096


@dataclass(frozen=True, slots=True)
class CancelledStageExecution:
    stage_execution: StageExecutionRecord
    disposition: CancellationDisposition
    delegated_workflow_id: str | None


@dataclass(frozen=True, slots=True)
class WorkCancellationResult:
    work_item_id: int
    cancellations: tuple[CancelledStageExecution, ...]
    disposition: CancellationDisposition
    delegated_workflow_id: str | None
    stage_execution: StageExecutionRecord


def cancel_work(  # noqa: PLR0913 -- two explicit identity forms
    *,
    engine: Engine,
    client: WorkflowCanceller,
    work_item_id: int | None = None,
    campaign_key: CampaignKey | str | None = None,
    work_key: WorkKey | str | None = None,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> WorkCancellationResult:
    """Cancel every nonterminal execution for one work item.

    Item-level cancellation commits logical intent for all READY, ADMITTED, and
    FAILED executions before delegating to DBOS for each admitted attempt.
    """
    selected_schema = schema or LedgerSchema()
    delegated: list[str] = []
    with engine.begin() as connection:
        resolved_work_item_id = _resolve_work_item_id(
            connection,
            work_item_id=work_item_id,
            campaign_key=campaign_key,
            work_key=work_key,
            schema=selected_schema,
        )
        locked = _lock_nonterminal_executions(
            connection,
            work_item_id=resolved_work_item_id,
            schema=selected_schema,
        )
        if locked:
            cancelled_at = clock()
            cancellations = _cancel_locked_executions(
                connection,
                executions=locked,
                cancelled_at=cancelled_at,
                schema=selected_schema,
            )
            delegated.extend(
                item.delegated_workflow_id
                for item in cancellations
                if item.delegated_workflow_id is not None
            )
            representative = cancellations[0].stage_execution
            disposition = _aggregate_disposition(cancellations)
            result = WorkCancellationResult(
                work_item_id=resolved_work_item_id,
                cancellations=cancellations,
                disposition=disposition,
                delegated_workflow_id=(
                    cancellations[0].delegated_workflow_id
                    if len(cancellations) == 1
                    else None
                ),
                stage_execution=representative,
            )
        else:
            representative = _terminal_representative(
                connection,
                work_item_id=resolved_work_item_id,
                schema=selected_schema,
            )
            repair_workflow_id = _redelegable_workflow_id(
                connection,
                current=representative,
                schema=selected_schema,
            )
            result = WorkCancellationResult(
                work_item_id=resolved_work_item_id,
                cancellations=(),
                disposition=CancellationDisposition.ALREADY_TERMINAL,
                delegated_workflow_id=repair_workflow_id,
                stage_execution=representative,
            )
            if repair_workflow_id is not None:
                delegated.append(repair_workflow_id)

    for workflow_id in delegated:
        client.cancel_workflow(workflow_id, cancel_children=False)
    return result


def _aggregate_disposition(
    cancellations: tuple[CancelledStageExecution, ...],
) -> CancellationDisposition:
    if len(cancellations) == 1:
        return cancellations[0].disposition
    if any(
        item.disposition is CancellationDisposition.CANCELLED_ADMITTED
        for item in cancellations
    ):
        return CancellationDisposition.CANCELLED_ADMITTED
    if any(
        item.disposition is CancellationDisposition.CANCELLED_FAILED
        for item in cancellations
    ):
        return CancellationDisposition.CANCELLED_FAILED
    return CancellationDisposition.CANCELLED_READY


def _resolve_work_item_id(
    connection: Connection,
    *,
    work_item_id: int | None,
    campaign_key: CampaignKey | str | None,
    work_key: WorkKey | str | None,
    schema: LedgerSchema,
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
            normalize_key(campaign_key, CampaignKey),
            normalize_key(work_key, WorkKey),
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


def _lock_nonterminal_executions(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> tuple[StageExecutionRecord, ...]:
    # A completion transaction needs FOR UPDATE on its ADMITTED row before it
    # can insert successors; locking every ADMITTED row blocks new successors.
    # Re-check closes the select/lock window under READ COMMITTED.
    table = schema.stage_executions
    nonterminal_values = tuple(state.value for state in _NONTERMINAL_STATES)
    for _ in range(_MAX_CURRENT_STAGE_RESELECTS):
        locked_ids = (
            connection.execute(
                select(table.c.stage_execution_id)
                .where(
                    table.c.work_item_id == work_item_id,
                    table.c.state.in_(nonterminal_values),
                )
                .order_by(table.c.stage_execution_id)
                .with_for_update()
            )
            .scalars()
            .all()
        )
        if not locked_ids:
            still_nonterminal = connection.execute(
                select(
                    select(1)
                    .select_from(table)
                    .where(
                        table.c.work_item_id == work_item_id,
                        table.c.state.in_(nonterminal_values),
                    )
                    .exists()
                )
            ).scalar_one()
            if not still_nonterminal:
                return ()
            continue
        missed = connection.execute(
            select(table.c.stage_execution_id)
            .where(
                table.c.work_item_id == work_item_id,
                table.c.state.in_(nonterminal_values),
                table.c.stage_execution_id.not_in(locked_ids),
            )
            .limit(1)
        ).scalar_one_or_none()
        if missed is None:
            locked = []
            for stage_execution_id in locked_ids:
                current = get_stage_execution(
                    connection,
                    stage_execution_id=stage_execution_id,
                    schema=schema,
                )
                assert current is not None
                locked.append(current)
            return tuple(locked)
    raise RuntimeError(
        "nonterminal executions kept appearing while locking work item: "
        f"{work_item_id}"
    )


def _cancel_locked_executions(
    connection: Connection,
    *,
    executions: tuple[StageExecutionRecord, ...],
    cancelled_at: datetime,
    schema: LedgerSchema,
) -> tuple[CancelledStageExecution, ...]:
    return tuple(
        _cancel_one_execution(
            connection,
            current=current,
            cancelled_at=cancelled_at,
            schema=schema,
        )
        for current in executions
    )


def _cancel_one_execution(
    connection: Connection,
    *,
    current: StageExecutionRecord,
    cancelled_at: datetime,
    schema: LedgerSchema,
) -> CancelledStageExecution:
    workflow_id: str | None = None
    if current.state is StageExecutionState.FAILED:
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
    if workflow_id is not None or (
        current.state is StageExecutionState.READY
        and current.current_attempt > 0
    ):
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=current.stage_execution_id,
            attempt_number=current.current_attempt,
            terminal_at=cancelled_at,
            terminal_summary=build_terminal_summary(
                outcome=StageExecutionState.CANCELLED.value,
                producer=TerminalSummaryProducer.CANCELLATION,
                reason="operator_requested",
            ),
            terminal_reference=workflow_id,
            schema=schema,
        )
    return CancelledStageExecution(
        stage_execution=execution,
        disposition=disposition,
        delegated_workflow_id=workflow_id,
    )


def _terminal_representative(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> StageExecutionRecord:
    status = work_item_status_rows(
        schema,
        select(literal(work_item_id).label("work_item_id")),
    )
    stage_execution_id = connection.execute(
        select(status.c.stage_execution_id)
    ).scalar_one()
    representative = get_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        schema=schema,
    )
    if representative is None:
        raise LookupError(f"work item has no stage execution: {work_item_id}")
    return representative


def _redelegable_workflow_id(
    connection: Connection,
    *,
    current: StageExecutionRecord,
    schema: LedgerSchema,
) -> str | None:
    """Return a workflow eligible to repair lost post-commit delegation.

    This is safe only while DBOS guards cancellation updates against SUCCESS
    and ERROR statuses.
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
