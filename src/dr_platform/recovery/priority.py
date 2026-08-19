from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select, text, update

from dr_platform._core.clock import utc_now
from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    WorkKey,
    normalize_key,
)
from dr_platform._core.ledger.attempts import get_stage_attempt
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import validate_work_priority
from dr_platform.runtime.dbos import DBOS_WORKFLOW_STATUS_TABLE

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from sqlalchemy import Connection, Engine


_UPDATE_ADMITTED_WORKFLOW_PRIORITY = (
    "UPDATE "
    + DBOS_WORKFLOW_STATUS_TABLE
    + " SET priority = :priority "
    + "WHERE workflow_uuid = :workflow_id"
)

_NONTERMINAL_STATES = frozenset(
    {
        StageExecutionState.READY,
        StageExecutionState.ADMITTED,
        StageExecutionState.FAILED,
    }
)
_MAX_NONTERMINAL_EXECUTION_RESELECTS = 4_096


@dataclass(frozen=True, slots=True)
class WorkPriorityResult:
    work_item_id: int
    priority: int
    updated_stage_execution_ids: tuple[int, ...]
    updated_workflow_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class WorkPrioritySyncResult:
    stage_execution_ids: tuple[int, ...]
    updated_workflow_ids: tuple[str, ...]


def set_work_priority(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    campaign_key: CampaignKey | str,
    work_key: WorkKey | str,
    priority: int,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> WorkPriorityResult:
    """Raise a work item's admission and queue priority.

    Ready executions pick up the new order on the next admission pass. Admitted
    executions also update the colocated DBOS ``workflow_status.priority`` row
    for each current attempt workflow id.
    """
    normalized_campaign = normalize_key(campaign_key, CampaignKey)
    normalized_work = normalize_key(work_key, WorkKey)
    validate_work_priority(priority)
    selected_schema = schema or LedgerSchema()
    updated_at = clock()

    with engine.begin() as connection:
        work_item_id = _resolve_work_item_id(
            connection,
            campaign_key=normalized_campaign,
            work_key=normalized_work,
            schema=selected_schema,
        )
        sync = sync_work_priority_in_transaction(
            connection,
            work_item_id=work_item_id,
            priority=priority,
            updated_at=updated_at,
            schema=selected_schema,
        )

    return WorkPriorityResult(
        work_item_id=work_item_id,
        priority=priority,
        updated_stage_execution_ids=sync.stage_execution_ids,
        updated_workflow_ids=sync.updated_workflow_ids,
    )


def sync_work_priority_in_transaction(
    connection: Connection,
    *,
    work_item_id: int,
    priority: int,
    updated_at: datetime,
    schema: LedgerSchema,
) -> WorkPrioritySyncResult:
    """Apply a priority change under execution-first lock order.

    Blocks on in-flight handoffs, re-scans nonterminal executions after the
    work-item row is serialized, and keeps admitted DBOS queue priority
    aligned.
    """
    validate_work_priority(priority)
    _block_on_nonterminal_executions(
        connection,
        work_item_id=work_item_id,
        schema=schema,
    )
    _lock_work_item_by_id(
        connection,
        work_item_id=work_item_id,
        schema=schema,
    )
    stage_execution_ids = _lock_nonterminal_execution_ids(
        connection,
        work_item_id=work_item_id,
        schema=schema,
    )
    connection.execute(
        update(schema.work_items)
        .where(schema.work_items.c.work_item_id == work_item_id)
        .values(priority=priority)
    )
    executions = schema.stage_executions
    if stage_execution_ids:
        connection.execute(
            update(executions)
            .where(executions.c.stage_execution_id.in_(stage_execution_ids))
            .values(priority=priority, updated_at=updated_at)
        )
    workflow_ids = _update_admitted_workflow_priorities(
        connection,
        stage_execution_ids=stage_execution_ids,
        priority=priority,
        schema=schema,
    )
    return WorkPrioritySyncResult(
        stage_execution_ids=stage_execution_ids,
        updated_workflow_ids=workflow_ids,
    )


def _resolve_work_item_id(
    connection: Connection,
    *,
    campaign_key: CampaignKey,
    work_key: WorkKey,
    schema: LedgerSchema,
) -> int:
    work_items = schema.work_items
    work_item_id = connection.execute(
        select(work_items.c.work_item_id).where(
            work_items.c.campaign_key == campaign_key.value,
            work_items.c.work_key == work_key.value,
        )
    ).scalar_one_or_none()
    if work_item_id is None:
        raise LookupError(
            "work item does not exist: "
            f"{CampaignWorkIdentity(campaign_key, work_key)!r}"
        )
    return work_item_id


def _block_on_nonterminal_executions(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> None:
    # Preserve execution-before-work-item ordering used by stage handoff.
    executions = schema.stage_executions
    connection.execute(
        select(executions.c.stage_execution_id)
        .where(
            executions.c.work_item_id == work_item_id,
            executions.c.state.in_(
                tuple(state.value for state in _NONTERMINAL_STATES)
            ),
        )
        .order_by(executions.c.stage_execution_id)
        .with_for_update(of=executions)
    )


def _lock_nonterminal_execution_ids(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> tuple[int, ...]:
    # A completion transaction needs FOR UPDATE on its ADMITTED row before it
    # can insert successors; re-check closes the select/lock window under
    # READ COMMITTED once the work item row is serialized.
    executions = schema.stage_executions
    nonterminal_values = tuple(state.value for state in _NONTERMINAL_STATES)
    for _ in range(_MAX_NONTERMINAL_EXECUTION_RESELECTS):
        locked_ids = tuple(
            connection.execute(
                select(executions.c.stage_execution_id)
                .where(
                    executions.c.work_item_id == work_item_id,
                    executions.c.state.in_(nonterminal_values),
                )
                .order_by(executions.c.stage_execution_id)
                .with_for_update(of=executions)
            )
            .scalars()
            .all()
        )
        if not locked_ids:
            still_nonterminal = connection.execute(
                select(
                    select(1)
                    .select_from(executions)
                    .where(
                        executions.c.work_item_id == work_item_id,
                        executions.c.state.in_(nonterminal_values),
                    )
                    .exists()
                )
            ).scalar_one()
            if not still_nonterminal:
                return ()
            continue
        missed = connection.execute(
            select(executions.c.stage_execution_id)
            .where(
                executions.c.work_item_id == work_item_id,
                executions.c.state.in_(nonterminal_values),
                executions.c.stage_execution_id.not_in(locked_ids),
            )
            .limit(1)
        ).scalar_one_or_none()
        if missed is None:
            return locked_ids
    raise RuntimeError(
        "nonterminal executions kept appearing while syncing work priority: "
        f"{work_item_id}"
    )


def _lock_work_item_by_id(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> None:
    work_items = schema.work_items
    locked = connection.execute(
        select(work_items.c.work_item_id)
        .where(work_items.c.work_item_id == work_item_id)
        .with_for_update(of=work_items)
    ).scalar_one_or_none()
    if locked is None:
        raise LookupError(f"work item does not exist: {work_item_id}")


def _update_admitted_workflow_priorities(
    connection: Connection,
    *,
    stage_execution_ids: tuple[int, ...],
    priority: int,
    schema: LedgerSchema,
) -> tuple[str, ...]:
    if not stage_execution_ids:
        return ()
    executions = schema.stage_executions
    admitted_rows = connection.execute(
        select(
            executions.c.stage_execution_id,
            executions.c.current_attempt,
        ).where(
            executions.c.stage_execution_id.in_(stage_execution_ids),
            executions.c.state == StageExecutionState.ADMITTED.value,
            executions.c.current_attempt > 0,
        )
    ).mappings()
    workflow_ids: list[str] = []
    for row in admitted_rows:
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=row["stage_execution_id"],
            attempt_number=row["current_attempt"],
            schema=schema,
        )
        if attempt is None or not attempt.workflow_id:
            continue
        workflow_ids.append(attempt.workflow_id)
        connection.execute(
            text(_UPDATE_ADMITTED_WORKFLOW_PRIORITY),
            {"priority": priority, "workflow_id": attempt.workflow_id},
        )
    return tuple(workflow_ids)
