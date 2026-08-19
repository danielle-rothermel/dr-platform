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
    "UPDATE dbos.workflow_status "
    "SET priority = :priority "
    "WHERE workflow_uuid = :workflow_id"
)
assert DBOS_WORKFLOW_STATUS_TABLE == "dbos.workflow_status"

_NONTERMINAL_STATES = frozenset(
    {
        StageExecutionState.READY,
        StageExecutionState.ADMITTED,
        StageExecutionState.FAILED,
    }
)


@dataclass(frozen=True, slots=True)
class WorkPriorityResult:
    work_item_id: int
    priority: int
    updated_stage_execution_ids: tuple[int, ...]
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
        work_item_id = _lock_work_item(
            connection,
            campaign_key=normalized_campaign,
            work_key=normalized_work,
            schema=selected_schema,
        )
        connection.execute(
            update(selected_schema.work_items)
            .where(selected_schema.work_items.c.work_item_id == work_item_id)
            .values(priority=priority)
        )
        executions = selected_schema.stage_executions
        stage_execution_ids = tuple(
            connection.execute(
                select(executions.c.stage_execution_id).where(
                    executions.c.work_item_id == work_item_id,
                    executions.c.state.in_(
                        tuple(state.value for state in _NONTERMINAL_STATES)
                    ),
                )
            ).scalars()
        )
        if stage_execution_ids:
            connection.execute(
                update(executions)
                .where(
                    executions.c.stage_execution_id.in_(stage_execution_ids)
                )
                .values(priority=priority, updated_at=updated_at)
            )
        workflow_ids = _update_admitted_workflow_priorities(
            connection,
            stage_execution_ids=stage_execution_ids,
            priority=priority,
            schema=selected_schema,
        )

    return WorkPriorityResult(
        work_item_id=work_item_id,
        priority=priority,
        updated_stage_execution_ids=stage_execution_ids,
        updated_workflow_ids=workflow_ids,
    )


def _lock_work_item(
    connection: Connection,
    *,
    campaign_key: CampaignKey,
    work_key: WorkKey,
    schema: LedgerSchema,
) -> int:
    work_items = schema.work_items
    work_item_id = connection.execute(
        select(work_items.c.work_item_id)
        .where(
            work_items.c.campaign_key == campaign_key.value,
            work_items.c.work_key == work_key.value,
        )
        .with_for_update(of=work_items)
    ).scalar_one_or_none()
    if work_item_id is None:
        raise LookupError(
            "work item does not exist: "
            f"{CampaignWorkIdentity(campaign_key, work_key)!r}"
        )
    return work_item_id


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
