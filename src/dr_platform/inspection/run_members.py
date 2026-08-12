from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, select

from dr_platform._core.identities import RunKey, StageKey, WorkKey
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.inspection._validation import (
    DEFAULT_INSPECTION_LIMIT,
    normalize_run_key,
    require_run,
    validate_limit,
    validate_run_member_cursor,
)
from dr_platform.inspection.statuses import current_stage_indexes

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.engine import RowMapping


@dataclass(frozen=True, slots=True)
class RunMemberSummary:
    member_ordinal: int
    work_key: WorkKey
    work_item_id: int
    input_reference: str
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None


def list_run_members(
    run_key: RunKey | str,
    *,
    engine: Engine,
    cursor: int | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    schema: StagingSchema | None = None,
) -> tuple[RunMemberSummary, ...]:
    validate_limit(limit)
    selected_schema = schema or StagingSchema()
    normalized_run = normalize_run_key(run_key)
    memberships = selected_schema.run_memberships
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    scoped_item_ids = select(memberships.c.work_item_id).where(
        memberships.c.run_key == normalized_run.value
    )
    current = current_stage_indexes(selected_schema, scoped_item_ids)
    statement = (
        select(
            memberships.c.member_ordinal,
            items.c.work_key,
            items.c.work_item_id,
            items.c.input_reference,
            executions.c.stage_key,
            executions.c.stage_index,
            executions.c.state,
        )
        .select_from(
            memberships.join(
                items,
                memberships.c.work_item_id == items.c.work_item_id,
            )
            .outerjoin(
                current,
                current.c.work_item_id == items.c.work_item_id,
            )
            .outerjoin(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .where(memberships.c.run_key == normalized_run.value)
        .order_by(memberships.c.member_ordinal)
        .limit(limit)
    )
    with engine.connect() as connection:
        require_run(
            connection,
            run_key=normalized_run,
            schema=selected_schema,
        )
        if cursor is not None:
            validate_run_member_cursor(
                connection,
                cursor=cursor,
                run_key=normalized_run,
                schema=selected_schema,
            )
            statement = statement.where(memberships.c.member_ordinal > cursor)
        return tuple(
            _decode_run_member_summary(row)
            for row in connection.execute(statement).mappings()
        )


def _decode_run_member_summary(row: RowMapping) -> RunMemberSummary:
    stage_key = row["stage_key"]
    stage_index = row["stage_index"]
    state = row["state"]
    return RunMemberSummary(
        member_ordinal=row["member_ordinal"],
        work_key=WorkKey(row["work_key"]),
        work_item_id=row["work_item_id"],
        input_reference=row["input_reference"],
        current_stage_key=(None if stage_key is None else StageKey(stage_key)),
        current_stage_index=stage_index,
        state=(None if state is None else StageExecutionState(state)),
    )
