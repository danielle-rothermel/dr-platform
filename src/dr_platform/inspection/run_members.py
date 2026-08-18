from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import and_, select

from dr_platform._core.frozen import immutable_json_mapping
from dr_platform._core.identities import (
    RunKey,
    StageKey,
    WorkKey,
    normalize_key,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.work_item_status import work_item_status_rows
from dr_platform.inspection._validation import (
    DEFAULT_INSPECTION_LIMIT,
    require_run,
    validate_limit,
    validate_run_member_cursor,
)
from dr_platform.inspection.terminal_filters import (
    TerminalSummaryFilter,
    apply_terminal_summary_filter,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

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
    stage_execution_id: int | None = None
    terminal_summary: Mapping[str, object] | None = None
    evidence_reference: str | None = None


def list_run_members(  # noqa: PLR0913 -- explicit reader filters
    run_key: RunKey | str,
    *,
    engine: Engine,
    cursor: int | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    terminal_filter: TerminalSummaryFilter | None = None,
    schema: LedgerSchema | None = None,
) -> tuple[RunMemberSummary, ...]:
    validate_limit(limit)
    selected_schema = schema or LedgerSchema()
    normalized_run = normalize_key(run_key, RunKey)
    memberships = selected_schema.run_memberships
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    attempts = selected_schema.stage_attempts
    scoped_item_ids = select(memberships.c.work_item_id).where(
        memberships.c.run_key == normalized_run.value
    )
    status = work_item_status_rows(selected_schema, scoped_item_ids)
    if terminal_filter is None:
        statement = (
            select(
                memberships.c.member_ordinal,
                items.c.work_key,
                items.c.work_item_id,
                items.c.input_reference,
                status.c.stage_key,
                status.c.stage_index,
                status.c.state,
            )
            .select_from(
                memberships.join(
                    items,
                    memberships.c.work_item_id == items.c.work_item_id,
                ).outerjoin(
                    status,
                    status.c.work_item_id == items.c.work_item_id,
                )
            )
            .where(memberships.c.run_key == normalized_run.value)
            .order_by(memberships.c.member_ordinal)
            .limit(limit)
        )
    else:
        statement = (
            select(
                memberships.c.member_ordinal,
                items.c.work_key,
                items.c.work_item_id,
                items.c.input_reference,
                status.c.stage_key,
                status.c.stage_index,
                status.c.state,
                status.c.stage_execution_id,
                attempts.c.terminal_summary,
                attempts.c.evidence_reference,
            )
            .select_from(
                memberships.join(
                    items,
                    memberships.c.work_item_id == items.c.work_item_id,
                )
                .join(
                    status,
                    status.c.work_item_id == items.c.work_item_id,
                )
                .join(
                    executions,
                    executions.c.stage_execution_id
                    == status.c.stage_execution_id,
                )
                .outerjoin(
                    attempts,
                    and_(
                        attempts.c.stage_execution_id
                        == executions.c.stage_execution_id,
                        attempts.c.attempt_number
                        == executions.c.current_attempt,
                    ),
                )
            )
            .where(memberships.c.run_key == normalized_run.value)
            .order_by(memberships.c.member_ordinal)
            .limit(limit)
        )
        statement = apply_terminal_summary_filter(
            statement,
            terminal_filter=terminal_filter,
            schema=selected_schema,
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
            _decode_run_member_summary(
                row, filtered=terminal_filter is not None
            )
            for row in connection.execute(statement).mappings()
        )


def _decode_run_member_summary(
    row: RowMapping,
    *,
    filtered: bool,
) -> RunMemberSummary:
    stage_key = row["stage_key"]
    stage_index = row["stage_index"]
    state = row["state"]
    summary = row["terminal_summary"] if filtered else None
    return RunMemberSummary(
        member_ordinal=row["member_ordinal"],
        work_key=WorkKey(row["work_key"]),
        work_item_id=row["work_item_id"],
        input_reference=row["input_reference"],
        current_stage_key=(None if stage_key is None else StageKey(stage_key)),
        current_stage_index=stage_index,
        state=(None if state is None else StageExecutionState(state)),
        stage_execution_id=(
            None if not filtered else row["stage_execution_id"]
        ),
        terminal_summary=(
            None
            if summary is None
            else immutable_json_mapping(cast("Mapping[str, object]", summary))
        ),
        evidence_reference=(
            None if not filtered else row["evidence_reference"]
        ),
    )
