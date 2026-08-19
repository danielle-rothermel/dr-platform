"""Canonical work-item status derived from all stage executions.

Precedence (highest wins): FAILED > CANCELLED > ADMITTED > READY > SUCCEEDED.

The representative execution for an item is among rows in the selected state:
lowest ``stage_index`` for non-SUCCEEDED states (first failure or frontier),
highest ``stage_index`` when every execution succeeded (final output).
Tie-break ``stage_execution_id``.

Under a linear pipeline, prior rows are all SUCCEEDED, so precedence selects
the max-index row and behavior matches the former ``max(stage_index)`` query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Select, case, exists, select
from sqlalchemy.sql import ColumnElement  # noqa: TC002
from sqlalchemy.sql.selectable import Subquery  # noqa: TC002

from dr_platform._core.ledger.schema import LedgerSchema  # noqa: TC001
from dr_platform._core.ledger.states import StageExecutionState

if TYPE_CHECKING:
    from sqlalchemy.sql.schema import Table


def _state_precedence(
    state_column: ColumnElement[object],
) -> ColumnElement[int]:
    return case(
        (state_column == StageExecutionState.FAILED.value, 0),
        (state_column == StageExecutionState.CANCELLED.value, 1),
        (state_column == StageExecutionState.ADMITTED.value, 2),
        (state_column == StageExecutionState.READY.value, 3),
        else_=4,
    )


def _representative_index_order(
    state_column: ColumnElement[object],
    stage_index_column: ColumnElement[int],
) -> ColumnElement[int]:
    return case(
        (
            state_column == StageExecutionState.SUCCEEDED.value,
            -stage_index_column,
        ),
        else_=stage_index_column,
    )


def work_item_status_rows(
    schema: LedgerSchema,
    work_item_ids: Select,
) -> Subquery:
    """One derived status row per scoped work item."""
    executions = schema.stage_executions
    scoped_ids = work_item_ids.subquery()
    precedence = _state_precedence(executions.c.state)
    index_order = _representative_index_order(
        executions.c.state,
        executions.c.stage_index,
    )
    return (
        select(
            executions.c.work_item_id,
            executions.c.state,
            executions.c.stage_execution_id,
            executions.c.stage_key,
            executions.c.stage_index,
        )
        .select_from(
            executions.join(
                scoped_ids,
                scoped_ids.c.work_item_id == executions.c.work_item_id,
            )
        )
        .distinct(executions.c.work_item_id)
        .order_by(
            executions.c.work_item_id,
            precedence,
            index_order,
            executions.c.stage_execution_id,
        )
        .subquery()
    )


def work_item_status_rows_by_run(
    schema: LedgerSchema,
    run_keys: tuple[str, ...],
) -> Subquery:
    """Derived status per (run_key, work_item_id) for named runs."""
    if not run_keys:
        raise ValueError("run_keys must be non-empty")
    memberships = schema.run_memberships
    scoped = (
        select(memberships.c.run_key, memberships.c.work_item_id)
        .where(memberships.c.run_key.in_(run_keys))
        .subquery()
    )
    status = work_item_status_rows(
        schema,
        select(scoped.c.work_item_id).distinct(),
    )
    return (
        select(
            scoped.c.run_key,
            status.c.work_item_id,
            status.c.state,
            status.c.stage_execution_id,
            status.c.stage_key,
            status.c.stage_index,
        )
        .select_from(
            scoped.join(
                status,
                status.c.work_item_id == scoped.c.work_item_id,
            )
        )
        .subquery()
    )


def lower_stage_not_succeeded_exists(
    schema: LedgerSchema,
    *,
    work_item_id: int,
    below_stage_index: int,
) -> Select:
    """True when any lower-index sibling for the item is not SUCCEEDED."""
    executions = schema.stage_executions
    return select(
        exists(
            select(1)
            .select_from(executions)
            .where(
                executions.c.work_item_id == work_item_id,
                executions.c.stage_index < below_stage_index,
                executions.c.state != StageExecutionState.SUCCEEDED.value,
            )
        )
    )


def _apply_stage_execution_filters(  # noqa: PLR0913 -- explicit SQL filters
    statement: Select,
    executions_table: Table,
    *,
    work_item_id: int,
    stage_key: str | None = None,
    min_stage_index: int | None = None,
    max_stage_index: int | None = None,
    state: StageExecutionState | None = None,
) -> Select:
    columns = executions_table.c
    filtered = statement.where(columns.work_item_id == work_item_id)
    if stage_key is not None:
        filtered = filtered.where(columns.stage_key == stage_key)
    if min_stage_index is not None:
        filtered = filtered.where(columns.stage_index > min_stage_index)
    if max_stage_index is not None:
        filtered = filtered.where(columns.stage_index < max_stage_index)
    if state is not None:
        filtered = filtered.where(columns.state == state.value)
    return filtered


def list_predecessor_stage_outputs_statement(  # noqa: PLR0913 -- explicit SQL filters
    schema: LedgerSchema,
    *,
    work_item_id: int,
    below_stage_index: int,
    stage_key: str | None = None,
    min_stage_index: int | None = None,
    max_stage_index: int | None = None,
) -> Select:
    """Succeeded lower-index executions with output references."""
    executions = schema.stage_executions
    effective_max = (
        max_stage_index if max_stage_index is not None else below_stage_index
    )
    statement = select(
        executions.c.stage_key,
        executions.c.stage_index,
        executions.c.input_reference,
        executions.c.output_reference,
    )
    statement = _apply_stage_execution_filters(
        statement,
        executions,
        work_item_id=work_item_id,
        stage_key=stage_key,
        min_stage_index=min_stage_index,
        max_stage_index=effective_max,
        state=StageExecutionState.SUCCEEDED,
    )
    return statement.where(
        executions.c.output_reference.is_not(None)
    ).order_by(executions.c.stage_index)


def list_stage_executions_statement(  # noqa: PLR0913 -- explicit SQL filters
    schema: LedgerSchema,
    *,
    work_item_id: int,
    stage_key: str | None = None,
    min_stage_index: int | None = None,
    max_stage_index: int | None = None,
    state: StageExecutionState | None = None,
) -> Select:
    """Stage execution rows for one work item with optional filters."""
    executions = schema.stage_executions
    statement = select(executions)
    statement = _apply_stage_execution_filters(
        statement,
        executions,
        work_item_id=work_item_id,
        stage_key=stage_key,
        min_stage_index=min_stage_index,
        max_stage_index=max_stage_index,
        state=state,
    )
    return statement.order_by(
        executions.c.stage_index,
        executions.c.stage_execution_id,
    )


__all__ = [
    "list_predecessor_stage_outputs_statement",
    "list_stage_executions_statement",
    "lower_stage_not_succeeded_exists",
    "work_item_status_rows",
    "work_item_status_rows_by_run",
]
