from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from dr_platform._core.identities import StageKey, normalize_key
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import (
    validate_nonnegative_integer,
    validate_positive_integer,
)

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


VALID_STAGE_TRANSITIONS = {
    StageExecutionState.READY: frozenset(
        {StageExecutionState.ADMITTED, StageExecutionState.CANCELLED}
    ),
    StageExecutionState.ADMITTED: frozenset(
        {
            StageExecutionState.SUCCEEDED,
            StageExecutionState.FAILED,
            StageExecutionState.CANCELLED,
        }
    ),
    StageExecutionState.FAILED: frozenset(
        {StageExecutionState.READY, StageExecutionState.CANCELLED}
    ),
    StageExecutionState.SUCCEEDED: frozenset(),
    StageExecutionState.CANCELLED: frozenset(),
}
_OUTPUT_REFERENCE_UNSET = object()


@dataclass(frozen=True, slots=True)
class StageExecutionRecord:
    stage_execution_id: int
    work_item_id: int
    stage_key: StageKey
    stage_index: int
    state: StageExecutionState
    current_attempt: int
    rank: int
    output_reference: str | None
    created_at: datetime
    updated_at: datetime


class StageExecutionConflictError(RuntimeError):
    pass


class StageTransitionError(RuntimeError):
    pass


def insert_stage_execution(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    work_item_id: int,
    stage_key: StageKey | str,
    stage_index: int,
    created_at: datetime,
    schema: StagingSchema | None = None,
) -> StageExecutionRecord:
    selected_schema = schema or StagingSchema()
    normalized_stage_key = normalize_key(stage_key, StageKey)
    validate_positive_integer(work_item_id, label="work item id")
    validate_nonnegative_integer(stage_index, label="stage index")

    work_items = selected_schema.work_items
    rank = connection.execute(
        select(work_items.c.rank).where(
            work_items.c.work_item_id == work_item_id
        )
    ).scalar_one_or_none()
    if rank is None:
        raise LookupError(f"work item does not exist: {work_item_id}")

    table = selected_schema.stage_executions
    row = (
        connection.execute(
            insert(table)
            .values(
                work_item_id=work_item_id,
                stage_key=normalized_stage_key.value,
                stage_index=stage_index,
                state=StageExecutionState.READY.value,
                current_attempt=0,
                rank=rank,
                output_reference=None,
                created_at=created_at,
                updated_at=created_at,
            )
            .on_conflict_do_nothing(
                index_elements=["work_item_id", "stage_key"]
            )
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        existing = _get_stage_execution_for_work(
            connection,
            work_item_id=work_item_id,
            stage_key=normalized_stage_key,
            schema=selected_schema,
        )
        if existing is None:
            # A concurrent insert is visible on read-back only under the
            # required READ COMMITTED isolation level.
            raise RuntimeError(
                "stage execution conflicted but no row was found on "
                f"read-back (work_item_id={work_item_id!r}, "
                f"stage_key={normalized_stage_key.value!r}); this requires "
                "READ COMMITTED isolation"
            )
        if existing.stage_index != stage_index or existing.rank != rank:
            raise StageExecutionConflictError(
                "work item stage is already bound to different immutable facts"
            )
        return existing
    return _decode_stage_execution(row)


def transition_stage_execution(  # noqa: PLR0913 -- explicit transition facts
    connection: Connection,
    *,
    stage_execution_id: int,
    new_state: StageExecutionState,
    updated_at: datetime,
    output_reference: str | object = _OUTPUT_REFERENCE_UNSET,
    schema: StagingSchema | None = None,
) -> StageExecutionRecord:
    """Only SUCCEEDED accepts an output reference; all others preserve it."""
    selected_schema = schema or StagingSchema()
    if not isinstance(new_state, StageExecutionState):
        raise TypeError("new state must be a StageExecutionState")
    if new_state is StageExecutionState.SUCCEEDED:
        if not isinstance(output_reference, str) or not output_reference:
            raise ValueError(
                "a SUCCEEDED transition requires a non-empty output reference"
            )
    elif output_reference is not _OUTPUT_REFERENCE_UNSET:
        raise ValueError(
            "output reference is only valid for a SUCCEEDED transition"
        )
    table = selected_schema.stage_executions
    row = (
        connection.execute(
            select(table)
            .where(table.c.stage_execution_id == stage_execution_id)
            .with_for_update(of=table)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            f"stage execution does not exist: {stage_execution_id}"
        )
    if updated_at < row["updated_at"]:
        raise ValueError("stage execution updated_at cannot move backwards")
    current = StageExecutionState(row["state"])
    if new_state not in VALID_STAGE_TRANSITIONS[current]:
        raise StageTransitionError(
            f"invalid stage transition: {current.name} -> {new_state.name}"
        )
    values: dict[str, object] = {
        "state": new_state.value,
        "updated_at": updated_at,
    }
    if new_state is StageExecutionState.SUCCEEDED:
        values["output_reference"] = output_reference
    updated_row = (
        connection.execute(
            update(table)
            .where(table.c.stage_execution_id == stage_execution_id)
            .values(**values)
            .returning(*table.c)
        )
        .mappings()
        .one()
    )
    return _decode_stage_execution(updated_row)


def get_stage_execution(
    connection: Connection,
    *,
    stage_execution_id: int,
    schema: StagingSchema | None = None,
) -> StageExecutionRecord | None:
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_executions
    row = (
        connection.execute(
            select(table).where(
                table.c.stage_execution_id == stage_execution_id
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_stage_execution(row)


def _get_stage_execution_for_work(
    connection: Connection,
    *,
    work_item_id: int,
    stage_key: StageKey | str,
    schema: StagingSchema | None = None,
) -> StageExecutionRecord | None:
    selected_schema = schema or StagingSchema()
    normalized_stage_key = normalize_key(stage_key, StageKey)
    table = selected_schema.stage_executions
    row = (
        connection.execute(
            select(table).where(
                table.c.work_item_id == work_item_id,
                table.c.stage_key == normalized_stage_key.value,
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_stage_execution(row)


def _decode_stage_execution(row: RowMapping) -> StageExecutionRecord:
    return StageExecutionRecord(
        stage_execution_id=row["stage_execution_id"],
        work_item_id=row["work_item_id"],
        stage_key=StageKey(row["stage_key"]),
        stage_index=row["stage_index"],
        state=StageExecutionState(row["state"]),
        current_attempt=row["current_attempt"],
        rank=row["rank"],
        output_reference=row["output_reference"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
