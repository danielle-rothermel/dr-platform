from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dr_platform._core.clock import utc_now
from dr_platform._core.ledger.attempts import (
    StageAttemptRecord,
    append_stage_attempt,
    get_stage_attempt,
)
from dr_platform._core.ledger.executions import (
    StageExecutionRecord,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from sqlalchemy import Engine


@dataclass(frozen=True, slots=True)
class StageRetryResult:
    stage_execution: StageExecutionRecord
    new_attempt: StageAttemptRecord


def retry_stage(
    stage_execution_id: int,
    *,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: StagingSchema | None = None,
) -> StageRetryResult:
    """Only FAILED stages may prepare a new attempt for later admission."""
    selected_schema = schema or StagingSchema()
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
        retried_at = clock()
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
