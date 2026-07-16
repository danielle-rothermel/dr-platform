"""Operation lifecycle aggregation over current Attempt state."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Connection, and_, select, update

from dr_platform.cancellation_truth import (
    operation_has_unresolved_cancellation,
)
from dr_platform.records import RetryPolicy
from dr_platform.status import (
    TERMINAL_EXECUTION_STATES,
    AttemptEnqueueState,
    AttemptExecutionState,
    OperationStatus,
)

if TYPE_CHECKING:
    from datetime import datetime

    from dr_platform.db import PlatformSchema


def refresh_operation_lifecycle(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    now: datetime,
) -> None:
    """Refresh aggregate Operation lifecycle fields from current Attempts."""
    rows = list(
        connection.execute(
            select(
                schema.item_attempts.c.enqueue_state,
                schema.item_attempts.c.enqueue_try,
                schema.item_attempts.c.execution_state,
                schema.item_attempts.c.failure,
                schema.item_attempts.c.cancellation_request_id,
                schema.item_attempts.c.cancellation_disposition,
            )
            .select_from(schema.items)
            .join(
                schema.item_attempts,
                and_(
                    schema.item_attempts.c.item_id == schema.items.c.item_id,
                    schema.item_attempts.c.attempt
                    == schema.items.c.current_attempt,
                ),
            )
            .where(schema.items.c.operation_key == operation_key)
        ).mappings()
    )
    execution_states = [
        AttemptExecutionState(row["execution_state"]) for row in rows
    ]
    enqueue_states = [
        AttemptEnqueueState(row["enqueue_state"]) for row in rows
    ]
    retry_policy = RetryPolicy.model_validate(
        connection.execute(
            select(schema.operations.c.retry_policy).where(
                schema.operations.c.operation_key == operation_key
            )
        ).scalar_one()
    )
    retryable_enqueue_errors = [
        enqueue_state is AttemptEnqueueState.ENQUEUE_ERROR
        and row["enqueue_try"] < retry_policy.max_enqueue_tries
        and row["failure"] is not None
        and row["failure"].get("failure_class")
        in retry_policy.retryable_failure_classes
        for enqueue_state, row in zip(enqueue_states, rows, strict=True)
    ]
    enqueued = enqueue_states.count(AttemptEnqueueState.ENQUEUED)
    workflow_already_present = enqueue_states.count(
        AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT
    )
    enqueue_failed = enqueue_states.count(AttemptEnqueueState.ENQUEUE_ERROR)
    succeeded = execution_states.count(AttemptExecutionState.SUCCEEDED)
    cancelled = execution_states.count(AttemptExecutionState.CANCELLED)
    terminal_failed = sum(
        execution_state
        in {
            AttemptExecutionState.ERROR,
            AttemptExecutionState.RECOVERY_EXHAUSTED,
            AttemptExecutionState.MISSING,
        }
        or (
            enqueue_state is AttemptEnqueueState.ENQUEUE_ERROR
            and not retryable_enqueue_error
        )
        for execution_state, enqueue_state, retryable_enqueue_error in zip(
            execution_states,
            enqueue_states,
            retryable_enqueue_errors,
            strict=True,
        )
    )
    active = sum(
        enqueue_state
        in {
            AttemptEnqueueState.ENQUEUED,
            AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT,
        }
        and execution_state not in TERMINAL_EXECUTION_STATES
        for execution_state, enqueue_state in zip(
            execution_states, enqueue_states, strict=True
        )
    )
    cancellation_incomplete = operation_has_unresolved_cancellation(
        connection, schema=schema, operation_key=operation_key
    )
    if cancellation_incomplete:
        status = OperationStatus.CANCELLING
    elif any(
        state in {AttemptEnqueueState.PENDING, AttemptEnqueueState.CLAIMING}
        and execution_state not in TERMINAL_EXECUTION_STATES
        for state, execution_state in zip(
            enqueue_states, execution_states, strict=True
        )
    ) or any(
        retryable and execution_state not in TERMINAL_EXECUTION_STATES
        for retryable, execution_state in zip(
            retryable_enqueue_errors, execution_states, strict=True
        )
    ):
        status = OperationStatus.ENQUEUEING
    elif active:
        status = OperationStatus.RUNNING
    elif succeeded == len(rows):
        status = OperationStatus.SUCCEEDED
    elif cancelled == len(rows):
        status = OperationStatus.CANCELLED
    elif succeeded:
        status = OperationStatus.PARTIAL
    else:
        status = OperationStatus.FAILED
    terminal = status in {
        OperationStatus.SUCCEEDED,
        OperationStatus.PARTIAL,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
    }
    connection.execute(
        update(schema.operations)
        .where(schema.operations.c.operation_key == operation_key)
        .values(
            status=status.value,
            platform_cut_version=schema.operations.c.platform_cut_version + 1,
            enqueued_count=enqueued,
            workflow_already_present_count=workflow_already_present,
            enqueue_failed_count=enqueue_failed,
            active_count=active,
            succeeded_count=succeeded,
            terminal_failed_count=terminal_failed,
            cancelled_count=cancelled,
            completed_at=now if terminal else None,
            updated_at=now,
        )
    )
