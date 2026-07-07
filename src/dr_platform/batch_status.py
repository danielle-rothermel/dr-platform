"""The batch submission status state machine — pure, no I/O.

Operation lifecycle: ENQUEUING until every item reaches a terminal
enqueue status, then COMPLETED / PARTIAL / ERROR by failure counts.
Item lifecycle: PENDING -> CLAIMING -> one of ENQUEUED /
WORKFLOW_ALREADY_PRESENT / FAILED; recovery resets FAILED and stale
CLAIMING items to PENDING.

The status string values are persisted row content for adopters with
existing data (whetstone) — frozen.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, StrictInt

if TYPE_CHECKING:
    from collections.abc import Iterable


class BatchOperationStatus(StrEnum):
    ENQUEUING = "enqueuing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    ERROR = "error"


TERMINAL_OPERATION_STATUSES = frozenset(
    {
        BatchOperationStatus.COMPLETED,
        BatchOperationStatus.PARTIAL,
        BatchOperationStatus.ERROR,
    }
)


class BatchItemInsertStatus(StrEnum):
    INSERTED = "inserted"
    ALREADY_PRESENT = "already_present"


class BatchItemEnqueueStatus(StrEnum):
    PENDING = "pending"
    CLAIMING = "claiming"
    ENQUEUED = "enqueued"
    WORKFLOW_ALREADY_PRESENT = "workflow_already_present"
    FAILED = "failed"


TERMINAL_ENQUEUE_STATUSES = frozenset(
    {
        BatchItemEnqueueStatus.ENQUEUED,
        BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT,
        BatchItemEnqueueStatus.FAILED,
    }
)


def is_terminal_enqueue_status(status: BatchItemEnqueueStatus) -> bool:
    return status in TERMINAL_ENQUEUE_STATUSES


class InsertOutcome(StrEnum):
    INSERTED = "inserted"
    ALREADY_PRESENT = "already_present"


def insert_outcome_from_rowcount(rowcount: int) -> InsertOutcome:
    if rowcount == 1:
        return InsertOutcome.INSERTED
    if rowcount == 0:
        return InsertOutcome.ALREADY_PRESENT
    raise ValueError(f"unexpected insert rowcount: {rowcount}")


class BatchOperationCounts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inserted_count: StrictInt
    already_present_count: StrictInt
    enqueued_count: StrictInt
    already_scheduled_count: StrictInt
    failed_count: StrictInt


def terminal_enqueue_total(
    *,
    enqueued_count: int,
    already_scheduled_count: int,
    failed_count: int,
) -> int:
    return enqueued_count + already_scheduled_count + failed_count


def terminal_enqueue_total_from_counts(counts: BatchOperationCounts) -> int:
    return terminal_enqueue_total(
        enqueued_count=counts.enqueued_count,
        already_scheduled_count=counts.already_scheduled_count,
        failed_count=counts.failed_count,
    )


def operation_status_from_counts(
    *,
    requested_count: int,
    enqueued_count: int,
    already_scheduled_count: int,
    failed_count: int,
) -> BatchOperationStatus:
    terminal_total = terminal_enqueue_total(
        enqueued_count=enqueued_count,
        already_scheduled_count=already_scheduled_count,
        failed_count=failed_count,
    )
    if terminal_total < requested_count:
        return BatchOperationStatus.ENQUEUING
    if failed_count >= requested_count:
        return BatchOperationStatus.ERROR
    if failed_count > 0:
        return BatchOperationStatus.PARTIAL
    return BatchOperationStatus.COMPLETED


@runtime_checkable
class BatchItemStatuses(Protocol):
    """Structural view of an item row's two status columns."""

    @property
    def insert_status(self) -> BatchItemInsertStatus: ...

    @property
    def enqueue_status(self) -> BatchItemEnqueueStatus: ...


def batch_operation_counts(
    items: Iterable[BatchItemStatuses],
) -> BatchOperationCounts:
    materialized = tuple(items)
    return BatchOperationCounts(
        inserted_count=sum(
            item.insert_status is BatchItemInsertStatus.INSERTED
            for item in materialized
        ),
        already_present_count=sum(
            item.insert_status is BatchItemInsertStatus.ALREADY_PRESENT
            for item in materialized
        ),
        enqueued_count=sum(
            item.enqueue_status is BatchItemEnqueueStatus.ENQUEUED
            for item in materialized
        ),
        already_scheduled_count=sum(
            item.enqueue_status
            is BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT
            for item in materialized
        ),
        failed_count=sum(
            item.enqueue_status is BatchItemEnqueueStatus.FAILED
            for item in materialized
        ),
    )
