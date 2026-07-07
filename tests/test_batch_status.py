from __future__ import annotations

import pytest
from pydantic import BaseModel

from dr_platform import (
    BatchItemEnqueueStatus,
    BatchItemInsertStatus,
    BatchOperationStatus,
    batch_operation_counts,
    insert_outcome_from_rowcount,
    is_terminal_enqueue_status,
    operation_status_from_counts,
    terminal_enqueue_total_from_counts,
)


class ItemStatuses(BaseModel):
    insert_status: BatchItemInsertStatus
    enqueue_status: BatchItemEnqueueStatus


def test_insert_outcome_from_rowcount() -> None:
    assert insert_outcome_from_rowcount(1).value == "inserted"
    assert insert_outcome_from_rowcount(0).value == "already_present"
    with pytest.raises(ValueError, match="unexpected insert rowcount"):
        insert_outcome_from_rowcount(2)


def test_batch_operation_counts_from_status_views() -> None:
    items = (
        ItemStatuses(
            insert_status=BatchItemInsertStatus.INSERTED,
            enqueue_status=BatchItemEnqueueStatus.ENQUEUED,
        ),
        ItemStatuses(
            insert_status=BatchItemInsertStatus.ALREADY_PRESENT,
            enqueue_status=BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT,
        ),
        ItemStatuses(
            insert_status=BatchItemInsertStatus.INSERTED,
            enqueue_status=BatchItemEnqueueStatus.FAILED,
        ),
    )

    counts = batch_operation_counts(items)

    assert counts.inserted_count == 2
    assert counts.already_present_count == 1
    assert counts.enqueued_count == 1
    assert counts.already_scheduled_count == 1
    assert counts.failed_count == 1
    assert terminal_enqueue_total_from_counts(counts) == 3


def test_operation_status_all_already_scheduled_is_completed() -> None:
    status = operation_status_from_counts(
        requested_count=2,
        enqueued_count=0,
        already_scheduled_count=2,
        failed_count=0,
    )
    assert status is BatchOperationStatus.COMPLETED


def test_operation_status_partial_with_mixed_outcomes() -> None:
    status = operation_status_from_counts(
        requested_count=3,
        enqueued_count=1,
        already_scheduled_count=1,
        failed_count=1,
    )
    assert status is BatchOperationStatus.PARTIAL


def test_operation_status_incomplete_enqueue_is_enqueuing() -> None:
    status = operation_status_from_counts(
        requested_count=3,
        enqueued_count=1,
        already_scheduled_count=0,
        failed_count=0,
    )
    assert status is BatchOperationStatus.ENQUEUING


def test_operation_status_all_failed_is_error() -> None:
    status = operation_status_from_counts(
        requested_count=2,
        enqueued_count=0,
        already_scheduled_count=0,
        failed_count=2,
    )
    assert status is BatchOperationStatus.ERROR


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (BatchItemEnqueueStatus.ENQUEUED, True),
        (BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT, True),
        (BatchItemEnqueueStatus.FAILED, True),
        (BatchItemEnqueueStatus.PENDING, False),
        (BatchItemEnqueueStatus.CLAIMING, False),
    ],
)
def test_is_terminal_enqueue_status(
    status: BatchItemEnqueueStatus,
    expected: bool,  # noqa: FBT001 -- parametrized expectation
) -> None:
    assert is_terminal_enqueue_status(status) is expected


def test_status_string_values_are_frozen() -> None:
    # Persisted row content for adopters with existing data.
    assert [status.value for status in BatchOperationStatus] == [
        "enqueuing",
        "completed",
        "partial",
        "error",
    ]
    assert [status.value for status in BatchItemEnqueueStatus] == [
        "pending",
        "claiming",
        "enqueued",
        "workflow_already_present",
        "failed",
    ]
    assert [status.value for status in BatchItemInsertStatus] == [
        "inserted",
        "already_present",
    ]
