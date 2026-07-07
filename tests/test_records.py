from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest
from pydantic import ValidationError

from dr_platform import (
    BatchItemEnqueueStatus,
    BatchItemInsertStatus,
    BatchItemRecord,
    BatchOperationRecord,
    BatchOperationStatus,
    EnqueueFailure,
    batch_operation_counts,
)
from dr_platform import records as records_module

NOW = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)


def _item(
    *,
    item_index: int,
    insert_status: BatchItemInsertStatus,
    enqueue_status: BatchItemEnqueueStatus,
) -> BatchItemRecord:
    failure = None
    if enqueue_status is BatchItemEnqueueStatus.FAILED:
        failure = EnqueueFailure(
            error_type="builtins.RuntimeError",
            message="enqueue failed",
        )
    return BatchItemRecord(
        batch_submit_item_id=f"item-{item_index}",
        operation_key="op-1",
        item_index=item_index,
        item_id=f"work-{item_index}",
        order_key=f"fair-{item_index}",
        insert_status=insert_status,
        enqueue_status=enqueue_status,
        created_at=NOW,
        failure=failure,
    )


def test_item_records_feed_batch_operation_counts() -> None:
    items = (
        _item(
            item_index=0,
            insert_status=BatchItemInsertStatus.INSERTED,
            enqueue_status=BatchItemEnqueueStatus.ENQUEUED,
        ),
        _item(
            item_index=1,
            insert_status=BatchItemInsertStatus.ALREADY_PRESENT,
            enqueue_status=BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT,
        ),
        _item(
            item_index=2,
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


def test_completed_operation_requires_full_enqueue_accounting() -> None:
    with pytest.raises(ValidationError, match="every requested item"):
        BatchOperationRecord(
            operation_key="op-1",
            group_key="exp",
            status=BatchOperationStatus.COMPLETED,
            requested_count=2,
            inserted_count=2,
            enqueued_count=1,
            failed_count=0,
            created_at=NOW,
            completed_at=NOW,
        )


def test_completed_operation_allows_already_scheduled_accounting() -> None:
    operation = BatchOperationRecord(
        operation_key="op-1",
        group_key="exp",
        status=BatchOperationStatus.COMPLETED,
        requested_count=2,
        inserted_count=2,
        enqueued_count=0,
        already_scheduled_count=2,
        failed_count=0,
        created_at=NOW,
        completed_at=NOW,
    )
    assert operation.already_scheduled_count == 2


def test_terminal_operation_requires_completed_at() -> None:
    with pytest.raises(ValidationError, match="completed_at"):
        BatchOperationRecord(
            operation_key="op-1",
            group_key="exp",
            status=BatchOperationStatus.PARTIAL,
            requested_count=1,
            failed_count=1,
            created_at=NOW,
        )


def _operation(**overrides: object) -> BatchOperationRecord:
    payload = {
        "operation_key": "op-1",
        "group_key": "exp",
        "status": BatchOperationStatus.ENQUEUING,
        "requested_count": 2,
        "created_at": NOW,
    }
    payload.update(overrides)
    return BatchOperationRecord(**cast("Any", payload))


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"requested_count": -1}, "non-negative"),
        ({"enqueued_count": 3}, "cannot exceed requested_count"),
        (
            {"inserted_count": 2, "already_present_count": 1},
            "already_present_count cannot exceed",
        ),
        (
            {
                "enqueued_count": 2,
                "already_scheduled_count": 1,
                "failed_count": 1,
            },
            "failed_count cannot exceed",
        ),
        (
            {
                "status": BatchOperationStatus.COMPLETED,
                "enqueued_count": 2,
                "completed_at": NOW,
                "created_at": NOW.replace(year=NOW.year + 1),
            },
            "completed_at must not precede created_at",
        ),
    ],
)
def test_operation_rejects_invalid_counts(
    overrides: dict[str, object],
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        _operation(**overrides)


def test_operation_rejects_oversized_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(records_module, "BATCH_SPEC_MAX_BYTES", 1024)
    with pytest.raises(ValidationError, match="batch submit spec"):
        _operation(spec={"x": "y" * (records_module.BATCH_SPEC_MAX_BYTES + 1)})


def _batch_item(**overrides: object) -> BatchItemRecord:
    payload = {
        "batch_submit_item_id": "item-1",
        "operation_key": "op-1",
        "item_index": 0,
        "item_id": "work-1",
        "order_key": "abc",
        "insert_status": BatchItemInsertStatus.INSERTED,
        "enqueue_status": BatchItemEnqueueStatus.PENDING,
        "created_at": NOW,
    }
    payload.update(overrides)
    return BatchItemRecord(**cast("Any", payload))


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"item_index": -1}, "item_index must be non-negative"),
        (
            {
                "enqueue_status": BatchItemEnqueueStatus.FAILED,
                "failure": None,
            },
            "failed batch submit items require failure",
        ),
        (
            {
                "enqueue_metadata": {"workflow_id": "w"},
            },
            "pending batch submit items require empty enqueue_metadata",
        ),
        (
            {
                "enqueue_status": BatchItemEnqueueStatus.CLAIMING,
                "enqueue_metadata": {"enqueue_claim_id": "claim-1"},
            },
            "require claimed_at",
        ),
        (
            {
                "enqueue_status": BatchItemEnqueueStatus.CLAIMING,
                "enqueue_metadata": {
                    "enqueue_claim_id": "",
                    "claimed_at": NOW.isoformat(),
                },
            },
            "require enqueue_claim_id",
        ),
    ],
)
def test_batch_item_rejects_invalid_shape(
    overrides: dict[str, object],
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        _batch_item(**overrides)
