"""Claim record lifecycle validation behavior."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

import dr_platform


def _claim_record(**updates: Any) -> Any:
    claimed_at = datetime(2026, 1, 1, tzinfo=UTC)
    values = {
        "item_id": "item",
        "attempt": 0,
        "claim_id": "claim",
        "workflow_id": "workflow",
        "enqueue_try": 1,
        "claimed_at": claimed_at,
        "lease_expires_at": claimed_at + timedelta(minutes=1),
        "disposition": "claimed",
        "created_at": claimed_at,
        "change_seq": 1,
    }
    values.update(updates)
    return dr_platform.EnqueueClaimRecord.model_validate(values)


@pytest.mark.parametrize(
    "updates",
    [
        {
            "disposition": "call_started",
            "enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
        {
            "disposition": "outcome_recorded",
            "enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC),
            "resolved_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
        {
            "disposition": "invalidated",
            "enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC),
            "invalidated_at": datetime(2026, 1, 1, tzinfo=UTC),
            "invalidated_by": "operator",
            "resolved_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
        {
            "disposition": "expired",
            "enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC),
            "resolved_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
        {
            "disposition": "replaced",
            "enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC),
            "replacement_claim_id": "replacement",
            "resolved_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
    ],
)
def test_claim_model_accepts_consistent_call_started_fields(
    updates: dict[str, Any],
) -> None:
    assert _claim_record(**updates).enqueue_call_started_at is not None


@pytest.mark.parametrize(
    "updates",
    [
        {"enqueue_call_started_at": datetime(2026, 1, 1, tzinfo=UTC)},
        {"disposition": "call_started"},
        {
            "disposition": "outcome_recorded",
            "resolved_at": datetime(2026, 1, 1, tzinfo=UTC),
        },
    ],
)
def test_claim_model_rejects_inconsistent_call_started_fields(
    updates: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        _claim_record(**updates)
