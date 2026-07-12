"""Final P1 vocabulary and immutable persisted-model contracts."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

import dr_platform

EXPECTED_ENUM_VALUES: dict[str, frozenset[str]] = {
    "OperationStatus": frozenset(
        {
        "registering",
        "enqueuing",
        "running",
        "cancelling",
        "succeeded",
        "partial",
        "failed",
        "cancelled",
        }
    ),
    "ItemInsertStatus": frozenset({"inserted", "already_present"}),
    "AttemptEnqueueState": frozenset(
        {
        "pending",
        "claiming",
        "enqueued",
        "workflow_already_present",
        "enqueue_error",
        }
    ),
    "AttemptExecutionState": frozenset(
        {
        "not_started",
        "active",
        "succeeded",
        "error",
        "recovery_exhausted",
        "cancel_requested",
        "cancelled",
        "missing",
        }
    ),
    "RetryDisposition": frozenset(
        {"retryable", "permanent", "exhausted"}
    ),
    "ServiceClass": frozenset({"urgent", "standard", "backfill"}),
}

EXPECTED_INTERNAL_ENUM_VALUES: dict[str, frozenset[str]] = {
    "EnqueueClaimDisposition": frozenset(
        {
            "claimed",
            "call_started",
            "outcome_recorded",
            "expired",
            "replaced",
            "invalidated",
        }
    ),
    "EnqueueCompensationDisposition": frozenset(
        {
            "pending",
            "failed",
            "cancelled",
            "observed_terminal",
            "skipped_shared",
            "no_workflow_found",
        }
    ),
}

PERSISTED_RECORD_FIELDS: dict[str, frozenset[str]] = {
    "OperationRecord": frozenset(
        {
            "operation_key",
            "status",
            "manifest_digest",
            "operation_execution_recipe_digest",
            "target_key",
            "target_version",
            "target_contract_digest",
            "platform_cut_version",
            "retry_policy",
            "change_seq",
        }
    ),
    "ItemRecord": frozenset(
        {
            "item_id",
            "operation_key",
            "item_key",
            "item_index",
            "service_class",
            "service_priority",
            "shuffle_rank",
            "current_attempt",
            "change_seq",
        }
    ),
    "AttemptRecord": frozenset(
        {
            "item_id",
            "attempt",
            "workflow_id",
            "execution_key",
            "execution_recipe_digest",
            "enqueue_state",
            "execution_state",
            "current_claim_id",
            "change_seq",
        }
    ),
    "EnqueueClaimRecord": frozenset(
        {
            "item_id",
            "attempt",
            "claim_id",
            "workflow_id",
            "enqueue_try",
            "enqueue_call_started_at",
            "disposition",
            "change_seq",
        }
    ),
    "EnqueueCompensationRecord": frozenset(
        {
            "item_id",
            "attempt",
            "claim_id",
            "workflow_id",
            "reason",
            "cancel_disposition",
            "change_seq",
        }
    ),
}

LEGACY_ROOT_EXPORTS = frozenset(
    {
        "BatchItemEnqueueStatus",
        "BatchItemInsertStatus",
        "BatchItemRecord",
        "BatchItemStatuses",
        "BatchOperationCounts",
        "BatchOperationRecord",
        "BatchOperationStatus",
        "BatchSubmitResult",
        "EnqueueItem",
        "EnqueueOutcome",
        "InsertOutcome",
        "ItemIdentity",
        "PlatformNaming",
        "dedup_enqueue",
        "stamp_platform_schema",
        "submit_batch",
        "submit_batch_jsonl",
    }
)


def _required_export(name: str) -> Any:
    value = getattr(dr_platform, name, None)
    assert value is not None, f"dr_platform must export {name}"
    assert name in dr_platform.__all__, (
        f"{name} must be intentional public API"
    )
    return value


def test_final_lifecycle_enums_are_closed() -> None:
    for name, expected_values in EXPECTED_ENUM_VALUES.items():
        enum_type = _required_export(name)
        assert inspect.isclass(enum_type)
        assert issubclass(enum_type, StrEnum)
        assert frozenset(value.value for value in enum_type) == expected_values


def test_claim_and_compensation_dispositions_are_closed() -> None:
    from dr_platform import status

    for name, expected_values in EXPECTED_INTERNAL_ENUM_VALUES.items():
        enum_type = getattr(status, name)
        assert inspect.isclass(enum_type)
        assert issubclass(enum_type, StrEnum)
        assert frozenset(value.value for value in enum_type) == expected_values


def test_service_class_priorities_are_fixed() -> None:
    service_class = _required_export("ServiceClass")

    assert service_class.URGENT.priority == 100
    assert service_class.STANDARD.priority == 1_000
    assert service_class.BACKFILL.priority == 10_000


def test_persisted_record_models_are_frozen_and_strict() -> None:
    for name, required_fields in PERSISTED_RECORD_FIELDS.items():
        model_type = _required_export(name)
        assert inspect.isclass(model_type)
        assert issubclass(model_type, BaseModel)
        assert model_type.model_config.get("frozen") is True
        assert model_type.model_config.get("extra") == "forbid"
        assert required_fields <= model_type.model_fields.keys()


def test_item_and_attempt_models_have_no_callback_era_fields() -> None:
    item_type = _required_export("ItemRecord")
    attempt_type = _required_export("AttemptRecord")

    assert {
        "batch_submit_item_id",
        "order_key",
        "enqueue_status",
        "enqueue_metadata",
        "failure",
    }.isdisjoint(item_type.model_fields)
    assert "enqueue_metadata" not in attempt_type.model_fields


def test_platform_schema_constructor_has_only_prefix() -> None:
    schema_type = _required_export("PlatformSchema")
    parameters = tuple(inspect.signature(schema_type).parameters.values())

    assert tuple(parameter.name for parameter in parameters) == ("prefix",)
    assert parameters[0].default == "platform"
    assert parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


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
def test_claim_model_accepts_exact_call_started_shapes(
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
def test_claim_model_rejects_invalid_call_started_shapes(
    updates: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        _claim_record(**updates)


def test_legacy_root_exports_are_removed() -> None:
    exported = set(dr_platform.__all__)

    assert LEGACY_ROOT_EXPORTS.isdisjoint(exported)
    assert all("Batch" not in name for name in exported)
    for name in LEGACY_ROOT_EXPORTS:
        assert not hasattr(dr_platform, name)
