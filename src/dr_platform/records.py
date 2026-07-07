"""Typed row records for the library-owned batch tables.

Field names are neutral (``item_id`` / ``order_key`` / ``group_key``);
the SQL layer maps them onto the physical column names configured by
``PlatformNaming``. Validators mirror the table check constraints so
bad rows fail before SQL does.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dr_providers.kernel.failures import (
    FailureClass,  # noqa: TC002 -- pydantic runtime type
)
from dr_serialize import (
    POSTGRES_JSONB_PAYLOAD_MAX_BYTES,
    SerializationError,
    postgres_jsonb_limits,
    to_jsonable,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)

from dr_platform.batch_status import (
    TERMINAL_OPERATION_STATUSES,
    BatchItemEnqueueStatus,
    BatchItemInsertStatus,
    BatchOperationStatus,
)

BATCH_SPEC_MAX_BYTES = POSTGRES_JSONB_PAYLOAD_MAX_BYTES

# Persisted JSONB keys inside enqueue_metadata — frozen row content.
ENQUEUE_CLAIM_ID_METADATA_KEY = "enqueue_claim_id"
ENQUEUE_CLAIMED_AT_METADATA_KEY = "claimed_at"
WORKFLOW_ID_METADATA_KEY = "workflow_id"


def _validate_payload_size(value: Any, *, max_bytes: int, label: str) -> None:
    try:
        to_jsonable(value, limits=postgres_jsonb_limits(max_bytes))
    except SerializationError as exc:
        raise ValueError(f"{label}: {exc}") from exc


class EnqueueFailure(BaseModel):
    """Failure snapshot persisted on FAILED batch items.

    Same JSONB shape as whetstone's FailureMetadataPayload so existing
    rows read back cleanly.
    """

    model_config = ConfigDict(extra="forbid")

    failure_class: FailureClass | None = None
    error_type: StrictStr
    underlying_exception_type: StrictStr | None = None
    message: StrictStr
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)


class BatchOperationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_key: StrictStr
    group_key: StrictStr
    status: BatchOperationStatus
    requested_count: StrictInt
    inserted_count: StrictInt = 0
    already_present_count: StrictInt = 0
    enqueued_count: StrictInt = 0
    already_scheduled_count: StrictInt = 0
    failed_count: StrictInt = 0
    spec: dict[StrictStr, Any] = Field(default_factory=dict)
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    @model_validator(mode="after")
    def validate_counts(self) -> BatchOperationRecord:
        counts = (
            self.requested_count,
            self.inserted_count,
            self.already_present_count,
            self.enqueued_count,
            self.already_scheduled_count,
            self.failed_count,
        )
        if any(count < 0 for count in counts):
            raise ValueError("batch submit counts must be non-negative")
        if any(count > self.requested_count for count in counts[1:]):
            raise ValueError(
                "batch submit counts cannot exceed requested_count"
            )
        if (
            self.inserted_count + self.already_present_count
            > self.requested_count
        ):
            raise ValueError(
                "inserted_count + already_present_count cannot exceed "
                "requested_count"
            )
        terminal_enqueue = (
            self.enqueued_count
            + self.already_scheduled_count
            + self.failed_count
        )
        if terminal_enqueue > self.requested_count:
            raise ValueError(
                "enqueued_count + already_scheduled_count + failed_count "
                "cannot exceed requested_count"
            )
        _validate_payload_size(
            self.spec,
            max_bytes=BATCH_SPEC_MAX_BYTES,
            label="batch submit spec",
        )
        if (
            self.status in TERMINAL_OPERATION_STATUSES
            and self.completed_at is None
        ):
            raise ValueError(
                "terminal batch submit operations require completed_at"
            )
        if (
            self.status is BatchOperationStatus.COMPLETED
            and terminal_enqueue != self.requested_count
        ):
            raise ValueError(
                "completed batch submit operations must account for "
                "every requested item in enqueued_count, "
                "already_scheduled_count, or failed_count"
            )
        if (
            self.completed_at is not None
            and self.completed_at < self.created_at
        ):
            raise ValueError("completed_at must not precede created_at")
        return self


class BatchItemRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_submit_item_id: StrictStr
    operation_key: StrictStr
    item_index: StrictInt
    item_id: StrictStr
    order_key: StrictStr
    insert_status: BatchItemInsertStatus
    enqueue_status: BatchItemEnqueueStatus
    enqueue_metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    failure: EnqueueFailure | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_item_shape(self) -> BatchItemRecord:
        if self.item_index < 0:
            raise ValueError("item_index must be non-negative")
        if (
            self.enqueue_status is BatchItemEnqueueStatus.FAILED
            and self.failure is None
        ):
            raise ValueError("failed batch submit items require failure")
        if (
            self.enqueue_status is BatchItemEnqueueStatus.PENDING
            and self.enqueue_metadata
        ):
            raise ValueError(
                "pending batch submit items require empty enqueue_metadata"
            )
        if self.enqueue_status is BatchItemEnqueueStatus.CLAIMING:
            claim_id = self.enqueue_metadata.get(ENQUEUE_CLAIM_ID_METADATA_KEY)
            claimed_at = self.enqueue_metadata.get(
                ENQUEUE_CLAIMED_AT_METADATA_KEY
            )
            if not isinstance(claim_id, str) or not claim_id:
                raise ValueError(
                    "claiming batch submit items require enqueue_claim_id"
                )
            if not isinstance(claimed_at, str) or not claimed_at:
                raise ValueError(
                    "claiming batch submit items require claimed_at"
                )
        return self
