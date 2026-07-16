"""Frozen persistence models for the platform kernel lifecycle."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 -- pydantic resolves at runtime
from typing import Annotated, Any

from dr_serialize import (
    POSTGRES_JSONB_PAYLOAD_MAX_BYTES,
    SerializationError,
    Serializer,
    postgres_jsonb_limits,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_serializer,
    model_validator,
)

from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    AttemptRetryReason,
    CancellationDisposition,
    CancellationOrigin,
    EnqueueClaimDisposition,
    EnqueueCompensationDisposition,
    EnqueueCompensationReason,
    FailureClass,
    ItemInsertStatus,
    NextAttemptDisposition,
    NextAttemptReason,
    OperationStatus,
    PrioritySource,
    RetryDisposition,
    ServiceClass,
)

JSONB_PAYLOAD_MAX_BYTES = POSTGRES_JSONB_PAYLOAD_MAX_BYTES
NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
PositiveChangeSeq = Annotated[StrictInt, Field(gt=0)]


def validate_payload_size(value: Any, *, label: str) -> None:
    try:
        Serializer(
            limits=postgres_jsonb_limits(JSONB_PAYLOAD_MAX_BYTES)
        ).to_jsonable(value)
    except SerializationError as exc:
        raise ValueError(f"{label}: {exc}") from exc


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_attempts: PositiveInt = 3
    max_enqueue_tries: PositiveInt = 3
    retryable_failure_classes: frozenset[FailureClass] = Field(
        default_factory=lambda: frozenset(
            {FailureClass.TRANSIENT, FailureClass.RATE_LIMITED}
        )
    )

    @field_serializer("retryable_failure_classes")
    def serialize_retryable_failure_classes(
        self, value: frozenset[FailureClass]
    ) -> list[str]:
        return sorted(failure_class.value for failure_class in value)


class FailureSnapshot(BaseModel):
    """Safe persistence facts, never raw application payloads."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    failure_class: FailureClass
    error_type: NonEmptyStr
    underlying_exception_type: NonEmptyStr | None = None
    message: StrictStr
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_metadata(self) -> FailureSnapshot:
        validate_payload_size(self.metadata, label="failure metadata")
        return self


class OperationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    group_key: NonEmptyStr
    workflow_role: NonEmptyStr
    status: OperationStatus
    requested_count: NonNegativeInt
    registration_page_size: PositiveInt
    registration_page_count: NonNegativeInt
    target_key: NonEmptyStr
    target_version: PositiveInt
    target_contract_digest: NonEmptyStr
    platform_cut_version: PositiveInt
    registration_cursor: NonNegativeInt = 0
    registration_lease_id: NonEmptyStr | None = None
    registration_lease_expires_at: datetime | None = None
    registration_abandoned_at: datetime | None = None
    registration_abandoned_by: NonEmptyStr | None = None
    registration_abandonment_reason: NonEmptyStr | None = None
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    inserted_count: NonNegativeInt = 0
    already_present_count: NonNegativeInt = 0
    enqueued_count: NonNegativeInt = 0
    workflow_already_present_count: NonNegativeInt = 0
    enqueue_failed_count: NonNegativeInt = 0
    active_count: NonNegativeInt = 0
    succeeded_count: NonNegativeInt = 0
    terminal_failed_count: NonNegativeInt = 0
    cancelled_count: NonNegativeInt = 0
    spec: dict[StrictStr, Any] = Field(default_factory=dict)
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    terminal_reason: NonEmptyStr | None = None
    cancel_requested_at: datetime | None = None
    created_at: datetime
    registration_completed_at: datetime | None = None
    updated_at: datetime
    completed_at: datetime | None = None
    change_seq: PositiveChangeSeq


class ItemRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    operation_key: NonEmptyStr
    item_key: NonEmptyStr
    item_index: NonNegativeInt
    shuffle_rank: PositiveInt
    service_class: ServiceClass = ServiceClass.STANDARD
    service_priority: PositiveInt = ServiceClass.STANDARD.priority
    spec: dict[StrictStr, Any] = Field(default_factory=dict)
    insert_status: ItemInsertStatus
    current_attempt: NonNegativeInt = 0
    created_at: datetime
    updated_at: datetime
    change_seq: PositiveChangeSeq


class AttemptRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    workflow_role: NonEmptyStr
    execution_key: NonEmptyStr
    workflow_id: NonEmptyStr
    execution_recipe_digest: NonEmptyStr
    enqueue_state: AttemptEnqueueState = AttemptEnqueueState.PENDING
    enqueue_try: NonNegativeInt = 0
    execution_state: AttemptExecutionState = AttemptExecutionState.NOT_STARTED
    dbos_status: NonEmptyStr | None = None
    retry_disposition: RetryDisposition | None = None
    current_claim_id: NonEmptyStr | None = None
    failure: FailureSnapshot | None = None
    source_attempt: NonNegativeInt | None = None
    source_workflow_id: NonEmptyStr | None = None
    retry_reason: AttemptRetryReason | None = None
    next_attempt_request_id: NonEmptyStr | None = None
    source_application_version: NonEmptyStr
    missing_observation_count: NonNegativeInt = 0
    missing_first_observed_at: datetime | None = None
    missing_last_observed_at: datetime | None = None
    cancellation_request_id: NonEmptyStr | None = None
    cancellation_requested_at: datetime | None = None
    cancellation_requested_by: NonEmptyStr | None = None
    cancellation_disposition: CancellationDisposition | None = None
    cancellation_origin: CancellationOrigin | None = None
    cancellation_origin_operation_key: NonEmptyStr | None = None
    foreign_cancellation_request_id: NonEmptyStr | None = None
    requested_service_class: ServiceClass
    requested_service_priority: PositiveInt
    effective_service_priority: PositiveInt | None = None
    priority_source: PrioritySource | None = None
    created_at: datetime
    enqueued_at: datetime | None = None
    terminal_at: datetime | None = None
    updated_at: datetime
    change_seq: PositiveChangeSeq


class EligibilityReference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: NonEmptyStr
    record_id: NonEmptyStr
    digest: NonEmptyStr


class NextAttemptRequestRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: NonEmptyStr
    item_id: NonEmptyStr
    request_key: NonEmptyStr
    source_attempt: NonNegativeInt
    reason: NextAttemptReason
    eligibility_kind: NonEmptyStr
    eligibility_record_id: NonEmptyStr
    eligibility_digest: NonEmptyStr
    requested_by: NonEmptyStr
    operator_confirmed_at: datetime | None = None
    max_attempts: PositiveInt | None = None
    effective_max_attempts: PositiveInt
    disposition: NextAttemptDisposition
    created_attempt: NonNegativeInt | None = None
    rejection_detail: StrictStr | None = None
    created_at: datetime
    resolved_at: datetime
    change_seq: PositiveChangeSeq


class EnqueueClaimRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    claim_id: NonEmptyStr
    workflow_id: NonEmptyStr
    enqueue_try: PositiveInt
    claimed_at: datetime
    lease_expires_at: datetime
    enqueue_call_started_at: datetime | None = None
    disposition: EnqueueClaimDisposition
    invalidated_at: datetime | None = None
    invalidated_by: NonEmptyStr | None = None
    replacement_claim_id: NonEmptyStr | None = None
    resolved_at: datetime | None = None
    created_at: datetime
    change_seq: PositiveChangeSeq


class EnqueueCompensationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    claim_id: NonEmptyStr
    workflow_id: NonEmptyStr
    reason: EnqueueCompensationReason
    cancel_disposition: EnqueueCompensationDisposition
    failure: FailureSnapshot | None = None
    first_absent_at: datetime | None = None
    last_absent_at: datetime | None = None
    absence_observation_count: NonNegativeInt = 0
    created_at: datetime
    resolved_at: datetime | None = None
    change_seq: PositiveChangeSeq


class EnqueueCompensationHazardRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    claim_id: NonEmptyStr
    hazard_seq: PositiveInt
    workflow_id: NonEmptyStr
    cancel_disposition: EnqueueCompensationDisposition
    failure: FailureSnapshot | None = None
    created_at: datetime
    resolved_at: datetime | None = None
    change_seq: PositiveChangeSeq


class ThrottleState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    throttle_key: NonEmptyStr
    blocked_until: datetime | None = None
    consecutive_failures: NonNegativeInt = 0
    failure_class: FailureClass | None = None
    last_error_type: NonEmptyStr | None = None
    last_message: StrictStr | None = None
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    updated_at: datetime
    hold_until: datetime | None = None
    hold_reason: NonEmptyStr | None = None
    tags: dict[StrictStr, StrictStr] = Field(default_factory=dict)
    change_seq: PositiveChangeSeq
