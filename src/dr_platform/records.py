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

from dr_platform.manifests import MANIFEST_FORMAT_VERSION
from dr_platform.status import (
    CONFIRMED_ENQUEUE_STATES,
    RESOLVED_COMPENSATION_DISPOSITIONS,
    TERMINAL_EXECUTION_STATES,
    TERMINAL_OPERATION_STATUSES,
    AttemptEnqueueState,
    AttemptExecutionState,
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


def _validate_payload_size(value: Any, *, label: str) -> None:
    try:
        Serializer(
            limits=postgres_jsonb_limits(JSONB_PAYLOAD_MAX_BYTES)
        ).to_jsonable(value)
    except SerializationError as exc:
        raise ValueError(f"{label}: {exc}") from exc


def _validate_time_order(
    *, start: datetime, end: datetime | None, label: str
) -> None:
    if end is not None and end < start:
        raise ValueError(f"{label} must not precede {start=}")


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
    """Safe, persistence-ready failure facts; never raw provider payloads."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    failure_class: FailureClass
    error_type: NonEmptyStr
    underlying_exception_type: NonEmptyStr | None = None
    message: StrictStr
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_metadata(self) -> FailureSnapshot:
        _validate_payload_size(self.metadata, label="failure metadata")
        return self


class OperationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    group_key: NonEmptyStr
    workflow_role: NonEmptyStr
    status: OperationStatus
    requested_count: NonNegativeInt
    manifest_version: PositiveInt
    manifest_digest: NonEmptyStr
    manifest_page_size: PositiveInt
    manifest_page_count: NonNegativeInt
    operation_execution_recipe_digest: NonEmptyStr
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

    @model_validator(mode="after")
    def validate_operation(self) -> OperationRecord:
        self._validate_manifest_and_counts()
        self._validate_registration()
        self._validate_lifecycle()
        _validate_payload_size(self.spec, label="operation spec")
        _validate_payload_size(self.metadata, label="operation metadata")
        for end, label in (
            (self.registration_completed_at, "registration_completed_at"),
            (self.registration_abandoned_at, "registration_abandoned_at"),
            (self.cancel_requested_at, "cancel_requested_at"),
            (self.updated_at, "updated_at"),
            (self.completed_at, "completed_at"),
        ):
            _validate_time_order(start=self.created_at, end=end, label=label)
        return self

    def _validate_manifest_and_counts(self) -> None:
        if self.manifest_version != MANIFEST_FORMAT_VERSION:
            raise ValueError(
                f"manifest_version must be {MANIFEST_FORMAT_VERSION}"
            )
        count_values = (
            self.inserted_count,
            self.already_present_count,
            self.enqueued_count,
            self.workflow_already_present_count,
            self.enqueue_failed_count,
            self.active_count,
            self.succeeded_count,
            self.terminal_failed_count,
            self.cancelled_count,
        )
        if any(value > self.requested_count for value in count_values):
            raise ValueError("operation counts cannot exceed requested_count")
        if (
            self.inserted_count + self.already_present_count
            > self.requested_count
        ):
            raise ValueError(
                "registration counts cannot exceed requested_count"
            )
        enqueue_total = (
            self.enqueued_count
            + self.workflow_already_present_count
            + self.enqueue_failed_count
        )
        if enqueue_total > self.requested_count:
            raise ValueError("enqueue counts cannot exceed requested_count")
        execution_total = (
            self.active_count
            + self.succeeded_count
            + self.terminal_failed_count
            + self.cancelled_count
        )
        if execution_total > self.requested_count:
            raise ValueError("execution counts cannot exceed requested_count")
        if self.registration_cursor > self.manifest_page_count:
            raise ValueError("registration_cursor exceeds manifest_page_count")
        expected_page_count = (
            self.requested_count + self.manifest_page_size - 1
        ) // self.manifest_page_size
        if self.manifest_page_count != expected_page_count:
            raise ValueError("manifest page count does not cover item count")
        if self.registration_completed_at is not None and (
            self.registration_cursor != self.manifest_page_count
        ):
            raise ValueError(
                "registration completion requires the final cursor"
            )
        if self.registration_completed_at is not None and (
            self.inserted_count + self.already_present_count
            != self.requested_count
        ):
            raise ValueError(
                "completed registration must account for every item"
            )

    def _validate_registration(self) -> None:
        lease_values = (
            self.registration_lease_id,
            self.registration_lease_expires_at,
        )
        if any(value is None for value in lease_values) and any(
            value is not None for value in lease_values
        ):
            raise ValueError("registration lease fields must be set together")
        abandonment_values = (
            self.registration_abandoned_at,
            self.registration_abandoned_by,
            self.registration_abandonment_reason,
        )
        if any(value is None for value in abandonment_values) and any(
            value is not None for value in abandonment_values
        ):
            raise ValueError(
                "registration abandonment fields must be set together"
            )
        if self.registration_completed_at is not None and any(
            value is not None for value in lease_values
        ):
            raise ValueError("completed registration cannot retain a lease")
        if self.registration_abandoned_at is not None:
            if any(value is not None for value in lease_values):
                raise ValueError(
                    "abandoned registration cannot retain a lease"
                )
            if self.registration_completed_at is not None:
                raise ValueError(
                    "registration cannot be completed and abandoned"
                )
            if self.status is not OperationStatus.FAILED:
                raise ValueError("abandoned registration must be failed")

    def _validate_lifecycle(self) -> None:
        if (
            self.status in TERMINAL_OPERATION_STATUSES
            and self.completed_at is None
        ):
            raise ValueError("terminal operations require completed_at")
        if (
            self.status not in TERMINAL_OPERATION_STATUSES
            and self.completed_at is not None
        ):
            raise ValueError("nonterminal operations cannot have completed_at")


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

    @model_validator(mode="after")
    def validate_item(self) -> ItemRecord:
        if self.service_priority != self.service_class.priority:
            raise ValueError("service_priority does not match service_class")
        _validate_payload_size(self.spec, label="item spec")
        _validate_time_order(
            start=self.created_at, end=self.updated_at, label="updated_at"
        )
        return self


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
    retry_reason: NextAttemptReason | None = None
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

    @model_validator(mode="after")
    def validate_attempt(self) -> AttemptRecord:
        self._validate_identity_and_priority()
        self._validate_enqueue()
        self._validate_execution()
        self._validate_missing_observations()
        self._validate_cancellation()
        for end, label in (
            (self.enqueued_at, "enqueued_at"),
            (self.terminal_at, "terminal_at"),
            (self.updated_at, "updated_at"),
        ):
            _validate_time_order(start=self.created_at, end=end, label=label)
        return self

    def _validate_identity_and_priority(self) -> None:
        if (
            self.requested_service_priority
            != self.requested_service_class.priority
        ):
            raise ValueError("requested priority does not match service class")
        source_values = (
            self.source_attempt,
            self.source_workflow_id,
            self.retry_reason,
        )
        if self.attempt == 0 and any(
            value is not None for value in source_values
        ):
            raise ValueError("attempt zero cannot have retry provenance")
        if self.attempt == 0 and self.next_attempt_request_id is not None:
            raise ValueError(
                "attempt zero cannot reference a next-attempt request"
            )
        if self.attempt > 0 and any(value is None for value in source_values):
            raise ValueError(
                "later attempts require complete retry provenance"
            )
        if self.attempt > 0 and self.source_attempt != self.attempt - 1:
            raise ValueError(
                "later attempts must reference the preceding attempt"
            )

    def _validate_enqueue(self) -> None:
        if self.enqueue_state is AttemptEnqueueState.CLAIMING:
            if self.current_claim_id is None:
                raise ValueError("claiming attempts require current_claim_id")
        elif self.current_claim_id is not None:
            raise ValueError(
                "only claiming attempts may point at a current claim"
            )
        if self.enqueue_state in CONFIRMED_ENQUEUE_STATES:
            if (
                self.enqueued_at is None
                or self.effective_service_priority is None
            ):
                raise ValueError(
                    "confirmed enqueue requires time and effective priority"
                )
            if self.priority_source is None:
                raise ValueError("confirmed enqueue requires priority_source")
        elif any(
            value is not None
            for value in (
                self.enqueued_at,
                self.effective_service_priority,
                self.priority_source,
            )
        ):
            raise ValueError(
                "unconfirmed enqueue cannot carry confirmed enqueue facts"
            )

    def _validate_execution(self) -> None:
        if self.execution_state in TERMINAL_EXECUTION_STATES:
            if self.terminal_at is None:
                raise ValueError("terminal attempts require terminal_at")
        elif self.terminal_at is not None:
            raise ValueError("nonterminal attempts cannot have terminal_at")
        if (
            self.execution_state is AttemptExecutionState.ERROR
            or self.enqueue_state is AttemptEnqueueState.ENQUEUE_ERROR
        ) and self.failure is None:
            raise ValueError("error attempts require a failure snapshot")
        if self.execution_state is AttemptExecutionState.ERROR and (
            self.retry_disposition is None
        ):
            raise ValueError("execution errors require retry_disposition")
        if self.execution_state is not AttemptExecutionState.ERROR and (
            self.retry_disposition is not None
        ):
            raise ValueError(
                "only execution errors may carry retry_disposition"
            )

    def _validate_missing_observations(self) -> None:
        missing_values = (
            self.missing_first_observed_at,
            self.missing_last_observed_at,
        )
        if self.missing_observation_count == 0 and any(
            value is not None for value in missing_values
        ):
            raise ValueError("missing timestamps require observations")
        if self.missing_observation_count > 0 and any(
            value is None for value in missing_values
        ):
            raise ValueError(
                "missing observations require first and last timestamps"
            )
        if (
            self.missing_first_observed_at is not None
            and self.missing_last_observed_at is not None
            and self.missing_last_observed_at
            < self.missing_first_observed_at
        ):
            raise ValueError(
                "last missing observation cannot precede the first"
            )

    def _validate_cancellation(self) -> None:
        cancellation_origin_values = (
            self.cancellation_origin,
            self.cancellation_request_id,
            self.cancellation_requested_at,
            self.cancellation_requested_by,
        )
        if any(value is None for value in cancellation_origin_values) and any(
            value is not None for value in cancellation_origin_values
        ):
            raise ValueError(
                "cancellation origin and request must be set together"
            )
        if (
            self.cancellation_origin is CancellationOrigin.FOREIGN_OPERATION
            and (
                self.cancellation_origin_operation_key is None
                or self.foreign_cancellation_request_id is None
            )
        ):
            raise ValueError(
                "foreign cancellation requires operation and request "
                "provenance"
            )


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

    @model_validator(mode="after")
    def validate_request(self) -> NextAttemptRequestRecord:
        if self.reason is NextAttemptReason.OPERATOR_CANCEL_RETRY and (
            self.operator_confirmed_at is None
        ):
            raise ValueError("cancel retry requires operator confirmation")
        if self.reason is NextAttemptReason.DOMAIN_OUTCOME and (
            self.operator_confirmed_at is not None
        ):
            raise ValueError(
                "domain-outcome retry cannot carry operator confirmation"
            )
        if self.max_attempts is not None and (
            self.effective_max_attempts > self.max_attempts
        ):
            raise ValueError(
                "effective_max_attempts exceeds requested tightening"
            )
        if self.disposition is NextAttemptDisposition.CREATED:
            if self.created_attempt != self.source_attempt + 1:
                raise ValueError(
                    "created disposition requires source attempt + 1"
                )
        elif self.created_attempt is not None:
            raise ValueError(
                "only created requests may name a created attempt"
            )
        _validate_time_order(
            start=self.created_at, end=self.resolved_at, label="resolved_at"
        )
        return self


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

    @model_validator(mode="after")
    def validate_claim(self) -> EnqueueClaimRecord:
        if self.lease_expires_at <= self.claimed_at:
            raise ValueError("claim lease must expire after claimed_at")
        if (
            self.disposition
            in {
                EnqueueClaimDisposition.CALL_STARTED,
                EnqueueClaimDisposition.OUTCOME_RECORDED,
            }
            and self.enqueue_call_started_at is None
        ):
            raise ValueError(
                "DBOS-call dispositions require a start timestamp"
            )
        call_started_dispositions = {
            EnqueueClaimDisposition.CALL_STARTED,
            EnqueueClaimDisposition.OUTCOME_RECORDED,
            EnqueueClaimDisposition.EXPIRED,
            EnqueueClaimDisposition.REPLACED,
            EnqueueClaimDisposition.INVALIDATED,
        }
        if (
            self.enqueue_call_started_at is not None
            and self.disposition not in call_started_dispositions
        ):
            raise ValueError(
                "call-start facts require a call-started, outcome-recorded, "
                "expired, replaced, or invalidated disposition"
            )
        if self.disposition is EnqueueClaimDisposition.REPLACED and (
            self.replacement_claim_id is None
        ):
            raise ValueError("replaced claims require replacement_claim_id")
        if self.disposition is not EnqueueClaimDisposition.REPLACED and (
            self.replacement_claim_id is not None
        ):
            raise ValueError(
                "only replaced claims may name a replacement claim"
            )
        invalidation_values = (self.invalidated_at, self.invalidated_by)
        if any(value is None for value in invalidation_values) and any(
            value is not None for value in invalidation_values
        ):
            raise ValueError("claim invalidation fields must be set together")
        if self.disposition is EnqueueClaimDisposition.INVALIDATED and any(
            value is None for value in invalidation_values
        ):
            raise ValueError(
                "invalidated claims require invalidation facts"
            )
        if self.disposition is not EnqueueClaimDisposition.INVALIDATED and any(
            value is not None for value in invalidation_values
        ):
            raise ValueError(
                "only invalidated claims may carry invalidation facts"
            )
        if self.enqueue_call_started_at is not None:
            _validate_time_order(
                start=self.claimed_at,
                end=self.enqueue_call_started_at,
                label="enqueue_call_started_at",
            )
        resolved_dispositions = {
            EnqueueClaimDisposition.OUTCOME_RECORDED,
            EnqueueClaimDisposition.EXPIRED,
            EnqueueClaimDisposition.REPLACED,
            EnqueueClaimDisposition.INVALIDATED,
        }
        if (self.disposition in resolved_dispositions) != (
            self.resolved_at is not None
        ):
            raise ValueError("resolved claim dispositions require resolved_at")
        return self


class EnqueueCompensationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    claim_id: NonEmptyStr
    workflow_id: NonEmptyStr
    reason: EnqueueCompensationReason
    cancel_disposition: EnqueueCompensationDisposition
    failure: FailureSnapshot | None = None
    created_at: datetime
    resolved_at: datetime | None = None
    change_seq: PositiveChangeSeq

    @model_validator(mode="after")
    def validate_compensation(self) -> EnqueueCompensationRecord:
        if self.cancel_disposition is EnqueueCompensationDisposition.FAILED:
            if self.failure is None:
                raise ValueError("failed compensation requires failure")
        elif self.failure is not None:
            raise ValueError("only failed compensation may carry failure")
        if self.cancel_disposition in RESOLVED_COMPENSATION_DISPOSITIONS:
            if self.resolved_at is None:
                raise ValueError("resolved compensation requires resolved_at")
        elif self.resolved_at is not None:
            raise ValueError("unresolved compensation cannot have resolved_at")
        return self


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

    @model_validator(mode="after")
    def validate_throttle(self) -> ThrottleState:
        if (self.hold_until is None) != (self.hold_reason is None):
            raise ValueError("hold_until and hold_reason must be set together")
        if self.consecutive_failures == 0 and self.failure_class is not None:
            raise ValueError("failure class requires consecutive failures")
        _validate_payload_size(self.metadata, label="throttle metadata")
        _validate_payload_size(self.tags, label="throttle tags")
        return self
