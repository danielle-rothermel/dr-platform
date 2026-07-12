"""Closed persisted value sets for the platform kernel."""

from __future__ import annotations

from enum import StrEnum


class OperationStatus(StrEnum):
    REGISTERING = "registering"
    ENQUEUEING = "enqueuing"
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_OPERATION_STATUSES = frozenset(
    {
        OperationStatus.SUCCEEDED,
        OperationStatus.PARTIAL,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
    }
)


class ItemInsertStatus(StrEnum):
    INSERTED = "inserted"
    ALREADY_PRESENT = "already_present"


class AttemptEnqueueState(StrEnum):
    PENDING = "pending"
    CLAIMING = "claiming"
    ENQUEUED = "enqueued"
    WORKFLOW_ALREADY_PRESENT = "workflow_already_present"
    ENQUEUE_ERROR = "enqueue_error"


CONFIRMED_ENQUEUE_STATES = frozenset(
    {
        AttemptEnqueueState.ENQUEUED,
        AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT,
    }
)


class AttemptExecutionState(StrEnum):
    NOT_STARTED = "not_started"
    ACTIVE = "active"
    CANCEL_REQUESTED = "cancel_requested"
    SUCCEEDED = "succeeded"
    ERROR = "error"
    RECOVERY_EXHAUSTED = "recovery_exhausted"
    CANCELLED = "cancelled"
    MISSING = "missing"


TERMINAL_EXECUTION_STATES = frozenset(
    {
        AttemptExecutionState.SUCCEEDED,
        AttemptExecutionState.ERROR,
        AttemptExecutionState.RECOVERY_EXHAUSTED,
        AttemptExecutionState.CANCELLED,
        AttemptExecutionState.MISSING,
    }
)


class RetryDisposition(StrEnum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    EXHAUSTED = "exhausted"


class FailureClass(StrEnum):
    PERMANENT = "permanent"
    TRANSIENT = "transient"
    RATE_LIMITED = "rate_limited"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    UNKNOWN = "unknown"


class ServiceClass(StrEnum):
    URGENT = "urgent"
    STANDARD = "standard"
    BACKFILL = "backfill"

    @property
    def priority(self) -> int:
        return SERVICE_CLASS_PRIORITIES[self]


SERVICE_CLASS_PRIORITIES = {
    ServiceClass.URGENT: 100,
    ServiceClass.STANDARD: 1_000,
    ServiceClass.BACKFILL: 10_000,
}


class WorkflowTopology(StrEnum):
    TOP_LEVEL_ONLY = "top_level_only"


class PrioritySource(StrEnum):
    ENQUEUED_HERE = "enqueued_here"
    LINKED_EXISTING = "linked_existing"


class NextAttemptReason(StrEnum):
    DOMAIN_OUTCOME = "domain_outcome"
    OPERATOR_CANCEL_RETRY = "operator_cancel_retry"


class NextAttemptDisposition(StrEnum):
    CREATED = "created"
    MAX_ATTEMPTS_EXHAUSTED = "max_attempts_exhausted"
    INELIGIBLE = "ineligible"
    SOURCE_ADVANCED = "source_advanced"


class EnqueueClaimDisposition(StrEnum):
    CLAIMED = "claimed"
    CALL_STARTED = "call_started"
    OUTCOME_RECORDED = "outcome_recorded"
    EXPIRED = "expired"
    REPLACED = "replaced"
    INVALIDATED = "invalidated"


class EnqueueCompensationReason(StrEnum):
    INVALIDATED_CALL_STARTED_CLAIM = "invalidated_call_started_claim"


class EnqueueCompensationDisposition(StrEnum):
    PENDING = "pending"
    FAILED = "failed"
    CANCELLED = "cancelled"
    OBSERVED_TERMINAL = "observed_terminal"
    SKIPPED_SHARED = "skipped_shared"
    NO_WORKFLOW_FOUND = "no_workflow_found"


RESOLVED_COMPENSATION_DISPOSITIONS = frozenset(
    {
        EnqueueCompensationDisposition.CANCELLED,
        EnqueueCompensationDisposition.OBSERVED_TERMINAL,
        EnqueueCompensationDisposition.SKIPPED_SHARED,
        EnqueueCompensationDisposition.NO_WORKFLOW_FOUND,
    }
)


class CancellationOrigin(StrEnum):
    LOCAL_OPERATION = "local_operation"
    FOREIGN_OPERATION = "foreign_operation"


class CancellationDisposition(StrEnum):
    NOT_ENQUEUED = "not_enqueued"
    DBOS_CANCELLED = "dbos_cancelled"
    ALREADY_CANCELLED = "already_cancelled"
    OBSERVED_TERMINAL = "observed_terminal"
    SKIPPED_SHARED = "skipped_shared"
    FAILED = "failed"
