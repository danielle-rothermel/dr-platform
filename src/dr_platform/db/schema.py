"""Canonical SQLAlchemy schema for the platform kernel.

The schema is intentionally a fresh baseline. ``prefix`` is the only physical
naming option; domain column names and lifecycle constraints are fixed.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

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

DEFAULT_PREFIX = "platform"
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
POSTGRES_IDENTIFIER_MAX_BYTES = 63
# The longest generated object name is
# ``<prefix>_ck_operations_registration_completed_time``.
MAX_GENERATED_IDENTIFIER_OVERHEAD_BYTES = 42
MAX_PREFIX_BYTES = (
    POSTGRES_IDENTIFIER_MAX_BYTES - MAX_GENERATED_IDENTIFIER_OVERHEAD_BYTES
)

OPERATION_COUNT_CHECK = """
requested_count >= 0
AND inserted_count >= 0
AND already_present_count >= 0
AND enqueued_count >= 0
AND workflow_already_present_count >= 0
AND enqueue_failed_count >= 0
AND active_count >= 0
AND succeeded_count >= 0
AND terminal_failed_count >= 0
AND cancelled_count >= 0
AND inserted_count + already_present_count <= requested_count
AND enqueued_count + workflow_already_present_count + enqueue_failed_count
    <= requested_count
AND active_count + succeeded_count + terminal_failed_count + cancelled_count
    <= requested_count
""".strip()

MANIFEST_BOUNDS_CHECK = """
manifest_page_size > 0
AND manifest_page_count >= 0
AND registration_cursor >= 0
AND registration_cursor <= manifest_page_count
AND (
  (requested_count = 0 AND manifest_page_count = 0)
  OR (
    requested_count > 0
    AND manifest_page_count =
      (requested_count + manifest_page_size - 1) / manifest_page_size
  )
)
""".strip()

REGISTRATION_LEASE_CHECK = """
(
  registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)
OR (
  registration_lease_id IS NOT NULL
  AND registration_lease_expires_at IS NOT NULL
)
""".strip()

REGISTRATION_COMPLETION_CHECK = """
registration_completed_at IS NULL
OR (
  registration_cursor = manifest_page_count
  AND inserted_count + already_present_count = requested_count
  AND registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)
""".strip()

REGISTRATION_ABANDONMENT_CHECK = """
(
  registration_abandoned_at IS NULL
  AND registration_abandoned_by IS NULL
  AND registration_abandonment_reason IS NULL
)
OR (
  registration_abandoned_at IS NOT NULL
  AND
  registration_abandoned_by IS NOT NULL
  AND registration_abandonment_reason IS NOT NULL
  AND registration_completed_at IS NULL
  AND registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
  AND status = 'failed'
)
""".strip()

ATTEMPT_SOURCE_CHECK = """
(
  attempt = 0
  AND source_attempt IS NULL
  AND source_workflow_id IS NULL
  AND retry_reason IS NULL
  AND next_attempt_request_id IS NULL
)
OR (
  attempt > 0
  AND source_attempt = attempt - 1
  AND source_workflow_id IS NOT NULL
  AND retry_reason IS NOT NULL
)
""".strip()

ATTEMPT_CLAIM_CHECK = """
(enqueue_state = 'claiming') = (current_claim_id IS NOT NULL)
""".strip()

ATTEMPT_WORKFLOW_CHECK = """
enqueue_state NOT IN ('enqueued', 'workflow_already_present')
OR workflow_id IS NOT NULL
""".strip()

ATTEMPT_TERMINAL_CHECK = """
(
  execution_state IN (
    'succeeded', 'error', 'recovery_exhausted', 'cancelled', 'missing'
  )
  AND terminal_at IS NOT NULL
)
OR (
  execution_state NOT IN (
    'succeeded', 'error', 'recovery_exhausted', 'cancelled', 'missing'
  )
  AND terminal_at IS NULL
)
""".strip()

ATTEMPT_FAILURE_CHECK = """
NOT (
  enqueue_state = 'enqueue_error'
  OR execution_state = 'error'
)
OR failure IS NOT NULL
""".strip()

ATTEMPT_MISSING_CHECK = """
(
  missing_observation_count = 0
  AND missing_first_observed_at IS NULL
  AND missing_last_observed_at IS NULL
)
OR (
  missing_observation_count > 0
  AND missing_first_observed_at IS NOT NULL
  AND missing_last_observed_at IS NOT NULL
  AND missing_last_observed_at >= missing_first_observed_at
)
""".strip()

ATTEMPT_CANCELLATION_CHECK = """
(
  cancellation_origin IS NULL
  AND cancellation_request_id IS NULL
  AND cancellation_requested_at IS NULL
  AND cancellation_requested_by IS NULL
)
OR (
  cancellation_origin IS NOT NULL
  AND cancellation_request_id IS NOT NULL
  AND cancellation_requested_at IS NOT NULL
  AND cancellation_requested_by IS NOT NULL
)
""".strip()

CLAIM_CALL_CHECK = """
(
  disposition NOT IN ('call_started', 'outcome_recorded')
  OR enqueue_call_started_at IS NOT NULL
)
AND (
  enqueue_call_started_at IS NULL
  OR disposition IN (
    'call_started', 'outcome_recorded', 'expired', 'replaced', 'invalidated'
  )
)
""".strip()

CLAIM_REPLACEMENT_CHECK = """
(
  disposition = 'replaced'
  AND replacement_claim_id IS NOT NULL
  AND resolved_at IS NOT NULL
)
OR (
  disposition != 'replaced'
  AND replacement_claim_id IS NULL
)
""".strip()

NEXT_ATTEMPT_REASON_CHECK = """
(
  reason = 'domain_outcome'
  AND operator_confirmed_at IS NULL
)
OR (
  reason = 'operator_cancel_retry'
  AND operator_confirmed_at IS NOT NULL
)
""".strip()

NEXT_ATTEMPT_RESULT_CHECK = """
(
  disposition = 'created'
  AND created_attempt IS NOT NULL
)
OR (
  disposition != 'created'
  AND created_attempt IS NULL
)
""".strip()


def enum_check(column_name: str, enum_type: type[StrEnum]) -> str:
    """Return a stable SQL check expression for a closed string enum."""

    values = ", ".join(f"'{value.value}'" for value in enum_type)
    return f"{column_name} IN ({values})"


class PlatformSchema:
    """Library-owned kernel tables under one fixed naming prefix."""

    def __init__(self, prefix: str = DEFAULT_PREFIX) -> None:
        if PREFIX_PATTERN.fullmatch(prefix) is None:
            raise ValueError(
                "prefix must be a lowercase SQL identifier using letters, "
                "numbers, or _"
            )
        if len(prefix.encode()) > MAX_PREFIX_BYTES:
            raise ValueError(
                "prefix is too long for generated PostgreSQL identifiers: "
                f"maximum is {MAX_PREFIX_BYTES} ASCII bytes"
            )

        self.prefix = prefix
        self.metadata = MetaData()

        def name(suffix: str) -> str:
            return f"{prefix}_{suffix}"

        self.operations = Table(
            name("operations"),
            self.metadata,
            Column("operation_key", Text, primary_key=True),
            Column("group_key", Text, nullable=False),
            Column("workflow_role", Text, nullable=False),
            Column("status", Text, nullable=False),
            Column("requested_count", Integer, nullable=False),
            Column("manifest_version", Integer, nullable=False),
            Column("manifest_digest", Text, nullable=False),
            Column("manifest_page_size", Integer, nullable=False),
            Column("manifest_page_count", Integer, nullable=False),
            Column("operation_execution_recipe_digest", Text, nullable=False),
            Column("target_key", Text, nullable=False),
            Column("target_version", Integer, nullable=False),
            Column("target_contract_digest", Text, nullable=False),
            Column("platform_cut_version", BigInteger, nullable=False),
            Column("registration_cursor", Integer, nullable=False),
            Column("registration_lease_id", Text),
            Column("registration_lease_expires_at", DateTime(timezone=True)),
            Column("registration_abandoned_at", DateTime(timezone=True)),
            Column("registration_abandoned_by", Text),
            Column("registration_abandonment_reason", Text),
            Column("retry_policy", JSONB, nullable=False),
            Column("inserted_count", Integer, nullable=False),
            Column("already_present_count", Integer, nullable=False),
            Column("enqueued_count", Integer, nullable=False),
            Column("workflow_already_present_count", Integer, nullable=False),
            Column("enqueue_failed_count", Integer, nullable=False),
            Column("active_count", Integer, nullable=False),
            Column("succeeded_count", Integer, nullable=False),
            Column("terminal_failed_count", Integer, nullable=False),
            Column("cancelled_count", Integer, nullable=False),
            Column("spec", JSONB, nullable=False),
            Column("metadata", JSONB, nullable=False),
            Column("terminal_reason", Text),
            Column("cancel_requested_at", DateTime(timezone=True)),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("registration_completed_at", DateTime(timezone=True)),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            Column("completed_at", DateTime(timezone=True)),
            Column("change_seq", BigInteger, nullable=False),
            CheckConstraint(
                enum_check("status", OperationStatus),
                name=name("ck_operations_status"),
            ),
            CheckConstraint(
                OPERATION_COUNT_CHECK,
                name=name("ck_operations_counts"),
            ),
            CheckConstraint(
                MANIFEST_BOUNDS_CHECK,
                name=name("ck_operations_manifest"),
            ),
            CheckConstraint(
                REGISTRATION_LEASE_CHECK,
                name=name("ck_operations_registration_lease"),
            ),
            CheckConstraint(
                REGISTRATION_COMPLETION_CHECK,
                name=name("ck_operations_registration_completed"),
            ),
            CheckConstraint(
                REGISTRATION_ABANDONMENT_CHECK,
                name=name("ck_operations_registration_abandoned"),
            ),
            CheckConstraint(
                "manifest_version = 3",
                name=name("ck_operations_manifest_version"),
            ),
            CheckConstraint(
                "platform_cut_version > 0",
                name=name("ck_operations_platform_cut_version"),
            ),
            CheckConstraint(
                "(status IN ('succeeded', 'partial', 'failed', 'cancelled')) "
                "= (completed_at IS NOT NULL)",
                name=name("ck_operations_terminal"),
            ),
            CheckConstraint(
                "registration_completed_at IS NULL "
                "OR registration_completed_at >= created_at",
                name=name("ck_operations_registration_completed_time"),
            ),
            CheckConstraint(
                "registration_abandoned_at IS NULL "
                "OR registration_abandoned_at >= created_at",
                name=name("ck_operations_registration_abandoned_time"),
            ),
            CheckConstraint(
                "cancel_requested_at IS NULL "
                "OR cancel_requested_at >= created_at",
                name=name("ck_operations_cancel_requested_time"),
            ),
            CheckConstraint(
                "updated_at >= created_at",
                name=name("ck_operations_updated_time"),
            ),
            CheckConstraint(
                "completed_at IS NULL OR completed_at >= created_at",
                name=name("ck_operations_time_order"),
            ),
        )

        self.items = Table(
            name("items"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column(
                "operation_key",
                Text,
                ForeignKey(
                    f"{self.operations.name}.operation_key",
                    ondelete="RESTRICT",
                ),
                nullable=False,
            ),
            Column("item_index", Integer, nullable=False),
            Column("item_key", Text, nullable=False),
            Column("shuffle_rank", BigInteger, nullable=False),
            Column("service_class", Text, nullable=False),
            Column("service_priority", Integer, nullable=False),
            Column("spec", JSONB, nullable=False),
            Column("insert_status", Text, nullable=False),
            Column("current_attempt", Integer, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            CheckConstraint("item_index >= 0", name=name("ck_items_index")),
            CheckConstraint(
                "shuffle_rank > 0",
                name=name("ck_items_shuffle_rank"),
            ),
            CheckConstraint(
                "current_attempt >= 0",
                name=name("ck_items_current_attempt"),
            ),
            CheckConstraint(
                enum_check("service_class", ServiceClass),
                name=name("ck_items_service_class"),
            ),
            CheckConstraint(
                "(service_class = 'urgent' AND service_priority = 100) "
                "OR (service_class = 'standard' AND service_priority = 1000) "
                "OR (service_class = 'backfill' AND service_priority = 10000)",
                name=name("ck_items_service_priority"),
            ),
            CheckConstraint(
                enum_check("insert_status", ItemInsertStatus),
                name=name("ck_items_insert_status"),
            ),
            CheckConstraint(
                "updated_at >= created_at",
                name=name("ck_items_updated_time"),
            ),
            UniqueConstraint(
                "operation_key",
                "item_index",
                name=name("uq_items_operation_index"),
            ),
            UniqueConstraint(
                "operation_key",
                "item_key",
                name=name("uq_items_operation_item"),
            ),
        )

        self.item_attempts = Table(
            name("item_attempts"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column("attempt", Integer, primary_key=True),
            Column("workflow_role", Text, nullable=False),
            Column("execution_key", Text, nullable=False),
            Column("workflow_id", Text, nullable=False),
            Column("execution_recipe_digest", Text, nullable=False),
            Column("enqueue_state", Text, nullable=False),
            Column("enqueue_try", Integer, nullable=False),
            Column("execution_state", Text, nullable=False),
            Column("dbos_status", Text),
            Column("retry_disposition", Text),
            Column("current_claim_id", Text),
            Column("failure", JSONB),
            Column("source_attempt", Integer),
            Column("source_workflow_id", Text),
            Column("retry_reason", Text),
            Column("next_attempt_request_id", Text),
            Column("source_application_version", Text, nullable=False),
            Column("missing_observation_count", Integer, nullable=False),
            Column("missing_first_observed_at", DateTime(timezone=True)),
            Column("missing_last_observed_at", DateTime(timezone=True)),
            Column("cancellation_request_id", Text),
            Column("cancellation_requested_at", DateTime(timezone=True)),
            Column("cancellation_requested_by", Text),
            Column("cancellation_disposition", Text),
            Column("cancellation_origin", Text),
            Column("cancellation_origin_operation_key", Text),
            Column("foreign_cancellation_request_id", Text),
            Column("requested_service_class", Text, nullable=False),
            Column("requested_service_priority", Integer, nullable=False),
            Column("effective_service_priority", Integer),
            Column("priority_source", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("enqueued_at", DateTime(timezone=True)),
            Column("terminal_at", DateTime(timezone=True)),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id"],
                [f"{self.items.name}.item_id"],
                ondelete="RESTRICT",
                name=name("fk_attempts_item"),
            ),
            CheckConstraint("attempt >= 0", name=name("ck_attempts_attempt")),
            CheckConstraint(
                "enqueue_try >= 0",
                name=name("ck_attempts_enqueue_try"),
            ),
            CheckConstraint(
                "missing_observation_count >= 0",
                name=name("ck_attempts_missing_count"),
            ),
            CheckConstraint(
                enum_check("enqueue_state", AttemptEnqueueState),
                name=name("ck_attempts_enqueue_state"),
            ),
            CheckConstraint(
                enum_check("execution_state", AttemptExecutionState),
                name=name("ck_attempts_execution_state"),
            ),
            CheckConstraint(
                "retry_disposition IS NULL OR "
                + enum_check("retry_disposition", RetryDisposition),
                name=name("ck_attempts_retry_disposition"),
            ),
            CheckConstraint(
                "(execution_state = 'error') = "
                "(retry_disposition IS NOT NULL)",
                name=name("ck_attempts_retry_disposition_shape"),
            ),
            CheckConstraint(
                "priority_source IS NULL OR "
                + enum_check("priority_source", PrioritySource),
                name=name("ck_attempts_priority_source"),
            ),
            CheckConstraint(
                "retry_reason IS NULL OR "
                + enum_check("retry_reason", AttemptRetryReason),
                name=name("ck_attempts_retry_reason"),
            ),
            CheckConstraint(
                "cancellation_disposition IS NULL OR "
                + enum_check(
                    "cancellation_disposition", CancellationDisposition
                ),
                name=name("ck_attempts_cancellation_disposition"),
            ),
            CheckConstraint(
                "cancellation_origin IS NULL OR "
                + enum_check("cancellation_origin", CancellationOrigin),
                name=name("ck_attempts_cancellation_origin"),
            ),
            CheckConstraint(
                enum_check("requested_service_class", ServiceClass),
                name=name("ck_attempts_service_class"),
            ),
            CheckConstraint(
                "((requested_service_class = 'urgent' "
                "AND requested_service_priority = 100) "
                "OR (requested_service_class = 'standard' "
                "AND requested_service_priority = 1000) "
                "OR (requested_service_class = 'backfill' "
                "AND requested_service_priority = 10000)) "
                "AND (effective_service_priority IS NULL "
                "OR effective_service_priority > 0)",
                name=name("ck_attempts_priorities"),
            ),
            CheckConstraint(
                "(enqueue_state IN ('enqueued', 'workflow_already_present')) "
                "= (enqueued_at IS NOT NULL)",
                name=name("ck_attempts_enqueued_at"),
            ),
            CheckConstraint(
                "(enqueue_state IN ('enqueued', 'workflow_already_present')) "
                "= (effective_service_priority IS NOT NULL "
                "AND priority_source IS NOT NULL)",
                name=name("ck_attempts_effective_priority"),
            ),
            CheckConstraint(
                ATTEMPT_SOURCE_CHECK,
                name=name("ck_attempts_source"),
            ),
            CheckConstraint(
                ATTEMPT_CLAIM_CHECK,
                name=name("ck_attempts_claim"),
            ),
            CheckConstraint(
                ATTEMPT_WORKFLOW_CHECK,
                name=name("ck_attempts_workflow"),
            ),
            CheckConstraint(
                ATTEMPT_TERMINAL_CHECK,
                name=name("ck_attempts_terminal"),
            ),
            CheckConstraint(
                ATTEMPT_FAILURE_CHECK,
                name=name("ck_attempts_failure"),
            ),
            CheckConstraint(
                ATTEMPT_MISSING_CHECK,
                name=name("ck_attempts_missing_observations"),
            ),
            CheckConstraint(
                ATTEMPT_CANCELLATION_CHECK,
                name=name("ck_attempts_cancellation"),
            ),
            CheckConstraint(
                "enqueued_at IS NULL OR enqueued_at >= created_at",
                name=name("ck_attempts_enqueued_time"),
            ),
            CheckConstraint(
                "terminal_at IS NULL OR terminal_at >= created_at",
                name=name("ck_attempts_terminal_time"),
            ),
            CheckConstraint(
                "updated_at >= created_at",
                name=name("ck_attempts_updated_time"),
            ),
            CheckConstraint(
                "cancellation_origin != 'foreign_operation' OR "
                "(cancellation_origin_operation_key IS NOT NULL AND "
                "foreign_cancellation_request_id IS NOT NULL)",
                name=name("ck_attempts_foreign_cancellation"),
            ),
        )

        self.enqueue_claims = Table(
            name("enqueue_claims"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column("attempt", Integer, primary_key=True),
            Column("claim_id", Text, primary_key=True),
            Column("workflow_id", Text, nullable=False),
            Column("enqueue_try", Integer, nullable=False),
            Column("claimed_at", DateTime(timezone=True), nullable=False),
            Column(
                "lease_expires_at", DateTime(timezone=True), nullable=False
            ),
            Column("enqueue_call_started_at", DateTime(timezone=True)),
            Column("disposition", Text, nullable=False),
            Column("invalidated_at", DateTime(timezone=True)),
            Column("invalidated_by", Text),
            Column("replacement_claim_id", Text),
            Column("resolved_at", DateTime(timezone=True)),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id", "attempt"],
                [
                    f"{self.item_attempts.name}.item_id",
                    f"{self.item_attempts.name}.attempt",
                ],
                ondelete="RESTRICT",
                name=name("fk_claims_attempt"),
            ),
            ForeignKeyConstraint(
                ["item_id", "attempt", "replacement_claim_id"],
                [
                    f"{name('enqueue_claims')}.item_id",
                    f"{name('enqueue_claims')}.attempt",
                    f"{name('enqueue_claims')}.claim_id",
                ],
                ondelete="RESTRICT",
                name=name("fk_claims_replacement"),
            ),
            CheckConstraint(
                "enqueue_try > 0",
                name=name("ck_claims_enqueue_try"),
            ),
            CheckConstraint(
                enum_check("disposition", EnqueueClaimDisposition),
                name=name("ck_claims_disposition"),
            ),
            CheckConstraint(
                CLAIM_CALL_CHECK,
                name=name("ck_claims_call_started"),
            ),
            CheckConstraint(
                CLAIM_REPLACEMENT_CHECK,
                name=name("ck_claims_replacement"),
            ),
            CheckConstraint(
                "lease_expires_at > claimed_at",
                name=name("ck_claims_lease"),
            ),
            CheckConstraint(
                "enqueue_call_started_at IS NULL "
                "OR enqueue_call_started_at >= claimed_at",
                name=name("ck_claims_call_time"),
            ),
            CheckConstraint(
                "(invalidated_at IS NULL) = (invalidated_by IS NULL)",
                name=name("ck_claims_invalidation"),
            ),
            CheckConstraint(
                "(disposition = 'invalidated') = (invalidated_at IS NOT NULL)",
                name=name("ck_claims_invalidation_disposition"),
            ),
            CheckConstraint(
                "(disposition IN "
                "('outcome_recorded', 'expired', 'replaced', 'invalidated')) "
                "= (resolved_at IS NOT NULL)",
                name=name("ck_claims_resolution"),
            ),
            UniqueConstraint(
                "item_id",
                "attempt",
                "claim_id",
                "workflow_id",
                name=name("uq_claims_workflow_provenance"),
            ),
        )

        self.missing_reobservations = Table(
            name("missing_reobservations"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column("attempt", Integer, primary_key=True),
            Column(
                "last_reobserved_at",
                DateTime(timezone=True),
                nullable=False,
            ),
            Column("observation_count", Integer, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id", "attempt"],
                [
                    f"{self.item_attempts.name}.item_id",
                    f"{self.item_attempts.name}.attempt",
                ],
                ondelete="RESTRICT",
                name=name("fk_missing_reobservations_attempt"),
            ),
            CheckConstraint(
                "observation_count > 0",
                name=name("ck_missing_reobservations_count"),
            ),
        )

        self.next_attempt_requests = Table(
            name("next_attempt_requests"),
            self.metadata,
            Column("request_id", Text, primary_key=True),
            Column("item_id", Text, nullable=False),
            Column("request_key", Text, nullable=False),
            Column("source_attempt", Integer, nullable=False),
            Column("reason", Text, nullable=False),
            Column("eligibility_kind", Text, nullable=False),
            Column("eligibility_record_id", Text, nullable=False),
            Column("eligibility_digest", Text, nullable=False),
            Column("requested_by", Text, nullable=False),
            Column("operator_confirmed_at", DateTime(timezone=True)),
            Column("max_attempts", Integer),
            Column("effective_max_attempts", Integer, nullable=False),
            Column("disposition", Text, nullable=False),
            Column("created_attempt", Integer),
            Column("rejection_detail", Text),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("resolved_at", DateTime(timezone=True), nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id", "source_attempt"],
                [
                    f"{self.item_attempts.name}.item_id",
                    f"{self.item_attempts.name}.attempt",
                ],
                ondelete="RESTRICT",
                name=name("fk_requests_source_attempt"),
            ),
            ForeignKeyConstraint(
                ["item_id", "created_attempt"],
                [
                    f"{self.item_attempts.name}.item_id",
                    f"{self.item_attempts.name}.attempt",
                ],
                ondelete="RESTRICT",
                name=name("fk_requests_created_attempt"),
            ),
            UniqueConstraint(
                "item_id",
                "request_key",
                name=name("uq_requests_item_key"),
            ),
            CheckConstraint(
                enum_check("reason", NextAttemptReason),
                name=name("ck_requests_reason"),
            ),
            CheckConstraint(
                enum_check("disposition", NextAttemptDisposition),
                name=name("ck_requests_disposition"),
            ),
            CheckConstraint(
                "source_attempt >= 0",
                name=name("ck_requests_source_attempt"),
            ),
            CheckConstraint(
                "(max_attempts IS NULL OR max_attempts > 0) "
                "AND effective_max_attempts > 0 "
                "AND (max_attempts IS NULL "
                "OR effective_max_attempts <= max_attempts)",
                name=name("ck_requests_attempt_bounds"),
            ),
            CheckConstraint(
                NEXT_ATTEMPT_REASON_CHECK,
                name=name("ck_requests_reason_shape"),
            ),
            CheckConstraint(
                NEXT_ATTEMPT_RESULT_CHECK,
                name=name("ck_requests_result_shape"),
            ),
            CheckConstraint(
                "created_attempt IS NULL "
                "OR created_attempt = source_attempt + 1",
                name=name("ck_requests_created_attempt"),
            ),
        )

        self.enqueue_compensations = Table(
            name("enqueue_compensations"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column("attempt", Integer, primary_key=True),
            Column("claim_id", Text, primary_key=True),
            Column("workflow_id", Text, nullable=False),
            Column("reason", Text, nullable=False),
            Column("cancel_disposition", Text, nullable=False),
            Column("failure", JSONB),
            Column("first_absent_at", DateTime(timezone=True)),
            Column("last_absent_at", DateTime(timezone=True)),
            Column(
                "absence_observation_count",
                Integer,
                nullable=False,
                server_default="0",
            ),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("resolved_at", DateTime(timezone=True)),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id", "attempt", "claim_id", "workflow_id"],
                [
                    f"{self.enqueue_claims.name}.item_id",
                    f"{self.enqueue_claims.name}.attempt",
                    f"{self.enqueue_claims.name}.claim_id",
                    f"{self.enqueue_claims.name}.workflow_id",
                ],
                ondelete="RESTRICT",
                name=name("fk_compensations_claim"),
            ),
            CheckConstraint(
                enum_check(
                    "cancel_disposition", EnqueueCompensationDisposition
                ),
                name=name("ck_compensations_disposition"),
            ),
            CheckConstraint(
                enum_check("reason", EnqueueCompensationReason),
                name=name("ck_compensations_reason"),
            ),
            CheckConstraint(
                "(cancel_disposition IN ('pending', 'failed')) "
                "= (resolved_at IS NULL)",
                name=name("ck_compensations_resolution"),
            ),
            CheckConstraint(
                "(cancel_disposition = 'failed') = (failure IS NOT NULL)",
                name=name("ck_compensations_failure"),
            ),
            CheckConstraint(
                "(absence_observation_count = 0) = "
                "(first_absent_at IS NULL AND last_absent_at IS NULL) "
                "AND (first_absent_at IS NULL OR "
                "last_absent_at >= first_absent_at)",
                name=name("ck_compensations_absence_observations"),
            ),
            UniqueConstraint(
                "item_id",
                "attempt",
                "claim_id",
                "workflow_id",
                name=name("uq_compensations_workflow"),
            ),
        )

        self.enqueue_compensation_hazards = Table(
            name("enqueue_compensation_hazards"),
            self.metadata,
            Column("item_id", Text, primary_key=True),
            Column("attempt", Integer, primary_key=True),
            Column("claim_id", Text, primary_key=True),
            Column("hazard_seq", Integer, primary_key=True),
            Column("workflow_id", Text, nullable=False),
            Column("cancel_disposition", Text, nullable=False),
            Column("failure", JSONB),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("resolved_at", DateTime(timezone=True)),
            Column("change_seq", BigInteger, nullable=False),
            ForeignKeyConstraint(
                ["item_id", "attempt", "claim_id", "workflow_id"],
                [
                    f"{self.enqueue_compensations.name}.item_id",
                    f"{self.enqueue_compensations.name}.attempt",
                    f"{self.enqueue_compensations.name}.claim_id",
                    f"{self.enqueue_compensations.name}.workflow_id",
                ],
                ondelete="RESTRICT",
                name=name("fk_compensation_hazards_predecessor"),
            ),
            CheckConstraint(
                "hazard_seq = 1",
                name=name("ck_compensation_hazards_bounded"),
            ),
            CheckConstraint(
                enum_check(
                    "cancel_disposition", EnqueueCompensationDisposition
                ),
                name=name("ck_compensation_hazards_disposition"),
            ),
            CheckConstraint(
                "cancel_disposition != 'no_workflow_found'",
                name=name("ck_compensation_hazards_not_absent"),
            ),
            CheckConstraint(
                "(cancel_disposition IN ('pending', 'failed')) "
                "= (resolved_at IS NULL)",
                name=name("ck_compensation_hazards_resolution"),
            ),
            CheckConstraint(
                "(cancel_disposition = 'failed') = (failure IS NOT NULL)",
                name=name("ck_compensation_hazards_failure"),
            ),
        )

        self.throttle_state = Table(
            name("throttle_state"),
            self.metadata,
            Column("throttle_key", Text, primary_key=True),
            Column("blocked_until", DateTime(timezone=True)),
            Column("consecutive_failures", Integer, nullable=False),
            Column("failure_class", Text),
            Column("last_error_type", Text),
            Column("last_message", Text),
            Column("metadata", JSONB, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            Column("hold_until", DateTime(timezone=True)),
            Column("hold_reason", Text),
            Column("tags", JSONB, nullable=False),
            Column("change_seq", BigInteger, nullable=False),
            CheckConstraint(
                "consecutive_failures >= 0",
                name=name("ck_throttle_state_failures"),
            ),
            CheckConstraint(
                "failure_class IS NULL OR "
                + enum_check("failure_class", FailureClass),
                name=name("ck_throttle_state_failure_class"),
            ),
            CheckConstraint(
                "(hold_until IS NULL) = (hold_reason IS NULL)",
                name=name("ck_throttle_state_hold"),
            ),
        )

        self.items.append_constraint(
            ForeignKeyConstraint(
                ["item_id", "current_attempt"],
                [
                    f"{self.item_attempts.name}.item_id",
                    f"{self.item_attempts.name}.attempt",
                ],
                deferrable=True,
                initially="DEFERRED",
                ondelete="RESTRICT",
                name=name("fk_items_current_attempt"),
                use_alter=True,
            )
        )
        self.item_attempts.append_constraint(
            ForeignKeyConstraint(
                ["item_id", "attempt", "current_claim_id"],
                [
                    f"{self.enqueue_claims.name}.item_id",
                    f"{self.enqueue_claims.name}.attempt",
                    f"{self.enqueue_claims.name}.claim_id",
                ],
                deferrable=True,
                initially="DEFERRED",
                ondelete="RESTRICT",
                name=name("fk_attempts_current_claim"),
                use_alter=True,
            )
        )

        self._create_indexes()

    def _create_indexes(self) -> None:
        prefix = self.prefix
        Index(f"ix_{prefix}_operations_group", self.operations.c.group_key)
        Index(
            f"ix_{prefix}_operations_status_updated",
            self.operations.c.status,
            self.operations.c.updated_at,
        )
        Index(
            f"ix_{prefix}_operations_registration_lease",
            self.operations.c.registration_lease_expires_at,
        )
        Index(
            f"ix_{prefix}_operations_change_seq",
            self.operations.c.change_seq,
        )
        Index(
            f"ix_{prefix}_items_operation_index",
            self.items.c.operation_key,
            self.items.c.item_index,
        )
        Index(
            f"ix_{prefix}_items_operation_key",
            self.items.c.operation_key,
            self.items.c.item_key,
        )
        Index(
            f"ix_{prefix}_items_schedule",
            self.items.c.service_priority,
            self.items.c.shuffle_rank,
            self.items.c.item_id,
        )
        Index(f"ix_{prefix}_items_change_seq", self.items.c.change_seq)
        Index(
            f"ix_{prefix}_attempts_workflow",
            self.item_attempts.c.workflow_id,
        )
        Index(
            f"ix_{prefix}_attempts_execution_key",
            self.item_attempts.c.execution_key,
            self.item_attempts.c.attempt,
        )
        Index(
            f"ix_{prefix}_attempts_enqueue_state",
            self.item_attempts.c.enqueue_state,
        )
        Index(
            f"ix_{prefix}_attempts_execution_state",
            self.item_attempts.c.execution_state,
        )
        Index(
            f"ix_{prefix}_attempts_change_seq",
            self.item_attempts.c.change_seq,
        )
        Index(
            f"ix_{prefix}_claims_workflow_disposition",
            self.enqueue_claims.c.workflow_id,
            self.enqueue_claims.c.disposition,
        )
        Index(
            f"ix_{prefix}_claims_lease",
            self.enqueue_claims.c.lease_expires_at,
        )
        Index(
            f"ix_{prefix}_claims_change_seq",
            self.enqueue_claims.c.change_seq,
        )
        Index(
            f"ix_{prefix}_requests_item_source",
            self.next_attempt_requests.c.item_id,
            self.next_attempt_requests.c.source_attempt,
        )
        Index(
            f"ix_{prefix}_requests_disposition",
            self.next_attempt_requests.c.disposition,
        )
        Index(
            f"ix_{prefix}_requests_change_seq",
            self.next_attempt_requests.c.change_seq,
        )
        Index(
            f"ix_{prefix}_compensations_workflow",
            self.enqueue_compensations.c.workflow_id,
        )
        Index(
            f"ix_{prefix}_compensations_unresolved",
            self.enqueue_compensations.c.cancel_disposition,
            postgresql_where=self.enqueue_compensations.c.cancel_disposition.in_(
                ["pending", "failed"]
            ),
        )
        Index(
            f"ix_{prefix}_missing_reobservations_schedule",
            self.missing_reobservations.c.last_reobserved_at,
            self.missing_reobservations.c.item_id,
            self.missing_reobservations.c.attempt,
        )
        Index(
            f"ix_{prefix}_missing_reobservations_change_seq",
            self.missing_reobservations.c.change_seq,
        )
        Index(
            f"ix_{prefix}_compensations_change_seq",
            self.enqueue_compensations.c.change_seq,
        )
        Index(
            f"ix_{prefix}_compensation_hazards_workflow",
            self.enqueue_compensation_hazards.c.workflow_id,
        )
        Index(
            f"ix_{prefix}_compensation_hazards_unresolved",
            self.enqueue_compensation_hazards.c.cancel_disposition,
            postgresql_where=(
                self.enqueue_compensation_hazards.c.cancel_disposition.in_(
                    ["pending", "failed"]
                )
            ),
        )
        Index(
            f"ix_{prefix}_compensation_hazards_change_seq",
            self.enqueue_compensation_hazards.c.change_seq,
        )
        Index(
            f"ix_{prefix}_throttle_blocked_until",
            self.throttle_state.c.blocked_until,
        )
        Index(
            f"ix_{prefix}_throttle_hold_until",
            self.throttle_state.c.hold_until,
        )
        Index(
            f"ix_{prefix}_throttle_change_seq",
            self.throttle_state.c.change_seq,
        )
