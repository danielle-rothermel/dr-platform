# ruff: noqa: E501 -- frozen PostgreSQL DDL and validated identifiers
"""Immutable final platform kernel baseline.

Revision ID: 0001_platform_baseline
Revises:
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op

revision = "0001_platform_baseline"
down_revision = None
branch_labels = None
depends_on = None

DEFAULT_PREFIX = "platform"
MAX_PREFIX_BYTES = 21
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")
TABLE_SUFFIXES = (
    "operations",
    "throttle_state",
    "items",
    "item_attempts",
    "enqueue_claims",
    "missing_reobservations",
    "next_attempt_requests",
    "enqueue_compensations",
    "enqueue_compensation_hazards",
)
TERMINAL_STATES = (
    "'cancelled', 'error', 'missing', 'recovery_exhausted', 'succeeded'"
)

# This PostgreSQL DDL is deliberately frozen in the baseline rather than
# derived from PlatformSchema, which may evolve after this hard cut.
BASELINE_DDL = (
    """CREATE TABLE xbase_operations (
	operation_key TEXT NOT NULL,
	group_key TEXT NOT NULL,
	workflow_role TEXT NOT NULL,
	status TEXT NOT NULL,
	requested_count INTEGER NOT NULL,
	manifest_version INTEGER NOT NULL,
	manifest_digest TEXT NOT NULL,
	manifest_page_size INTEGER NOT NULL,
	manifest_page_count INTEGER NOT NULL,
	operation_execution_recipe_digest TEXT NOT NULL,
	target_key TEXT NOT NULL,
	target_version INTEGER NOT NULL,
	target_contract_digest TEXT NOT NULL,
	platform_cut_version BIGINT NOT NULL,
	registration_cursor INTEGER NOT NULL,
	registration_lease_id TEXT,
	registration_lease_expires_at TIMESTAMP WITH TIME ZONE,
	registration_abandoned_at TIMESTAMP WITH TIME ZONE,
	registration_abandoned_by TEXT,
	registration_abandonment_reason TEXT,
	retry_policy JSONB NOT NULL,
	inserted_count INTEGER NOT NULL,
	already_present_count INTEGER NOT NULL,
	enqueued_count INTEGER NOT NULL,
	workflow_already_present_count INTEGER NOT NULL,
	enqueue_failed_count INTEGER NOT NULL,
	active_count INTEGER NOT NULL,
	succeeded_count INTEGER NOT NULL,
	terminal_failed_count INTEGER NOT NULL,
	cancelled_count INTEGER NOT NULL,
	spec JSONB NOT NULL,
	metadata JSONB NOT NULL,
	terminal_reason TEXT,
	cancel_requested_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	registration_completed_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	completed_at TIMESTAMP WITH TIME ZONE,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (operation_key),
	CONSTRAINT xbase_ck_operations_status CHECK (status IN ('registering', 'enqueuing', 'running', 'cancelling', 'succeeded', 'partial', 'failed', 'cancelled')),
	CONSTRAINT xbase_ck_operations_counts CHECK (requested_count >= 0
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
    <= requested_count),
	CONSTRAINT xbase_ck_operations_manifest CHECK (manifest_page_size > 0
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
)),
	CONSTRAINT xbase_ck_operations_registration_lease CHECK ((
  registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)
OR (
  registration_lease_id IS NOT NULL
  AND registration_lease_expires_at IS NOT NULL
)),
	CONSTRAINT xbase_ck_operations_registration_completed CHECK (registration_completed_at IS NULL
OR (
  registration_cursor = manifest_page_count
  AND inserted_count + already_present_count = requested_count
  AND registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)),
	CONSTRAINT xbase_ck_operations_registration_abandoned CHECK ((
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
)),
	CONSTRAINT xbase_ck_operations_manifest_version CHECK (manifest_version = 3),
	CONSTRAINT xbase_ck_operations_platform_cut_version CHECK (platform_cut_version > 0),
	CONSTRAINT xbase_ck_operations_terminal CHECK ((status IN ('succeeded', 'partial', 'failed', 'cancelled')) = (completed_at IS NOT NULL)),
	CONSTRAINT xbase_ck_operations_registration_completed_time CHECK (registration_completed_at IS NULL OR registration_completed_at >= created_at),
	CONSTRAINT xbase_ck_operations_registration_abandoned_time CHECK (registration_abandoned_at IS NULL OR registration_abandoned_at >= created_at),
	CONSTRAINT xbase_ck_operations_cancel_requested_time CHECK (cancel_requested_at IS NULL OR cancel_requested_at >= created_at),
	CONSTRAINT xbase_ck_operations_updated_time CHECK (updated_at >= created_at),
	CONSTRAINT xbase_ck_operations_time_order CHECK (completed_at IS NULL OR completed_at >= created_at)
)""",
    """CREATE TABLE xbase_throttle_state (
	throttle_key TEXT NOT NULL,
	blocked_until TIMESTAMP WITH TIME ZONE,
	consecutive_failures INTEGER NOT NULL,
	failure_class TEXT,
	last_error_type TEXT,
	last_message TEXT,
	metadata JSONB NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	hold_until TIMESTAMP WITH TIME ZONE,
	hold_reason TEXT,
	tags JSONB NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (throttle_key),
	CONSTRAINT xbase_ck_throttle_state_failures CHECK (consecutive_failures >= 0),
	CONSTRAINT xbase_ck_throttle_state_failure_class CHECK (failure_class IS NULL OR failure_class IN ('permanent', 'transient', 'rate_limited', 'resource_exhaustion', 'unknown')),
	CONSTRAINT xbase_ck_throttle_state_failure_count CHECK (failure_class IS NULL OR consecutive_failures > 0),
	CONSTRAINT xbase_ck_throttle_state_hold CHECK ((hold_until IS NULL) = (hold_reason IS NULL))
)""",
    """CREATE TABLE xbase_items (
	item_id TEXT NOT NULL,
	operation_key TEXT NOT NULL,
	item_index INTEGER NOT NULL,
	item_key TEXT NOT NULL,
	shuffle_rank BIGINT NOT NULL,
	service_class TEXT NOT NULL,
	service_priority INTEGER NOT NULL,
	spec JSONB NOT NULL,
	insert_status TEXT NOT NULL,
	current_attempt INTEGER NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id),
	CONSTRAINT xbase_ck_items_index CHECK (item_index >= 0),
	CONSTRAINT xbase_ck_items_shuffle_rank CHECK (shuffle_rank > 0),
	CONSTRAINT xbase_ck_items_current_attempt CHECK (current_attempt >= 0),
	CONSTRAINT xbase_ck_items_service_class CHECK (service_class IN ('urgent', 'standard', 'backfill')),
	CONSTRAINT xbase_ck_items_service_priority CHECK ((service_class = 'urgent' AND service_priority = 100) OR (service_class = 'standard' AND service_priority = 1000) OR (service_class = 'backfill' AND service_priority = 10000)),
	CONSTRAINT xbase_ck_items_insert_status CHECK (insert_status IN ('inserted', 'already_present')),
	CONSTRAINT xbase_ck_items_updated_time CHECK (updated_at >= created_at),
	CONSTRAINT xbase_uq_items_operation_index UNIQUE (operation_key, item_index),
	CONSTRAINT xbase_uq_items_operation_item UNIQUE (operation_key, item_key),
	FOREIGN KEY(operation_key) REFERENCES xbase_operations (operation_key) ON DELETE RESTRICT
)""",
    """CREATE TABLE xbase_item_attempts (
	item_id TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	workflow_role TEXT NOT NULL,
	execution_key TEXT NOT NULL,
	workflow_id TEXT NOT NULL,
	execution_recipe_digest TEXT NOT NULL,
	enqueue_state TEXT NOT NULL,
	enqueue_try INTEGER NOT NULL,
	execution_state TEXT NOT NULL,
	dbos_status TEXT,
	retry_disposition TEXT,
	current_claim_id TEXT,
	failure JSONB,
	source_attempt INTEGER,
	source_workflow_id TEXT,
	retry_reason TEXT,
	next_attempt_request_id TEXT,
	source_application_version TEXT NOT NULL,
	missing_observation_count INTEGER NOT NULL,
	missing_first_observed_at TIMESTAMP WITH TIME ZONE,
	missing_last_observed_at TIMESTAMP WITH TIME ZONE,
	cancellation_request_id TEXT,
	cancellation_requested_at TIMESTAMP WITH TIME ZONE,
	cancellation_requested_by TEXT,
	cancellation_disposition TEXT,
	cancellation_origin TEXT,
	cancellation_origin_operation_key TEXT,
	foreign_cancellation_request_id TEXT,
	requested_service_class TEXT NOT NULL,
	requested_service_priority INTEGER NOT NULL,
	effective_service_priority INTEGER,
	priority_source TEXT,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	enqueued_at TIMESTAMP WITH TIME ZONE,
	terminal_at TIMESTAMP WITH TIME ZONE,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id, attempt),
	CONSTRAINT xbase_fk_attempts_item FOREIGN KEY(item_id) REFERENCES xbase_items (item_id) ON DELETE RESTRICT,
	CONSTRAINT xbase_ck_attempts_attempt CHECK (attempt >= 0),
	CONSTRAINT xbase_ck_attempts_enqueue_try CHECK (enqueue_try >= 0),
	CONSTRAINT xbase_ck_attempts_missing_count CHECK (missing_observation_count >= 0),
	CONSTRAINT xbase_ck_attempts_enqueue_state CHECK (enqueue_state IN ('pending', 'claiming', 'enqueued', 'workflow_already_present', 'enqueue_error')),
	CONSTRAINT xbase_ck_attempts_execution_state CHECK (execution_state IN ('not_started', 'active', 'cancel_requested', 'succeeded', 'error', 'recovery_exhausted', 'cancelled', 'missing')),
	CONSTRAINT xbase_ck_attempts_retry_disposition CHECK (retry_disposition IS NULL OR retry_disposition IN ('retryable', 'permanent', 'exhausted')),
	CONSTRAINT xbase_ck_attempts_retry_disposition_shape CHECK ((execution_state = 'error') = (retry_disposition IS NOT NULL)),
	CONSTRAINT xbase_ck_attempts_priority_source CHECK (priority_source IS NULL OR priority_source IN ('enqueued_here', 'linked_existing')),
	CONSTRAINT xbase_ck_attempts_retry_reason CHECK (retry_reason IS NULL OR retry_reason IN ('automatic_execution_error', 'domain_outcome', 'operator_cancel_retry')),
	CONSTRAINT xbase_ck_attempts_cancellation_disposition CHECK (cancellation_disposition IS NULL OR cancellation_disposition IN ('not_enqueued', 'dbos_cancelled', 'already_cancelled', 'observed_terminal', 'skipped_shared', 'failed')),
	CONSTRAINT xbase_ck_attempts_cancellation_origin CHECK (cancellation_origin IS NULL OR cancellation_origin IN ('local_operation', 'foreign_operation')),
	CONSTRAINT xbase_ck_attempts_service_class CHECK (requested_service_class IN ('urgent', 'standard', 'backfill')),
	CONSTRAINT xbase_ck_attempts_priorities CHECK (((requested_service_class = 'urgent' AND requested_service_priority = 100) OR (requested_service_class = 'standard' AND requested_service_priority = 1000) OR (requested_service_class = 'backfill' AND requested_service_priority = 10000)) AND (effective_service_priority IS NULL OR effective_service_priority > 0)),
	CONSTRAINT xbase_ck_attempts_enqueued_at CHECK ((enqueue_state IN ('enqueued', 'workflow_already_present')) = (enqueued_at IS NOT NULL)),
	CONSTRAINT xbase_ck_attempts_effective_priority CHECK ((enqueue_state IN ('enqueued', 'workflow_already_present')) = (effective_service_priority IS NOT NULL AND priority_source IS NOT NULL)),
	CONSTRAINT xbase_ck_attempts_source CHECK ((
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
)),
	CONSTRAINT xbase_ck_attempts_claim CHECK ((enqueue_state = 'claiming') = (current_claim_id IS NOT NULL)),
	CONSTRAINT xbase_ck_attempts_terminal CHECK ((
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
)),
	CONSTRAINT xbase_ck_attempts_failure CHECK (NOT (
  enqueue_state = 'enqueue_error'
  OR execution_state = 'error'
)
OR failure IS NOT NULL),
	CONSTRAINT xbase_ck_attempts_missing_observations CHECK ((
  missing_observation_count = 0
  AND missing_first_observed_at IS NULL
  AND missing_last_observed_at IS NULL
)
OR (
  missing_observation_count > 0
  AND missing_first_observed_at IS NOT NULL
  AND missing_last_observed_at IS NOT NULL
  AND missing_last_observed_at >= missing_first_observed_at
)),
	CONSTRAINT xbase_ck_attempts_cancellation CHECK ((
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
)),
	CONSTRAINT xbase_ck_attempts_enqueued_time CHECK (enqueued_at IS NULL OR enqueued_at >= created_at),
	CONSTRAINT xbase_ck_attempts_terminal_time CHECK (terminal_at IS NULL OR terminal_at >= created_at),
	CONSTRAINT xbase_ck_attempts_updated_time CHECK (updated_at >= created_at),
	CONSTRAINT xbase_ck_attempts_foreign_cancellation CHECK (cancellation_origin != 'foreign_operation' OR (cancellation_origin_operation_key IS NOT NULL AND foreign_cancellation_request_id IS NOT NULL))
)""",
    """CREATE TABLE xbase_enqueue_claims (
	item_id TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	claim_id TEXT NOT NULL,
	workflow_id TEXT NOT NULL,
	enqueue_try INTEGER NOT NULL,
	claimed_at TIMESTAMP WITH TIME ZONE NOT NULL,
	lease_expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
	enqueue_call_started_at TIMESTAMP WITH TIME ZONE,
	disposition TEXT NOT NULL,
	invalidated_at TIMESTAMP WITH TIME ZONE,
	invalidated_by TEXT,
	replacement_claim_id TEXT,
	resolved_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id, attempt, claim_id),
	CONSTRAINT xbase_fk_claims_attempt FOREIGN KEY(item_id, attempt) REFERENCES xbase_item_attempts (item_id, attempt) ON DELETE RESTRICT,
	CONSTRAINT xbase_fk_claims_replacement FOREIGN KEY(item_id, attempt, replacement_claim_id) REFERENCES xbase_enqueue_claims (item_id, attempt, claim_id) ON DELETE RESTRICT,
	CONSTRAINT xbase_ck_claims_enqueue_try CHECK (enqueue_try > 0),
	CONSTRAINT xbase_ck_claims_disposition CHECK (disposition IN ('claimed', 'call_started', 'outcome_recorded', 'expired', 'replaced', 'invalidated')),
	CONSTRAINT xbase_ck_claims_call_started CHECK ((
  disposition NOT IN ('call_started', 'outcome_recorded')
  OR enqueue_call_started_at IS NOT NULL
)
AND (
  enqueue_call_started_at IS NULL
  OR disposition IN (
    'call_started', 'outcome_recorded', 'expired', 'replaced', 'invalidated'
  )
)),
	CONSTRAINT xbase_ck_claims_replacement CHECK ((
  disposition = 'replaced'
  AND replacement_claim_id IS NOT NULL
  AND resolved_at IS NOT NULL
)
OR (
  disposition != 'replaced'
  AND replacement_claim_id IS NULL
)),
	CONSTRAINT xbase_ck_claims_lease CHECK (lease_expires_at > claimed_at),
	CONSTRAINT xbase_ck_claims_call_time CHECK (enqueue_call_started_at IS NULL OR enqueue_call_started_at >= claimed_at),
	CONSTRAINT xbase_ck_claims_invalidation CHECK ((invalidated_at IS NULL) = (invalidated_by IS NULL)),
	CONSTRAINT xbase_ck_claims_invalidation_disposition CHECK ((disposition = 'invalidated') = (invalidated_at IS NOT NULL)),
	CONSTRAINT xbase_ck_claims_resolution CHECK ((disposition IN ('outcome_recorded', 'expired', 'replaced', 'invalidated')) = (resolved_at IS NOT NULL)),
	CONSTRAINT xbase_uq_claims_workflow_provenance UNIQUE (item_id, attempt, claim_id, workflow_id)
)""",
    """CREATE TABLE xbase_missing_reobservations (
	item_id TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	last_reobserved_at TIMESTAMP WITH TIME ZONE NOT NULL,
	observation_count INTEGER NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id, attempt),
	CONSTRAINT xbase_fk_missing_reobservations_attempt FOREIGN KEY(item_id, attempt) REFERENCES xbase_item_attempts (item_id, attempt) ON DELETE RESTRICT,
	CONSTRAINT xbase_ck_missing_reobservations_count CHECK (observation_count > 0)
)""",
    """CREATE TABLE xbase_next_attempt_requests (
	request_id TEXT NOT NULL,
	item_id TEXT NOT NULL,
	request_key TEXT NOT NULL,
	source_attempt INTEGER NOT NULL,
	reason TEXT NOT NULL,
	eligibility_kind TEXT NOT NULL,
	eligibility_record_id TEXT NOT NULL,
	eligibility_digest TEXT NOT NULL,
	requested_by TEXT NOT NULL,
	operator_confirmed_at TIMESTAMP WITH TIME ZONE,
	max_attempts INTEGER,
	effective_max_attempts INTEGER NOT NULL,
	disposition TEXT NOT NULL,
	created_attempt INTEGER,
	rejection_detail TEXT,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	resolved_at TIMESTAMP WITH TIME ZONE NOT NULL,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (request_id),
	CONSTRAINT xbase_fk_requests_source_attempt FOREIGN KEY(item_id, source_attempt) REFERENCES xbase_item_attempts (item_id, attempt) ON DELETE RESTRICT,
	CONSTRAINT xbase_fk_requests_created_attempt FOREIGN KEY(item_id, created_attempt) REFERENCES xbase_item_attempts (item_id, attempt) ON DELETE RESTRICT,
	CONSTRAINT xbase_uq_requests_item_key UNIQUE (item_id, request_key),
	CONSTRAINT xbase_ck_requests_reason CHECK (reason IN ('domain_outcome', 'operator_cancel_retry')),
	CONSTRAINT xbase_ck_requests_disposition CHECK (disposition IN ('created', 'max_attempts_exhausted', 'ineligible', 'source_advanced')),
	CONSTRAINT xbase_ck_requests_source_attempt CHECK (source_attempt >= 0),
	CONSTRAINT xbase_ck_requests_attempt_bounds CHECK ((max_attempts IS NULL OR max_attempts > 0) AND effective_max_attempts > 0 AND (max_attempts IS NULL OR effective_max_attempts <= max_attempts)),
	CONSTRAINT xbase_ck_requests_reason_shape CHECK ((
  reason = 'domain_outcome'
  AND operator_confirmed_at IS NULL
)
OR (
  reason = 'operator_cancel_retry'
  AND operator_confirmed_at IS NOT NULL
)),
	CONSTRAINT xbase_ck_requests_result_shape CHECK ((
  disposition = 'created'
  AND created_attempt IS NOT NULL
)
OR (
  disposition != 'created'
  AND created_attempt IS NULL
)),
	CONSTRAINT xbase_ck_requests_created_attempt CHECK (created_attempt IS NULL OR created_attempt = source_attempt + 1),
	CONSTRAINT xbase_ck_requests_resolved_time CHECK (resolved_at >= created_at)
)""",
    """CREATE TABLE xbase_enqueue_compensations (
	item_id TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	claim_id TEXT NOT NULL,
	workflow_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	cancel_disposition TEXT NOT NULL,
	failure JSONB,
	first_absent_at TIMESTAMP WITH TIME ZONE,
	last_absent_at TIMESTAMP WITH TIME ZONE,
	absence_observation_count INTEGER DEFAULT 0 NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	resolved_at TIMESTAMP WITH TIME ZONE,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id, attempt, claim_id),
	CONSTRAINT xbase_fk_compensations_claim FOREIGN KEY(item_id, attempt, claim_id, workflow_id) REFERENCES xbase_enqueue_claims (item_id, attempt, claim_id, workflow_id) ON DELETE RESTRICT,
	CONSTRAINT xbase_ck_compensations_disposition CHECK (cancel_disposition IN ('pending', 'failed', 'cancelled', 'observed_terminal', 'skipped_shared', 'no_workflow_found')),
	CONSTRAINT xbase_ck_compensations_reason CHECK (reason IN ('invalidated_call_started_claim')),
	CONSTRAINT xbase_ck_compensations_resolution CHECK ((cancel_disposition IN ('pending', 'failed')) = (resolved_at IS NULL)),
	CONSTRAINT xbase_ck_compensations_failure CHECK ((cancel_disposition = 'failed') = (failure IS NOT NULL)),
	CONSTRAINT xbase_ck_compensations_absence_observations CHECK ((absence_observation_count = 0) = (first_absent_at IS NULL AND last_absent_at IS NULL) AND (first_absent_at IS NULL OR last_absent_at >= first_absent_at)),
	CONSTRAINT xbase_uq_compensations_workflow UNIQUE (item_id, attempt, claim_id, workflow_id)
)""",
    """CREATE TABLE xbase_enqueue_compensation_hazards (
	item_id TEXT NOT NULL,
	attempt INTEGER NOT NULL,
	claim_id TEXT NOT NULL,
	hazard_seq INTEGER NOT NULL,
	workflow_id TEXT NOT NULL,
	cancel_disposition TEXT NOT NULL,
	failure JSONB,
	created_at TIMESTAMP WITH TIME ZONE NOT NULL,
	resolved_at TIMESTAMP WITH TIME ZONE,
	change_seq BIGINT NOT NULL,
	PRIMARY KEY (item_id, attempt, claim_id, hazard_seq),
	CONSTRAINT xbase_fk_compensation_hazards_predecessor FOREIGN KEY(item_id, attempt, claim_id, workflow_id) REFERENCES xbase_enqueue_compensations (item_id, attempt, claim_id, workflow_id) ON DELETE RESTRICT,
	CONSTRAINT xbase_ck_compensation_hazards_bounded CHECK (hazard_seq = 1),
	CONSTRAINT xbase_ck_compensation_hazards_disposition CHECK (cancel_disposition IN ('pending', 'failed', 'cancelled', 'observed_terminal', 'skipped_shared', 'no_workflow_found')),
	CONSTRAINT xbase_ck_compensation_hazards_not_absent CHECK (cancel_disposition != 'no_workflow_found'),
	CONSTRAINT xbase_ck_compensation_hazards_resolution CHECK ((cancel_disposition IN ('pending', 'failed')) = (resolved_at IS NULL)),
	CONSTRAINT xbase_ck_compensation_hazards_failure CHECK ((cancel_disposition = 'failed') = (failure IS NOT NULL))
)""",
    """ALTER TABLE xbase_items ADD CONSTRAINT xbase_fk_items_current_attempt FOREIGN KEY(item_id, current_attempt) REFERENCES xbase_item_attempts (item_id, attempt) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED""",
    """ALTER TABLE xbase_item_attempts ADD CONSTRAINT xbase_fk_attempts_current_claim FOREIGN KEY(item_id, attempt, current_claim_id) REFERENCES xbase_enqueue_claims (item_id, attempt, claim_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED""",
    """CREATE INDEX ix_xbase_operations_change_seq ON xbase_operations (change_seq)""",
    """CREATE INDEX ix_xbase_operations_group ON xbase_operations (group_key)""",
    """CREATE INDEX ix_xbase_operations_registration_lease ON xbase_operations (registration_lease_expires_at)""",
    """CREATE INDEX ix_xbase_operations_status_updated ON xbase_operations (status, updated_at)""",
    """CREATE INDEX ix_xbase_throttle_blocked_until ON xbase_throttle_state (blocked_until)""",
    """CREATE INDEX ix_xbase_throttle_change_seq ON xbase_throttle_state (change_seq)""",
    """CREATE INDEX ix_xbase_throttle_hold_until ON xbase_throttle_state (hold_until)""",
    """CREATE INDEX ix_xbase_items_change_seq ON xbase_items (change_seq)""",
    """CREATE INDEX ix_xbase_items_operation_index ON xbase_items (operation_key, item_index)""",
    """CREATE INDEX ix_xbase_items_operation_key ON xbase_items (operation_key, item_key)""",
    """CREATE INDEX ix_xbase_items_schedule ON xbase_items (service_priority, shuffle_rank, item_id)""",
    """CREATE INDEX ix_xbase_attempts_change_seq ON xbase_item_attempts (change_seq)""",
    """CREATE INDEX ix_xbase_attempts_enqueue_state ON xbase_item_attempts (enqueue_state)""",
    """CREATE INDEX ix_xbase_attempts_execution_key ON xbase_item_attempts (execution_key, attempt)""",
    """CREATE INDEX ix_xbase_attempts_execution_state ON xbase_item_attempts (execution_state)""",
    """CREATE INDEX ix_xbase_attempts_workflow ON xbase_item_attempts (workflow_id)""",
    """CREATE INDEX ix_xbase_claims_change_seq ON xbase_enqueue_claims (change_seq)""",
    """CREATE INDEX ix_xbase_claims_lease ON xbase_enqueue_claims (lease_expires_at)""",
    """CREATE INDEX ix_xbase_claims_workflow_disposition ON xbase_enqueue_claims (workflow_id, disposition)""",
    """CREATE INDEX ix_xbase_missing_reobservations_change_seq ON xbase_missing_reobservations (change_seq)""",
    """CREATE INDEX ix_xbase_missing_reobservations_schedule ON xbase_missing_reobservations (last_reobserved_at, item_id, attempt)""",
    """CREATE INDEX ix_xbase_requests_change_seq ON xbase_next_attempt_requests (change_seq)""",
    """CREATE INDEX ix_xbase_requests_disposition ON xbase_next_attempt_requests (disposition)""",
    """CREATE INDEX ix_xbase_requests_item_source ON xbase_next_attempt_requests (item_id, source_attempt)""",
    """CREATE INDEX ix_xbase_compensations_change_seq ON xbase_enqueue_compensations (change_seq)""",
    """CREATE INDEX ix_xbase_compensations_unresolved ON xbase_enqueue_compensations (cancel_disposition) WHERE cancel_disposition IN ('pending', 'failed')""",
    """CREATE INDEX ix_xbase_compensations_workflow ON xbase_enqueue_compensations (workflow_id)""",
    """CREATE INDEX ix_xbase_compensation_hazards_change_seq ON xbase_enqueue_compensation_hazards (change_seq)""",
    """CREATE INDEX ix_xbase_compensation_hazards_unresolved ON xbase_enqueue_compensation_hazards (cancel_disposition) WHERE cancel_disposition IN ('pending', 'failed')""",
    """CREATE INDEX ix_xbase_compensation_hazards_workflow ON xbase_enqueue_compensation_hazards (workflow_id)""",
)


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
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
    return prefix


def _name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def _install_change_tracking(prefix: str) -> None:
    sequence = f"{prefix}_change_seq"
    function = f"{prefix}_assign_change_seq"
    trigger = f"{prefix}_assign_change_seq"
    _execute(f"CREATE SEQUENCE {sequence}")
    _execute(
        f"""
        CREATE FUNCTION {function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          NEW.change_seq := nextval(
            format('%I.%I', TG_TABLE_SCHEMA, '{sequence}')::regclass
          );
          RETURN NEW;
        END;
        $$
        """
    )

    hazard_function = f"{prefix}_guard_compensation_hazard_update"
    hazard_trigger = f"{prefix}_00_guard_compensation_hazard_update"
    _execute(
        f"""
        CREATE FUNCTION {hazard_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF OLD.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION 'resolved compensation hazards are immutable';
          END IF;
          IF ROW(NEW.item_id, NEW.attempt, NEW.claim_id, NEW.hazard_seq,
                 NEW.workflow_id, NEW.created_at) IS DISTINCT FROM
             ROW(OLD.item_id, OLD.attempt, OLD.claim_id, OLD.hazard_seq,
                 OLD.workflow_id, OLD.created_at) THEN
            RAISE EXCEPTION 'compensation hazard identity is immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {hazard_trigger}
        BEFORE UPDATE ON {_name(prefix, "enqueue_compensation_hazards")}
        FOR EACH ROW EXECUTE FUNCTION {hazard_function}()
        """
    )
    for suffix in TABLE_SUFFIXES:
        _execute(
            f"""
            CREATE TRIGGER {trigger}
            BEFORE INSERT OR UPDATE ON {_name(prefix, suffix)}
            FOR EACH ROW EXECUTE FUNCTION {function}()
            """
        )


def _install_lifecycle_guards(prefix: str) -> None:
    delete_function = f"{prefix}_reject_kernel_delete"
    delete_trigger = f"{prefix}_reject_kernel_delete"
    _execute(
        f"""
        CREATE FUNCTION {delete_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'kernel lifecycle rows cannot be deleted';
        END;
        $$
        """
    )
    for suffix in TABLE_SUFFIXES:
        _execute(
            f"""
            CREATE TRIGGER {delete_trigger}
            BEFORE DELETE ON {_name(prefix, suffix)}
            FOR EACH ROW EXECUTE FUNCTION {delete_function}()
            """
        )

    terminal_function = f"{prefix}_reject_terminal_attempt_mutation"
    terminal_trigger = f"{prefix}_00_reject_terminal_attempt_mutation"
    terminal_states = TERMINAL_STATES
    _execute(
        f"""
        CREATE FUNCTION {terminal_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.execution_state IN ({terminal_states}) THEN
            IF NEW IS DISTINCT FROM OLD THEN
              RAISE EXCEPTION 'terminal item attempts are immutable';
            END IF;
            RETURN NULL;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {terminal_trigger}
        BEFORE UPDATE ON {_name(prefix, "item_attempts")}
        FOR EACH ROW EXECUTE FUNCTION {terminal_function}()
        """
    )

    operation_function = f"{prefix}_guard_operation_update"
    operation_trigger = f"{prefix}_00_guard_operation_update"
    _execute(
        f"""
        CREATE FUNCTION {operation_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF ROW(
            NEW.operation_key,
            NEW.group_key,
            NEW.workflow_role,
            NEW.requested_count,
            NEW.manifest_version,
            NEW.manifest_digest,
            NEW.manifest_page_size,
            NEW.manifest_page_count,
            NEW.operation_execution_recipe_digest,
            NEW.target_key,
            NEW.target_version,
            NEW.target_contract_digest,
            NEW.retry_policy,
            NEW.spec,
            NEW.metadata,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.operation_key,
            OLD.group_key,
            OLD.workflow_role,
            OLD.requested_count,
            OLD.manifest_version,
            OLD.manifest_digest,
            OLD.manifest_page_size,
            OLD.manifest_page_count,
            OLD.operation_execution_recipe_digest,
            OLD.target_key,
            OLD.target_version,
            OLD.target_contract_digest,
            OLD.retry_policy,
            OLD.spec,
            OLD.metadata,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'Operation identity fields are immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {operation_trigger}
        BEFORE UPDATE ON {_name(prefix, "operations")}
        FOR EACH ROW EXECUTE FUNCTION {operation_function}()
        """
    )

    item_function = f"{prefix}_guard_item_update"
    item_trigger = f"{prefix}_00_guard_item_update"
    _execute(
        f"""
        CREATE FUNCTION {item_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.operation_key,
            NEW.item_index,
            NEW.item_key,
            NEW.shuffle_rank,
            NEW.service_class,
            NEW.service_priority,
            NEW.spec,
            NEW.insert_status,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.operation_key,
            OLD.item_index,
            OLD.item_key,
            OLD.shuffle_rank,
            OLD.service_class,
            OLD.service_priority,
            OLD.spec,
            OLD.insert_status,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'Item identity fields are immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {item_trigger}
        BEFORE UPDATE ON {_name(prefix, "items")}
        FOR EACH ROW EXECUTE FUNCTION {item_function}()
        """
    )

    claim_function = f"{prefix}_guard_enqueue_claim_update"
    claim_trigger = f"{prefix}_00_guard_enqueue_claim_update"
    _execute(
        f"""
        CREATE FUNCTION {claim_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF OLD.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION 'resolved enqueue Claims are immutable';
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.attempt,
            NEW.claim_id,
            NEW.workflow_id,
            NEW.enqueue_try,
            NEW.claimed_at,
            NEW.lease_expires_at,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.attempt,
            OLD.claim_id,
            OLD.workflow_id,
            OLD.enqueue_try,
            OLD.claimed_at,
            OLD.lease_expires_at,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'enqueue Claim identity is immutable';
          END IF;
          IF OLD.enqueue_call_started_at IS NOT NULL
             AND NEW.enqueue_call_started_at IS DISTINCT FROM
                 OLD.enqueue_call_started_at THEN
            RAISE EXCEPTION 'enqueue Claim call-start fact is immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {claim_trigger}
        BEFORE UPDATE ON {_name(prefix, "enqueue_claims")}
        FOR EACH ROW EXECUTE FUNCTION {claim_function}()
        """
    )

    compensation_function = f"{prefix}_guard_compensation_update"
    compensation_trigger = f"{prefix}_00_guard_compensation_update"
    _execute(
        f"""
        CREATE FUNCTION {compensation_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          IF OLD.resolved_at IS NOT NULL THEN
            RAISE EXCEPTION 'resolved enqueue compensations are immutable';
          END IF;
          IF ROW(
            NEW.item_id,
            NEW.attempt,
            NEW.claim_id,
            NEW.workflow_id,
            NEW.reason,
            NEW.created_at
          ) IS DISTINCT FROM ROW(
            OLD.item_id,
            OLD.attempt,
            OLD.claim_id,
            OLD.workflow_id,
            OLD.reason,
            OLD.created_at
          ) THEN
            RAISE EXCEPTION 'enqueue compensation identity is immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {compensation_trigger}
        BEFORE UPDATE ON {_name(prefix, "enqueue_compensations")}
        FOR EACH ROW EXECUTE FUNCTION {compensation_function}()
        """
    )

    request_function = f"{prefix}_guard_next_attempt_request_update"
    request_trigger = f"{prefix}_00_guard_next_attempt_request_update"
    _execute(
        f"""
        CREATE FUNCTION {request_function}() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
          END IF;
          RAISE EXCEPTION 'next-Attempt request ledger is immutable';
        END;
        $$
        """
    )
    _execute(
        f"""
        CREATE TRIGGER {request_trigger}
        BEFORE UPDATE ON {_name(prefix, "next_attempt_requests")}
        FOR EACH ROW EXECUTE FUNCTION {request_function}()
        """
    )


def upgrade() -> None:
    prefix = _prefix()
    for statement in BASELINE_DDL:
        _execute(statement.replace("xbase", prefix))
    _install_change_tracking(prefix)
    _install_lifecycle_guards(prefix)


def downgrade() -> None:
    prefix = _prefix()
    _execute(
        f"ALTER TABLE {_name(prefix, 'items')} DROP CONSTRAINT "
        f"{_name(prefix, 'fk_items_current_attempt')}"
    )
    _execute(
        f"ALTER TABLE {_name(prefix, 'item_attempts')} DROP CONSTRAINT "
        f"{_name(prefix, 'fk_attempts_current_claim')}"
    )
    for suffix in (
        "enqueue_compensation_hazards",
        "enqueue_compensations",
        "next_attempt_requests",
        "missing_reobservations",
        "enqueue_claims",
        "item_attempts",
        "items",
        "throttle_state",
        "operations",
    ):
        _execute(f"DROP TABLE {_name(prefix, suffix)}")
    _execute(
        f"DROP FUNCTION {_name(prefix, 'guard_next_attempt_request_update')}()"
    )
    _execute(f"DROP FUNCTION {_name(prefix, 'guard_compensation_update')}()")
    _execute(
        f"DROP FUNCTION {_name(prefix, 'guard_compensation_hazard_update')}()"
    )
    _execute(f"DROP FUNCTION {_name(prefix, 'guard_enqueue_claim_update')}()")
    _execute(f"DROP FUNCTION {_name(prefix, 'guard_item_update')}()")
    _execute(f"DROP FUNCTION {_name(prefix, 'guard_operation_update')}()")
    _execute(
        f"DROP FUNCTION {_name(prefix, 'reject_terminal_attempt_mutation')}()"
    )
    _execute(f"DROP FUNCTION {_name(prefix, 'reject_kernel_delete')}()")
    _execute(f"DROP FUNCTION {_name(prefix, 'assign_change_seq')}()")
    _execute(f"DROP SEQUENCE {_name(prefix, 'change_seq')}")
