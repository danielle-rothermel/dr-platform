"""Append-only enqueue Claim selection and authority transitions."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from sqlalchemy import Connection, Engine, and_, insert, select, text, update

from dr_platform.db import PlatformSchema
from dr_platform.manifests import ExecutionTargetRef
from dr_platform.records import (
    AttemptRecord,
    EnqueueClaimRecord,
    FailureSnapshot,
    ItemRecord,
)
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    EnqueueClaimDisposition,
    EnqueueCompensationDisposition,
    EnqueueCompensationReason,
    FailureClass,
    OperationStatus,
    PrioritySource,
)
from dr_platform.submission import EXPORT_BARRIER_ADVISORY_KEY

DEFAULT_CLAIM_PAGE_SIZE = 500
DEFAULT_CLAIM_LEASE_SECONDS = 60
CLAIM_SELECTION_ADVISORY_KEY = 4_592_991_109_185_853_313

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
ClaimIdFactory = Callable[[], str]


class ClaimTargetAdmission(Protocol):
    def __call__(
        self,
        target_refs: tuple[ExecutionTargetRef, ...],
    ) -> None: ...


class ClaimAuthorityError(RuntimeError):
    """A caller does not own the current valid enqueue Claim."""


class ClaimConflictError(RuntimeError):
    """A Claim replay conflicts with its durable facts."""


class ClaimPageOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_size: PositiveInt = DEFAULT_CLAIM_PAGE_SIZE
    lease_seconds: PositiveInt = DEFAULT_CLAIM_LEASE_SECONDS


class ClaimedAttempt(BaseModel):
    """Bounded receipt granting authority to prepare one enqueue call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target_ref: ExecutionTargetRef
    item: ItemRecord
    attempt_record: AttemptRecord
    claim: EnqueueClaimRecord

    @property
    def operation_key(self) -> str:
        return self.item.operation_key

    @property
    def item_id(self) -> str:
        return self.item.item_id

    @property
    def attempt(self) -> int:
        return self.attempt_record.attempt

    @property
    def claim_id(self) -> str:
        return self.claim.claim_id

    @property
    def workflow_id(self) -> str:
        return self.claim.workflow_id

    @property
    def enqueue_try(self) -> int:
        return self.claim.enqueue_try

    @property
    def service_priority(self) -> int:
        return self.item.service_priority

    @property
    def claimed_at(self) -> datetime:
        return self.claim.claimed_at

    @property
    def lease_expires_at(self) -> datetime:
        return self.claim.lease_expires_at


class ClaimPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claims: tuple[ClaimedAttempt, ...]


class _PhysicalOutcomeDisposition(StrEnum):
    ENQUEUED = "enqueued"
    WORKFLOW_ALREADY_PRESENT = "workflow_already_present"
    ENQUEUE_ERROR = "enqueue_error"
    UNCERTAIN = "uncertain"


def claim_pending_attempts(  # noqa: PLR0913 -- operation-scoped facade
    engine: Engine,
    *,
    admit_targets: ClaimTargetAdmission,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    claim_id_factory: ClaimIdFactory | None = None,
    operation_key: str | None = None,
) -> ClaimPage:
    """Claim one scheduling-ordered bounded page of pending Attempts."""
    selected_schema = schema or PlatformSchema()
    selected_options = options or ClaimPageOptions()
    make_claim_id = claim_id_factory or _random_claim_id
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_claim_selection_lock(connection)
        candidates = _pending_candidates(
            connection=connection,
            schema=selected_schema,
            limit=selected_options.page_size,
            operation_key=operation_key,
        )
        if not candidates:
            return ClaimPage(claims=())
        admit_targets(_candidate_target_refs(candidates))
        locked = _lock_candidate_hierarchy(
            connection,
            schema=selected_schema,
            candidates=candidates,
        )
        now = _database_now(connection)
        lease_expires_at = now + timedelta(
            seconds=selected_options.lease_seconds
        )
        claimed: list[ClaimedAttempt] = []
        changed_operations: set[str] = set()
        for candidate in candidates:
            key = (candidate["item_id"], candidate["attempt"])
            attempt_row = locked.attempts.get(key)
            if (
                not _locked_candidate_is_enqueue_eligible(
                    locked=locked,
                    candidate=candidate,
                )
                or attempt_row is None
                or attempt_row["enqueue_state"]
                != AttemptEnqueueState.PENDING.value
                or attempt_row["current_claim_id"] is not None
            ):
                continue
            enqueue_try = int(attempt_row["enqueue_try"]) + 1
            max_enqueue_tries = _max_enqueue_tries(
                locked.operations[candidate["operation_key"]]
            )
            if enqueue_try > max_enqueue_tries:
                continue
            claim_id = _validated_claim_id(make_claim_id())
            connection.execute(
                insert(selected_schema.enqueue_claims).values(
                    item_id=candidate["item_id"],
                    attempt=candidate["attempt"],
                    claim_id=claim_id,
                    workflow_id=candidate["workflow_id"],
                    enqueue_try=enqueue_try,
                    claimed_at=now,
                    lease_expires_at=lease_expires_at,
                    disposition=EnqueueClaimDisposition.CLAIMED.value,
                    created_at=now,
                )
            )
            outcome = connection.execute(
                update(selected_schema.item_attempts)
                .where(
                    and_(
                        selected_schema.item_attempts.c.item_id
                        == candidate["item_id"],
                        selected_schema.item_attempts.c.attempt
                        == candidate["attempt"],
                        selected_schema.item_attempts.c.enqueue_state
                        == AttemptEnqueueState.PENDING.value,
                        selected_schema.item_attempts.c.current_claim_id.is_(
                            None
                        ),
                        selected_schema.item_attempts.c.execution_state
                        == AttemptExecutionState.NOT_STARTED.value,
                        selected_schema.item_attempts.c.cancellation_request_id.is_(
                            None
                        ),
                    )
                )
                .values(
                    enqueue_state=AttemptEnqueueState.CLAIMING.value,
                    enqueue_try=enqueue_try,
                    current_claim_id=claim_id,
                    updated_at=now,
                )
            )
            if outcome.rowcount != 1:
                raise ClaimConflictError(
                    "pending Attempt Claim CAS lost after hierarchy locks"
                )
            changed_operations.add(candidate["operation_key"])
            claimed.append(
                _load_claimed_attempt(
                    connection,
                    schema=selected_schema,
                    candidate=candidate,
                    claim_id=claim_id,
                )
            )
        _advance_operation_cuts(
            connection,
            schema=selected_schema,
            operation_keys=changed_operations,
            now=now,
        )
        return ClaimPage(claims=tuple(claimed))


def replace_expired_unstarted_claims(  # noqa: PLR0913 -- operation-scoped facade
    engine: Engine,
    *,
    admit_targets: ClaimTargetAdmission,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    claim_id_factory: ClaimIdFactory | None = None,
    operation_key: str | None = None,
) -> ClaimPage:
    """Replace expired Claims that provably never crossed the DBOS call."""
    selected_schema = schema or PlatformSchema()
    selected_options = options or ClaimPageOptions()
    make_claim_id = claim_id_factory or _random_claim_id
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_claim_selection_lock(connection)
        candidates = _expired_unstarted_candidates(
            connection=connection,
            schema=selected_schema,
            limit=selected_options.page_size,
            operation_key=operation_key,
        )
        if not candidates:
            return ClaimPage(claims=())
        admit_targets(_candidate_target_refs(candidates))
        locked = _lock_candidate_hierarchy(
            connection,
            schema=selected_schema,
            candidates=candidates,
            claim_keys=[_candidate_claim_key(row) for row in candidates],
        )
        now = _database_now(connection)
        lease_expires_at = now + timedelta(
            seconds=selected_options.lease_seconds
        )
        replacements: list[ClaimedAttempt] = []
        changed_operations: set[str] = set()
        for candidate in candidates:
            key = (candidate["item_id"], candidate["attempt"])
            attempt_row = locked.attempts.get(key)
            claim_row = locked.claims.get(_candidate_claim_key(candidate))
            if not _is_replaceable_expired_claim(
                attempt_row=attempt_row,
                claim_row=claim_row,
                expected_claim_id=candidate["claim_id"],
                now=now,
            ) or not _locked_candidate_is_enqueue_eligible(
                locked=locked,
                candidate=candidate,
            ):
                continue
            assert attempt_row is not None
            replacement_claim_id = _validated_claim_id(make_claim_id())
            enqueue_try = int(attempt_row["enqueue_try"])
            connection.execute(
                insert(selected_schema.enqueue_claims).values(
                    item_id=candidate["item_id"],
                    attempt=candidate["attempt"],
                    claim_id=replacement_claim_id,
                    workflow_id=candidate["workflow_id"],
                    enqueue_try=enqueue_try,
                    claimed_at=now,
                    lease_expires_at=lease_expires_at,
                    disposition=EnqueueClaimDisposition.CLAIMED.value,
                    created_at=now,
                )
            )
            replaced = connection.execute(
                update(selected_schema.enqueue_claims)
                .where(
                    and_(
                        selected_schema.enqueue_claims.c.item_id
                        == candidate["item_id"],
                        selected_schema.enqueue_claims.c.attempt
                        == candidate["attempt"],
                        selected_schema.enqueue_claims.c.claim_id
                        == candidate["claim_id"],
                        selected_schema.enqueue_claims.c.disposition
                        == EnqueueClaimDisposition.CLAIMED.value,
                        selected_schema.enqueue_claims.c.lease_expires_at
                        <= now,
                        selected_schema.enqueue_claims.c.enqueue_call_started_at.is_(
                            None
                        ),
                    )
                )
                .values(
                    disposition=EnqueueClaimDisposition.REPLACED.value,
                    replacement_claim_id=replacement_claim_id,
                    resolved_at=now,
                )
            )
            if replaced.rowcount != 1:
                raise ClaimConflictError(
                    "expired Claim replacement CAS lost after hierarchy locks"
                )
            advanced = connection.execute(
                update(selected_schema.item_attempts)
                .where(
                    and_(
                        selected_schema.item_attempts.c.item_id
                        == candidate["item_id"],
                        selected_schema.item_attempts.c.attempt
                        == candidate["attempt"],
                        selected_schema.item_attempts.c.enqueue_state
                        == AttemptEnqueueState.CLAIMING.value,
                        selected_schema.item_attempts.c.current_claim_id
                        == candidate["claim_id"],
                        selected_schema.item_attempts.c.execution_state
                        == AttemptExecutionState.NOT_STARTED.value,
                        selected_schema.item_attempts.c.cancellation_request_id.is_(
                            None
                        ),
                    )
                )
                .values(
                    current_claim_id=replacement_claim_id,
                    updated_at=now,
                )
            )
            if advanced.rowcount != 1:
                raise ClaimConflictError(
                    "Attempt replacement Claim pointer CAS lost"
                )
            changed_operations.add(candidate["operation_key"])
            replacements.append(
                _load_claimed_attempt(
                    connection,
                    schema=selected_schema,
                    candidate=candidate,
                    claim_id=replacement_claim_id,
                )
            )
        _advance_operation_cuts(
            connection,
            schema=selected_schema,
            operation_keys=changed_operations,
            now=now,
        )
        return ClaimPage(claims=tuple(replacements))


def load_call_started_recovery_page(
    engine: Engine,
    *,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    operation_key: str | None = None,
) -> ClaimPage:
    """Load a bounded page requiring authoritative DBOS observation."""
    selected_schema = schema or PlatformSchema()
    selected_options = options or ClaimPageOptions()
    with engine.connect() as connection:
        candidates = _call_started_recovery_candidates(
            connection=connection,
            schema=selected_schema,
            limit=selected_options.page_size,
            operation_key=operation_key,
        )
        return ClaimPage(
            claims=tuple(
                _load_claimed_attempt(
                    connection,
                    schema=selected_schema,
                    candidate=candidate,
                    claim_id=candidate["claim_id"],
                )
                for candidate in candidates
            )
        )


def start_enqueue_call(
    engine: Engine,
    *,
    item_id: str,
    attempt: int,
    claim_id: str,
    schema: PlatformSchema | None = None,
) -> EnqueueClaimRecord:
    """Commit the Claim's one-way call-start fact before DBOS is invoked."""
    _, record = _start_enqueue_call_transition(
        engine,
        item_id=item_id,
        attempt=attempt,
        claim_id=claim_id,
        schema=schema,
    )
    return record


def _start_enqueue_call_transition(
    engine: Engine,
    *,
    item_id: str,
    attempt: int,
    claim_id: str,
    schema: PlatformSchema | None = None,
) -> tuple[bool, EnqueueClaimRecord]:
    selected_schema = schema or PlatformSchema()
    context = _claim_context(
        engine=engine,
        schema=selected_schema,
        item_id=item_id,
        attempt=attempt,
        claim_id=claim_id,
    )
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        locked = _lock_candidate_hierarchy(
            connection,
            schema=selected_schema,
            candidates=[context],
            claim_keys=[(item_id, attempt, claim_id)],
        )
        now = _database_now(connection)
        attempt_row = locked.attempts.get((item_id, attempt))
        claim_row = locked.claims.get((item_id, attempt, claim_id))
        if attempt_row is None or claim_row is None:
            raise ClaimAuthorityError("unknown enqueue Claim")
        if (
            claim_row["disposition"]
            == EnqueueClaimDisposition.CALL_STARTED.value
        ):
            return False, _claim_record(claim_row)
        if (
            not _locked_candidate_is_enqueue_eligible(
                locked=locked,
                candidate=context,
            )
            or attempt_row["enqueue_state"]
            != AttemptEnqueueState.CLAIMING.value
            or attempt_row["current_claim_id"] != claim_id
            or claim_row["disposition"]
            != EnqueueClaimDisposition.CLAIMED.value
            or claim_row["lease_expires_at"] <= now
        ):
            raise ClaimAuthorityError(
                "enqueue Claim is not current, valid, and unexpired"
            )
        outcome = connection.execute(
            update(selected_schema.enqueue_claims)
            .where(
                and_(
                    selected_schema.enqueue_claims.c.item_id == item_id,
                    selected_schema.enqueue_claims.c.attempt == attempt,
                    selected_schema.enqueue_claims.c.claim_id == claim_id,
                    selected_schema.enqueue_claims.c.disposition
                    == EnqueueClaimDisposition.CLAIMED.value,
                    selected_schema.enqueue_claims.c.lease_expires_at > now,
                    selected_schema.enqueue_claims.c.enqueue_call_started_at.is_(
                        None
                    ),
                )
            )
            .values(
                enqueue_call_started_at=now,
                disposition=EnqueueClaimDisposition.CALL_STARTED.value,
            )
        )
        if outcome.rowcount != 1:
            raise ClaimAuthorityError("enqueue call-start Claim CAS lost")
        updated = dict(
            connection.execute(
                select(selected_schema.enqueue_claims).where(
                    and_(
                        selected_schema.enqueue_claims.c.item_id == item_id,
                        selected_schema.enqueue_claims.c.attempt == attempt,
                        selected_schema.enqueue_claims.c.claim_id == claim_id,
                    )
                )
            )
            .mappings()
            .one()
        )
        return True, _claim_record(updated)


def invalidate_stale_claim(  # noqa: PLR0913 -- exact Claim key + actor
    engine: Engine,
    *,
    item_id: str,
    attempt: int,
    claim_id: str,
    invalidated_by: str,
    schema: PlatformSchema | None = None,
) -> EnqueueClaimRecord:
    """Resolve a non-current Claim without mutating its Attempt."""
    if not invalidated_by:
        raise ValueError("invalidated_by must be non-empty")
    selected_schema = schema or PlatformSchema()
    context = _claim_context(
        engine=engine,
        schema=selected_schema,
        item_id=item_id,
        attempt=attempt,
        claim_id=claim_id,
    )
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        locked = _lock_candidate_hierarchy(
            connection,
            schema=selected_schema,
            candidates=[context],
            claim_keys=[(item_id, attempt, claim_id)],
        )
        now = _database_now(connection)
        attempt_row = locked.attempts.get((item_id, attempt))
        claim_row = locked.claims.get((item_id, attempt, claim_id))
        if attempt_row is None or claim_row is None:
            raise ClaimAuthorityError("unknown enqueue Claim")
        if claim_row["disposition"] == EnqueueClaimDisposition.INVALIDATED:
            if claim_row["invalidated_by"] != invalidated_by:
                raise ClaimConflictError(
                    "Claim invalidation replay has a different actor"
                )
            return _claim_record(claim_row)
        if attempt_row["current_claim_id"] == claim_id:
            raise ClaimAuthorityError(
                "current Claim must be transitioned with its Attempt"
            )
        if claim_row["disposition"] not in {
            EnqueueClaimDisposition.CLAIMED.value,
            EnqueueClaimDisposition.CALL_STARTED.value,
        }:
            raise ClaimConflictError("resolved Claim cannot be invalidated")
        connection.execute(
            update(selected_schema.enqueue_claims)
            .where(
                and_(
                    selected_schema.enqueue_claims.c.item_id == item_id,
                    selected_schema.enqueue_claims.c.attempt == attempt,
                    selected_schema.enqueue_claims.c.claim_id == claim_id,
                    selected_schema.enqueue_claims.c.disposition.in_(
                        (
                            EnqueueClaimDisposition.CLAIMED.value,
                            EnqueueClaimDisposition.CALL_STARTED.value,
                        )
                    ),
                )
            )
            .values(
                disposition=EnqueueClaimDisposition.INVALIDATED.value,
                invalidated_at=now,
                invalidated_by=invalidated_by,
                resolved_at=now,
            )
        )
        updated = dict(
            connection.execute(
                select(selected_schema.enqueue_claims).where(
                    and_(
                        selected_schema.enqueue_claims.c.item_id == item_id,
                        selected_schema.enqueue_claims.c.attempt == attempt,
                        selected_schema.enqueue_claims.c.claim_id == claim_id,
                    )
                )
            )
            .mappings()
            .one()
        )
        return _claim_record(updated)


class PostgresClaimTransitionStore:
    """Concrete transaction store consumed structurally by enqueue runtime."""

    def __init__(
        self,
        engine: Engine,
        *,
        schema: PlatformSchema | None = None,
        options: ClaimPageOptions | None = None,
        claim_id_factory: ClaimIdFactory | None = None,
        admit_targets: ClaimTargetAdmission | None = None,
    ) -> None:
        self._engine = engine
        self._schema = schema or PlatformSchema()
        self._options = options or ClaimPageOptions()
        self._claim_id_factory = claim_id_factory or _random_claim_id
        self._admit_targets = admit_targets

    def mark_enqueue_call_started(
        self,
        *,
        claim: EnqueueClaimRecord,
    ) -> bool:
        try:
            started, _ = _start_enqueue_call_transition(
                self._engine,
                item_id=claim.item_id,
                attempt=claim.attempt,
                claim_id=claim.claim_id,
                schema=self._schema,
            )
        except ClaimAuthorityError:
            return False
        return started

    def record_enqueue_outcome(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: Any,
    ) -> bool:
        disposition = _physical_outcome_disposition(outcome)
        if disposition is _PhysicalOutcomeDisposition.UNCERTAIN:
            raise ValueError("uncertain enqueue outcomes remain CALL_STARTED")
        context = _claim_context(
            engine=self._engine,
            schema=self._schema,
            item_id=claim.item_id,
            attempt=claim.attempt,
            claim_id=claim.claim_id,
        )
        with self._engine.begin() as connection:
            _acquire_export_writer_lock(connection)
            locked = _lock_candidate_hierarchy(
                connection,
                schema=self._schema,
                candidates=[context],
                claim_keys=[(claim.item_id, claim.attempt, claim.claim_id)],
            )
            now = _database_now(connection)
            attempt_row = locked.attempts.get((claim.item_id, claim.attempt))
            claim_row = locked.claims.get(
                (claim.item_id, claim.attempt, claim.claim_id)
            )
            if attempt_row is None or claim_row is None:
                return False
            if (
                outcome.workflow_id != claim.workflow_id
                or claim_row["workflow_id"] != claim.workflow_id
            ):
                raise ClaimConflictError(
                    "physical enqueue outcome workflow identity changed"
                )
            if (
                attempt_row["enqueue_state"]
                != AttemptEnqueueState.CLAIMING.value
                or attempt_row["current_claim_id"] != claim.claim_id
                or claim_row["disposition"]
                != EnqueueClaimDisposition.CALL_STARTED.value
            ):
                return False

            attempt_values = _attempt_outcome_values(
                disposition=disposition,
                outcome=outcome,
                now=now,
            )
            attempt_outcome = connection.execute(
                update(self._schema.item_attempts)
                .where(
                    and_(
                        self._schema.item_attempts.c.item_id == claim.item_id,
                        self._schema.item_attempts.c.attempt == claim.attempt,
                        self._schema.item_attempts.c.enqueue_state
                        == AttemptEnqueueState.CLAIMING.value,
                        self._schema.item_attempts.c.current_claim_id
                        == claim.claim_id,
                    )
                )
                .values(**attempt_values)
            )
            if attempt_outcome.rowcount != 1:
                return False
            claim_outcome = connection.execute(
                update(self._schema.enqueue_claims)
                .where(
                    and_(
                        self._schema.enqueue_claims.c.item_id == claim.item_id,
                        self._schema.enqueue_claims.c.attempt == claim.attempt,
                        self._schema.enqueue_claims.c.claim_id
                        == claim.claim_id,
                        self._schema.enqueue_claims.c.disposition
                        == EnqueueClaimDisposition.CALL_STARTED.value,
                    )
                )
                .values(
                    disposition=EnqueueClaimDisposition.OUTCOME_RECORDED.value,
                    resolved_at=now,
                )
            )
            if claim_outcome.rowcount != 1:
                raise ClaimConflictError(
                    "Claim outcome CAS lost after Attempt outcome CAS"
                )
            _refresh_operation_enqueue_aggregate(
                connection,
                schema=self._schema,
                operation_key=context["operation_key"],
                now=now,
            )
            return True

    def ensure_lost_outcome_compensation(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: Any,
    ) -> None:
        disposition = _physical_outcome_disposition(outcome)
        if disposition not in {
            _PhysicalOutcomeDisposition.ENQUEUED,
            _PhysicalOutcomeDisposition.WORKFLOW_ALREADY_PRESENT,
        }:
            return
        if outcome.workflow_id != claim.workflow_id:
            raise ClaimConflictError("compensation workflow identity changed")
        context = _claim_context(
            engine=self._engine,
            schema=self._schema,
            item_id=claim.item_id,
            attempt=claim.attempt,
            claim_id=claim.claim_id,
        )
        compensations = self._schema.enqueue_compensations
        with self._engine.begin() as connection:
            _acquire_export_writer_lock(connection)
            locked = _lock_candidate_hierarchy(
                connection,
                schema=self._schema,
                candidates=[context],
                claim_keys=[(claim.item_id, claim.attempt, claim.claim_id)],
            )
            claim_row = locked.claims.get(
                (claim.item_id, claim.attempt, claim.claim_id)
            )
            attempt_row = locked.attempts.get((claim.item_id, claim.attempt))
            if claim_row is None:
                raise ClaimAuthorityError("unknown enqueue Claim")
            durable_workflow_id = claim_row["workflow_id"]
            if (
                durable_workflow_id != claim.workflow_id
                or outcome.workflow_id != durable_workflow_id
            ):
                raise ClaimConflictError(
                    "compensation Claim workflow provenance changed"
                )
            if claim_row["disposition"] == (
                EnqueueClaimDisposition.OUTCOME_RECORDED.value
            ):
                expected_enqueue_state = {
                    _PhysicalOutcomeDisposition.ENQUEUED: (
                        AttemptEnqueueState.ENQUEUED.value
                    ),
                    _PhysicalOutcomeDisposition.WORKFLOW_ALREADY_PRESENT: (
                        AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT.value
                    ),
                }[disposition]
                if (
                    attempt_row is None
                    or attempt_row["workflow_id"] != durable_workflow_id
                    or attempt_row["enqueue_state"] != expected_enqueue_state
                ):
                    raise ClaimConflictError(
                        "recorded enqueue outcome conflicts with durable "
                        "Attempt"
                    )
                return
            if (
                claim_row["disposition"]
                != EnqueueClaimDisposition.INVALIDATED.value
                or claim_row["enqueue_call_started_at"] is None
            ):
                raise ClaimAuthorityError(
                    "lost enqueue outcome has no invalidated call-started "
                    "Claim"
                )
            existing = (
                connection.execute(
                    select(compensations)
                    .where(
                        and_(
                            compensations.c.item_id == claim.item_id,
                            compensations.c.attempt == claim.attempt,
                            compensations.c.claim_id == claim.claim_id,
                        )
                    )
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["workflow_id"] != claim.workflow_id or existing[
                    "reason"
                ] != (
                    EnqueueCompensationReason.INVALIDATED_CALL_STARTED_CLAIM.value
                ):
                    raise ClaimConflictError(
                        "enqueue compensation replay conflicts"
                    )
                return
            now = _database_now(connection)
            connection.execute(
                insert(compensations).values(
                    item_id=claim.item_id,
                    attempt=claim.attempt,
                    claim_id=claim.claim_id,
                    workflow_id=durable_workflow_id,
                    reason=(
                        EnqueueCompensationReason.INVALIDATED_CALL_STARTED_CLAIM.value
                    ),
                    cancel_disposition=(
                        EnqueueCompensationDisposition.PENDING.value
                    ),
                    created_at=now,
                )
            )

    def replace_call_started_after_absence(
        self,
        *,
        claimed: ClaimedAttempt,
    ) -> ClaimedAttempt | None:
        """Replace an expired call-started Claim after proven DBOS absence."""
        if self._admit_targets is None:
            raise ClaimAuthorityError(
                "call-started replacement requires target admission"
            )
        self._admit_targets((claimed.target_ref,))
        context = _claim_context(
            engine=self._engine,
            schema=self._schema,
            item_id=claimed.item_id,
            attempt=claimed.attempt,
            claim_id=claimed.claim_id,
        )
        if _candidate_target_refs([context]) != (claimed.target_ref,):
            raise ClaimConflictError("recovery target reference changed")
        with self._engine.begin() as connection:
            _acquire_export_writer_lock(connection)
            locked = _lock_candidate_hierarchy(
                connection,
                schema=self._schema,
                candidates=[context],
                claim_keys=[
                    (claimed.item_id, claimed.attempt, claimed.claim_id)
                ],
            )
            now = _database_now(connection)
            attempt_row = locked.attempts.get(
                (claimed.item_id, claimed.attempt)
            )
            claim_row = locked.claims.get(
                (claimed.item_id, claimed.attempt, claimed.claim_id)
            )
            if (
                attempt_row is None
                or claim_row is None
                or not _locked_candidate_is_enqueue_eligible(
                    locked=locked,
                    candidate=context,
                )
                or attempt_row["enqueue_state"]
                != AttemptEnqueueState.CLAIMING.value
                or attempt_row["current_claim_id"] != claimed.claim_id
                or claim_row["disposition"]
                != EnqueueClaimDisposition.CALL_STARTED.value
                or claim_row["lease_expires_at"] > now
                or claim_row["workflow_id"] != claimed.workflow_id
            ):
                return None
            enqueue_try = int(attempt_row["enqueue_try"]) + 1
            operation_row = locked.operations[claimed.operation_key]
            if enqueue_try > _max_enqueue_tries(operation_row):
                _resolve_exhausted_call_started_claim(
                    connection,
                    schema=self._schema,
                    claimed=claimed,
                    current_enqueue_try=int(attempt_row["enqueue_try"]),
                    max_enqueue_tries=_max_enqueue_tries(operation_row),
                    now=now,
                )
                return None
            replacement_claim_id = _validated_claim_id(
                self._claim_id_factory()
            )
            if replacement_claim_id == claimed.claim_id:
                raise ClaimConflictError(
                    "replacement Claim must use a fresh identity"
                )
            lease_expires_at = now + timedelta(
                seconds=self._options.lease_seconds
            )
            connection.execute(
                insert(self._schema.enqueue_claims).values(
                    item_id=claimed.item_id,
                    attempt=claimed.attempt,
                    claim_id=replacement_claim_id,
                    workflow_id=claimed.workflow_id,
                    enqueue_try=enqueue_try,
                    claimed_at=now,
                    lease_expires_at=lease_expires_at,
                    disposition=EnqueueClaimDisposition.CLAIMED.value,
                    created_at=now,
                )
            )
            old_outcome = connection.execute(
                update(self._schema.enqueue_claims)
                .where(
                    and_(
                        self._schema.enqueue_claims.c.item_id
                        == claimed.item_id,
                        self._schema.enqueue_claims.c.attempt
                        == claimed.attempt,
                        self._schema.enqueue_claims.c.claim_id
                        == claimed.claim_id,
                        self._schema.enqueue_claims.c.disposition
                        == EnqueueClaimDisposition.CALL_STARTED.value,
                        self._schema.enqueue_claims.c.lease_expires_at <= now,
                    )
                )
                .values(
                    disposition=EnqueueClaimDisposition.REPLACED.value,
                    replacement_claim_id=replacement_claim_id,
                    resolved_at=now,
                )
            )
            attempt_outcome = connection.execute(
                update(self._schema.item_attempts)
                .where(
                    and_(
                        self._schema.item_attempts.c.item_id
                        == claimed.item_id,
                        self._schema.item_attempts.c.attempt
                        == claimed.attempt,
                        self._schema.item_attempts.c.enqueue_state
                        == AttemptEnqueueState.CLAIMING.value,
                        self._schema.item_attempts.c.current_claim_id
                        == claimed.claim_id,
                        self._schema.item_attempts.c.execution_state
                        == AttemptExecutionState.NOT_STARTED.value,
                        self._schema.item_attempts.c.cancellation_request_id.is_(
                            None
                        ),
                    )
                )
                .values(
                    enqueue_try=enqueue_try,
                    current_claim_id=replacement_claim_id,
                    updated_at=now,
                )
            )
            if old_outcome.rowcount != 1 or attempt_outcome.rowcount != 1:
                raise ClaimConflictError(
                    "call-started recovery replacement CAS lost"
                )
            _advance_operation_cuts(
                connection,
                schema=self._schema,
                operation_keys={claimed.operation_key},
                now=now,
            )
            return _load_claimed_attempt(
                connection,
                schema=self._schema,
                candidate=context,
                claim_id=replacement_claim_id,
            )


class _LockedHierarchy(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    operations: dict[str, Mapping[str, Any]]
    items: dict[str, Mapping[str, Any]]
    attempts: dict[tuple[str, int], Mapping[str, Any]]
    claims: dict[tuple[str, int, str], Mapping[str, Any]]


def _pending_candidates(
    *,
    connection: Connection,
    schema: PlatformSchema,
    limit: int,
    operation_key: str | None = None,
) -> list[dict[str, Any]]:
    statement = _candidate_select(schema).where(
        and_(
            schema.item_attempts.c.enqueue_state
            == AttemptEnqueueState.PENDING.value,
            schema.item_attempts.c.current_claim_id.is_(None),
        )
    )
    if operation_key is not None:
        statement = statement.where(
            schema.operations.c.operation_key == operation_key
        )
    return [
        dict(row)
        for row in connection.execute(
            statement.order_by(
                schema.items.c.service_priority,
                schema.items.c.shuffle_rank,
                schema.items.c.item_id,
            ).limit(limit)
        ).mappings()
    ]


def _expired_unstarted_candidates(
    *,
    connection: Connection,
    schema: PlatformSchema,
    limit: int,
    operation_key: str | None = None,
) -> list[dict[str, Any]]:
    now = _database_now(connection)
    statement = (
        _candidate_select(schema)
        .join(
            schema.enqueue_claims,
            and_(
                schema.enqueue_claims.c.item_id
                == schema.item_attempts.c.item_id,
                schema.enqueue_claims.c.attempt
                == schema.item_attempts.c.attempt,
                schema.enqueue_claims.c.claim_id
                == schema.item_attempts.c.current_claim_id,
            ),
        )
        .add_columns(schema.enqueue_claims.c.claim_id)
        .where(
            and_(
                schema.item_attempts.c.enqueue_state
                == AttemptEnqueueState.CLAIMING.value,
                schema.enqueue_claims.c.disposition
                == EnqueueClaimDisposition.CLAIMED.value,
                schema.enqueue_claims.c.enqueue_call_started_at.is_(None),
                schema.enqueue_claims.c.lease_expires_at <= now,
            )
        )
    )
    if operation_key is not None:
        statement = statement.where(
            schema.operations.c.operation_key == operation_key
        )
    return [
        dict(row)
        for row in connection.execute(
            statement.order_by(
                schema.items.c.service_priority,
                schema.items.c.shuffle_rank,
                schema.items.c.item_id,
            ).limit(limit)
        ).mappings()
    ]


def _call_started_recovery_candidates(
    *,
    connection: Connection,
    schema: PlatformSchema,
    limit: int,
    operation_key: str | None = None,
) -> list[dict[str, Any]]:
    now = _database_now(connection)
    statement = (
        _candidate_select(schema)
        .join(
            schema.enqueue_claims,
            and_(
                schema.enqueue_claims.c.item_id
                == schema.item_attempts.c.item_id,
                schema.enqueue_claims.c.attempt
                == schema.item_attempts.c.attempt,
                schema.enqueue_claims.c.claim_id
                == schema.item_attempts.c.current_claim_id,
            ),
        )
        .add_columns(schema.enqueue_claims.c.claim_id)
        .where(
            and_(
                schema.item_attempts.c.enqueue_state
                == AttemptEnqueueState.CLAIMING.value,
                schema.enqueue_claims.c.disposition
                == EnqueueClaimDisposition.CALL_STARTED.value,
                schema.enqueue_claims.c.lease_expires_at <= now,
            )
        )
    )
    if operation_key is not None:
        statement = statement.where(
            schema.operations.c.operation_key == operation_key
        )
    return [
        dict(row)
        for row in connection.execute(
            statement.order_by(
                schema.items.c.service_priority,
                schema.items.c.shuffle_rank,
                schema.items.c.item_id,
            ).limit(limit)
        ).mappings()
    ]


def _candidate_select(schema: PlatformSchema) -> Any:
    return (
        select(
            schema.operations.c.operation_key,
            schema.operations.c.target_key,
            schema.operations.c.target_version,
            schema.operations.c.target_contract_digest,
            schema.items.c.item_id,
            schema.items.c.current_attempt.label("attempt"),
            schema.items.c.service_priority,
            schema.items.c.shuffle_rank,
            schema.item_attempts.c.workflow_id,
        )
        .select_from(schema.items)
        .join(
            schema.operations,
            schema.operations.c.operation_key == schema.items.c.operation_key,
        )
        .join(
            schema.item_attempts,
            and_(
                schema.item_attempts.c.item_id == schema.items.c.item_id,
                schema.item_attempts.c.attempt
                == schema.items.c.current_attempt,
            ),
        )
        .where(
            and_(
                schema.operations.c.registration_completed_at.is_not(None),
                schema.operations.c.registration_abandoned_at.is_(None),
                schema.operations.c.cancel_requested_at.is_(None),
                schema.item_attempts.c.execution_state
                == AttemptExecutionState.NOT_STARTED.value,
                schema.item_attempts.c.cancellation_request_id.is_(None),
            )
        )
    )


def _candidate_target_refs(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[ExecutionTargetRef, ...]:
    refs = {
        (
            str(candidate["target_key"]),
            int(candidate["target_version"]),
            str(candidate["target_contract_digest"]),
        )
        for candidate in candidates
    }
    return tuple(
        ExecutionTargetRef(
            target_key=target_key,
            target_version=target_version,
            target_contract_digest=target_contract_digest,
        )
        for target_key, target_version, target_contract_digest in sorted(refs)
    )


def _candidate_claim_key(
    candidate: Mapping[str, Any],
) -> tuple[str, int, str]:
    return (
        str(candidate["item_id"]),
        int(candidate["attempt"]),
        str(candidate["claim_id"]),
    )


def _locked_candidate_is_enqueue_eligible(
    *,
    locked: _LockedHierarchy,
    candidate: Mapping[str, Any],
) -> bool:
    operation = locked.operations.get(str(candidate["operation_key"]))
    item_id = str(candidate["item_id"])
    attempt = int(candidate["attempt"])
    item = locked.items.get(item_id)
    attempt_row = locked.attempts.get((item_id, attempt))
    return bool(
        operation is not None
        and item is not None
        and attempt_row is not None
        and operation["registration_completed_at"] is not None
        and operation["registration_abandoned_at"] is None
        and operation["cancel_requested_at"] is None
        and item["current_attempt"] == attempt
        and attempt_row["execution_state"]
        == AttemptExecutionState.NOT_STARTED.value
        and attempt_row["cancellation_request_id"] is None
    )


def _load_claimed_attempt(
    connection: Connection,
    *,
    schema: PlatformSchema,
    candidate: Mapping[str, Any],
    claim_id: str,
) -> ClaimedAttempt:
    item_id = str(candidate["item_id"])
    attempt = int(candidate["attempt"])
    item_row = dict(
        connection.execute(
            select(schema.items).where(schema.items.c.item_id == item_id)
        )
        .mappings()
        .one()
    )
    attempt_row = dict(
        connection.execute(
            select(schema.item_attempts).where(
                and_(
                    schema.item_attempts.c.item_id == item_id,
                    schema.item_attempts.c.attempt == attempt,
                )
            )
        )
        .mappings()
        .one()
    )
    claim_row = dict(
        connection.execute(
            select(schema.enqueue_claims).where(
                and_(
                    schema.enqueue_claims.c.item_id == item_id,
                    schema.enqueue_claims.c.attempt == attempt,
                    schema.enqueue_claims.c.claim_id == claim_id,
                )
            )
        )
        .mappings()
        .one()
    )
    return ClaimedAttempt(
        target_ref=ExecutionTargetRef(
            target_key=candidate["target_key"],
            target_version=candidate["target_version"],
            target_contract_digest=candidate["target_contract_digest"],
        ),
        item=ItemRecord.model_validate(item_row),
        attempt_record=AttemptRecord.model_validate(attempt_row),
        claim=EnqueueClaimRecord.model_validate(claim_row),
    )


def _claim_context(
    *,
    engine: Engine,
    schema: PlatformSchema,
    item_id: str,
    attempt: int,
    claim_id: str,
) -> dict[str, Any]:
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(
                    schema.operations.c.operation_key,
                    schema.operations.c.target_key,
                    schema.operations.c.target_version,
                    schema.operations.c.target_contract_digest,
                    schema.items.c.item_id,
                    schema.items.c.service_priority,
                    schema.items.c.shuffle_rank,
                    schema.item_attempts.c.attempt,
                    schema.item_attempts.c.workflow_id,
                )
                .select_from(schema.enqueue_claims)
                .join(
                    schema.item_attempts,
                    and_(
                        schema.item_attempts.c.item_id
                        == schema.enqueue_claims.c.item_id,
                        schema.item_attempts.c.attempt
                        == schema.enqueue_claims.c.attempt,
                    ),
                )
                .join(
                    schema.items,
                    schema.items.c.item_id == schema.item_attempts.c.item_id,
                )
                .join(
                    schema.operations,
                    schema.operations.c.operation_key
                    == schema.items.c.operation_key,
                )
                .where(
                    and_(
                        schema.enqueue_claims.c.item_id == item_id,
                        schema.enqueue_claims.c.attempt == attempt,
                        schema.enqueue_claims.c.claim_id == claim_id,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
    if row is None:
        raise ClaimAuthorityError("unknown enqueue Claim")
    return dict(row)


def _lock_candidate_hierarchy(
    connection: Connection,
    *,
    schema: PlatformSchema,
    candidates: Sequence[Mapping[str, Any]],
    claim_keys: Sequence[tuple[str, int, str]] = (),
) -> _LockedHierarchy:
    _acquire_workflow_reference_locks(
        connection,
        sorted({str(row["workflow_id"]) for row in candidates}),
    )
    operation_keys = sorted({str(row["operation_key"]) for row in candidates})
    operation_rows = [
        dict(row)
        for row in connection.execute(
            select(schema.operations)
            .where(schema.operations.c.operation_key.in_(operation_keys))
            .order_by(schema.operations.c.operation_key)
            .with_for_update()
        ).mappings()
    ]
    item_ids = sorted({str(row["item_id"]) for row in candidates})
    item_rows = [
        dict(row)
        for row in connection.execute(
            select(schema.items)
            .where(schema.items.c.item_id.in_(item_ids))
            .order_by(schema.items.c.item_id)
            .with_for_update()
        ).mappings()
    ]
    attempt_keys = sorted(
        {(str(row["item_id"]), int(row["attempt"])) for row in candidates}
    )
    attempt_rows: list[Mapping[str, Any]] = []
    for item_id, attempt in attempt_keys:
        row = (
            connection.execute(
                select(schema.item_attempts)
                .where(
                    and_(
                        schema.item_attempts.c.item_id == item_id,
                        schema.item_attempts.c.attempt == attempt,
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            attempt_rows.append(dict(row))
    claim_rows: list[Mapping[str, Any]] = []
    ordered_claim_keys = sorted(
        set(claim_keys),
        key=lambda key: (key[2], key[0], key[1]),
    )
    for item_id, attempt, claim_id in ordered_claim_keys:
        row = (
            connection.execute(
                select(schema.enqueue_claims)
                .where(
                    and_(
                        schema.enqueue_claims.c.item_id == item_id,
                        schema.enqueue_claims.c.attempt == attempt,
                        schema.enqueue_claims.c.claim_id == claim_id,
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            claim_rows.append(dict(row))
    return _LockedHierarchy(
        operations={row["operation_key"]: row for row in operation_rows},
        items={row["item_id"]: row for row in item_rows},
        attempts={
            (row["item_id"], row["attempt"]): row for row in attempt_rows
        },
        claims={
            (row["item_id"], row["attempt"], row["claim_id"]): row
            for row in claim_rows
        },
    )


def _is_replaceable_expired_claim(
    *,
    attempt_row: Mapping[str, Any] | None,
    claim_row: Mapping[str, Any] | None,
    expected_claim_id: str,
    now: datetime,
) -> bool:
    return bool(
        attempt_row is not None
        and claim_row is not None
        and attempt_row["enqueue_state"] == AttemptEnqueueState.CLAIMING.value
        and attempt_row["current_claim_id"] == expected_claim_id
        and claim_row["disposition"] == EnqueueClaimDisposition.CLAIMED.value
        and claim_row["enqueue_call_started_at"] is None
        and claim_row["lease_expires_at"] <= now
    )


def _resolve_exhausted_call_started_claim(  # noqa: PLR0913
    connection: Connection,
    *,
    schema: PlatformSchema,
    claimed: ClaimedAttempt,
    current_enqueue_try: int,
    max_enqueue_tries: int,
    now: datetime,
) -> None:
    failure = FailureSnapshot(
        failure_class=FailureClass.PERMANENT,
        error_type="MaxEnqueueTriesExceeded",
        message=(
            "authoritative workflow absence exhausted the enqueue try bound"
        ),
        metadata={
            "max_enqueue_tries": max_enqueue_tries,
            "enqueue_try": current_enqueue_try,
        },
    )
    claim_outcome = connection.execute(
        update(schema.enqueue_claims)
        .where(
            and_(
                schema.enqueue_claims.c.item_id == claimed.item_id,
                schema.enqueue_claims.c.attempt == claimed.attempt,
                schema.enqueue_claims.c.claim_id == claimed.claim_id,
                schema.enqueue_claims.c.disposition
                == EnqueueClaimDisposition.CALL_STARTED.value,
                schema.enqueue_claims.c.lease_expires_at <= now,
            )
        )
        .values(
            disposition=EnqueueClaimDisposition.EXPIRED.value,
            resolved_at=now,
        )
    )
    attempt_outcome = connection.execute(
        update(schema.item_attempts)
        .where(
            and_(
                schema.item_attempts.c.item_id == claimed.item_id,
                schema.item_attempts.c.attempt == claimed.attempt,
                schema.item_attempts.c.enqueue_state
                == AttemptEnqueueState.CLAIMING.value,
                schema.item_attempts.c.current_claim_id == claimed.claim_id,
                schema.item_attempts.c.enqueue_try == current_enqueue_try,
                schema.item_attempts.c.execution_state
                == AttemptExecutionState.NOT_STARTED.value,
                schema.item_attempts.c.cancellation_request_id.is_(None),
            )
        )
        .values(
            enqueue_state=AttemptEnqueueState.ENQUEUE_ERROR.value,
            current_claim_id=None,
            failure=failure.model_dump(mode="json"),
            updated_at=now,
        )
    )
    if claim_outcome.rowcount != 1 or attempt_outcome.rowcount != 1:
        raise ClaimConflictError("exhausted call-started recovery CAS lost")
    _refresh_operation_enqueue_aggregate(
        connection,
        schema=schema,
        operation_key=claimed.operation_key,
        now=now,
    )


def _physical_outcome_disposition(outcome: Any) -> _PhysicalOutcomeDisposition:
    raw = getattr(outcome, "disposition", None)
    try:
        return _PhysicalOutcomeDisposition(raw)
    except ValueError as error:
        raise ValueError("unsupported physical enqueue disposition") from error


def _attempt_outcome_values(
    *,
    disposition: _PhysicalOutcomeDisposition,
    outcome: Any,
    now: datetime,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "current_claim_id": None,
        "updated_at": now,
    }
    if disposition is _PhysicalOutcomeDisposition.ENQUEUE_ERROR:
        failure = getattr(outcome, "failure", None)
        if failure is None:
            raise ValueError("enqueue_error outcome requires failure")
        return {
            **base,
            "enqueue_state": AttemptEnqueueState.ENQUEUE_ERROR.value,
            "failure": failure.model_dump(mode="json"),
        }
    priority = getattr(outcome, "effective_service_priority", None)
    if (
        not isinstance(priority, int)
        or isinstance(priority, bool)
        or priority <= 0
    ):
        raise ValueError("successful enqueue outcome requires priority")
    enqueue_state = (
        AttemptEnqueueState.ENQUEUED
        if disposition is _PhysicalOutcomeDisposition.ENQUEUED
        else AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT
    )
    priority_source = (
        PrioritySource.ENQUEUED_HERE
        if disposition is _PhysicalOutcomeDisposition.ENQUEUED
        else PrioritySource.LINKED_EXISTING
    )
    return {
        **base,
        "enqueue_state": enqueue_state.value,
        "enqueued_at": now,
        "effective_service_priority": priority,
        "priority_source": priority_source.value,
    }


def _refresh_operation_enqueue_aggregate(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    now: datetime,
) -> None:
    operation = dict(
        connection.execute(
            select(schema.operations)
            .where(schema.operations.c.operation_key == operation_key)
            .with_for_update()
        )
        .mappings()
        .one()
    )
    attempts = list(
        connection.execute(
            select(
                schema.item_attempts.c.enqueue_state,
                schema.item_attempts.c.enqueue_try,
                schema.item_attempts.c.failure,
            )
            .select_from(schema.items)
            .join(
                schema.item_attempts,
                and_(
                    schema.item_attempts.c.item_id == schema.items.c.item_id,
                    schema.item_attempts.c.attempt
                    == schema.items.c.current_attempt,
                ),
            )
            .where(schema.items.c.operation_key == operation_key)
            .order_by(schema.items.c.item_id)
        ).mappings()
    )
    states = [AttemptEnqueueState(row["enqueue_state"]) for row in attempts]
    retry_policy = operation["retry_policy"]
    max_enqueue_tries = _max_enqueue_tries(operation)
    retryable_classes = set(retry_policy["retryable_failure_classes"])
    retryable_error_exists = any(
        state is AttemptEnqueueState.ENQUEUE_ERROR
        and row["enqueue_try"] < max_enqueue_tries
        and row["failure"] is not None
        and row["failure"].get("failure_class") in retryable_classes
        for state, row in zip(states, attempts, strict=True)
    )
    in_enqueue = (
        any(
            state
            in {
                AttemptEnqueueState.PENDING,
                AttemptEnqueueState.CLAIMING,
            }
            for state in states
        )
        or retryable_error_exists
    )
    confirmed_exists = any(
        state
        in {
            AttemptEnqueueState.ENQUEUED,
            AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT,
        }
        for state in states
    )
    values: dict[str, Any] = {
        "enqueued_count": states.count(AttemptEnqueueState.ENQUEUED),
        "workflow_already_present_count": states.count(
            AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT
        ),
        "enqueue_failed_count": states.count(
            AttemptEnqueueState.ENQUEUE_ERROR
        ),
        "platform_cut_version": (schema.operations.c.platform_cut_version + 1),
        "updated_at": now,
    }
    if in_enqueue:
        values.update(
            status=OperationStatus.ENQUEUEING.value,
            terminal_reason=None,
            completed_at=None,
        )
    elif confirmed_exists:
        values.update(
            status=OperationStatus.RUNNING.value,
            terminal_reason=None,
            completed_at=None,
        )
    else:
        values.update(
            status=OperationStatus.FAILED.value,
            terminal_reason="enqueue_exhausted",
            completed_at=now,
        )
    connection.execute(
        update(schema.operations)
        .where(schema.operations.c.operation_key == operation_key)
        .values(**values)
    )


def _advance_operation_cuts(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_keys: set[str],
    now: datetime,
) -> None:
    for operation_key in sorted(operation_keys):
        connection.execute(
            update(schema.operations)
            .where(schema.operations.c.operation_key == operation_key)
            .values(
                platform_cut_version=(
                    schema.operations.c.platform_cut_version + 1
                ),
                updated_at=now,
            )
        )


def _max_enqueue_tries(operation_row: Mapping[str, Any]) -> int:
    retry_policy = operation_row["retry_policy"]
    value = retry_policy.get("max_enqueue_tries")
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ClaimConflictError("stored max_enqueue_tries is invalid")
    return value


def _claim_record(row: Mapping[str, Any]) -> EnqueueClaimRecord:
    return EnqueueClaimRecord.model_validate(dict(row))


def _validated_claim_id(value: str) -> str:
    if not value:
        raise ValueError("claim_id_factory returned an empty Claim ID")
    return value


def _random_claim_id() -> str:
    return uuid4().hex


def _invalidate_attempt_claims(  # noqa: PLR0913
    connection: Connection,
    *,
    schema: PlatformSchema,
    item_id: str,
    attempt: int,
    invalidated_by: str,
    now: datetime,
) -> None:
    """Invalidate outstanding Claims through the Claim-owned boundary."""
    connection.execute(
        update(schema.enqueue_claims)
        .where(
            and_(
                schema.enqueue_claims.c.item_id == item_id,
                schema.enqueue_claims.c.attempt == attempt,
                schema.enqueue_claims.c.disposition.in_(
                    [
                        EnqueueClaimDisposition.CLAIMED.value,
                        EnqueueClaimDisposition.CALL_STARTED.value,
                    ]
                ),
            )
        )
        .values(
            disposition=EnqueueClaimDisposition.INVALIDATED.value,
            invalidated_at=now,
            invalidated_by=invalidated_by,
            resolved_at=now,
        )
    )


def _database_now(connection: Connection) -> datetime:
    return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def _acquire_export_writer_lock(connection: Connection) -> None:
    connection.execute(
        text("SELECT pg_advisory_xact_lock_shared(:lock_key)"),
        {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
    )


def _acquire_claim_selection_lock(connection: Connection) -> None:
    connection.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": CLAIM_SELECTION_ADVISORY_KEY},
    )


def _acquire_workflow_reference_locks(
    connection: Connection,
    workflow_ids: Sequence[str],
) -> None:
    for workflow_id in sorted(set(workflow_ids)):
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 0))"),
            {"id": workflow_id},
        )
