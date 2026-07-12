"""Durable bounded workflow reconciliation and next-Attempt requests."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003 -- Pydantic resolves at runtime
from typing import TYPE_CHECKING, Annotated, Any

from dr_serialize import sha256_json_digest
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)
from sqlalchemy import (
    Connection,
    Engine,
    Integer,
    and_,
    cast,
    func,
    insert,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from dr_platform.claims import (
    _acquire_export_writer_lock,
    _acquire_workflow_reference_locks,
    _database_now,
)
from dr_platform.db import PlatformSchema
from dr_platform.manifests import ExecutionTargetRef
from dr_platform.records import (
    AttemptRecord,
    EligibilityReference,
    FailureSnapshot,
    ItemRecord,
    NextAttemptRequestRecord,
    RetryPolicy,
)
from dr_platform.status import (
    CONFIRMED_ENQUEUE_STATES,
    TERMINAL_EXECUTION_STATES,
    AttemptEnqueueState,
    AttemptExecutionState,
    AttemptRetryReason,
    NextAttemptDisposition,
    NextAttemptReason,
    OperationStatus,
    RetryDisposition,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dr_platform.reconciliation_runtime import (
        ReconcileOptions,
        ReconciliationCandidate,
        ReconciliationObservation,
    )
    from dr_platform.targets import ExecutionIdentity, TargetResolver

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]


class ReconciliationConflictError(RuntimeError):
    """A reconciliation or request CAS conflicts with durable truth."""


class ReconciliationPersistenceResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    observed_count: NonNegativeInt
    changed_count: NonNegativeInt
    enqueue_reset_count: NonNegativeInt
    execution_retry_count: NonNegativeInt
    missing_count: NonNegativeInt


class NextAttemptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    source_attempt: NonNegativeInt
    request_key: NonEmptyStr
    reason: NextAttemptReason
    eligibility: EligibilityReference
    requested_by: NonEmptyStr
    operator_confirmed_at: datetime | None = None
    max_attempts: PositiveInt | None = None

    @model_validator(mode="after")
    def validate_reason(self) -> NextAttemptRequest:
        if self.reason is NextAttemptReason.DOMAIN_OUTCOME:
            if self.operator_confirmed_at is not None:
                raise ValueError(
                    "domain outcome cannot carry operator confirmation"
                )
        elif self.operator_confirmed_at is None:
            raise ValueError("cancel retry requires operator confirmation")
        return self

    def request_id(self) -> str:
        return sha256_json_digest(
            {"item_id": self.item_id, "request_key": self.request_key}
        )


class NextAttemptResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: NonEmptyStr
    item_id: NonEmptyStr
    source_attempt: NonNegativeInt
    disposition: NextAttemptDisposition
    created_attempt: NonNegativeInt | None = None
    effective_max_attempts: PositiveInt
    rejection_detail: StrictStr | None = None


def request_next_attempt(
    request: NextAttemptRequest,
    *,
    engine: Engine,
    resolver: TargetResolver,
    schema: PlatformSchema | None = None,
) -> NextAttemptResult:
    """Persist one idempotent caller-requested next Attempt transition."""
    selected_schema = schema or PlatformSchema()
    with engine.connect() as connection:
        item_row = dict(
            connection.execute(
                select(selected_schema.items).where(
                    selected_schema.items.c.item_id == request.item_id
                )
            )
            .mappings()
            .one()
        )
        source_row = dict(
            connection.execute(
                select(selected_schema.item_attempts).where(
                    and_(
                        selected_schema.item_attempts.c.item_id
                        == request.item_id,
                        selected_schema.item_attempts.c.attempt
                        == request.source_attempt,
                    )
                )
            )
            .mappings()
            .one()
        )
        seed = dict(
            connection.execute(
                select(selected_schema.operations).where(
                    selected_schema.operations.c.operation_key
                    == item_row["operation_key"]
                )
            )
            .mappings()
            .one()
        )
    item = ItemRecord.model_validate(item_row)
    source = AttemptRecord.model_validate(source_row)
    target_ref = ExecutionTargetRef(
        target_key=seed["target_key"],
        target_version=seed["target_version"],
        target_contract_digest=seed["target_contract_digest"],
    )
    next_attempt = request.source_attempt + 1
    execution = resolver.resolve(target_ref).execution_for(item, next_attempt)
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(connection, [execution.workflow_id])
        operation = _lock_operation_for_item(
            connection,
            schema=selected_schema,
            operation_key=seed["operation_key"],
        )
        locked_item = _lock_item(
            connection,
            schema=selected_schema,
            item_id=request.item_id,
        )
        locked_source = _lock_attempt(
            connection,
            schema=selected_schema,
            item_id=request.item_id,
            attempt=request.source_attempt,
        )
        existing = (
            connection.execute(
                select(selected_schema.next_attempt_requests)
                .where(
                    and_(
                        selected_schema.next_attempt_requests.c.item_id
                        == request.item_id,
                        selected_schema.next_attempt_requests.c.request_key
                        == request.request_key,
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if existing is not None:
            existing_values = dict(existing)
            _validate_request_replay(
                existing=existing_values,
                request=request,
            )
            return _next_attempt_result(existing_values)
        policy = RetryPolicy.model_validate(operation["retry_policy"])
        effective_max = min(
            policy.max_attempts,
            request.max_attempts or policy.max_attempts,
        )
        disposition, rejection = _request_disposition(
            request=request,
            item=locked_item,
            source=locked_source,
            effective_max_attempts=effective_max,
        )
        now = _database_now(connection)
        created_attempt: int | None = None
        if disposition is NextAttemptDisposition.CREATED:
            created_attempt = next_attempt
            connection.execute(
                insert(selected_schema.item_attempts).values(
                    item_id=request.item_id,
                    attempt=next_attempt,
                    workflow_role=source.workflow_role,
                    execution_key=execution.execution_key,
                    workflow_id=execution.workflow_id,
                    execution_recipe_digest=source.execution_recipe_digest,
                    enqueue_state=AttemptEnqueueState.PENDING.value,
                    enqueue_try=0,
                    execution_state=AttemptExecutionState.NOT_STARTED.value,
                    source_attempt=request.source_attempt,
                    source_workflow_id=source.workflow_id,
                    retry_reason=AttemptRetryReason(
                        request.reason.value
                    ).value,
                    next_attempt_request_id=request.request_id(),
                    source_application_version=source.source_application_version,
                    missing_observation_count=0,
                    requested_service_class=source.requested_service_class,
                    requested_service_priority=source.requested_service_priority,
                    created_at=now,
                    updated_at=now,
                )
            )
            item_outcome = connection.execute(
                update(selected_schema.items)
                .where(
                    and_(
                        selected_schema.items.c.item_id == request.item_id,
                        selected_schema.items.c.current_attempt
                        == request.source_attempt,
                    )
                )
                .values(current_attempt=next_attempt, updated_at=now)
            )
            if item_outcome.rowcount != 1:
                raise ReconciliationConflictError("next-Attempt Item CAS lost")
        values = {
            "request_id": request.request_id(),
            "item_id": request.item_id,
            "request_key": request.request_key,
            "source_attempt": request.source_attempt,
            "reason": request.reason.value,
            "eligibility_kind": request.eligibility.kind,
            "eligibility_record_id": request.eligibility.record_id,
            "eligibility_digest": request.eligibility.digest,
            "requested_by": request.requested_by,
            "operator_confirmed_at": request.operator_confirmed_at,
            "max_attempts": request.max_attempts,
            "effective_max_attempts": effective_max,
            "disposition": disposition.value,
            "created_attempt": created_attempt,
            "rejection_detail": rejection,
            "created_at": now,
            "resolved_at": now,
        }
        connection.execute(
            insert(selected_schema.next_attempt_requests).values(**values)
        )
        if created_attempt is not None:
            _refresh_operation_lifecycle(
                connection,
                schema=selected_schema,
                operation_key=seed["operation_key"],
                now=now,
            )
        row = NextAttemptRequestRecord.model_validate(
            {**values, "change_seq": 1}
        )
        return NextAttemptResult(
            request_id=row.request_id,
            item_id=row.item_id,
            source_attempt=row.source_attempt,
            disposition=row.disposition,
            created_attempt=row.created_attempt,
            effective_max_attempts=row.effective_max_attempts,
            rejection_detail=row.rejection_detail,
        )


def _request_disposition(
    *,
    request: NextAttemptRequest,
    item: Mapping[str, Any],
    source: Mapping[str, Any],
    effective_max_attempts: int,
) -> tuple[NextAttemptDisposition, str | None]:
    if item["current_attempt"] != request.source_attempt:
        return (
            NextAttemptDisposition.SOURCE_ADVANCED,
            "current Attempt advanced",
        )
    if request.source_attempt + 1 >= effective_max_attempts:
        return (
            NextAttemptDisposition.MAX_ATTEMPTS_EXHAUSTED,
            "maximum Attempts exhausted",
        )
    expected_state = (
        AttemptExecutionState.SUCCEEDED
        if request.reason is NextAttemptReason.DOMAIN_OUTCOME
        else AttemptExecutionState.CANCELLED
    )
    if source["execution_state"] != expected_state.value:
        return NextAttemptDisposition.INELIGIBLE, "source state is ineligible"
    if (
        request.reason is NextAttemptReason.OPERATOR_CANCEL_RETRY
        and source["cancellation_request_id"] is None
    ):
        return (
            NextAttemptDisposition.INELIGIBLE,
            "cancel retry lacks cancellation provenance",
        )
    return NextAttemptDisposition.CREATED, None


def _validate_request_replay(
    *,
    existing: Mapping[str, Any],
    request: NextAttemptRequest,
) -> None:
    expected = {
        "request_id": request.request_id(),
        "source_attempt": request.source_attempt,
        "reason": request.reason.value,
        "eligibility_kind": request.eligibility.kind,
        "eligibility_record_id": request.eligibility.record_id,
        "eligibility_digest": request.eligibility.digest,
        "requested_by": request.requested_by,
        "operator_confirmed_at": request.operator_confirmed_at,
        "max_attempts": request.max_attempts,
    }
    unequal = [
        key for key, value in expected.items() if existing[key] != value
    ]
    if unequal:
        raise ReconciliationConflictError(
            "next-Attempt request replay conflict: "
            + ", ".join(sorted(unequal))
        )


def _next_attempt_result(row: Mapping[str, Any]) -> NextAttemptResult:
    return NextAttemptResult(
        request_id=row["request_id"],
        item_id=row["item_id"],
        source_attempt=row["source_attempt"],
        disposition=NextAttemptDisposition(row["disposition"]),
        created_attempt=row["created_attempt"],
        effective_max_attempts=row["effective_max_attempts"],
        rejection_detail=row["rejection_detail"],
    )


def load_reconciliation_page(
    engine: Engine,
    *,
    page_size: int,
    schema: PlatformSchema | None = None,
) -> tuple[ReconciliationCandidate, ...]:
    """Load one scheduling-ordered page of actionable current Attempts."""
    return _load_reconciliation_candidates(
        engine,
        page_size=page_size,
        schema=schema,
        missing_only=False,
    )


def load_missing_reobservation_page(
    engine: Engine,
    *,
    page_size: int,
    schema: PlatformSchema | None = None,
) -> tuple[ReconciliationCandidate, ...]:
    """Load lower-priority terminal MISSING candidates for A2 rechecks."""
    return _load_reconciliation_candidates(
        engine,
        page_size=page_size,
        schema=schema,
        missing_only=True,
    )


def _load_reconciliation_candidates(
    engine: Engine,
    *,
    page_size: int,
    schema: PlatformSchema | None,
    missing_only: bool,
) -> tuple[ReconciliationCandidate, ...]:
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    from dr_platform.reconciliation_runtime import (  # noqa: PLC0415
        ReconciliationCandidate,
    )

    selected_schema = schema or PlatformSchema()
    lifecycle_predicate = (
        selected_schema.item_attempts.c.execution_state
        == AttemptExecutionState.MISSING.value
        if missing_only
        else or_(
            and_(
                selected_schema.item_attempts.c.enqueue_state
                == AttemptEnqueueState.ENQUEUE_ERROR.value,
                selected_schema.item_attempts.c.failure.is_not(None),
                selected_schema.item_attempts.c.enqueue_try
                < cast(
                    selected_schema.operations.c.retry_policy[
                        "max_enqueue_tries"
                    ].astext,
                    Integer,
                ),
                selected_schema.operations.c.retry_policy[
                    "retryable_failure_classes"
                ].op("?")(
                    selected_schema.item_attempts.c.failure[
                        "failure_class"
                    ].astext
                ),
            ),
            and_(
                selected_schema.item_attempts.c.enqueue_state.in_(
                    tuple(state.value for state in CONFIRMED_ENQUEUE_STATES)
                ),
                selected_schema.item_attempts.c.execution_state.not_in(
                    tuple(state.value for state in TERMINAL_EXECUTION_STATES)
                ),
            ),
        )
    )
    statement = (
        select(
            selected_schema.operations.c.target_key,
            selected_schema.operations.c.target_version,
            selected_schema.operations.c.target_contract_digest,
            selected_schema.items.c.item_id,
            selected_schema.items.c.current_attempt,
        )
        .select_from(selected_schema.items)
        .join(
            selected_schema.operations,
            selected_schema.operations.c.operation_key
            == selected_schema.items.c.operation_key,
        )
        .join(
            selected_schema.item_attempts,
            and_(
                selected_schema.item_attempts.c.item_id
                == selected_schema.items.c.item_id,
                selected_schema.item_attempts.c.attempt
                == selected_schema.items.c.current_attempt,
            ),
        )
        .where(
            and_(
                selected_schema.operations.c.registration_completed_at.is_not(
                    None
                ),
                selected_schema.operations.c.registration_abandoned_at.is_(
                    None
                ),
                lifecycle_predicate,
            )
        )
    )
    if missing_only:
        statement = statement.outerjoin(
            selected_schema.missing_reobservations,
            and_(
                selected_schema.missing_reobservations.c.item_id
                == selected_schema.item_attempts.c.item_id,
                selected_schema.missing_reobservations.c.attempt
                == selected_schema.item_attempts.c.attempt,
            ),
        ).order_by(
            func.coalesce(
                selected_schema.missing_reobservations.c.last_reobserved_at,
                selected_schema.item_attempts.c.missing_last_observed_at,
            ),
            selected_schema.items.c.service_priority,
            selected_schema.items.c.shuffle_rank,
            selected_schema.items.c.item_id,
        )
    else:
        statement = statement.order_by(
            selected_schema.items.c.service_priority,
            selected_schema.items.c.shuffle_rank,
            selected_schema.items.c.item_id,
        )
    with engine.connect() as connection:
        rows = connection.execute(statement.limit(page_size)).mappings()
        return tuple(
            _load_reconciliation_candidate(
                connection,
                schema=selected_schema,
                row=dict(row),
                candidate_type=ReconciliationCandidate,
            )
            for row in rows
        )


def _load_reconciliation_candidate(
    connection: Connection,
    *,
    schema: PlatformSchema,
    row: Mapping[str, Any],
    candidate_type: Any,
) -> ReconciliationCandidate:
    item = ItemRecord.model_validate(
        dict(
            connection.execute(
                select(schema.items).where(
                    schema.items.c.item_id == row["item_id"]
                )
            )
            .mappings()
            .one()
        )
    )
    attempt = AttemptRecord.model_validate(
        dict(
            connection.execute(
                select(schema.item_attempts).where(
                    and_(
                        schema.item_attempts.c.item_id == row["item_id"],
                        schema.item_attempts.c.attempt
                        == row["current_attempt"],
                    )
                )
            )
            .mappings()
            .one()
        )
    )
    return candidate_type(
        item=item,
        attempt=attempt,
        target_ref=ExecutionTargetRef(
            target_key=row["target_key"],
            target_version=row["target_version"],
            target_contract_digest=row["target_contract_digest"],
        ),
    )


def apply_reconciliation_observations(  # noqa: PLR0913 -- persistence facade
    engine: Engine,
    *,
    observations: Mapping[str, ReconciliationObservation],
    resolver: TargetResolver,
    options: ReconcileOptions,
    schema: PlatformSchema | None = None,
    candidates: tuple[ReconciliationCandidate, ...] | None = None,
) -> ReconciliationPersistenceResult:
    """Apply one bounded set of typed DBOS observations."""
    selected_schema = schema or PlatformSchema()
    if candidates is None:
        actionable = load_reconciliation_page(
            engine,
            page_size=options.page_size,
            schema=selected_schema,
        )
        remaining = options.page_size - len(actionable)
        missing = (
            load_missing_reobservation_page(
                engine,
                page_size=remaining,
                schema=selected_schema,
            )
            if remaining > 0
            else ()
        )
        candidates = actionable + missing
    counts = {
        "observed_count": 0,
        "changed_count": 0,
        "enqueue_reset_count": 0,
        "execution_retry_count": 0,
        "missing_count": 0,
    }
    for candidate in candidates:
        if (
            candidate.attempt.enqueue_state
            is AttemptEnqueueState.ENQUEUE_ERROR
        ):
            counts["observed_count"] += 1
            if _reset_retryable_enqueue_error(
                engine=engine,
                schema=selected_schema,
                candidate=candidate,
            ):
                counts["changed_count"] += 1
                counts["enqueue_reset_count"] += 1
            continue
        observation = observations.get(candidate.attempt.workflow_id)
        if observation is None:
            continue
        counts["observed_count"] += 1
        outcome = _apply_one_observation(
            engine=engine,
            schema=selected_schema,
            candidate=candidate,
            observation=observation,
            resolver=resolver,
            options=options,
        )
        if outcome is None:
            continue
        counts["changed_count"] += 1
        if outcome != "changed":
            counts[outcome] += 1
    return ReconciliationPersistenceResult(**counts)


def _reset_retryable_enqueue_error(
    *,
    engine: Engine,
    schema: PlatformSchema,
    candidate: ReconciliationCandidate,
) -> bool:
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(
            connection,
            [candidate.attempt.workflow_id],
        )
        operation = _lock_operation_for_item(
            connection,
            schema=schema,
            operation_key=candidate.item.operation_key,
        )
        item = _lock_item(
            connection,
            schema=schema,
            item_id=candidate.item.item_id,
        )
        attempt = _lock_attempt(
            connection,
            schema=schema,
            item_id=candidate.item.item_id,
            attempt=candidate.attempt.attempt,
        )
        if (
            item["current_attempt"] != candidate.attempt.attempt
            or attempt["enqueue_state"]
            != AttemptEnqueueState.ENQUEUE_ERROR.value
            or attempt["failure"] is None
        ):
            return False
        policy = RetryPolicy.model_validate(operation["retry_policy"])
        failure = FailureSnapshot.model_validate(attempt["failure"])
        if (
            failure.failure_class not in policy.retryable_failure_classes
            or attempt["enqueue_try"] >= policy.max_enqueue_tries
        ):
            return False
        now = _database_now(connection)
        connection.execute(
            update(schema.item_attempts)
            .where(
                and_(
                    schema.item_attempts.c.item_id == candidate.item.item_id,
                    schema.item_attempts.c.attempt
                    == candidate.attempt.attempt,
                    schema.item_attempts.c.enqueue_state
                    == AttemptEnqueueState.ENQUEUE_ERROR.value,
                )
            )
            .values(
                enqueue_state=AttemptEnqueueState.PENDING.value,
                failure=None,
                updated_at=now,
            )
        )
        _refresh_operation_lifecycle(
            connection,
            schema=schema,
            operation_key=candidate.item.operation_key,
            now=now,
        )
        return True


def _apply_one_observation(  # noqa: PLR0911,PLR0912,PLR0913,PLR0915
    *,
    engine: Engine,
    schema: PlatformSchema,
    candidate: ReconciliationCandidate,
    observation: ReconciliationObservation,
    resolver: TargetResolver,
    options: ReconcileOptions,
) -> str | None:
    if observation.workflow_id != candidate.attempt.workflow_id:
        raise ReconciliationConflictError("observation workflow changed")
    successor_execution: ExecutionIdentity | None = None
    if observation.disposition.value == "error":
        successor_execution = resolver.resolve(
            candidate.target_ref
        ).execution_for(
            candidate.item,
            candidate.attempt.attempt + 1,
        )
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(
            connection,
            [
                candidate.attempt.workflow_id,
                *(
                    [successor_execution.workflow_id]
                    if successor_execution is not None
                    else []
                ),
            ],
        )
        operation = _lock_operation_for_item(
            connection,
            schema=schema,
            operation_key=candidate.item.operation_key,
        )
        item = _lock_item(
            connection, schema=schema, item_id=candidate.item.item_id
        )
        attempt = _lock_attempt(
            connection,
            schema=schema,
            item_id=candidate.item.item_id,
            attempt=candidate.attempt.attempt,
        )
        terminal_attempt = attempt["execution_state"] in {
            state.value for state in TERMINAL_EXECUTION_STATES
        }
        if (
            item["current_attempt"] != candidate.attempt.attempt
            or attempt["workflow_id"] != observation.workflow_id
        ):
            return None
        if terminal_attempt:
            if (
                attempt["execution_state"]
                == AttemptExecutionState.MISSING.value
                and observation.disposition.value != "uncertain"
            ):
                _record_missing_reobservation(
                    connection,
                    schema=schema,
                    candidate=candidate,
                    now=_database_now(connection),
                )
            return None
        if successor_execution is not None:
            if (
                operation["target_key"]
                != candidate.target_ref.target_key
                or operation["target_version"]
                != candidate.target_ref.target_version
                or operation["target_contract_digest"]
                != candidate.target_ref.target_contract_digest
            ):
                raise ReconciliationConflictError(
                    "automatic retry target reference changed under lock"
                )
            locked_successor = resolver.resolve(
                candidate.target_ref
            ).execution_for(
                candidate.item,
                candidate.attempt.attempt + 1,
            )
            if locked_successor != successor_execution:
                raise ReconciliationConflictError(
                    "automatic retry target identity changed under lock"
                )
            if (
                attempt["execution_key"]
                != candidate.attempt.execution_key
                or attempt["execution_recipe_digest"]
                != candidate.attempt.execution_recipe_digest
            ):
                raise ReconciliationConflictError(
                    "automatic retry source identity changed under lock"
                )
        disposition = observation.disposition.value
        if (
            attempt["execution_state"]
            == AttemptExecutionState.CANCEL_REQUESTED.value
            and disposition != "cancelled"
        ):
            return None
        if (
            disposition == "active"
            and attempt["execution_state"]
            == AttemptExecutionState.ACTIVE.value
            and observation.dbos_status is not None
            and attempt["dbos_status"] == observation.dbos_status.value
            and attempt["missing_observation_count"] == 0
        ):
            return None
        now = _database_now(connection)
        changed_kind: str | None = "changed"
        if disposition == "uncertain":
            return None
        if disposition == "absent":
            if AttemptEnqueueState(attempt["enqueue_state"]) not in (
                CONFIRMED_ENQUEUE_STATES
            ):
                return None
            missing_count = int(attempt["missing_observation_count"]) + 1
            first = attempt["missing_first_observed_at"] or now
            becomes_missing = (
                missing_count >= options.missing_required_observations
                and (now - first).total_seconds()
                >= options.missing_grace_seconds
            )
            values: dict[str, Any] = {
                "missing_observation_count": missing_count,
                "missing_first_observed_at": first,
                "missing_last_observed_at": now,
                "updated_at": now,
            }
            if becomes_missing:
                values.update(
                    execution_state=AttemptExecutionState.MISSING.value,
                    terminal_at=now,
                )
                changed_kind = "missing_count"
            connection.execute(
                update(schema.item_attempts)
                .where(
                    and_(
                        schema.item_attempts.c.item_id
                        == candidate.item.item_id,
                        schema.item_attempts.c.attempt
                        == candidate.attempt.attempt,
                        schema.item_attempts.c.execution_state
                        == attempt["execution_state"],
                    )
                )
                .values(**values)
            )
        elif observation.dbos_status is None:
            raise ReconciliationConflictError(
                "observed workflow requires DBOS status"
            )
        elif disposition == "active":
            connection.execute(
                update(schema.item_attempts)
                .where(
                    and_(
                        schema.item_attempts.c.item_id
                        == candidate.item.item_id,
                        schema.item_attempts.c.attempt
                        == candidate.attempt.attempt,
                    )
                )
                .values(
                    execution_state=AttemptExecutionState.ACTIVE.value,
                    dbos_status=observation.dbos_status.value,
                    missing_observation_count=0,
                    missing_first_observed_at=None,
                    missing_last_observed_at=None,
                    updated_at=now,
                )
            )
        elif disposition in {"succeeded", "cancelled", "recovery_exhausted"}:
            terminal_state = {
                "succeeded": AttemptExecutionState.SUCCEEDED,
                "cancelled": AttemptExecutionState.CANCELLED,
                "recovery_exhausted": AttemptExecutionState.RECOVERY_EXHAUSTED,
            }[disposition]
            connection.execute(
                update(schema.item_attempts)
                .where(
                    and_(
                        schema.item_attempts.c.item_id
                        == candidate.item.item_id,
                        schema.item_attempts.c.attempt
                        == candidate.attempt.attempt,
                    )
                )
                .values(
                    execution_state=terminal_state.value,
                    dbos_status=(
                        observation.dbos_status.value
                        if observation.dbos_status is not None
                        else None
                    ),
                    terminal_at=now,
                    updated_at=now,
                )
            )
        elif disposition == "error":
            if observation.failure is None:
                raise ReconciliationConflictError(
                    "error observation requires classified failure"
                )
            policy = RetryPolicy.model_validate(operation["retry_policy"])
            next_attempt = int(attempt["attempt"]) + 1
            retryable = (
                observation.failure.failure_class
                in policy.retryable_failure_classes
            )
            retry_disposition = (
                RetryDisposition.RETRYABLE
                if retryable and next_attempt < policy.max_attempts
                else (
                    RetryDisposition.EXHAUSTED
                    if retryable
                    else RetryDisposition.PERMANENT
                )
            )
            connection.execute(
                update(schema.item_attempts)
                .where(
                    and_(
                        schema.item_attempts.c.item_id
                        == candidate.item.item_id,
                        schema.item_attempts.c.attempt
                        == candidate.attempt.attempt,
                    )
                )
                .values(
                    execution_state=AttemptExecutionState.ERROR.value,
                    dbos_status=observation.dbos_status.value,
                    failure=observation.failure.model_dump(mode="json"),
                    retry_disposition=retry_disposition.value,
                    terminal_at=now,
                    updated_at=now,
                )
            )
            if retry_disposition is RetryDisposition.RETRYABLE:
                assert successor_execution is not None
                _insert_automatic_attempt(
                    connection,
                    schema=schema,
                    candidate=candidate,
                    execution=successor_execution,
                    next_attempt=next_attempt,
                    now=now,
                )
                changed_kind = "execution_retry_count"
        else:
            raise ReconciliationConflictError(
                f"unsupported observation disposition {disposition!r}"
            )
        _refresh_operation_lifecycle(
            connection,
            schema=schema,
            operation_key=candidate.item.operation_key,
            now=now,
        )
        return changed_kind


def _record_missing_reobservation(
    connection: Connection,
    *,
    schema: PlatformSchema,
    candidate: ReconciliationCandidate,
    now: datetime,
) -> None:
    markers = schema.missing_reobservations
    statement = pg_insert(markers).values(
        item_id=candidate.item.item_id,
        attempt=candidate.attempt.attempt,
        last_reobserved_at=now,
        observation_count=1,
        created_at=now,
    )
    connection.execute(
        statement.on_conflict_do_update(
            index_elements=[markers.c.item_id, markers.c.attempt],
            set_={
                "last_reobserved_at": now,
                "observation_count": markers.c.observation_count + 1,
            },
        )
    )


def _insert_automatic_attempt(  # noqa: PLR0913
    connection: Connection,
    *,
    schema: PlatformSchema,
    candidate: ReconciliationCandidate,
    execution: ExecutionIdentity,
    next_attempt: int,
    now: datetime,
) -> None:
    connection.execute(
        insert(schema.item_attempts).values(
            item_id=candidate.item.item_id,
            attempt=next_attempt,
            workflow_role=candidate.attempt.workflow_role,
            execution_key=execution.execution_key,
            workflow_id=execution.workflow_id,
            execution_recipe_digest=(
                candidate.attempt.execution_recipe_digest
            ),
            enqueue_state=AttemptEnqueueState.PENDING.value,
            enqueue_try=0,
            execution_state=AttemptExecutionState.NOT_STARTED.value,
            source_attempt=candidate.attempt.attempt,
            source_workflow_id=candidate.attempt.workflow_id,
            retry_reason=AttemptRetryReason.AUTOMATIC_EXECUTION_ERROR.value,
            source_application_version=(
                candidate.attempt.source_application_version
            ),
            missing_observation_count=0,
            requested_service_class=candidate.attempt.requested_service_class,
            requested_service_priority=(
                candidate.attempt.requested_service_priority
            ),
            created_at=now,
            updated_at=now,
        )
    )
    outcome = connection.execute(
        update(schema.items)
        .where(
            and_(
                schema.items.c.item_id == candidate.item.item_id,
                schema.items.c.current_attempt == candidate.attempt.attempt,
            )
        )
        .values(current_attempt=next_attempt, updated_at=now)
    )
    if outcome.rowcount != 1:
        raise ReconciliationConflictError("automatic retry Item CAS lost")


def _lock_operation_for_item(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
) -> Mapping[str, Any]:
    return dict(
        connection.execute(
            select(schema.operations)
            .where(schema.operations.c.operation_key == operation_key)
            .with_for_update()
        )
        .mappings()
        .one()
    )


def _lock_item(
    connection: Connection,
    *,
    schema: PlatformSchema,
    item_id: str,
) -> Mapping[str, Any]:
    return dict(
        connection.execute(
            select(schema.items)
            .where(schema.items.c.item_id == item_id)
            .with_for_update()
        )
        .mappings()
        .one()
    )


def _lock_attempt(
    connection: Connection,
    *,
    schema: PlatformSchema,
    item_id: str,
    attempt: int,
) -> Mapping[str, Any]:
    return dict(
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
        .one()
    )


def _refresh_operation_lifecycle(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    now: datetime,
) -> None:
    rows = list(
        connection.execute(
            select(
                schema.item_attempts.c.enqueue_state,
                schema.item_attempts.c.execution_state,
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
        ).mappings()
    )
    execution_states = [
        AttemptExecutionState(row["execution_state"]) for row in rows
    ]
    enqueue_states = [
        AttemptEnqueueState(row["enqueue_state"]) for row in rows
    ]
    enqueued = enqueue_states.count(AttemptEnqueueState.ENQUEUED)
    workflow_already_present = enqueue_states.count(
        AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT
    )
    enqueue_failed = enqueue_states.count(AttemptEnqueueState.ENQUEUE_ERROR)
    succeeded = execution_states.count(AttemptExecutionState.SUCCEEDED)
    cancelled = execution_states.count(AttemptExecutionState.CANCELLED)
    terminal_failed = sum(
        state
        in {
            AttemptExecutionState.ERROR,
            AttemptExecutionState.RECOVERY_EXHAUSTED,
            AttemptExecutionState.MISSING,
        }
        for state in execution_states
    )
    active = sum(
        state not in TERMINAL_EXECUTION_STATES for state in execution_states
    )
    if any(
        state in {AttemptEnqueueState.PENDING, AttemptEnqueueState.CLAIMING}
        for state in enqueue_states
    ):
        status = OperationStatus.ENQUEUEING
    elif active:
        status = OperationStatus.RUNNING
    elif succeeded == len(rows):
        status = OperationStatus.SUCCEEDED
    elif cancelled == len(rows):
        status = OperationStatus.CANCELLED
    elif succeeded:
        status = OperationStatus.PARTIAL
    else:
        status = OperationStatus.FAILED
    terminal = status in {
        OperationStatus.SUCCEEDED,
        OperationStatus.PARTIAL,
        OperationStatus.FAILED,
        OperationStatus.CANCELLED,
    }
    connection.execute(
        update(schema.operations)
        .where(schema.operations.c.operation_key == operation_key)
        .values(
            status=status.value,
            platform_cut_version=schema.operations.c.platform_cut_version + 1,
            enqueued_count=enqueued,
            workflow_already_present_count=workflow_already_present,
            enqueue_failed_count=enqueue_failed,
            active_count=active,
            succeeded_count=succeeded,
            terminal_failed_count=terminal_failed,
            cancelled_count=cancelled,
            completed_at=now if terminal else None,
            updated_at=now,
        )
    )
