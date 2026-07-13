"""Logical cancellation facade; DBOS calls follow the intent commit."""
# ruff: noqa: BLE001, PLR0913, TRY301

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import (
    Connection,
    Engine,
    and_,
    insert,
    null,
    or_,
    select,
    text,
    update,
)

from dr_platform.claims import (
    _acquire_export_writer_lock,
    _acquire_workflow_reference_locks,
    _database_now,
    _invalidate_attempt_claims,
)
from dr_platform.db import PlatformSchema
from dr_platform.reconciliation import _refresh_operation_lifecycle
from dr_platform.records import FailureSnapshot
from dr_platform.status import (
    CONFIRMED_ENQUEUE_STATES,
    TERMINAL_EXECUTION_STATES,
    AttemptEnqueueState,
    AttemptExecutionState,
    CancellationDisposition,
    CancellationOrigin,
    EnqueueClaimDisposition,
    EnqueueCompensationDisposition,
    EnqueueCompensationReason,
    FailureClass,
    OperationStatus,
    RetryDisposition,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

NonEmptyStr = Annotated[str, Field(min_length=1)]
NonNegativeInt = Annotated[int, Field(ge=0)]


class CancellationConflictError(RuntimeError):
    """A request ID was replayed with unequal immutable intent."""


class CancellationInspectionDisposition(StrEnum):
    ACTIVE = "active"
    ABSENT = "absent"
    SUCCEEDED = "succeeded"
    ERROR = "error"
    CANCELLED = "cancelled"


class CancellationInspection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    disposition: CancellationInspectionDisposition
    has_children: bool = False
    dbos_status: str | None = None
    failure: FailureSnapshot | None = None
    retry_disposition: RetryDisposition | None = None

    @model_validator(mode="after")
    def validate_error(self) -> CancellationInspection:
        if self.disposition is CancellationInspectionDisposition.ERROR:
            if self.failure is None or self.retry_disposition is None:
                raise ValueError(
                    "error inspection requires failure and retry disposition"
                )
        elif self.failure is not None or self.retry_disposition is not None:
            raise ValueError("only error inspections carry failure facts")
        return self


@runtime_checkable
class WorkflowCanceller(Protocol):
    def inspect(self, *, workflow_id: str) -> CancellationInspection: ...

    def cancel_workflow(
        self, *, workflow_id: str, cancel_children: bool
    ) -> None: ...


class CancellationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    request_id: NonEmptyStr
    requested_by: NonEmptyStr


class CancellationAttemptResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    workflow_id: NonEmptyStr
    disposition: CancellationDisposition


class CancellationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: CancellationRequest
    results: tuple[CancellationAttemptResult, ...]


def cancel_operation(
    request: CancellationRequest,
    *,
    engine: Engine,
    canceller: WorkflowCanceller,
    schema: PlatformSchema | None = None,
) -> CancellationResult:
    """Cancel current Attempts, preserving durable intent for exact replay."""
    selected_schema = schema or PlatformSchema()
    planned = _persist_intent(engine, request=request, schema=selected_schema)
    for row in planned:
        if row["cancellation_disposition"] in {
            None,
            CancellationDisposition.FAILED.value,
        }:
            _cancel_one(
                engine=engine,
                request=request,
                schema=selected_schema,
                planned=row,
                canceller=canceller,
            )
    # A call-started claimant may cross DBOS after the intent transaction has
    # invalidated its Claim.  Repair that durable hazard in the same bounded
    # cancellation pass; later reconciliation can safely replay it as well.
    repair_late_enqueue_compensations(
        engine=engine, canceller=canceller, schema=selected_schema
    )
    return _load_result(engine, request=request, schema=selected_schema)


def repair_late_enqueue_compensations(
    *,
    engine: Engine,
    canceller: WorkflowCanceller,
    schema: PlatformSchema | None = None,
    limit: int = 100,
) -> int:
    """Repair one bounded page of invalidated call-started enqueue hazards.

    The compensation ledger is the only mutable artifact here: cancelled
    Attempts can already be terminal, so this path never updates them.
    Absence deliberately remains pending.  The frozen ledger has nowhere to
    persist per-compensation grace/count observations or a late-appearance
    successor after ``NO_WORKFLOW_FOUND``.
    """
    if limit <= 0:
        raise ValueError("compensation repair limit must be positive")
    selected_schema = schema or PlatformSchema()
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                selected_schema.items.c.operation_key,
                selected_schema.enqueue_claims.c.item_id,
                selected_schema.enqueue_claims.c.attempt,
                selected_schema.enqueue_claims.c.claim_id,
                selected_schema.enqueue_claims.c.workflow_id,
            )
            .select_from(selected_schema.enqueue_claims)
            .join(
                selected_schema.items,
                selected_schema.items.c.item_id
                == selected_schema.enqueue_claims.c.item_id,
            )
            .outerjoin(
                selected_schema.enqueue_compensations,
                and_(
                    selected_schema.enqueue_compensations.c.item_id
                    == selected_schema.enqueue_claims.c.item_id,
                    selected_schema.enqueue_compensations.c.attempt
                    == selected_schema.enqueue_claims.c.attempt,
                    selected_schema.enqueue_compensations.c.claim_id
                    == selected_schema.enqueue_claims.c.claim_id,
                ),
            )
            .where(
                and_(
                    selected_schema.enqueue_claims.c.disposition
                    == EnqueueClaimDisposition.INVALIDATED.value,
                    selected_schema.enqueue_claims.c.enqueue_call_started_at.is_not(
                        None
                    ),
                    or_(
                        selected_schema.enqueue_compensations.c.claim_id.is_(
                            None
                        ),
                        selected_schema.enqueue_compensations.c.cancel_disposition.in_(
                            [
                                EnqueueCompensationDisposition.PENDING.value,
                                EnqueueCompensationDisposition.FAILED.value,
                            ]
                        ),
                    ),
                )
            )
            .order_by(
                selected_schema.enqueue_claims.c.workflow_id,
                selected_schema.enqueue_claims.c.item_id,
                selected_schema.enqueue_claims.c.attempt,
                selected_schema.enqueue_claims.c.claim_id,
            )
            .limit(limit)
        ).mappings()
        candidates = [dict(row) for row in rows]
    repaired = 0
    for candidate in candidates:
        with engine.connect() as guard:
            workflow_id = str(candidate["workflow_id"])
            guard.execute(
                text(
                    "SELECT pg_advisory_lock("
                    "hashtextextended(:workflow_id, 1))"
                ),
                {"workflow_id": workflow_id},
            )
            guard.commit()
            try:
                prepared = _prepare_compensation_repair(
                    engine=engine, schema=selected_schema, candidate=candidate
                )
                if prepared is None:
                    continue
                repaired += 1
                disposition, failure = _perform_compensation_cancel(
                    canceller=canceller, workflow_id=workflow_id
                )
                _finalize_compensation_repair(
                    engine=engine,
                    schema=selected_schema,
                    candidate=candidate,
                    disposition=disposition,
                    failure=failure,
                )
            finally:
                guard.execute(
                    text(
                        "SELECT pg_advisory_unlock("
                        "hashtextextended(:workflow_id, 1))"
                    ),
                    {"workflow_id": workflow_id},
                )
                guard.commit()
    return repaired


def _persist_intent(
    engine: Engine, *, request: CancellationRequest, schema: PlatformSchema
) -> list[dict[str, Any]]:
    with engine.begin() as connection:
        # Retry creation and current-Attempt advancement also take this lock.
        # Select the cancellation set only after acquiring it so the locked
        # hierarchy cannot be based on a stale current_attempt pointer.
        _acquire_export_writer_lock(connection)
        candidates = _current_attempts(
            connection, schema=schema, operation_key=request.operation_key
        )
        _acquire_workflow_reference_locks(
            connection, [row["workflow_id"] for row in candidates]
        )
        operations, locked_attempts = _lock_cancellation_hierarchy(
            connection,
            schema=schema,
            operation_key=request.operation_key,
            candidates=candidates,
        )
        _lock_claims(connection, schema=schema, rows=candidates)
        operation = operations[request.operation_key]
        attempts = [
            locked_attempts[(row["item_id"], row["attempt"])]
            for row in candidates
        ]
        exact_replay = _validate_replay(attempts, request=request)
        if exact_replay:
            return _request_rows(connection, schema=schema, request=request)
        now = _database_now(connection)
        if operation["status"] not in {
            OperationStatus.SUCCEEDED.value,
            OperationStatus.PARTIAL.value,
            OperationStatus.FAILED.value,
            OperationStatus.CANCELLED.value,
        }:
            connection.execute(
                update(schema.operations)
                .where(
                    schema.operations.c.operation_key == request.operation_key
                )
                .values(
                    status=OperationStatus.CANCELLING.value,
                    cancel_requested_at=now,
                    completed_at=None,
                    updated_at=now,
                    platform_cut_version=(
                        schema.operations.c.platform_cut_version + 1
                    ),
                )
            )
        for row in attempts:
            state = AttemptExecutionState(row["execution_state"])
            if state in TERMINAL_EXECUTION_STATES:
                _set_intent(
                    connection,
                    schema=schema,
                    row=row,
                    request=request,
                    now=now,
                    disposition=(
                        CancellationDisposition.ALREADY_CANCELLED
                        if state is AttemptExecutionState.CANCELLED
                        else CancellationDisposition.OBSERVED_TERMINAL
                    ),
                )
                continue
            _set_intent(
                connection,
                schema=schema,
                row=row,
                request=request,
                now=now,
            )
            _invalidate_attempt_claims(
                connection,
                schema=schema,
                item_id=str(row["item_id"]),
                attempt=int(row["attempt"]),
                invalidated_by=request.request_id,
                now=now,
            )
            if AttemptEnqueueState(row["enqueue_state"]) not in (
                CONFIRMED_ENQUEUE_STATES
            ):
                _finalize(
                    connection,
                    schema=schema,
                    row=row,
                    request=request,
                    disposition=CancellationDisposition.NOT_ENQUEUED,
                    now=now,
                )
        _refresh_operation_lifecycle(
            connection,
            schema=schema,
            operation_key=request.operation_key,
            now=now,
        )
        return _request_rows(connection, schema=schema, request=request)


def _cancel_one(
    *,
    engine: Engine,
    request: CancellationRequest,
    schema: PlatformSchema,
    planned: Mapping[str, Any],
    canceller: WorkflowCanceller,
) -> None:
    workflow_id = str(planned["workflow_id"])
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(connection, [workflow_id])
        _, attempts = _lock_cancellation_hierarchy(
            connection,
            schema=schema,
            operation_key=request.operation_key,
            candidates=[planned],
        )
        row = attempts[(planned["item_id"], planned["attempt"])]
        if not _still_pending(row, request=request):
            return
        if _has_other_reference(
            connection,
            schema=schema,
            workflow_id=workflow_id,
            item_id=str(planned["item_id"]),
        ):
            now = _database_now(connection)
            _finalize(
                connection,
                schema=schema,
                row=row,
                request=request,
                disposition=CancellationDisposition.SKIPPED_SHARED,
                now=now,
            )
            _refresh_if_resolved(
                connection,
                schema=schema,
                operation_key=request.operation_key,
                now=now,
            )
            return
    try:
        inspection = canceller.inspect(workflow_id=workflow_id)
        if inspection.workflow_id != workflow_id:
            raise CancellationConflictError(
                "inspector changed workflow identity"
            )
        if inspection.has_children:
            disposition = CancellationDisposition.FAILED
        elif inspection.disposition in {
            CancellationInspectionDisposition.SUCCEEDED,
            CancellationInspectionDisposition.ERROR,
        }:
            disposition = CancellationDisposition.OBSERVED_TERMINAL
        elif (
            inspection.disposition
            is CancellationInspectionDisposition.CANCELLED
        ):
            disposition = CancellationDisposition.ALREADY_CANCELLED
        else:
            canceller.cancel_workflow(
                workflow_id=workflow_id, cancel_children=False
            )
            disposition = CancellationDisposition.DBOS_CANCELLED
    except CancellationConflictError:
        raise
    except Exception:  # external boundary; the durable result is FAILED
        disposition = CancellationDisposition.FAILED
        inspection = None
    _finalize_physical(
        engine,
        request=request,
        schema=schema,
        planned=planned,
        disposition=disposition,
        inspection=inspection,
    )


def _finalize_physical(
    engine: Engine,
    *,
    request: CancellationRequest,
    schema: PlatformSchema,
    planned: Mapping[str, Any],
    disposition: CancellationDisposition,
    inspection: CancellationInspection | None,
) -> None:
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(connection, [planned["workflow_id"]])
        _, attempts = _lock_cancellation_hierarchy(
            connection,
            schema=schema,
            operation_key=request.operation_key,
            candidates=[planned],
        )
        row = attempts[(planned["item_id"], planned["attempt"])]
        if not _still_pending(row, request=request):
            return
        now = _database_now(connection)
        _finalize(
            connection,
            schema=schema,
            row=row,
            request=request,
            disposition=disposition,
            now=now,
            inspection=inspection,
        )
        _refresh_if_resolved(
            connection,
            schema=schema,
            operation_key=request.operation_key,
            now=now,
        )


def _set_intent(
    connection: Connection,
    *,
    schema: PlatformSchema,
    row: Mapping[str, Any],
    request: CancellationRequest,
    now: datetime,
    disposition: CancellationDisposition | None = None,
) -> None:
    values: dict[str, Any] = {
        "cancellation_request_id": request.request_id,
        "cancellation_requested_at": now,
        "cancellation_requested_by": request.requested_by,
        "cancellation_origin": CancellationOrigin.LOCAL_OPERATION.value,
        "updated_at": now,
    }
    if disposition is None:
        values["execution_state"] = (
            AttemptExecutionState.CANCEL_REQUESTED.value
        )
    else:
        values["cancellation_disposition"] = disposition.value
    connection.execute(
        update(schema.item_attempts)
        .where(
            and_(
                schema.item_attempts.c.item_id == row["item_id"],
                schema.item_attempts.c.attempt == row["attempt"],
                schema.item_attempts.c.cancellation_request_id.is_(None),
            )
        )
        .values(**values)
    )


def _finalize(
    connection: Connection,
    *,
    schema: PlatformSchema,
    row: Mapping[str, Any],
    request: CancellationRequest,
    disposition: CancellationDisposition,
    now: datetime,
    inspection: CancellationInspection | None = None,
) -> None:
    values: dict[str, Any] = {
        "cancellation_disposition": disposition.value,
        "updated_at": now,
    }
    if disposition in {
        CancellationDisposition.DBOS_CANCELLED,
        CancellationDisposition.ALREADY_CANCELLED,
        CancellationDisposition.NOT_ENQUEUED,
        CancellationDisposition.SKIPPED_SHARED,
    }:
        values.update(
            execution_state=AttemptExecutionState.CANCELLED.value,
            terminal_at=now,
        )
        if disposition is CancellationDisposition.NOT_ENQUEUED:
            values.update(
                enqueue_state=AttemptEnqueueState.PENDING.value,
                current_claim_id=None,
            )
    elif disposition is CancellationDisposition.OBSERVED_TERMINAL:
        if inspection is None:
            raise CancellationConflictError("terminal observation was lost")
        if (
            inspection.disposition
            is CancellationInspectionDisposition.SUCCEEDED
        ):
            values.update(
                execution_state=AttemptExecutionState.SUCCEEDED.value,
                dbos_status=inspection.dbos_status,
                terminal_at=now,
            )
        elif (
            inspection.disposition
            is CancellationInspectionDisposition.CANCELLED
        ):
            values.update(
                execution_state=AttemptExecutionState.CANCELLED.value,
                dbos_status=inspection.dbos_status,
                terminal_at=now,
            )
        elif inspection.disposition is CancellationInspectionDisposition.ERROR:
            assert inspection.failure is not None
            assert inspection.retry_disposition is not None
            values.update(
                execution_state=AttemptExecutionState.ERROR.value,
                dbos_status=inspection.dbos_status,
                failure=inspection.failure.model_dump(mode="json"),
                retry_disposition=inspection.retry_disposition.value,
                terminal_at=now,
            )
    connection.execute(
        update(schema.item_attempts)
        .where(
            and_(
                schema.item_attempts.c.item_id == row["item_id"],
                schema.item_attempts.c.attempt == row["attempt"],
                schema.item_attempts.c.cancellation_request_id
                == request.request_id,
            )
        )
        .values(**values)
    )


def _validate_replay(
    rows: Sequence[Mapping[str, Any]], *, request: CancellationRequest
) -> bool:
    existing = {
        (row["cancellation_request_id"], row["cancellation_requested_by"])
        for row in rows
        if row["cancellation_request_id"] is not None
    }
    expected = {(request.request_id, request.requested_by)}
    if existing and existing != expected:
        raise CancellationConflictError(
            "cancellation request replay conflicts with durable intent"
        )
    return bool(existing)


def _current_attempts(
    connection: Connection, *, schema: PlatformSchema, operation_key: str
) -> list[dict[str, Any]]:
    statement = (
        select(schema.item_attempts)
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
        .order_by(
            schema.item_attempts.c.item_id, schema.item_attempts.c.attempt
        )
    )
    return [dict(row) for row in connection.execute(statement).mappings()]


def _request_rows(
    connection: Connection,
    *,
    schema: PlatformSchema,
    request: CancellationRequest,
) -> list[dict[str, Any]]:
    return [
        row
        for row in _current_attempts(
            connection, schema=schema, operation_key=request.operation_key
        )
        if row["cancellation_request_id"] == request.request_id
    ]


def _load_result(
    engine: Engine, *, request: CancellationRequest, schema: PlatformSchema
) -> CancellationResult:
    with engine.connect() as connection:
        rows = _request_rows(connection, schema=schema, request=request)
    return CancellationResult(
        request=request,
        results=tuple(
            CancellationAttemptResult(
                item_id=row["item_id"],
                attempt=row["attempt"],
                workflow_id=row["workflow_id"],
                disposition=CancellationDisposition(
                    row["cancellation_disposition"]
                    or CancellationDisposition.FAILED.value
                ),
            )
            for row in rows
        ),
    )


def _lock_cancellation_hierarchy(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, int], dict[str, Any]]]:
    """Lock every live reference before cancellation-owned domain rows."""
    workflow_ids = sorted({str(row["workflow_id"]) for row in candidates})
    _acquire_workflow_reference_locks(connection, workflow_ids)
    reference_rows = list(
        connection.execute(
            select(
                schema.items.c.operation_key,
                schema.item_attempts.c.item_id,
                schema.item_attempts.c.attempt,
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
            .where(schema.item_attempts.c.workflow_id.in_(workflow_ids))
            .order_by(
                schema.items.c.operation_key,
                schema.item_attempts.c.item_id,
                schema.item_attempts.c.attempt,
            )
        ).mappings()
    )
    operation_keys = sorted(
        {operation_key, *(str(row["operation_key"]) for row in reference_rows)}
    )
    operations = {
        str(row["operation_key"]): dict(row)
        for row in connection.execute(
            select(schema.operations)
            .where(schema.operations.c.operation_key.in_(operation_keys))
            .order_by(schema.operations.c.operation_key)
            .with_for_update()
        ).mappings()
    }
    if operation_key not in operations:
        raise CancellationConflictError("unknown cancellation Operation")
    item_ids = sorted(
        {
            *(str(row["item_id"]) for row in reference_rows),
            *(str(row["item_id"]) for row in candidates),
        }
    )
    if item_ids:
        connection.execute(
            select(schema.items.c.item_id)
            .where(schema.items.c.item_id.in_(item_ids))
            .order_by(schema.items.c.item_id)
            .with_for_update()
        ).all()
    attempt_keys = sorted(
        {
            *(
                (str(row["item_id"]), int(row["attempt"]))
                for row in reference_rows
            ),
            *(
                (str(row["item_id"]), int(row["attempt"]))
                for row in candidates
            ),
        }
    )
    attempts: dict[tuple[str, int], dict[str, Any]] = {}
    for item_id, attempt in attempt_keys:
        attempts[(item_id, attempt)] = dict(
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
    return operations, attempts


def _lock_claims(
    connection: Connection,
    *,
    schema: PlatformSchema,
    rows: Sequence[Mapping[str, Any]],
) -> None:
    for row in sorted(rows, key=lambda row: (row["item_id"], row["attempt"])):
        connection.execute(
            select(schema.enqueue_claims)
            .where(
                and_(
                    schema.enqueue_claims.c.item_id == row["item_id"],
                    schema.enqueue_claims.c.attempt == row["attempt"],
                )
            )
            .order_by(schema.enqueue_claims.c.claim_id)
            .with_for_update()
        ).all()


def _still_pending(
    row: Mapping[str, Any], *, request: CancellationRequest
) -> bool:
    return row["cancellation_request_id"] == request.request_id and row[
        "cancellation_disposition"
    ] in {None, CancellationDisposition.FAILED.value}


def _has_other_reference(
    connection: Connection,
    *,
    schema: PlatformSchema,
    workflow_id: str,
    item_id: str,
) -> bool:
    statement = (
        select(schema.item_attempts.c.item_id)
        .select_from(schema.items)
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
                schema.item_attempts.c.workflow_id == workflow_id,
                schema.item_attempts.c.item_id != item_id,
                schema.item_attempts.c.execution_state.not_in(
                    [state.value for state in TERMINAL_EXECUTION_STATES]
                ),
            )
        )
        .order_by(
            schema.items.c.operation_key,
            schema.item_attempts.c.item_id,
            schema.item_attempts.c.attempt,
        )
        .with_for_update()
    )
    return connection.execute(statement).first() is not None


def _refresh_if_resolved(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    now: datetime,
) -> None:
    _refresh_operation_lifecycle(
        connection, schema=schema, operation_key=operation_key, now=now
    )


def _prepare_compensation_repair(
    *,
    engine: Engine,
    schema: PlatformSchema,
    candidate: Mapping[str, Any],
) -> bool | None:
    """Insert/reload one exact compensation and decide if it is exclusive."""
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        # The hierarchy helper acquires lexical workflow locks before every
        # Operation/Item/Attempt row, including all current references.
        _, attempts = _lock_cancellation_hierarchy(
            connection,
            schema=schema,
            operation_key=str(candidate["operation_key"]),
            candidates=[candidate],
        )
        _lock_claims(connection, schema=schema, rows=[candidate])
        claim = (
            connection.execute(
                select(schema.enqueue_claims)
                .where(
                    and_(
                        schema.enqueue_claims.c.item_id
                        == candidate["item_id"],
                        schema.enqueue_claims.c.attempt
                        == candidate["attempt"],
                        schema.enqueue_claims.c.claim_id
                        == candidate["claim_id"],
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if claim is None:
            raise CancellationConflictError("compensation Claim disappeared")
        if (
            claim["workflow_id"] != candidate["workflow_id"]
            or claim["disposition"]
            != EnqueueClaimDisposition.INVALIDATED.value
            or claim["enqueue_call_started_at"] is None
        ):
            return None
        compensation = (
            connection.execute(
                select(schema.enqueue_compensations)
                .where(
                    and_(
                        schema.enqueue_compensations.c.item_id
                        == candidate["item_id"],
                        schema.enqueue_compensations.c.attempt
                        == candidate["attempt"],
                        schema.enqueue_compensations.c.claim_id
                        == candidate["claim_id"],
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if compensation is None:
            now = _database_now(connection)
            connection.execute(
                insert(schema.enqueue_compensations).values(
                    item_id=candidate["item_id"],
                    attempt=candidate["attempt"],
                    claim_id=candidate["claim_id"],
                    workflow_id=claim["workflow_id"],
                    reason=(
                        EnqueueCompensationReason.INVALIDATED_CALL_STARTED_CLAIM.value
                    ),
                    cancel_disposition=EnqueueCompensationDisposition.PENDING.value,
                    created_at=now,
                )
            )
        else:
            if compensation["workflow_id"] != claim[
                "workflow_id"
            ] or compensation["reason"] != (
                EnqueueCompensationReason.INVALIDATED_CALL_STARTED_CLAIM.value
            ):
                raise CancellationConflictError(
                    "enqueue compensation replay conflicts"
                )
            if compensation["resolved_at"] is not None:
                return None
        resolved_sibling = connection.execute(
            select(schema.enqueue_compensations.c.cancel_disposition)
            .where(
                and_(
                    schema.enqueue_compensations.c.workflow_id
                    == candidate["workflow_id"],
                    schema.enqueue_compensations.c.resolved_at.is_not(None),
                    schema.enqueue_compensations.c.cancel_disposition.in_(
                        [
                            EnqueueCompensationDisposition.CANCELLED.value,
                            EnqueueCompensationDisposition.OBSERVED_TERMINAL.value,
                        ]
                    ),
                    or_(
                        schema.enqueue_compensations.c.item_id
                        != candidate["item_id"],
                        schema.enqueue_compensations.c.attempt
                        != candidate["attempt"],
                        schema.enqueue_compensations.c.claim_id
                        != candidate["claim_id"],
                    ),
                )
            )
            .order_by(schema.enqueue_compensations.c.created_at)
            .limit(1)
            .with_for_update()
        ).scalar_one_or_none()
        if resolved_sibling is not None:
            _resolve_compensation(
                connection,
                schema=schema,
                candidate=candidate,
                disposition=EnqueueCompensationDisposition(resolved_sibling),
                failure=None,
            )
            return None
        if _has_other_reference(
            connection,
            schema=schema,
            workflow_id=str(candidate["workflow_id"]),
            item_id=str(candidate["item_id"]),
        ):
            _resolve_compensation(
                connection,
                schema=schema,
                candidate=candidate,
                disposition=EnqueueCompensationDisposition.SKIPPED_SHARED,
                failure=None,
            )
            return None
        # The attempt lock above is intentionally only a topology guard.  A
        # terminal Attempt is immutable and is never changed by compensation.
        if (candidate["item_id"], candidate["attempt"]) not in attempts:
            raise CancellationConflictError("compensation Attempt disappeared")
        return True


def _perform_compensation_cancel(
    *, canceller: WorkflowCanceller, workflow_id: str
) -> tuple[EnqueueCompensationDisposition | None, FailureSnapshot | None]:
    """Do the DBOS work after releasing all kernel row locks."""
    try:
        inspection = canceller.inspect(workflow_id=workflow_id)
        if inspection.workflow_id != workflow_id:
            raise CancellationConflictError(
                "inspector changed workflow identity"
            )
        if inspection.has_children:
            raise CancellationConflictError("workflow topology drift")
        if inspection.disposition is CancellationInspectionDisposition.ABSENT:
            return None, None
        if inspection.disposition in {
            CancellationInspectionDisposition.SUCCEEDED,
            CancellationInspectionDisposition.ERROR,
        }:
            return EnqueueCompensationDisposition.OBSERVED_TERMINAL, None
        if (
            inspection.disposition
            is CancellationInspectionDisposition.CANCELLED
        ):
            return EnqueueCompensationDisposition.CANCELLED, None
        canceller.cancel_workflow(
            workflow_id=workflow_id, cancel_children=False
        )
        return EnqueueCompensationDisposition.CANCELLED, None  # noqa: TRY300
    except CancellationConflictError:
        raise
    except Exception as error:  # external boundary; persist safe failure only
        return (
            EnqueueCompensationDisposition.FAILED,
            FailureSnapshot(
                failure_class=FailureClass.UNKNOWN,
                error_type="CompensationCancellationFailed",
                underlying_exception_type=type(error).__name__,
                message="late-enqueue compensation cancellation failed",
            ),
        )


def _finalize_compensation_repair(
    *,
    engine: Engine,
    schema: PlatformSchema,
    candidate: Mapping[str, Any],
    disposition: EnqueueCompensationDisposition | None,
    failure: FailureSnapshot | None,
) -> None:
    # ``None`` is the fail-closed absent-workflow result: no bounded durable
    # observation ledger exists in the frozen schema, so it stays pending.
    if disposition is None:
        return
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _lock_cancellation_hierarchy(
            connection,
            schema=schema,
            operation_key=str(candidate["operation_key"]),
            candidates=[candidate],
        )
        _lock_claims(connection, schema=schema, rows=[candidate])
        compensation = (
            connection.execute(
                select(schema.enqueue_compensations)
                .where(
                    and_(
                        schema.enqueue_compensations.c.item_id
                        == candidate["item_id"],
                        schema.enqueue_compensations.c.attempt
                        == candidate["attempt"],
                        schema.enqueue_compensations.c.claim_id
                        == candidate["claim_id"],
                    )
                )
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if compensation is None:
            raise CancellationConflictError("compensation row disappeared")
        if compensation["workflow_id"] != candidate["workflow_id"]:
            raise CancellationConflictError(
                "compensation workflow identity changed"
            )
        if compensation["resolved_at"] is None:
            _resolve_compensation(
                connection,
                schema=schema,
                candidate=candidate,
                disposition=disposition,
                failure=failure,
            )


def _resolve_compensation(
    connection: Connection,
    *,
    schema: PlatformSchema,
    candidate: Mapping[str, Any],
    disposition: EnqueueCompensationDisposition,
    failure: FailureSnapshot | None,
) -> None:
    now = _database_now(connection)
    values: dict[str, Any] = {
        "cancel_disposition": disposition.value,
        "resolved_at": (
            None
            if disposition is EnqueueCompensationDisposition.FAILED
            else now
        ),
        "failure": failure.model_dump(mode="json") if failure else null(),
    }
    connection.execute(
        update(schema.enqueue_compensations)
        .where(
            and_(
                schema.enqueue_compensations.c.item_id == candidate["item_id"],
                schema.enqueue_compensations.c.attempt == candidate["attempt"],
                schema.enqueue_compensations.c.claim_id
                == candidate["claim_id"],
                schema.enqueue_compensations.c.resolved_at.is_(None),
            )
        )
        .values(**values)
    )
