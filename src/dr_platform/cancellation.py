"""Logical cancellation facade; DBOS calls follow the intent commit."""
# ruff: noqa: BLE001, PLR0913, TRY301

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import Connection, Engine, and_, select, update

from dr_platform.claims import (
    _acquire_export_writer_lock,
    _acquire_workflow_reference_locks,
    _database_now,
    _invalidate_attempt_claims,
)
from dr_platform.db import PlatformSchema
from dr_platform.reconciliation import _refresh_operation_lifecycle
from dr_platform.records import FailureSnapshot  # noqa: TC001
from dr_platform.status import (
    CONFIRMED_ENQUEUE_STATES,
    TERMINAL_EXECUTION_STATES,
    AttemptEnqueueState,
    AttemptExecutionState,
    CancellationDisposition,
    CancellationOrigin,
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
    return _load_result(engine, request=request, schema=selected_schema)


def _persist_intent(
    engine: Engine, *, request: CancellationRequest, schema: PlatformSchema
) -> list[dict[str, Any]]:
    with engine.begin() as connection:
        candidates = _current_attempts(
            connection, schema=schema, operation_key=request.operation_key
        )
        _acquire_export_writer_lock(connection)
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
            operation_key=request.operation_key,
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
    operation_key: str,
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
                schema.items.c.operation_key != operation_key,
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
