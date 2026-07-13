"""Typed, bounded inspection, health, and lifecycle waiting facade."""

from __future__ import annotations

from collections.abc import Callable  # noqa: TC003 -- Pydantic resolves it
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Annotated, Any, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
)
from sqlalchemy import Connection, Engine, and_, func, or_, select

from dr_platform.db import PlatformSchema
from dr_platform.reconciliation_runtime import (
    DbosStepObservation,
    ReconcileOptions,
    reconcile,
)
from dr_platform.records import (
    AttemptRecord,
    EnqueueClaimRecord,
    EnqueueCompensationRecord,
    ItemRecord,
    OperationRecord,
)
from dr_platform.status import (
    TERMINAL_OPERATION_STATUSES,
    AttemptEnqueueState,
    AttemptExecutionState,
    RetryDisposition,
)

if TYPE_CHECKING:
    from dr_platform.cancellation import WorkflowCanceller
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.reconciliation_runtime import LifecycleObservationReader
    from dr_platform.targets import TargetResolver

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
NonNegativeFloat = Annotated[StrictFloat, Field(ge=0)]
PositiveFloat = Annotated[StrictFloat, Field(gt=0)]
DEFAULT_INSPECTION_PAGE_SIZE = 100


class ItemInspection(BaseModel):
    """One Item and its authoritative current Attempt, if registered."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    item: ItemRecord
    current_attempt: AttemptRecord | None


class AttemptInspection(BaseModel):
    """One append-only Attempt with its durable enqueue lineage."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt: AttemptRecord
    claims: tuple[EnqueueClaimRecord, ...]
    compensations: tuple[EnqueueCompensationRecord, ...]
    dbos_steps: tuple[DbosStepObservation, ...] = ()


class OperationInspection(BaseModel):
    """Aggregate Operation state plus bounded current lifecycle facts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation: OperationRecord
    current_item_count: NonNegativeInt
    current_attempt_count: NonNegativeInt


class HealthReport(BaseModel):
    """Machine-readable health facts derived only from platform rows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    observed_at: datetime
    oldest_queued_age_seconds: NonNegativeInt | None
    oldest_active_age_seconds: NonNegativeInt | None
    no_progress_operation_count: NonNegativeInt
    failure_count: NonNegativeInt
    permanent_failure_count: NonNegativeInt
    retryable_failure_count: NonNegativeInt
    missing_count: NonNegativeInt
    retry_exhausted_count: NonNegativeInt
    recovery_exhausted_count: NonNegativeInt
    active_throttle_hold_count: NonNegativeInt
    active_backoff_count: NonNegativeInt
    queue_priority_drift_count: NonNegativeInt
    queue_configuration_drift_count: NonNegativeInt | None
    application_version_mismatch_count: NonNegativeInt
    incomplete_cancellation_count: NonNegativeInt
    incomplete_compensation_count: NonNegativeInt
    threshold_breaches: tuple[StrictStr, ...]


class OperationWaitOptions(BaseModel):
    """Injected timing and bounded reconciliation controls for one wait."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    poll_interval_seconds: PositiveFloat
    timeout_seconds: PositiveFloat
    reconciliation_page_size: PositiveInt = DEFAULT_INSPECTION_PAGE_SIZE
    clock: Callable[[], datetime] = Field(exclude=True)
    sleeper: Callable[[float], None] = Field(exclude=True)


class OperationWaitResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    inspection: OperationInspection
    elapsed_seconds: NonNegativeFloat
    poll_count: NonNegativeInt


class OperationWaitTimeoutError(TimeoutError):
    """The wait budget elapsed; ``inspection`` is the final durable view."""

    def __init__(self, inspection: OperationInspection) -> None:
        self.inspection = inspection
        super().__init__(
            "operation "
            f"{inspection.operation.operation_key!r} did not reach "
            "a terminal state"
        )


class QueueHealthProbe(Protocol):
    """Private read seam for app-owned DBOS queue configuration checks."""

    def configuration_drift_count(self) -> int: ...


def list_operations(
    *,
    engine: Engine,
    cursor: str | None = None,
    limit: int = DEFAULT_INSPECTION_PAGE_SIZE,
    schema: PlatformSchema | None = None,
) -> tuple[OperationInspection, ...]:
    """Return one stable page; unknown cursors fail closed."""
    _validate_limit(limit)
    selected = schema or PlatformSchema()
    with engine.connect() as connection:
        if cursor is not None:
            cursor_row = connection.execute(
                select(
                    selected.operations.c.created_at,
                    selected.operations.c.operation_key,
                ).where(selected.operations.c.operation_key == cursor)
            ).one_or_none()
            if cursor_row is None:
                raise ValueError("operation cursor is unknown")
            created_at, operation_key = cursor_row
            statement = select(selected.operations).where(
                or_(
                    selected.operations.c.created_at > created_at,
                    and_(
                        selected.operations.c.created_at == created_at,
                        selected.operations.c.operation_key > operation_key,
                    ),
                )
            )
        else:
            statement = select(selected.operations)
        rows = connection.execute(
            statement.order_by(
                selected.operations.c.created_at,
                selected.operations.c.operation_key,
            ).limit(limit)
        ).mappings()
        return tuple(
            _operation_inspection(connection, selected, dict(row))
            for row in rows
        )


def inspect_operation(
    operation_key: str,
    *,
    engine: Engine,
    schema: PlatformSchema | None = None,
) -> OperationInspection:
    selected = schema or PlatformSchema()
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(selected.operations).where(
                    selected.operations.c.operation_key == operation_key
                )
            )
            .mappings()
            .one()
        )
        return _operation_inspection(connection, selected, dict(row))


def list_items(
    operation_key: str,
    *,
    engine: Engine,
    cursor: tuple[int, str] | None = None,
    limit: int = DEFAULT_INSPECTION_PAGE_SIZE,
    schema: PlatformSchema | None = None,
) -> tuple[ItemInspection, ...]:
    _validate_limit(limit)
    selected = schema or PlatformSchema()
    with engine.connect() as connection:
        _require_operation(connection, selected, operation_key)
        statement = select(selected.items).where(
            selected.items.c.operation_key == operation_key
        )
        if cursor is not None:
            index, item_id = cursor
            cursor_exists = connection.execute(
                select(selected.items.c.item_id).where(
                    and_(
                        selected.items.c.operation_key == operation_key,
                        selected.items.c.item_index == index,
                        selected.items.c.item_id == item_id,
                    )
                )
            ).scalar_one_or_none()
            if cursor_exists is None:
                raise ValueError("item cursor is unknown")
            statement = statement.where(
                or_(
                    selected.items.c.item_index > index,
                    and_(
                        selected.items.c.item_index == index,
                        selected.items.c.item_id > item_id,
                    ),
                )
            )
        rows = connection.execute(
            statement.order_by(
                selected.items.c.item_index, selected.items.c.item_id
            ).limit(limit)
        ).mappings()
        return tuple(
            _item_inspection(connection, selected, dict(row)) for row in rows
        )


def list_attempts(  # noqa: PLR0913
    operation_key: str,
    *,
    engine: Engine,
    cursor: tuple[str, int] | None = None,
    limit: int = DEFAULT_INSPECTION_PAGE_SIZE,
    step_limit: int = DEFAULT_INSPECTION_PAGE_SIZE,
    schema: PlatformSchema | None = None,
    reader: LifecycleObservationReader | None = None,
) -> tuple[AttemptInspection, ...]:
    _validate_limit(limit)
    _validate_limit(step_limit)
    selected = schema or PlatformSchema()
    with engine.connect() as connection:
        _require_operation(connection, selected, operation_key)
        statement = (
            select(selected.item_attempts)
            .join(
                selected.items,
                selected.items.c.item_id == selected.item_attempts.c.item_id,
            )
            .where(selected.items.c.operation_key == operation_key)
        )
        if cursor is not None:
            item_id, attempt = cursor
            cursor_exists = connection.execute(
                select(selected.item_attempts.c.item_id)
                .join(
                    selected.items,
                    selected.items.c.item_id
                    == selected.item_attempts.c.item_id,
                )
                .where(
                    and_(
                        selected.items.c.operation_key == operation_key,
                        selected.item_attempts.c.item_id == item_id,
                        selected.item_attempts.c.attempt == attempt,
                    )
                )
            ).scalar_one_or_none()
            if cursor_exists is None:
                raise ValueError("attempt cursor is unknown")
            statement = statement.where(
                or_(
                    selected.item_attempts.c.item_id > item_id,
                    and_(
                        selected.item_attempts.c.item_id == item_id,
                        selected.item_attempts.c.attempt > attempt,
                    ),
                )
            )
        rows = connection.execute(
            statement.order_by(
                selected.item_attempts.c.item_id,
                selected.item_attempts.c.attempt,
            ).limit(limit)
        ).mappings()
        return tuple(
            _attempt_inspection(
                connection, selected, dict(row), reader, step_limit
            )
            for row in rows
        )


def health_report(  # noqa: PLR0913
    *,
    engine: Engine,
    now: datetime,
    no_progress_after_seconds: int | None = None,
    queued_age_threshold_seconds: int | None = None,
    active_age_threshold_seconds: int | None = None,
    queue_health: QueueHealthProbe | None = None,
    schema: PlatformSchema | None = None,
) -> HealthReport:
    """Return health facts and explicit threshold breaches."""
    _validate_thresholds(
        no_progress_after_seconds,
        queued_age_threshold_seconds,
        active_age_threshold_seconds,
    )
    selected = schema or PlatformSchema()
    with engine.connect() as connection:
        attempts = selected.item_attempts
        queued_at = func.min(
            func.coalesce(attempts.c.enqueued_at, attempts.c.created_at)
        ).filter(
            and_(
                attempts.c.enqueue_state.in_(
                    [
                        AttemptEnqueueState.PENDING.value,
                        AttemptEnqueueState.CLAIMING.value,
                        AttemptEnqueueState.ENQUEUED.value,
                        AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT.value,
                    ]
                ),
                attempts.c.execution_state
                == AttemptExecutionState.NOT_STARTED.value,
            )
        )
        active_at = func.min(attempts.c.updated_at).filter(
            attempts.c.execution_state.in_(
                [
                    AttemptExecutionState.ACTIVE.value,
                    AttemptExecutionState.CANCEL_REQUESTED.value,
                ]
            )
        )
        queued_start, active_start = connection.execute(
            select(queued_at, active_at)
        ).one()
        failure_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(attempts.c.failure.is_not(None)),
        )
        missing_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                attempts.c.execution_state
                == AttemptExecutionState.MISSING.value
            ),
        )
        exhausted_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                attempts.c.retry_disposition
                == RetryDisposition.EXHAUSTED.value
            ),
        )
        permanent_failure_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                attempts.c.retry_disposition
                == RetryDisposition.PERMANENT.value
            ),
        )
        retryable_failure_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                attempts.c.retry_disposition
                == RetryDisposition.RETRYABLE.value
            ),
        )
        recovery_exhausted_count = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                attempts.c.execution_state
                == AttemptExecutionState.RECOVERY_EXHAUSTED.value
            ),
        )
        queue_priority_drift = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                and_(
                    attempts.c.effective_service_priority.is_not(None),
                    attempts.c.effective_service_priority
                    != attempts.c.requested_service_priority,
                )
            ),
        )
        throttle = selected.throttle_state
        holds = _count(
            connection,
            select(func.count())
            .select_from(throttle)
            .where(throttle.c.hold_until > now),
        )
        backoff = _count(
            connection,
            select(func.count())
            .select_from(throttle)
            .where(throttle.c.blocked_until > now),
        )
        operations = selected.operations
        no_progress = (
            0
            if no_progress_after_seconds is None
            else _count(
                connection,
                select(func.count())
                .select_from(operations)
                .where(
                    and_(
                        operations.c.status.not_in(
                            [
                                status.value
                                for status in TERMINAL_OPERATION_STATUSES
                            ]
                        ),
                        operations.c.updated_at
                        < now - timedelta(seconds=no_progress_after_seconds),
                    )
                ),
            )
        )
        application_mismatch = _count(
            connection,
            select(func.count())
            .select_from(operations)
            .where(
                select(
                    func.count(
                        func.distinct(attempts.c.source_application_version)
                    )
                )
                .select_from(attempts)
                .join(
                    selected.items,
                    selected.items.c.item_id == attempts.c.item_id,
                )
                .where(
                    selected.items.c.operation_key
                    == operations.c.operation_key
                )
                .scalar_subquery()
                > 1
            ),
        )
        incomplete_cancellation = _count(
            connection,
            select(func.count())
            .select_from(attempts)
            .where(
                and_(
                    attempts.c.cancellation_request_id.is_not(None),
                    attempts.c.cancellation_disposition.is_(None),
                )
            ),
        )
        compensations = selected.enqueue_compensations
        incomplete_compensation = _count(
            connection,
            select(func.count())
            .select_from(compensations)
            .where(compensations.c.resolved_at.is_(None)),
        )
    queued_age = _age_seconds(now, queued_start)
    active_age = _age_seconds(now, active_start)
    breaches = _health_breaches(
        queued_age,
        active_age,
        no_progress,
        queued_age_threshold_seconds,
        active_age_threshold_seconds,
        no_progress_after_seconds,
    )
    return HealthReport(
        observed_at=now,
        oldest_queued_age_seconds=queued_age,
        oldest_active_age_seconds=active_age,
        no_progress_operation_count=no_progress,
        failure_count=failure_count,
        permanent_failure_count=permanent_failure_count,
        retryable_failure_count=retryable_failure_count,
        missing_count=missing_count,
        retry_exhausted_count=exhausted_count,
        recovery_exhausted_count=recovery_exhausted_count,
        active_throttle_hold_count=holds,
        active_backoff_count=backoff,
        queue_priority_drift_count=queue_priority_drift,
        queue_configuration_drift_count=(
            None
            if queue_health is None
            else queue_health.configuration_drift_count()
        ),
        application_version_mismatch_count=application_mismatch,
        incomplete_cancellation_count=incomplete_cancellation,
        incomplete_compensation_count=incomplete_compensation,
        threshold_breaches=breaches,
    )


def wait_operation(  # noqa: PLR0913
    operation_key: str,
    *,
    engine: Engine,
    resolver: TargetResolver,
    options: OperationWaitOptions,
    queue_lookup: QueueLookup | None = None,
    schema: PlatformSchema | None = None,
    reader: LifecycleObservationReader | None = None,
    recovery_observer: WorkflowObserver | None = None,
    enqueue_adapter: PhysicalEnqueueAdapter | None = None,
    compensation_canceller: WorkflowCanceller | None = None,
) -> OperationWaitResult:
    """Reconcile bounded pages until the durable aggregate becomes terminal."""
    started_at = options.clock()
    polls = 0
    inspection = inspect_operation(operation_key, engine=engine, schema=schema)
    if inspection.operation.status in TERMINAL_OPERATION_STATUSES:
        return OperationWaitResult(
            inspection=inspection,
            elapsed_seconds=_elapsed_seconds(started_at, options.clock()),
            poll_count=polls,
        )
    while True:
        reconcile(
            engine,
            resolver=resolver,
            queue_lookup=queue_lookup,
            schema=schema,
            reader=reader,
            recovery_observer=recovery_observer,
            enqueue_adapter=enqueue_adapter,
            compensation_canceller=compensation_canceller,
            options=ReconcileOptions(
                page_size=options.reconciliation_page_size,
                operation_key=operation_key,
            ),
        )
        polls += 1
        inspection = inspect_operation(
            operation_key, engine=engine, schema=schema
        )
        elapsed = _elapsed_seconds(started_at, options.clock())
        if inspection.operation.status in TERMINAL_OPERATION_STATUSES:
            return OperationWaitResult(
                inspection=inspection,
                elapsed_seconds=elapsed,
                poll_count=polls,
            )
        if elapsed >= options.timeout_seconds:
            raise OperationWaitTimeoutError(inspection)
        options.sleeper(
            min(
                options.poll_interval_seconds,
                options.timeout_seconds - elapsed,
            )
        )


def _operation_inspection(
    connection: Connection, schema: PlatformSchema, row: dict[str, object]
) -> OperationInspection:
    operation = OperationRecord.model_validate(row)
    current_count = _count(
        connection,
        select(func.count())
        .select_from(schema.items)
        .where(schema.items.c.operation_key == operation.operation_key),
    )
    attempt_count = _count(
        connection,
        select(func.count())
        .select_from(schema.item_attempts)
        .join(
            schema.items,
            schema.items.c.item_id == schema.item_attempts.c.item_id,
        )
        .where(
            schema.items.c.operation_key == operation.operation_key,
            schema.item_attempts.c.attempt == schema.items.c.current_attempt,
        ),
    )
    return OperationInspection(
        operation=operation,
        current_item_count=current_count,
        current_attempt_count=attempt_count,
    )


def _item_inspection(
    connection: Connection, schema: PlatformSchema, row: dict[str, object]
) -> ItemInspection:
    item = ItemRecord.model_validate(row)
    attempt = (
        connection.execute(
            select(schema.item_attempts).where(
                and_(
                    schema.item_attempts.c.item_id == item.item_id,
                    schema.item_attempts.c.attempt == item.current_attempt,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    return ItemInspection(
        item=item,
        current_attempt=None
        if attempt is None
        else AttemptRecord.model_validate(dict(attempt)),
    )


def _attempt_inspection(
    connection: Connection,
    schema: PlatformSchema,
    row: dict[str, object],
    reader: LifecycleObservationReader | None,
    step_limit: int,
) -> AttemptInspection:
    attempt = AttemptRecord.model_validate(row)
    claims = tuple(
        EnqueueClaimRecord.model_validate(dict(value))
        for value in connection.execute(
            select(schema.enqueue_claims)
            .where(
                and_(
                    schema.enqueue_claims.c.item_id == attempt.item_id,
                    schema.enqueue_claims.c.attempt == attempt.attempt,
                )
            )
            .order_by(
                schema.enqueue_claims.c.created_at,
                schema.enqueue_claims.c.claim_id,
            )
        ).mappings()
    )
    compensations = tuple(
        EnqueueCompensationRecord.model_validate(dict(value))
        for value in connection.execute(
            select(schema.enqueue_compensations)
            .where(
                and_(
                    schema.enqueue_compensations.c.item_id == attempt.item_id,
                    schema.enqueue_compensations.c.attempt == attempt.attempt,
                )
            )
            .order_by(
                schema.enqueue_compensations.c.created_at,
                schema.enqueue_compensations.c.claim_id,
            )
        ).mappings()
    )
    steps = (
        ()
        if reader is None
        else reader.read_step_history(
            workflow_id=attempt.workflow_id,
            limit=step_limit,
        )
    )
    return AttemptInspection(
        attempt=attempt,
        claims=claims,
        compensations=compensations,
        dbos_steps=steps,
    )


def _require_operation(
    connection: Connection, schema: PlatformSchema, operation_key: str
) -> None:
    if (
        connection.execute(
            select(schema.operations.c.operation_key).where(
                schema.operations.c.operation_key == operation_key
            )
        ).scalar_one_or_none()
        is None
    ):
        raise LookupError(f"operation {operation_key!r} does not exist")


def _validate_limit(limit: int) -> None:
    if type(limit) is not int or limit <= 0:
        raise ValueError("limit must be a positive int")


def _validate_thresholds(*thresholds: int | None) -> None:
    if any(
        value is not None and (type(value) is not int or value <= 0)
        for value in thresholds
    ):
        raise ValueError("health thresholds must be positive ints")


def _count(connection: Connection, statement: Any) -> int:
    return int(connection.execute(statement).scalar_one())


def _age_seconds(now: datetime, started_at: datetime | None) -> int | None:
    return (
        None
        if started_at is None
        else max(0, int((now - started_at).total_seconds()))
    )


def _elapsed_seconds(started_at: datetime, now: datetime) -> float:
    return max(0.0, (now - started_at).total_seconds())


def _health_breaches(  # noqa: PLR0913
    queued: int | None,
    active: int | None,
    no_progress: int,
    queued_threshold: int | None,
    active_threshold: int | None,
    no_progress_threshold: int | None,
) -> tuple[str, ...]:
    result: list[str] = []
    if (
        queued_threshold is not None
        and queued is not None
        and queued >= queued_threshold
    ):
        result.append("queued_age")
    if (
        active_threshold is not None
        and active is not None
        and active >= active_threshold
    ):
        result.append("active_age")
    if no_progress_threshold is not None and no_progress:
        result.append("no_progress")
    return tuple(result)
