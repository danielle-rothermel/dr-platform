"""Single-read Item submission and bounded transactional registration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Protocol,
    runtime_checkable,
)
from uuid import uuid4

from dr_serialize import (
    POSTGRES_JSONB_PAYLOAD_MAX_BYTES,
    SerializationError,
    Serializer,
    postgres_jsonb_limits,
    sha256_json_digest,
)
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
    and_,
    func,
    insert,
    select,
    text,
    update,
)

from dr_platform.cancellation_truth import workflow_reference_conflict
from dr_platform.db import PlatformSchema
from dr_platform.items import item_id, shuffle_rank
from dr_platform.manifests import (  # noqa: TC001 -- Pydantic resolves these
    ExecutionRecipeEnvelope,
    SubmissionSource,
)
from dr_platform.records import ItemRecord, RetryPolicy
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    ItemInsertStatus,
    OperationStatus,
    ServiceClass,
)

if TYPE_CHECKING:
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.reconciliation_runtime import LifecycleObservationReader
    from dr_platform.targets import ExecutionTarget, TargetResolver

DEFAULT_PAGE_SIZE = 500
DEFAULT_REGISTRATION_LEASE_SECONDS = 60
DEFAULT_CLAIM_LEASE_SECONDS = 60
DEFAULT_MISSING_GRACE_SECONDS = 60
DEFAULT_MISSING_REQUIRED_OBSERVATIONS = 3
DEFAULT_FAILURE_PREVIEW_LIMIT = 100
EMPTY_SUBMISSION_REASON = "empty_submission"
REGISTRATION_ABANDONED_REASON = "registration_abandoned"
SOURCE_APPLICATION_VERSION_DEFAULT = "unknown"

# One stable shared transaction lock is taken by every kernel writer. Export
# takes the exclusive form of this same key before opening its snapshot.
EXPORT_BARRIER_ADVISORY_KEY = 2_129_927_185_611_267_111

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]


class RegistrationError(RuntimeError):
    """Base class for typed registration failures."""


class RegistrationConflictError(RegistrationError):
    """An Operation key was replayed with unequal immutable inputs."""


class RegistrationLeaseHeldError(RegistrationError):
    """A different live registrar owns the Operation registration Lease."""

    def __init__(self, *, operation_key: str, expires_at: datetime) -> None:
        super().__init__(
            f"registration Lease is held for {operation_key!r} until "
            f"{expires_at.isoformat()}"
        )
        self.operation_key = operation_key
        self.expires_at = expires_at


class RegistrationAbandonedError(RegistrationError):
    """Registration was permanently abandoned by an operator."""


class RegistrationIntegrityError(RegistrationError):
    """Source, hook, or cursor accounting violated registration invariants."""


class RegistrationIneligibleError(RegistrationError):
    """An operator transition is not eligible in the current state."""


class RegistrationItem(BaseModel):
    """Validated page Item passed to a caller-owned registration hook."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    operation_key: NonEmptyStr
    item_key: NonEmptyStr
    item_index: NonNegativeInt
    service_class: ServiceClass
    spec: dict[StrictStr, Any]
    execution_recipe: ExecutionRecipeEnvelope
    execution_recipe_digest: NonEmptyStr
    execution_key: NonEmptyStr
    workflow_id: NonEmptyStr


class RegistrationItemResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr
    insert_status: ItemInsertStatus


class RegistrationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[RegistrationItemResult, ...]


class RegistrationPageContext(BaseModel):
    """Frozen page facts available to a registration hook.

    ``is_final_page`` is authoritative only for this transaction: the
    completion cursor CAS immediately follows the hook and rolls its work back
    if the lease or cursor is no longer current.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    page_index: NonNegativeInt
    page_count: PositiveInt
    next_registration_cursor: PositiveInt
    is_final_page: bool


@runtime_checkable
class RegistrationHook(Protocol):
    def __call__(
        self,
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
        page: RegistrationPageContext,
    ) -> RegistrationResult: ...


class SubmitOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_size: PositiveInt = DEFAULT_PAGE_SIZE
    registration_lease_seconds: PositiveInt = (
        DEFAULT_REGISTRATION_LEASE_SECONDS
    )
    claim_lease_seconds: PositiveInt = DEFAULT_CLAIM_LEASE_SECONDS
    missing_grace_seconds: PositiveInt = DEFAULT_MISSING_GRACE_SECONDS
    missing_required_observations: PositiveInt = (
        DEFAULT_MISSING_REQUIRED_OBSERVATIONS
    )
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    failure_preview_limit: PositiveInt = DEFAULT_FAILURE_PREVIEW_LIMIT


class SubmitFailurePreview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    phase: NonEmptyStr
    failure: dict[StrictStr, Any]


class SubmitResult(BaseModel):
    """Bounded Operation receipt; it never materializes every Item."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    status: OperationStatus
    requested_count: NonNegativeInt
    registration_cursor: NonNegativeInt
    inserted_count: NonNegativeInt
    already_present_count: NonNegativeInt
    enqueued_count: NonNegativeInt
    workflow_already_present_count: NonNegativeInt
    enqueue_failed_count: NonNegativeInt
    total_failure_count: NonNegativeInt
    failure_previews: tuple[SubmitFailurePreview, ...] = ()
    failures_truncated: bool = False


class AbandonRegistrationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    committed_count: NonNegativeInt
    remaining_count: NonNegativeInt
    abandoned_at: datetime
    abandoned_by: NonEmptyStr
    reason: NonEmptyStr


class _ValidatedOperationInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    spec: dict[StrictStr, Any]
    metadata: dict[StrictStr, Any]
    source_application_version: NonEmptyStr

    @model_validator(mode="after")
    def validate_payloads(self) -> _ValidatedOperationInputs:
        _validate_jsonb_payload(self.spec, label="operation spec")
        _validate_jsonb_payload(self.metadata, label="operation metadata")
        return self


class _ValidatedSourceItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr
    spec: dict[StrictStr, Any]
    service_class: ServiceClass

    @model_validator(mode="after")
    def validate_spec(self) -> _ValidatedSourceItem:
        _validate_jsonb_payload(self.spec, label="Item spec")
        return self


class _PreparedPageItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_item: _ValidatedSourceItem
    item_index: NonNegativeInt
    item_id: NonEmptyStr
    execution_recipe: ExecutionRecipeEnvelope
    execution_recipe_digest: NonEmptyStr


def submit(  # noqa: PLR0913 -- explicit public facade contract
    *,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    source: SubmissionSource,
    engine: Engine,
    resolver: TargetResolver,
    spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    options: SubmitOptions | None = None,
    source_application_version: str = SOURCE_APPLICATION_VERSION_DEFAULT,
    schema: PlatformSchema | None = None,
    queue_lookup: QueueLookup | None = None,
    enqueue_adapter: PhysicalEnqueueAdapter | None = None,
    workflow_observer: WorkflowObserver | None = None,
    reconciliation_reader: LifecycleObservationReader | None = None,
) -> SubmitResult:
    """Read, register, and enqueue one Operation through one pipeline."""
    selected_schema = schema or PlatformSchema()
    selected_options = options or SubmitOptions()
    operation_inputs = _ValidatedOperationInputs(
        spec={} if spec is None else spec,
        metadata={} if metadata is None else metadata,
        source_application_version=source_application_version,
    )
    operation_spec = operation_inputs.spec
    operation_metadata = operation_inputs.metadata
    resolved_target = resolver.resolve(target.ref)
    _validate_submission_target(
        requested_target=target,
        resolved_target=resolved_target,
        workflow_role=workflow_role,
    )
    prepared_pages = _prepare_source(
        operation_key=operation_key,
        target=resolved_target,
        source=source,
        page_size=selected_options.page_size,
    )

    lease_id = uuid4().hex
    cursor = _create_or_claim_operation(
        engine=engine,
        schema=selected_schema,
        operation_key=operation_key,
        workflow_role=workflow_role,
        group_key=group_key,
        target=resolved_target,
        requested_count=source.item_count,
        page_count=len(prepared_pages),
        spec=operation_spec,
        metadata=operation_metadata,
        options=selected_options,
        lease_id=lease_id,
        prepared_pages=prepared_pages,
    )
    if source.item_count == 0:
        return _load_submit_result(
            engine=engine,
            schema=selected_schema,
            operation_key=operation_key,
        )

    while cursor < len(prepared_pages):
        cursor = _register_page(
            engine=engine,
            schema=selected_schema,
            operation_key=operation_key,
            workflow_role=workflow_role,
            target=resolved_target,
            options=selected_options,
            lease_id=lease_id,
            page_index=cursor,
            page_count=len(prepared_pages),
            prepared=prepared_pages[cursor],
            source_application_version=(
                operation_inputs.source_application_version
            ),
        )

    _enqueue_registered_page(
        engine=engine,
        resolver=resolver,
        schema=selected_schema,
        options=selected_options,
        queue_lookup=queue_lookup,
        enqueue_adapter=enqueue_adapter,
        workflow_observer=workflow_observer,
        reconciliation_reader=reconciliation_reader,
    )

    return _load_submit_result(
        engine=engine,
        schema=selected_schema,
        operation_key=operation_key,
    )


def _enqueue_registered_page(  # noqa: PLR0913
    *,
    engine: Engine,
    resolver: TargetResolver,
    schema: PlatformSchema,
    options: SubmitOptions,
    queue_lookup: QueueLookup | None,
    enqueue_adapter: PhysicalEnqueueAdapter | None,
    workflow_observer: WorkflowObserver | None = None,
    reconciliation_reader: LifecycleObservationReader | None = None,
) -> None:
    from dr_platform.reconciliation_runtime import (  # noqa: PLC0415
        ReconcileOptions,
        reconcile,
    )

    reconcile_options = ReconcileOptions(
        page_size=options.page_size,
        claim_lease_seconds=options.claim_lease_seconds,
        missing_grace_seconds=options.missing_grace_seconds,
        missing_required_observations=(options.missing_required_observations),
    )
    reconcile(
        engine,
        resolver=resolver,
        queue_lookup=queue_lookup,
        options=reconcile_options,
        schema=schema,
        reader=reconciliation_reader,
        recovery_observer=workflow_observer,
        enqueue_adapter=enqueue_adapter,
    )


def abandon_registration(  # noqa: PLR0913 -- explicit public facade contract
    operation_key: str,
    *,
    engine: Engine,
    abandoned_by: str,
    reason: str,
    operator_confirmed: bool,
    schema: PlatformSchema | None = None,
) -> AbandonRegistrationResult:
    """Permanently abandon an incomplete Operation after its Lease expires."""
    if not operator_confirmed:
        raise RegistrationIneligibleError(
            "registration abandonment requires explicit operator confirmation"
        )
    if not abandoned_by or not reason:
        raise RegistrationIneligibleError(
            "registration abandonment requires an operator and reason"
        )
    selected_schema = schema or PlatformSchema()
    operations = selected_schema.operations
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        row = (
            connection.execute(
                select(operations)
                .where(operations.c.operation_key == operation_key)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RegistrationIneligibleError(
                f"unknown Operation {operation_key!r}"
            )
        now = _database_now(connection)
        if row["registration_abandoned_at"] is not None:
            if (
                row["registration_abandoned_by"] != abandoned_by
                or row["registration_abandonment_reason"] != reason
            ):
                raise RegistrationConflictError(
                    "registration abandonment replay does not match the "
                    "stored operator and reason"
                )
            return _abandonment_result(row)
        if row["registration_completed_at"] is not None:
            raise RegistrationIneligibleError(
                "completed registration cannot be abandoned"
            )
        expires_at = row["registration_lease_expires_at"]
        if expires_at is not None and expires_at > now:
            raise RegistrationLeaseHeldError(
                operation_key=operation_key,
                expires_at=expires_at,
            )
        connection.execute(
            update(operations)
            .where(
                and_(
                    operations.c.operation_key == operation_key,
                    operations.c.registration_completed_at.is_(None),
                    operations.c.registration_abandoned_at.is_(None),
                )
            )
            .values(
                registration_lease_id=None,
                registration_lease_expires_at=None,
                registration_abandoned_at=now,
                registration_abandoned_by=abandoned_by,
                registration_abandonment_reason=reason,
                status=OperationStatus.FAILED.value,
                terminal_reason=REGISTRATION_ABANDONED_REASON,
                completed_at=now,
                updated_at=now,
                platform_cut_version=operations.c.platform_cut_version + 1,
            )
        )
        updated = (
            connection.execute(
                select(operations).where(
                    operations.c.operation_key == operation_key
                )
            )
            .mappings()
            .one()
        )
        return _abandonment_result(updated)


def _create_or_claim_operation(  # noqa: PLR0913
    *,
    engine: Engine,
    schema: PlatformSchema,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    requested_count: int,
    page_count: int,
    spec: dict[str, Any],
    metadata: dict[str, Any],
    options: SubmitOptions,
    lease_id: str,
    prepared_pages: tuple[tuple[_PreparedPageItem, ...], ...],
) -> int:
    operations = schema.operations
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_operation_registration_lock(
            connection,
            operation_key,
        )
        row = (
            connection.execute(
                select(operations)
                .where(operations.c.operation_key == operation_key)
                .with_for_update()
            )
            .mappings()
            .one_or_none()
        )
        now = _database_now(connection)
        if row is None:
            if requested_count == 0:
                connection.execute(
                    insert(operations).values(
                        **_operation_insert_values(
                            operation_key=operation_key,
                            workflow_role=workflow_role,
                            group_key=group_key,
                            target=target,
                            requested_count=requested_count,
                            page_count=page_count,
                            spec=spec,
                            metadata=metadata,
                            options=options,
                            now=now,
                            lease_id=None,
                            lease_expires_at=None,
                            status=OperationStatus.FAILED,
                            registration_completed_at=now,
                            completed_at=now,
                            terminal_reason=EMPTY_SUBMISSION_REASON,
                        )
                    )
                )
                return 0
            lease_expires_at = now + timedelta(
                seconds=options.registration_lease_seconds
            )
            connection.execute(
                insert(operations).values(
                    **_operation_insert_values(
                        operation_key=operation_key,
                        workflow_role=workflow_role,
                        group_key=group_key,
                        target=target,
                        requested_count=requested_count,
                        page_count=page_count,
                        spec=spec,
                        metadata=metadata,
                        options=options,
                        now=now,
                        lease_id=lease_id,
                        lease_expires_at=lease_expires_at,
                        status=OperationStatus.REGISTERING,
                    )
                )
            )
            return 0

        _validate_exact_replay(
            row=row,
            operation_key=operation_key,
            workflow_role=workflow_role,
            group_key=group_key,
            target=target,
            requested_count=requested_count,
            page_count=page_count,
            spec=spec,
            metadata=metadata,
            options=options,
        )
        _validate_persisted_source_prefix(
            connection,
            schema=schema,
            operation_key=operation_key,
            registration_cursor=int(row["registration_cursor"]),
            prepared_pages=prepared_pages,
        )
        if row["registration_abandoned_at"] is not None:
            raise RegistrationAbandonedError(
                f"registration for {operation_key!r} was abandoned"
            )
        if row["registration_completed_at"] is not None:
            return int(row["registration_cursor"])
        expires_at = row["registration_lease_expires_at"]
        if expires_at is not None and expires_at > now:
            raise RegistrationLeaseHeldError(
                operation_key=operation_key,
                expires_at=expires_at,
            )
        new_expiry = now + timedelta(
            seconds=options.registration_lease_seconds
        )
        connection.execute(
            update(operations)
            .where(
                and_(
                    operations.c.operation_key == operation_key,
                    operations.c.registration_completed_at.is_(None),
                    operations.c.registration_abandoned_at.is_(None),
                    operations.c.registration_lease_expires_at <= now,
                )
            )
            .values(
                registration_lease_id=lease_id,
                registration_lease_expires_at=new_expiry,
                updated_at=now,
            )
        )
        return int(row["registration_cursor"])


def _register_page(  # noqa: PLR0913
    *,
    engine: Engine,
    schema: PlatformSchema,
    operation_key: str,
    workflow_role: str,
    target: ExecutionTarget,
    options: SubmitOptions,
    lease_id: str,
    page_index: int,
    page_count: int,
    prepared: tuple[_PreparedPageItem, ...],
    source_application_version: str,
) -> int:
    candidate_items = _registration_items(
        operation_key=operation_key,
        target=target,
        prepared=prepared,
    )
    workflow_ids = sorted({item.workflow_id for item in candidate_items})
    operations = schema.operations
    with engine.begin() as connection:
        _acquire_export_writer_lock(connection)
        _acquire_workflow_reference_locks(connection, workflow_ids)
        row = (
            connection.execute(
                select(operations)
                .where(operations.c.operation_key == operation_key)
                .with_for_update()
            )
            .mappings()
            .one()
        )
        now = _database_now(connection)
        _validate_page_authority(
            row=row,
            operation_key=operation_key,
            page_index=page_index,
            lease_id=lease_id,
            now=now,
        )
        _validate_workflow_reference_guards(
            connection,
            schema=schema,
            workflow_ids=workflow_ids,
        )
        next_cursor = page_index + 1
        is_final = next_cursor == page_count
        hook_result = _invoke_registration_hook(
            connection=connection,
            target=target,
            operation_key=operation_key,
            items=candidate_items,
            page=RegistrationPageContext(
                page_index=page_index,
                page_count=page_count,
                next_registration_cursor=next_cursor,
                is_final_page=is_final,
            ),
        )
        _validate_hook_result(items=candidate_items, result=hook_result)
        now = _database_now(connection)
        _validate_page_authority(
            row=row,
            operation_key=operation_key,
            page_index=page_index,
            lease_id=lease_id,
            now=now,
        )
        result_by_key = {
            result.item_key: result.insert_status
            for result in hook_result.items
        }
        for item in candidate_items:
            insert_status = result_by_key[item.item_key]
            connection.execute(
                insert(schema.items).values(
                    item_id=item.item_id,
                    operation_key=item.operation_key,
                    item_key=item.item_key,
                    item_index=item.item_index,
                    shuffle_rank=shuffle_rank(item_id=item.item_id),
                    service_class=item.service_class,
                    service_priority=item.service_class.priority,
                    spec=item.spec,
                    insert_status=insert_status.value,
                    current_attempt=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            connection.execute(
                insert(schema.item_attempts).values(
                    item_id=item.item_id,
                    attempt=0,
                    workflow_role=workflow_role,
                    execution_key=item.execution_key,
                    workflow_id=item.workflow_id,
                    execution_recipe_digest=item.execution_recipe_digest,
                    enqueue_state=AttemptEnqueueState.PENDING.value,
                    enqueue_try=0,
                    execution_state=AttemptExecutionState.NOT_STARTED.value,
                    source_application_version=source_application_version,
                    missing_observation_count=0,
                    requested_service_class=item.service_class,
                    requested_service_priority=item.service_class.priority,
                    created_at=now,
                    updated_at=now,
                )
            )

        inserted = sum(
            result.insert_status is ItemInsertStatus.INSERTED
            for result in hook_result.items
        )
        already_present = len(hook_result.items) - inserted
        cas_now = func.clock_timestamp()
        values: dict[str, Any] = {
            "registration_cursor": next_cursor,
            "inserted_count": operations.c.inserted_count + inserted,
            "already_present_count": (
                operations.c.already_present_count + already_present
            ),
            "registration_lease_expires_at": cas_now
            + timedelta(seconds=options.registration_lease_seconds),
            "updated_at": cas_now,
            "platform_cut_version": operations.c.platform_cut_version + 1,
        }
        if is_final:
            values.update(
                status=OperationStatus.ENQUEUEING.value,
                registration_completed_at=cas_now,
                registration_lease_id=None,
                registration_lease_expires_at=None,
            )
        outcome = connection.execute(
            update(operations)
            .where(
                and_(
                    operations.c.operation_key == operation_key,
                    operations.c.registration_cursor == page_index,
                    operations.c.registration_lease_id == lease_id,
                    operations.c.registration_lease_expires_at
                    > func.clock_timestamp(),
                )
            )
            .values(**values)
        )
        if outcome.rowcount != 1:
            raise RegistrationIntegrityError(
                "registration cursor CAS lost after page validation"
            )
        return next_cursor


def _validate_workflow_reference_guards(
    connection: Connection,
    *,
    schema: PlatformSchema,
    workflow_ids: list[str],
) -> None:
    """Reject links while cancellation or late-enqueue repair is unresolved."""
    conflict = workflow_reference_conflict(
        connection, schema=schema, workflow_ids=workflow_ids
    )
    if conflict is not None:
        raise RegistrationConflictError(conflict)


def _operation_insert_values(  # noqa: PLR0913
    *,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    requested_count: int,
    page_count: int,
    spec: dict[str, Any],
    metadata: dict[str, Any],
    options: SubmitOptions,
    now: datetime,
    lease_id: str | None,
    lease_expires_at: datetime | None,
    status: OperationStatus,
    registration_completed_at: datetime | None = None,
    completed_at: datetime | None = None,
    terminal_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "operation_key": operation_key,
        "group_key": group_key,
        "workflow_role": workflow_role,
        "status": status.value,
        "requested_count": requested_count,
        "registration_page_size": options.page_size,
        "registration_page_count": page_count,
        "target_key": target.ref.target_key,
        "target_version": target.ref.target_version,
        "target_contract_digest": target.ref.target_contract_digest,
        "platform_cut_version": 1,
        "registration_cursor": 0,
        "registration_lease_id": lease_id,
        "registration_lease_expires_at": lease_expires_at,
        "retry_policy": options.retry_policy.model_dump(mode="json"),
        "inserted_count": 0,
        "already_present_count": 0,
        "enqueued_count": 0,
        "workflow_already_present_count": 0,
        "enqueue_failed_count": 0,
        "active_count": 0,
        "succeeded_count": 0,
        "terminal_failed_count": 0,
        "cancelled_count": 0,
        "spec": spec,
        "metadata": metadata,
        "terminal_reason": terminal_reason,
        "created_at": now,
        "registration_completed_at": registration_completed_at,
        "updated_at": now,
        "completed_at": completed_at,
    }


def _validate_exact_replay(  # noqa: PLR0913
    *,
    row: Any,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    requested_count: int,
    page_count: int,
    spec: dict[str, Any],
    metadata: dict[str, Any],
    options: SubmitOptions,
) -> None:
    expected = {
        "operation_key": operation_key,
        "group_key": group_key,
        "workflow_role": workflow_role,
        "requested_count": requested_count,
        "registration_page_size": options.page_size,
        "registration_page_count": page_count,
        "target_key": target.ref.target_key,
        "target_version": target.ref.target_version,
        "target_contract_digest": target.ref.target_contract_digest,
        "retry_policy": options.retry_policy.model_dump(mode="json"),
        "spec": spec,
        "metadata": metadata,
    }
    json_fields = {"retry_policy", "spec", "metadata"}
    unequal = [
        key
        for key, value in expected.items()
        if (
            not _canonical_json_equal(row[key], value)
            if key in json_fields
            else row[key] != value
        )
    ]
    if unequal:
        raise RegistrationConflictError(
            "Operation exact replay conflict in immutable fields: "
            + ", ".join(sorted(unequal))
        )


def _validate_persisted_source_prefix(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
    registration_cursor: int,
    prepared_pages: tuple[tuple[_PreparedPageItem, ...], ...],
) -> None:
    expected = tuple(
        item
        for page in prepared_pages[:registration_cursor]
        for item in page
    )
    persisted = (
        connection.execute(
            select(
                schema.items.c.item_id,
                schema.items.c.item_index,
                schema.items.c.item_key,
                schema.items.c.service_class,
                schema.items.c.spec,
            )
            .where(schema.items.c.operation_key == operation_key)
            .order_by(schema.items.c.item_index)
        )
        .mappings()
        .all()
    )
    if len(persisted) != len(expected):
        raise RegistrationIntegrityError(
            "persisted Item count does not match registration cursor"
        )
    mismatched = any(
        row["item_id"] != item.item_id
        or row["item_index"] != item.item_index
        or row["item_key"] != item.source_item.item_key
        or row["service_class"] != item.source_item.service_class
        or not _canonical_json_equal(row["spec"], item.source_item.spec)
        for row, item in zip(persisted, expected, strict=True)
    )
    if mismatched:
        raise RegistrationConflictError(
            "Operation exact replay conflict in persisted source Items"
        )


def _validate_submission_target(
    *,
    requested_target: ExecutionTarget,
    resolved_target: ExecutionTarget,
    workflow_role: str,
) -> None:
    if resolved_target.ref != requested_target.ref:
        raise RegistrationConflictError(
            "resolved execution target does not match the requested target"
        )
    if resolved_target.workflow_role != workflow_role:
        raise RegistrationConflictError(
            "submission workflow_role does not match its execution target"
        )


def _prepare_source(
    *,
    operation_key: str,
    target: ExecutionTarget,
    source: SubmissionSource,
    page_size: int,
) -> tuple[tuple[_PreparedPageItem, ...], ...]:
    if source.item_count < 0:
        raise RegistrationIntegrityError(
            "source item_count cannot be negative"
        )
    pages: list[tuple[_PreparedPageItem, ...]] = []
    seen_item_keys: set[str] = set()
    for start_index in range(0, source.item_count, page_size):
        prepared = _prepare_source_page(
            operation_key=operation_key,
            target=target,
            source=source,
            start_index=start_index,
            end_index=min(start_index + page_size, source.item_count),
        )
        _reject_duplicate_item_keys(prepared, seen_item_keys=seen_item_keys)
        pages.append(prepared)
    return tuple(pages)


def _prepare_source_page(
    *,
    operation_key: str,
    target: ExecutionTarget,
    source: SubmissionSource,
    start_index: int,
    end_index: int,
) -> tuple[_PreparedPageItem, ...]:
    source_items = source.read_items(
        start_index=start_index,
        end_index=end_index,
    )
    expected_count = end_index - start_index
    if len(source_items) != expected_count:
        raise RegistrationIntegrityError(
            "SubmissionSource returned the wrong number of Items for a page"
        )
    prepared: list[_PreparedPageItem] = []
    for offset, raw_source_item in enumerate(source_items):
        source_item = _ValidatedSourceItem(
            item_key=raw_source_item.item_key,
            spec=raw_source_item.spec,
            service_class=raw_source_item.service_class,
        )
        item_index = start_index + offset
        recipe = target.recipe_for(source_item)
        _validate_recipe_target(recipe=recipe, target=target)
        recipe_digest = recipe.digest()
        prepared.append(
            _PreparedPageItem(
                source_item=source_item,
                item_index=item_index,
                item_id=item_id(
                    operation_key=operation_key,
                    item_key=source_item.item_key,
                ),
                execution_recipe=recipe,
                execution_recipe_digest=recipe_digest,
            )
        )
    return tuple(prepared)


def _registration_items(
    *,
    operation_key: str,
    target: ExecutionTarget,
    prepared: tuple[_PreparedPageItem, ...],
) -> tuple[RegistrationItem, ...]:
    items: list[RegistrationItem] = []
    for prepared_item in prepared:
        source_item = prepared_item.source_item
        candidate = ItemRecord(
            item_id=prepared_item.item_id,
            operation_key=operation_key,
            item_key=source_item.item_key,
            item_index=prepared_item.item_index,
            shuffle_rank=shuffle_rank(item_id=prepared_item.item_id),
            service_class=source_item.service_class,
            service_priority=source_item.service_class.priority,
            spec=source_item.spec,
            insert_status=ItemInsertStatus.INSERTED,
            current_attempt=0,
            created_at=datetime.min.replace(tzinfo=UTC),
            updated_at=datetime.min.replace(tzinfo=UTC),
            change_seq=1,
        )
        execution = target.execution_for(candidate, 0)
        items.append(
            RegistrationItem(
                item_id=prepared_item.item_id,
                operation_key=operation_key,
                item_key=source_item.item_key,
                item_index=prepared_item.item_index,
                service_class=source_item.service_class,
                spec=source_item.spec,
                execution_recipe=prepared_item.execution_recipe,
                execution_recipe_digest=(
                    prepared_item.execution_recipe_digest
                ),
                execution_key=execution.execution_key,
                workflow_id=execution.workflow_id,
            )
        )
    return tuple(items)


def _invoke_registration_hook(
    *,
    connection: Connection,
    target: ExecutionTarget,
    operation_key: str,
    items: tuple[RegistrationItem, ...],
    page: RegistrationPageContext,
) -> RegistrationResult:
    if target.registration_hook is None:
        return RegistrationResult(
            items=tuple(
                RegistrationItemResult(
                    item_key=item.item_key,
                    insert_status=ItemInsertStatus.INSERTED,
                )
                for item in items
            )
        )
    hook_items = tuple(
        RegistrationItem.model_validate(item.model_dump(mode="python"))
        for item in items
    )
    result = RegistrationResult.model_validate(
        target.registration_hook(
            connection,
            operation_key=operation_key,
            items=hook_items,
            page=page,
        )
    )
    if any(
        not _canonical_json_equal(
            original.model_dump(mode="json"),
            hook_item.model_dump(mode="json"),
        )
        for original, hook_item in zip(items, hook_items, strict=True)
    ):
        raise RegistrationIntegrityError(
            "RegistrationHook mutated its frozen page inputs"
        )
    return result


def _validate_hook_result(
    *,
    items: tuple[RegistrationItem, ...],
    result: RegistrationResult,
) -> None:
    expected = tuple(item.item_key for item in items)
    actual = tuple(item.item_key for item in result.items)
    if actual != expected:
        raise RegistrationIntegrityError(
            "RegistrationHook result keys must exactly match page order"
        )


def _validate_page_authority(
    *,
    row: Any,
    operation_key: str,
    page_index: int,
    lease_id: str,
    now: datetime,
) -> None:
    if row["registration_abandoned_at"] is not None:
        raise RegistrationAbandonedError(
            f"registration for {operation_key!r} was abandoned"
        )
    if row["registration_cursor"] != page_index:
        raise RegistrationIntegrityError("registration cursor CAS is stale")
    if row["registration_lease_id"] != lease_id:
        raise RegistrationIntegrityError("registration Lease ownership lost")
    expires_at = row["registration_lease_expires_at"]
    if expires_at is None or expires_at <= now:
        raise RegistrationIntegrityError("registration Lease expired")


def _reject_duplicate_item_keys(
    prepared: tuple[_PreparedPageItem, ...],
    *,
    seen_item_keys: set[str],
) -> None:
    for item in prepared:
        item_key = item.source_item.item_key
        if item_key in seen_item_keys:
            raise RegistrationIntegrityError(
                f"duplicate submission item_key {item_key!r}"
            )
        seen_item_keys.add(item_key)


def _validate_recipe_target(
    *,
    recipe: ExecutionRecipeEnvelope,
    target: ExecutionTarget,
) -> None:
    expected = {
        "target_ref": target.ref,
        "managed_workflow_name": target.managed_workflow_name,
        "managed_workflow_version": target.managed_workflow_version,
        "topology": target.topology,
        "argument_recipe_version": target.argument_recipe_version,
    }
    unequal = [
        field
        for field, value in expected.items()
        if getattr(recipe, field) != value
    ]
    if unequal:
        raise RegistrationConflictError(
            "execution recipe does not match its resolved target: "
            + ", ".join(sorted(unequal))
        )


def _canonical_json_equal(left: Any, right: Any) -> bool:
    return sha256_json_digest(left) == sha256_json_digest(right)


def _validate_jsonb_payload(value: Any, *, label: str) -> None:
    try:
        Serializer(
            limits=postgres_jsonb_limits(POSTGRES_JSONB_PAYLOAD_MAX_BYTES)
        ).to_jsonable(value)
    except SerializationError as exc:
        raise ValueError(f"{label}: {exc}") from exc


def _database_now(connection: Connection) -> datetime:
    return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def _acquire_export_writer_lock(connection: Connection) -> None:
    connection.execute(
        text("SELECT pg_advisory_xact_lock_shared(:lock_key)"),
        {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
    )


def _acquire_operation_registration_lock(
    connection: Connection,
    operation_key: str,
) -> None:
    connection.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 1))"),
        {"id": operation_key},
    )


def _acquire_workflow_reference_locks(
    connection: Connection, workflow_ids: list[str]
) -> None:
    for workflow_id in workflow_ids:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 0))"),
            {"id": workflow_id},
        )


def _load_submit_result(
    *, engine: Engine, schema: PlatformSchema, operation_key: str
) -> SubmitResult:
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(schema.operations).where(
                    schema.operations.c.operation_key == operation_key
                )
            )
            .mappings()
            .one()
        )
    return SubmitResult(
        operation_key=row["operation_key"],
        status=OperationStatus(row["status"]),
        requested_count=row["requested_count"],
        registration_cursor=row["registration_cursor"],
        inserted_count=row["inserted_count"],
        already_present_count=row["already_present_count"],
        enqueued_count=row["enqueued_count"],
        workflow_already_present_count=(row["workflow_already_present_count"]),
        enqueue_failed_count=row["enqueue_failed_count"],
        total_failure_count=(
            row["enqueue_failed_count"] + row["terminal_failed_count"]
        ),
    )


def _abandonment_result(row: Any) -> AbandonRegistrationResult:
    committed_count = row["inserted_count"] + row["already_present_count"]
    return AbandonRegistrationResult(
        operation_key=row["operation_key"],
        committed_count=committed_count,
        remaining_count=row["requested_count"] - committed_count,
        abandoned_at=row["registration_abandoned_at"],
        abandoned_by=row["registration_abandoned_by"],
        reason=row["registration_abandonment_reason"],
    )
