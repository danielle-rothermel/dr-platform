"""Distinct durable lifecycle guarantees backed by PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, func, insert, select, update
from sqlalchemy.exc import DBAPIError

from dr_platform import (
    CancellationConflictError,
    CancellationInspection,
    CancellationInspectionDisposition,
    CancellationRequest,
    EligibilityReference,
    FailureClass,
    FailureSnapshot,
    NextAttemptDisposition,
    NextAttemptReason,
    NextAttemptRequest,
    PlatformSchema,
    ServiceClass,
    TargetRegistry,
    cancel_operation,
    inspect_operation,
    list_attempts,
    list_items,
    reconcile,
    request_next_attempt,
    submit,
    upgrade_platform_schema,
)
from dr_platform.claims import ClaimPageOptions, claim_pending_attempts
from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.enqueue_runtime import (
    PhysicalEnqueueDisposition,
    PhysicalEnqueueOutcome,
    PreparedEnqueueCall,
    QueueConfigurationError,
)
from dr_platform.items import item_id, shuffle_rank
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.reconciliation import ReconciliationConflictError
from dr_platform.reconciliation_runtime import (
    DbosStepObservation,
    ReconcileOptions,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
)
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    AttemptRetryReason,
    CancellationDisposition,
    RetryDisposition,
)
from dr_platform.submission import (
    RegistrationConflictError,
    SubmitOptions,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
)
from tests.conftest import engine_dsn


class _Item(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: str
    service_class: ServiceClass = ServiceClass.STANDARD
    spec: dict[str, Any] = {}


class _Source(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[_Item, ...]

    @property
    def item_count(self) -> int:
        return len(self.items)

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[_Item, ...]:
        return self.items[start_index:end_index]


class _QueueConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    database_backed_queue: bool = True
    priority_enabled: bool = True


class _QueueLookup:
    def retrieve_queue(self, name: str) -> _QueueConfiguration:
        assert name == "lifecycle-queue"
        return _QueueConfiguration()


class _MissingQueueLookup:
    def retrieve_queue(self, name: str) -> None:
        assert name == "lifecycle-queue"


class _EnqueueAdapter:
    def __init__(self) -> None:
        self.calls: list[PreparedEnqueueCall] = []

    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome:
        disposition = (
            PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
            if any(
                existing.workflow_id == call.workflow_id
                for existing in self.calls
            )
            else PhysicalEnqueueDisposition.ENQUEUED
        )
        self.calls.append(call)
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=disposition,
            effective_service_priority=call.service_priority,
        )


class _UncertainLifecycleReader:
    def observe(self, *, workflow_id: str) -> ReconciliationObservation:
        return ReconciliationObservation(
            workflow_id=workflow_id,
            disposition=ReconciliationObservationDisposition.UNCERTAIN,
            failure=FailureSnapshot(
                failure_class=FailureClass.UNKNOWN,
                error_type="Unavailable",
                message="lifecycle reader unavailable in test",
            ),
        )

    def read_step_history(
        self,
        *,
        workflow_id: str,
        limit: int = 100,
    ) -> tuple[DbosStepObservation, ...]:
        del workflow_id, limit
        return ()


class _RetryableErrorLifecycleReader:
    def __init__(self, *, workflow_id: str) -> None:
        self.workflow_id = workflow_id

    def observe(self, *, workflow_id: str) -> ReconciliationObservation:
        assert workflow_id == self.workflow_id
        return ReconciliationObservation(
            workflow_id=workflow_id,
            disposition=ReconciliationObservationDisposition.ERROR,
            dbos_status=DbosWorkflowStatus.ERROR,
            failure=FailureSnapshot(
                failure_class=FailureClass.TRANSIENT,
                error_type="RetryableWorkflowError",
                message="retryable workflow failure in test",
            ),
        )

    def read_step_history(
        self,
        *,
        workflow_id: str,
        limit: int = 100,
    ) -> tuple[DbosStepObservation, ...]:
        del workflow_id, limit
        return ()


class _Canceller:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    def inspect(self, *, workflow_id: str) -> CancellationInspection:
        return CancellationInspection(
            workflow_id=workflow_id,
            disposition=CancellationInspectionDisposition.ACTIVE,
        )

    def cancel_workflow(
        self,
        *,
        workflow_id: str,
        cancel_children: bool,
    ) -> None:
        assert not cancel_children
        self.cancelled.append(workflow_id)


def _target() -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name="lifecycle-queue",
        workflow_role="lifecycle",
        managed_workflow_name="lifecycle-workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
    )
    ref = declaration.target_ref(
        target_key="lifecycle",
        target_version=1,
    )
    return ExecutionTarget(
        ref=ref,
        **declaration.model_dump(),
        workflow=lambda: None,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"execution:{item.item_id}:{attempt}",
            workflow_id=f"workflow:{item.item_id}:{attempt}",
        ),
        args_for=lambda item, attempt: (item.item_id, attempt),
        recipe_for=lambda item: ExecutionRecipeEnvelope(
            target_ref=ref,
            managed_workflow_name=declaration.managed_workflow_name,
            managed_workflow_version=declaration.managed_workflow_version,
            argument_recipe_version=declaration.argument_recipe_version,
            payload={"item_key": item.item_key},
        ),
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
    )


def _registry(target: ExecutionTarget) -> TargetRegistry:
    registry = TargetRegistry()
    registry.register(target)
    return registry


def _shared_target() -> ExecutionTarget:
    return _target().model_copy(
        update={
            "execution_for": lambda item, attempt: ExecutionIdentity(
                execution_key=f"execution:{item.item_key}:{attempt}",
                workflow_id=f"workflow:{item.item_key}:{attempt}",
            ),
            "args_for": lambda item, attempt: (item.item_key, attempt),
        }
    )


def _source(count: int) -> _Source:
    return _Source(
        items=tuple(_Item(item_key=f"item-{index}") for index in range(count))
    )


def _migrate(engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return PlatformSchema()


def _register_pending(
    engine: Engine,
    *,
    count: int,
    operation_key: str,
) -> tuple[PlatformSchema, ExecutionTarget, TargetRegistry]:
    schema = _migrate(engine)
    target = _target()
    registry = _registry(target)
    with pytest.raises(QueueConfigurationError):
        submit(
            operation_key=operation_key,
            workflow_role=target.workflow_role,
            group_key="lifecycle-group",
            target=target,
            source=_source(count),
            engine=engine,
            resolver=registry,
            schema=schema,
            options=SubmitOptions(page_size=max(1, count)),
            queue_lookup=_MissingQueueLookup(),
        )
    return schema, target, registry


def _submit_enqueued(
    engine: Engine,
    *,
    operation_key: str,
) -> tuple[
    PlatformSchema,
    ExecutionTarget,
    TargetRegistry,
    _Source,
    _EnqueueAdapter,
]:
    schema = _migrate(engine)
    target = _target()
    registry = _registry(target)
    source = _source(1)
    adapter = _EnqueueAdapter()
    submit(
        operation_key=operation_key,
        workflow_role=target.workflow_role,
        group_key="lifecycle-group",
        target=target,
        source=source,
        engine=engine,
        resolver=registry,
        schema=schema,
        queue_lookup=_QueueLookup(),
        enqueue_adapter=adapter,
    )
    return schema, target, registry, source, adapter


def test_persisted_lifecycle_rows_enforce_immutability(
    pg_engine: Engine,
) -> None:
    schema, _target_value, _registry_value = _register_pending(
        pg_engine,
        count=1,
        operation_key="immutable-operation",
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                enqueued_at=func.clock_timestamp(),
                effective_service_priority=ServiceClass.STANDARD.priority,
                priority_source="enqueued_here",
                execution_state=AttemptExecutionState.SUCCEEDED.value,
                terminal_at=func.clock_timestamp(),
                updated_at=func.clock_timestamp(),
            )
        )
    with pytest.raises(DBAPIError), pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(execution_state="error")
        )
    with pytest.raises(DBAPIError), pg_engine.begin() as connection:
        connection.execute(update(schema.items).values(item_key="other"))

    with pg_engine.connect() as connection:
        attempt_state = connection.scalar(
            select(schema.item_attempts.c.execution_state)
        )
        item_key = connection.scalar(select(schema.items.c.item_key))
    assert attempt_state == AttemptExecutionState.SUCCEEDED.value
    assert item_key == "item-0"


def test_resumed_registration_rejects_a_changed_source_prefix(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    operation_key = "changed-source-replay"
    source = _source(4)
    options = SubmitOptions(page_size=2)
    target = _target()
    registry = _registry(target)
    with pg_engine.begin() as connection:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        connection.execute(
            insert(schema.operations).values(
                operation_key=operation_key,
                group_key="lifecycle-group",
                workflow_role=target.workflow_role,
                status="registering",
                requested_count=source.item_count,
                registration_page_size=options.page_size,
                registration_page_count=2,
                target_key=target.ref.target_key,
                target_version=target.ref.target_version,
                target_contract_digest=target.ref.target_contract_digest,
                platform_cut_version=1,
                registration_cursor=1,
                registration_lease_id="expired-registration-lease",
                registration_lease_expires_at=(now - timedelta(seconds=1)),
                retry_policy=options.retry_policy.model_dump(mode="json"),
                inserted_count=2,
                enqueued_count=0,
                workflow_already_present_count=0,
                enqueue_failed_count=0,
                active_count=0,
                succeeded_count=0,
                terminal_failed_count=0,
                cancelled_count=0,
                spec={},
                metadata={},
                created_at=now,
                updated_at=now,
            )
        )
        connection.execute(
            insert(schema.items),
            [
                {
                    "item_id": item_id(
                        operation_key=operation_key,
                        item_key=item.item_key,
                    ),
                    "operation_key": operation_key,
                    "item_key": item.item_key,
                    "item_index": index,
                    "shuffle_rank": shuffle_rank(
                        item_id=item_id(
                            operation_key=operation_key,
                            item_key=item.item_key,
                        )
                    ),
                    "service_class": item.service_class.value,
                    "service_priority": item.service_class.priority,
                    "spec": item.spec,
                    "current_attempt": 0,
                    "created_at": now,
                    "updated_at": now,
                }
                for index, item in enumerate(source.items[:2])
            ],
        )
        connection.execute(
            insert(schema.item_attempts),
            [
                {
                    "item_id": item_id(
                        operation_key=operation_key,
                        item_key=item.item_key,
                    ),
                    "attempt": 0,
                    "workflow_role": target.workflow_role,
                    "execution_key": f"execution:{item.item_key}:0",
                    "workflow_id": f"workflow:{item.item_key}:0",
                    "execution_recipe_digest": "seed-recipe-digest",
                    "enqueue_state": AttemptEnqueueState.PENDING.value,
                    "enqueue_try": 0,
                    "execution_state": (
                        AttemptExecutionState.NOT_STARTED.value
                    ),
                    "source_application_version": "test-v1",
                    "missing_observation_count": 0,
                    "requested_service_class": item.service_class.value,
                    "requested_service_priority": (
                        item.service_class.priority
                    ),
                    "created_at": now,
                    "updated_at": now,
                }
                for item in source.items[:2]
            ],
        )

    changed_source = _Source(
        items=tuple(
            _Item(
                item_key=f"item-{index}",
                spec={"changed": True} if index == 0 else {},
            )
            for index in range(4)
        )
    )
    with pytest.raises(
        RegistrationConflictError,
        match="persisted source Items",
    ):
        submit(
            operation_key=operation_key,
            workflow_role=target.workflow_role,
            group_key="lifecycle-group",
            target=target,
            source=changed_source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
            options=options,
        )

    with pg_engine.connect() as connection:
        persisted_items = tuple(
            connection.execute(
                select(schema.items.c.item_key, schema.items.c.spec).order_by(
                    schema.items.c.item_index
                )
            )
        )
        cursor = connection.scalar(
            select(schema.operations.c.registration_cursor)
        )
    assert persisted_items == (("item-0", {}), ("item-1", {}))
    assert cursor == 1


def test_concurrent_claimers_claim_each_attempt_once_by_durable_outcome(
    pg_engine: Engine,
) -> None:
    schema, _target_value, _registry_value = _register_pending(
        pg_engine,
        count=4,
        operation_key="concurrent-claims",
    )
    barrier = Barrier(2)

    def claim(prefix: str) -> tuple[tuple[str, int, str], ...]:
        ids: Iterator[str] = iter(f"{prefix}-{index}" for index in range(4))
        barrier.wait(timeout=10)
        page = claim_pending_attempts(
            pg_engine,
            admit_targets=lambda target_refs: None,
            options=ClaimPageOptions(page_size=2),
            schema=schema,
            claim_id_factory=lambda: next(ids),
        )
        return tuple(
            (claim.item_id, claim.attempt, claim.claim_id)
            for claim in page.claims
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(claim, "first"),
            executor.submit(claim, "second"),
        )
        claimed = tuple(
            entry for future in futures for entry in future.result(timeout=15)
        )

    with pg_engine.connect() as connection:
        durable_claims = tuple(
            connection.execute(
                select(
                    schema.enqueue_claims.c.item_id,
                    schema.enqueue_claims.c.attempt,
                    schema.enqueue_claims.c.claim_id,
                )
            )
        )
        current_claims = tuple(
            connection.execute(
                select(
                    schema.item_attempts.c.item_id,
                    schema.item_attempts.c.attempt,
                    schema.item_attempts.c.current_claim_id,
                )
            )
        )
    assert len(claimed) == len(set(claimed)) == 4
    assert set(durable_claims) == set(claimed)
    assert set(current_claims) == set(claimed)


def test_submit_exact_replay_is_idempotent_and_conflicts_on_redefinition(
    pg_engine: Engine,
) -> None:
    schema, target, registry, source, adapter = _submit_enqueued(
        pg_engine,
        operation_key="submit-replay",
    )
    kwargs = {
        "operation_key": "submit-replay",
        "workflow_role": target.workflow_role,
        "group_key": "lifecycle-group",
        "target": target,
        "source": source,
        "engine": pg_engine,
        "resolver": registry,
        "schema": schema,
        "queue_lookup": _QueueLookup(),
        "enqueue_adapter": adapter,
        "reconciliation_reader": _UncertainLifecycleReader(),
    }

    replay = submit(**kwargs)
    with pytest.raises(RegistrationConflictError):
        submit(**kwargs, metadata={"different": True})

    with pg_engine.connect() as connection:
        counts = (
            connection.scalar(select(func.count()).select_from(schema.items)),
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            ),
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            ),
        )
    assert replay.enqueued_count == 1
    assert counts == (1, 1, 1)
    assert len(adapter.calls) == 1


def test_request_next_attempt_replay_is_idempotent_and_conflict_checked(
    pg_engine: Engine,
) -> None:
    schema, _target_value, registry, _source_value, _adapter = (
        _submit_enqueued(pg_engine, operation_key="next-attempt-replay")
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state=AttemptExecutionState.SUCCEEDED.value,
                terminal_at=func.clock_timestamp(),
                updated_at=func.clock_timestamp(),
            )
        )
    with pg_engine.connect() as connection:
        item_id = connection.scalar(select(schema.items.c.item_id))
    assert item_id is not None
    request = NextAttemptRequest(
        item_id=item_id,
        source_attempt=0,
        request_key="stable-request",
        reason=NextAttemptReason.DOMAIN_OUTCOME,
        eligibility=EligibilityReference(
            kind="generation_run",
            record_id="run-1",
            digest="run-digest",
        ),
        requested_by="caller",
    )

    first = request_next_attempt(
        request,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )
    replay = request_next_attempt(
        request,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )
    with pytest.raises(ReconciliationConflictError):
        request_next_attempt(
            request.model_copy(update={"requested_by": "other"}),
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )

    with pg_engine.connect() as connection:
        attempt_count = connection.scalar(
            select(func.count()).select_from(schema.item_attempts)
        )
        request_count = connection.scalar(
            select(func.count()).select_from(schema.next_attempt_requests)
        )
    assert first == replay
    assert first.disposition is NextAttemptDisposition.CREATED
    assert (attempt_count, request_count) == (2, 1)


def test_reconcile_persists_retryable_error_and_enqueues_retry(
    pg_engine: Engine,
) -> None:
    schema, _target_value, registry, _source_value, adapter = _submit_enqueued(
        pg_engine, operation_key="reconcile-retry"
    )
    observed = list_attempts(
        "reconcile-retry", engine=pg_engine, schema=schema
    )[0].attempt

    result = reconcile(
        pg_engine,
        resolver=registry,
        queue_lookup=_QueueLookup(),
        options=ReconcileOptions(operation_key="reconcile-retry"),
        schema=schema,
        reader=_RetryableErrorLifecycleReader(
            workflow_id=observed.workflow_id
        ),
        enqueue_adapter=adapter,
    )

    attempts = {
        inspection.attempt.attempt: inspection.attempt
        for inspection in list_attempts(
            "reconcile-retry", engine=pg_engine, schema=schema
        )
    }
    item = list_items("reconcile-retry", engine=pg_engine, schema=schema)[0]
    source = attempts[0]
    retry = attempts[1]

    assert result.execution_retry_count == 1
    assert result.pending_enqueue_count == 1
    assert source.execution_state is AttemptExecutionState.ERROR
    assert source.retry_disposition is RetryDisposition.RETRYABLE
    assert source.failure is not None
    assert source.failure.failure_class is FailureClass.TRANSIENT
    assert retry.source_attempt == source.attempt
    assert retry.source_workflow_id == source.workflow_id
    assert retry.retry_reason is AttemptRetryReason.AUTOMATIC_EXECUTION_ERROR
    assert retry.enqueue_state is AttemptEnqueueState.ENQUEUED
    assert item.current_attempt == retry


def test_cancellation_replay_is_idempotent_and_conflict_checked(
    pg_engine: Engine,
) -> None:
    schema, _target_value, _registry_value, _source_value, _adapter = (
        _submit_enqueued(pg_engine, operation_key="cancel-replay")
    )
    request = CancellationRequest(
        operation_key="cancel-replay",
        request_id="cancel-request",
        requested_by="operator",
    )
    canceller = _Canceller()

    first = cancel_operation(
        request,
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )
    replay = cancel_operation(
        request,
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )
    with pytest.raises(CancellationConflictError):
        cancel_operation(
            request.model_copy(update={"requested_by": "other"}),
            engine=pg_engine,
            schema=schema,
            canceller=canceller,
        )

    with pg_engine.connect() as connection:
        state = connection.scalar(
            select(schema.item_attempts.c.execution_state)
        )
    assert first == replay
    assert first.complete
    assert len(canceller.cancelled) == 1
    assert state == AttemptExecutionState.CANCELLED.value


def test_cancellation_skips_a_workflow_referenced_by_another_operation(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    target = _shared_target()
    registry = _registry(target)
    source = _source(1)
    adapter = _EnqueueAdapter()
    for operation_key in ("shared-owner", "shared-peer"):
        submit(
            operation_key=operation_key,
            workflow_role=target.workflow_role,
            group_key="lifecycle-group",
            target=target,
            source=source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
            queue_lookup=_QueueLookup(),
            enqueue_adapter=adapter,
            reconciliation_reader=_UncertainLifecycleReader(),
        )
    canceller = _Canceller()

    result = cancel_operation(
        CancellationRequest(
            operation_key="shared-owner",
            request_id="shared-cancel",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    owner = list_attempts("shared-owner", engine=pg_engine, schema=schema)[
        0
    ].attempt
    peer = list_attempts("shared-peer", engine=pg_engine, schema=schema)[
        0
    ].attempt
    assert result.complete
    assert (
        result.results[0].disposition is CancellationDisposition.SKIPPED_SHARED
    )
    assert not canceller.cancelled
    assert owner.execution_state is AttemptExecutionState.CANCELLED
    assert (
        owner.cancellation_disposition
        is CancellationDisposition.SKIPPED_SHARED
    )
    assert peer.workflow_id == owner.workflow_id
    assert peer.enqueue_state is AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT
    assert peer.execution_state is AttemptExecutionState.NOT_STARTED
    assert peer.cancellation_request_id is None


def test_public_inspection_reads_the_persisted_lifecycle(
    pg_engine: Engine,
) -> None:
    schema, _target_value, _registry_value, _source_value, _adapter = (
        _submit_enqueued(pg_engine, operation_key="inspect-lifecycle")
    )

    operation = inspect_operation(
        "inspect-lifecycle",
        engine=pg_engine,
        schema=schema,
    )
    items = list_items(
        "inspect-lifecycle",
        engine=pg_engine,
        schema=schema,
    )
    attempts = list_attempts(
        "inspect-lifecycle",
        engine=pg_engine,
        schema=schema,
    )

    assert operation.current_item_count == 1
    assert operation.current_attempt_count == 1
    assert operation.operation.inserted_count == 1
    assert operation.operation.workflow_already_present_count == 0
    assert items[0].current_attempt == attempts[0].attempt
    assert attempts[0].claims[0].workflow_id == attempts[0].attempt.workflow_id
    assert attempts[0].attempt.enqueue_state == "enqueued"
