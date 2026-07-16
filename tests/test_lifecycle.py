"""Distinct durable lifecycle guarantees backed by PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, func, select, text, update
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
    RegistrationResult,
    ServiceClass,
    TargetRegistry,
    cancel_operation,
    inspect_operation,
    list_attempts,
    list_items,
    request_next_attempt,
    submit,
    upgrade_platform_schema,
)
from dr_platform.claims import ClaimPageOptions, claim_pending_attempts
from dr_platform.enqueue_runtime import (
    PhysicalEnqueueDisposition,
    PhysicalEnqueueOutcome,
    PreparedEnqueueCall,
    QueueConfigurationError,
)
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.reconciliation import ReconciliationConflictError
from dr_platform.reconciliation_runtime import (
    DbosStepObservation,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
)
from dr_platform.status import AttemptExecutionState
from dr_platform.submission import (
    RegistrationConflictError,
    RegistrationHook,
    RegistrationItem,
    RegistrationPageContext,
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
        self.calls.append(call)
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=PhysicalEnqueueDisposition.ENQUEUED,
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


def _target(
    *,
    registration_hook: RegistrationHook | None = None,
) -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name="lifecycle-queue",
        workflow_role="lifecycle",
        managed_workflow_name="lifecycle-workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
        registration_hook_name=(
            "failing-hook" if registration_hook is not None else None
        ),
        registration_hook_version=(
            1 if registration_hook is not None else None
        ),
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
        registration_hook=registration_hook,
    )


def _registry(target: ExecutionTarget) -> TargetRegistry:
    registry = TargetRegistry()
    registry.register(target)
    return registry


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


def test_submit_rolls_back_the_registration_page_when_its_hook_fails(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE registration_hook_effects "
            "(item_key text PRIMARY KEY)"
        )

    def failing_hook(
        connection: Any,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
        page: RegistrationPageContext,
    ) -> RegistrationResult:
        del operation_key, page
        connection.execute(
            text(
                "INSERT INTO registration_hook_effects (item_key) "
                "VALUES (:item_key)"
            ),
            {"item_key": items[0].item_key},
        )
        raise RuntimeError("hook failed")

    target = _target(registration_hook=failing_hook)
    registry = _registry(target)
    with pytest.raises(RuntimeError, match="hook failed"):
        submit(
            operation_key="hook-rollback",
            workflow_role=target.workflow_role,
            group_key="lifecycle-group",
            target=target,
            source=_source(1),
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )

    with pg_engine.connect() as connection:
        hook_effect_count = connection.scalar(
            text("SELECT count(*) FROM registration_hook_effects")
        )
        item_count = connection.scalar(
            select(func.count()).select_from(schema.items)
        )
        cursor = connection.scalar(
            select(schema.operations.c.registration_cursor)
        )
    assert (hook_effect_count, item_count, cursor) == (0, 0, 0)


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
    assert items[0].current_attempt == attempts[0].attempt
    assert attempts[0].claims[0].workflow_id == attempts[0].attempt.workflow_id
    assert attempts[0].attempt.enqueue_state == "enqueued"
