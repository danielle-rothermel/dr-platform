"""Executable P3 kernel enqueue and append-only Claim contract."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier, Lock
from typing import Any, Literal
from unittest.mock import patch

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, event, func, insert, select, update
from sqlalchemy.exc import IntegrityError

import dr_platform
from dr_platform import PlatformSchema, upgrade_platform_schema
from dr_platform import claims as claims_module
from dr_platform.claims import (
    ClaimAuthorityError,
    ClaimConflictError,
    ClaimPage,
    ClaimPageOptions,
    PostgresClaimTransitionStore,
    claim_pending_attempts,
    replace_expired_unstarted_claims,
    start_enqueue_call,
)
from dr_platform.db import coordination as coordination_module
from dr_platform.enqueue_runtime import (
    EnqueueClaimExecutionDisposition,
    EnqueuePageResult,
    PhysicalEnqueueDisposition,
    PhysicalEnqueueOutcome,
    PreparedEnqueueCall,
    QueueConfigurationError,
    WorkflowObservation,
    WorkflowObservationDisposition,
    enqueue_pending_page,
    execute_enqueue_claim,
    prepare_enqueue_call,
    recover_call_started_page,
)
from dr_platform.items import SubmittableItem, item_id, shuffle_rank
from dr_platform.manifests import ExecutionRecipeEnvelope, ExecutionTargetRef
from dr_platform.records import (
    EnqueueClaimRecord,
    FailureSnapshot,
    RetryPolicy,
)
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    EnqueueClaimDisposition,
    EnqueueCompensationDisposition,
    EnqueueCompensationReason,
    FailureClass,
    OperationStatus,
    ServiceClass,
)
from dr_platform.submission import SubmitOptions, submit
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
    TargetRegistry,
)
from tests.conftest import engine_dsn


class ClaimTestItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: str
    spec: dict[str, Any]
    service_class: ServiceClass


class ClaimTestSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[ClaimTestItem, ...]

    @property
    def item_count(self) -> int:
        return len(self.items)

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[ClaimTestItem, ...]:
        return self.items[start_index:end_index]


def _workflow() -> None:
    return None


def _target() -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name="claim-test-queue",
        workflow_role="claim-test",
        managed_workflow_name="claim_test_workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
    )
    target_ref = declaration.target_ref(
        target_key="claim-test",
        target_version=1,
    )

    def recipe_for(item: SubmittableItem) -> ExecutionRecipeEnvelope:
        return ExecutionRecipeEnvelope(
            target_ref=target_ref,
            managed_workflow_name=declaration.managed_workflow_name,
            managed_workflow_version=declaration.managed_workflow_version,
            argument_recipe_version=declaration.argument_recipe_version,
            payload={"item_key": item.item_key, "spec": item.spec},
        )

    return ExecutionTarget(
        ref=target_ref,
        **declaration.model_dump(),
        workflow=_workflow,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"execution:{item.item_key}:{attempt}",
            workflow_id=f"workflow:{item.item_key}:{attempt}",
        ),
        args_for=lambda item, attempt: (item.item_key, attempt),
        recipe_for=recipe_for,
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
    )


def _upgrade_schema(engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return PlatformSchema()


def _register_items(
    engine: Engine,
    *,
    schema: PlatformSchema,
    items: tuple[ClaimTestItem, ...],
    options: SubmitOptions | None = None,
) -> None:
    target = _target()
    source = ClaimTestSource(items=items)
    registry = TargetRegistry()
    registry.register(target)
    with patch("dr_platform.submission._enqueue_registered_page"):
        submit(
            operation_key="claim-operation",
            workflow_role=target.workflow_role,
            group_key="claim-group",
            target=target,
            source=source,
            engine=engine,
            resolver=registry,
            schema=schema,
            options=options,
        )


def _claim_ids(*values: str) -> Iterator[str]:
    return iter(values)


def _install_attempt_eligibility_race(
    monkeypatch: pytest.MonkeyPatch,
    *,
    mutation: Literal["cancelled", "terminal"],
) -> None:
    original = claims_module._lock_candidate_hierarchy
    applied = False

    def race_before_lock(
        connection: Any,
        *,
        schema: PlatformSchema,
        candidates: Sequence[Mapping[str, Any]],
        claim_keys: Sequence[tuple[str, int, str]] = (),
    ) -> Any:
        nonlocal applied
        if not applied:
            applied = True
            candidate = candidates[0]
            values: dict[str, Any]
            if mutation == "cancelled":
                values = {
                    "cancellation_request_id": "race-cancellation",
                    "cancellation_requested_at": func.clock_timestamp(),
                    "cancellation_requested_by": "race-operator",
                    "cancellation_origin": "local_operation",
                    "updated_at": func.clock_timestamp(),
                }
            else:
                assert mutation == "terminal"
                values = {
                    "execution_state": AttemptExecutionState.CANCELLED.value,
                    "terminal_at": func.clock_timestamp(),
                    "updated_at": func.clock_timestamp(),
                }
            connection.execute(
                update(schema.item_attempts)
                .where(
                    schema.item_attempts.c.item_id == candidate["item_id"],
                    schema.item_attempts.c.attempt == candidate["attempt"],
                )
                .values(**values)
            )
        return original(
            connection,
            schema=schema,
            candidates=candidates,
            claim_keys=claim_keys,
        )

    monkeypatch.setattr(
        claims_module,
        "_lock_candidate_hierarchy",
        race_before_lock,
    )


def _allow_targets(target_refs: tuple[ExecutionTargetRef, ...]) -> None:
    identities = tuple(
        (target_ref.target_key, target_ref.target_version)
        for target_ref in target_refs
    )
    assert identities == tuple(sorted(set(identities)))


class QueueConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    database_backed_queue: bool
    priority_enabled: bool


class FakeQueueLookup:
    def __init__(self, queue: QueueConfiguration | None) -> None:
        self.queue = queue

    def retrieve_queue(self, name: str) -> QueueConfiguration | None:
        del name
        return self.queue


class FakeTransitionStore:
    def __init__(
        self,
        *,
        start_wins: bool = True,
        outcome_wins: bool = True,
    ) -> None:
        self.start_wins = start_wins
        self.outcome_wins = outcome_wins
        self.events: list[str] = []
        self.compensations: list[PhysicalEnqueueOutcome] = []

    def mark_enqueue_call_started(
        self,
        *,
        claim: EnqueueClaimRecord,
    ) -> bool:
        del claim
        self.events.append("call-start-committed")
        return self.start_wins

    def record_enqueue_outcome(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> bool:
        del claim, outcome
        self.events.append("outcome-recorded")
        return self.outcome_wins

    def ensure_lost_outcome_compensation(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> None:
        del claim
        self.events.append("compensation-created")
        self.compensations.append(outcome)


class FakePhysicalAdapter:
    def __init__(
        self,
        *,
        store: FakeTransitionStore,
        outcome: PhysicalEnqueueOutcome,
    ) -> None:
        self.store = store
        self.outcome = outcome
        self.calls = 0

    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome:
        del call
        assert self.store.events == ["call-start-committed"]
        self.store.events.append("physical-enqueue")
        self.calls += 1
        return self.outcome


class ClaimIdSequence:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.index = 0
        self.lock = Lock()

    def __call__(self) -> str:
        with self.lock:
            self.index += 1
            return f"{self.prefix}-{self.index}"


class CallOutcomeAdapter:
    def __init__(self, disposition: PhysicalEnqueueDisposition) -> None:
        self.disposition = disposition
        self.calls: list[PreparedEnqueueCall] = []

    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome:
        self.calls.append(call)
        if self.disposition in {
            PhysicalEnqueueDisposition.ENQUEUED,
            PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
        }:
            return PhysicalEnqueueOutcome(
                workflow_id=call.workflow_id,
                disposition=self.disposition,
                effective_service_priority=call.service_priority,
            )
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=self.disposition,
            failure=_physical_failure(),
        )


class StaticWorkflowObserver:
    def __init__(self, observation: WorkflowObservation) -> None:
        self.observation = observation
        self.calls: list[PreparedEnqueueCall] = []

    def observe(self, call: PreparedEnqueueCall) -> WorkflowObservation:
        self.calls.append(call)
        return self.observation.model_copy(
            update={"workflow_id": call.workflow_id}
        )


def _runtime_claim() -> EnqueueClaimRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return EnqueueClaimRecord(
        item_id="runtime-item",
        attempt=0,
        claim_id="runtime-claim",
        workflow_id="runtime-workflow",
        enqueue_try=1,
        claimed_at=now,
        lease_expires_at=now + timedelta(seconds=60),
        disposition=EnqueueClaimDisposition.CLAIMED,
        created_at=now,
        change_seq=1,
    )


def _runtime_call() -> PreparedEnqueueCall:
    return PreparedEnqueueCall(
        item_id="runtime-item",
        workflow_id="runtime-workflow",
        execution_key="runtime-execution",
        workflow_role="claim-test",
        attempt=0,
        queue_name="claim-test-queue",
        managed_workflow_name="claim_test_workflow",
        service_priority=ServiceClass.STANDARD.priority,
        workflow=_workflow,
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
        args=("safe-argument",),
        attributes={
            "platform.execution_key": "runtime-execution",
            "platform.workflow_role": "claim-test",
            "platform.attempt": 0,
        },
    )


def _physical_failure() -> FailureSnapshot:
    return FailureSnapshot(
        failure_class=FailureClass.TRANSIENT,
        error_type="TransientQueueError",
        message="retry later",
    )


def test_claim_order_is_service_then_deterministic_shuffle() -> None:
    inputs = (
        ("operation-1", "model-a-1", ServiceClass.STANDARD),
        ("operation-1", "model-a-2", ServiceClass.BACKFILL),
        ("operation-1", "model-b-1", ServiceClass.URGENT),
        ("operation-1", "model-b-2", ServiceClass.STANDARD),
    )
    scheduling_rows = tuple(
        (
            service_class.priority,
            shuffle_rank(
                item_id=item_id(
                    operation_key=operation_key,
                    item_key=item_key,
                )
            ),
            item_id(
                operation_key=operation_key,
                item_key=item_key,
            ),
            item_key,
        )
        for operation_key, item_key, service_class in inputs
    )

    first_order = tuple(row[3] for row in sorted(scheduling_rows))
    replay_order = tuple(row[3] for row in sorted(scheduling_rows))

    assert first_order == replay_order
    assert first_order[0] == "model-b-1"
    assert first_order[-1] == "model-a-2"
    assert all(0 < row[1] < 2**63 for row in scheduling_rows)


def test_claim_page_persists_service_then_shuffle_order(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="standard-b",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
            ClaimTestItem(
                item_key="backfill",
                spec={},
                service_class=ServiceClass.BACKFILL,
            ),
            ClaimTestItem(
                item_key="urgent",
                spec={},
                service_class=ServiceClass.URGENT,
            ),
            ClaimTestItem(
                item_key="standard-a",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    claim_ids = _claim_ids("claim-1", "claim-2", "claim-3", "claim-4")

    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: next(claim_ids),
    )

    with pg_engine.connect() as connection:
        expected_item_ids = tuple(
            connection.execute(
                select(schema.items.c.item_id).order_by(
                    schema.items.c.service_priority,
                    schema.items.c.shuffle_rank,
                    schema.items.c.item_id,
                )
            ).scalars()
        )
    assert tuple(claim.item_id for claim in page.claims) == expected_item_ids
    assert tuple(claim.service_priority for claim in page.claims) == tuple(
        sorted(claim.service_priority for claim in page.claims)
    )


def test_concurrent_claimers_create_one_append_only_claim(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="only-item",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    first_ids = _claim_ids("claim-first")
    second_ids = _claim_ids("claim-second")

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            claim_pending_attempts,
            pg_engine,
            admit_targets=_allow_targets,
            schema=schema,
            claim_id_factory=lambda: next(first_ids),
        )
        second_future = executor.submit(
            claim_pending_attempts,
            pg_engine,
            admit_targets=_allow_targets,
            schema=schema,
            claim_id_factory=lambda: next(second_ids),
        )
        pages = (
            first_future.result(timeout=10),
            second_future.result(timeout=10),
        )

    assert sum(len(page.claims) for page in pages) == 1
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 1
        )
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        assert attempt["enqueue_state"] == AttemptEnqueueState.CLAIMING.value
        assert attempt["current_claim_id"] in {"claim-first", "claim-second"}


def test_concurrent_small_pages_fill_disjoint_eligible_claims(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=tuple(
            ClaimTestItem(
                item_key=f"page-item-{index}",
                spec={},
                service_class=ServiceClass.STANDARD,
            )
            for index in range(4)
        ),
    )
    concurrent_start = Barrier(2)

    def claim_page(claim_id_factory: ClaimIdSequence) -> ClaimPage:
        concurrent_start.wait(timeout=10)
        return claim_pending_attempts(
            pg_engine,
            admit_targets=_allow_targets,
            options=ClaimPageOptions(page_size=2),
            schema=schema,
            claim_id_factory=claim_id_factory,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(
                claim_page,
                ClaimIdSequence(f"claimer-{index}"),
            )
            for index in range(2)
        )
        pages = tuple(future.result(timeout=15) for future in futures)

    claimed_item_sets = tuple(
        {claim.item_id for claim in page.claims} for page in pages
    )
    assert tuple(len(page.claims) for page in pages) == (2, 2)
    assert claimed_item_sets[0].isdisjoint(claimed_item_sets[1])
    assert len(claimed_item_sets[0] | claimed_item_sets[1]) == 4


def test_same_claim_id_transitions_exact_composite_rows(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="shared-claim-a",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
            ClaimTestItem(
                item_key="shared-claim-b",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    claims = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "shared-claim-id",
    ).claims

    assert len(claims) == 2
    for claimed in claims:
        start_enqueue_call(
            pg_engine,
            item_id=claimed.item_id,
            attempt=claimed.attempt,
            claim_id=claimed.claim_id,
            schema=schema,
        )

    with pg_engine.connect() as connection:
        rows = tuple(
            connection.execute(
                select(schema.enqueue_claims).where(
                    schema.enqueue_claims.c.claim_id == "shared-claim-id"
                )
            ).mappings()
        )
    assert len(rows) == 2
    assert {row["item_id"] for row in rows} == {
        claim.item_id for claim in claims
    }
    assert all(
        row["disposition"] == EnqueueClaimDisposition.CALL_STARTED.value
        for row in rows
    )


def test_claim_transaction_observes_global_lock_order(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="z-workflow",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
            ClaimTestItem(
                item_key="a-workflow",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    observed_statements: list[tuple[str, object]] = []

    def record_statements(  # noqa: PLR0913
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,  # noqa: FBT001 -- SQLAlchemy event signature
    ) -> None:
        del connection, cursor, context, executemany
        observed_statements.append((statement, parameters))

    event.listen(pg_engine, "before_cursor_execute", record_statements)
    try:
        claim_ids = iter(("claim-a", "claim-z"))
        claim_pending_attempts(
            pg_engine,
            admit_targets=_allow_targets,
            schema=schema,
            claim_id_factory=claim_ids.__next__,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", record_statements)

    workflow_lock_values: list[str] = []
    for statement, parameters in observed_statements:
        if "hashtextextended" not in statement:
            continue
        if not isinstance(parameters, Mapping):
            continue
        workflow_id = parameters.get("id")
        if isinstance(workflow_id, str):
            workflow_lock_values.append(workflow_id)
    workflow_locks = tuple(workflow_lock_values)
    assert workflow_locks == tuple(sorted(workflow_locks))
    workflow_lock_indexes = tuple(
        index
        for index, (statement, _) in enumerate(observed_statements)
        if "hashtextextended" in statement
    )
    operation_lock_index = next(
        index
        for index, (statement, _) in enumerate(observed_statements)
        if "FROM platform_operations" in statement
        and "FOR UPDATE" in statement
    )
    item_lock_index = next(
        index
        for index, (statement, _) in enumerate(observed_statements)
        if "FROM platform_items" in statement and "FOR UPDATE" in statement
    )
    attempt_lock_index = next(
        index
        for index, (statement, _) in enumerate(observed_statements)
        if "FROM platform_item_attempts" in statement
        and "FOR UPDATE" in statement
    )
    assert max(workflow_lock_indexes) < operation_lock_index
    assert operation_lock_index < item_lock_index < attempt_lock_index


def test_claim_row_locks_are_acquired_by_claim_id_then_attempt_identity(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="claim-lock-order-a",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
            ClaimTestItem(
                item_key="claim-lock-order-b",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    original_claims = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        options=ClaimPageOptions(page_size=2, lease_seconds=1),
        claim_id_factory=_claim_ids("z-claim", "a-claim").__next__,
    ).claims
    assert len(original_claims) == 2
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )
    observed_claim_locks: list[tuple[str, int, str]] = []

    def record_claim_locks(  # noqa: PLR0913
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,  # noqa: FBT001 -- SQLAlchemy event signature
    ) -> None:
        del connection, cursor, context, executemany
        if (
            "FROM platform_enqueue_claims" not in statement
            or "FOR UPDATE" not in statement
            or not isinstance(parameters, Mapping)
        ):
            return

        def parameter_value(prefix: str) -> Any:
            return next(
                value
                for key, value in parameters.items()
                if str(key).startswith(prefix)
            )

        observed_claim_locks.append(
            (
                str(parameter_value("item_id")),
                int(parameter_value("attempt")),
                str(parameter_value("claim_id")),
            )
        )

    event.listen(pg_engine, "before_cursor_execute", record_claim_locks)
    try:
        replacement_ids = _claim_ids("replacement-a", "replacement-z")
        replace_expired_unstarted_claims(
            pg_engine,
            admit_targets=_allow_targets,
            schema=schema,
            options=ClaimPageOptions(page_size=2, lease_seconds=1),
            claim_id_factory=replacement_ids.__next__,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", record_claim_locks)

    assert tuple(claim_id for _, _, claim_id in observed_claim_locks) == (
        "a-claim",
        "z-claim",
    )
    assert observed_claim_locks == sorted(
        observed_claim_locks,
        key=lambda key: (key[2], key[0], key[1]),
    )


def test_registration_completion_gates_claim_admission(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="not-ready",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.operations).values(
                status="registering",
                registration_completed_at=None,
                registration_lease_id="registration-lease",
                registration_lease_expires_at=datetime(2099, 1, 1, tzinfo=UTC),
            )
        )

    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "must-not-be-used",
    )

    assert page.claims == ()
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 0
        )


@pytest.mark.parametrize(
    "queue",
    [
        pytest.param(None, id="missing"),
        pytest.param(
            QueueConfiguration(
                database_backed_queue=False,
                priority_enabled=True,
            ),
            id="not-database-backed",
        ),
        pytest.param(
            QueueConfiguration(
                database_backed_queue=True,
                priority_enabled=False,
            ),
            id="priority-disabled",
        ),
    ],
)
def test_queue_admission_fails_before_claim_mutation(
    pg_engine: Engine,
    queue: QueueConfiguration | None,
) -> None:
    schema = _upgrade_schema(pg_engine)
    target = _target()
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="queue-gated",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )

    registry = TargetRegistry()
    registry.register(target)

    with pytest.raises(QueueConfigurationError):
        enqueue_pending_page(
            pg_engine,
            resolver=registry,
            queue_lookup=FakeQueueLookup(queue),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 0
        )
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        assert attempt["enqueue_state"] == AttemptEnqueueState.PENDING.value
        assert attempt["current_claim_id"] is None


def test_cancelled_or_terminal_attempt_is_not_claimable(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="cancelled-before-claim",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                cancellation_request_id="cancel-request",
                cancellation_requested_at=func.clock_timestamp(),
                cancellation_requested_by="operator",
                cancellation_origin="local_operation",
            )
        )

    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "must-not-be-used",
    )

    assert page.claims == ()
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 0
        )


@pytest.mark.parametrize("mutation", ["cancelled", "terminal"])
def test_pending_claim_rechecks_eligibility_after_candidate_selection(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Literal["cancelled", "terminal"],
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key=f"pending-race-{mutation}",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    _install_attempt_eligibility_race(monkeypatch, mutation=mutation)

    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "must-not-be-used",
    )

    assert page.claims == ()
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 0
        )


def test_expired_uncalled_claim_is_replaced_without_identity_reuse(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="replace-me",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        options=ClaimPageOptions(lease_seconds=1),
        claim_id_factory=lambda: "original-claim",
    )
    original = page.claims[0]
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )

    replacement_page = replace_expired_unstarted_claims(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "replacement-claim",
    )

    assert len(replacement_page.claims) == 1
    replacement = replacement_page.claims[0]
    assert replacement.claim_id == "replacement-claim"
    assert replacement.workflow_id == original.workflow_id
    assert replacement.attempt == original.attempt
    assert replacement.enqueue_try == original.enqueue_try
    with pg_engine.connect() as connection:
        claims = tuple(
            connection.execute(
                select(schema.enqueue_claims).order_by(
                    schema.enqueue_claims.c.created_at,
                    schema.enqueue_claims.c.claim_id,
                )
            ).mappings()
        )
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    assert len(claims) == 2
    original_row = next(
        claim for claim in claims if claim["claim_id"] == "original-claim"
    )
    assert (
        original_row["disposition"] == EnqueueClaimDisposition.REPLACED.value
    )
    assert original_row["replacement_claim_id"] == "replacement-claim"
    assert attempt["current_claim_id"] == "replacement-claim"


@pytest.mark.parametrize("mutation", ["cancelled", "terminal"])
def test_expired_unstarted_replacement_rechecks_locked_eligibility(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Literal["cancelled", "terminal"],
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key=f"replacement-race-{mutation}",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    original = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        options=ClaimPageOptions(lease_seconds=1),
        claim_id_factory=lambda: "race-original-claim",
    ).claims[0]
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )
    _install_attempt_eligibility_race(monkeypatch, mutation=mutation)

    replacement_page = replace_expired_unstarted_claims(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "must-not-be-used",
    )

    assert replacement_page.claims == ()
    with pg_engine.connect() as connection:
        claims = tuple(
            connection.execute(select(schema.enqueue_claims)).mappings()
        )
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    assert len(claims) == 1
    assert claims[0]["claim_id"] == original.claim_id
    assert attempt["current_claim_id"] == original.claim_id


def test_call_start_cas_is_exact_and_blocks_expired_claim(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="call-start-valid",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
            ClaimTestItem(
                item_key="call-start-expired",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    claims = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        options=ClaimPageOptions(lease_seconds=1),
        claim_id_factory=iter(("call-claim", "expired-claim")).__next__,
    ).claims
    claimed = next(claim for claim in claims if claim.claim_id == "call-claim")
    expired = next(
        claim for claim in claims if claim.claim_id == "expired-claim"
    )

    started = start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )
    replay = start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )

    assert started == replay
    assert started.disposition is EnqueueClaimDisposition.CALL_STARTED
    assert started.enqueue_call_started_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )
    with pytest.raises(ClaimAuthorityError):
        start_enqueue_call(
            pg_engine,
            item_id=expired.item_id,
            attempt=expired.attempt,
            claim_id=expired.claim_id,
            schema=schema,
        )


def test_call_start_cas_precedes_physical_enqueue_and_blocks_loser() -> None:
    claim = _runtime_claim()
    call = _runtime_call()
    losing_store = FakeTransitionStore(start_wins=False)
    losing_adapter = FakePhysicalAdapter(
        store=losing_store,
        outcome=PhysicalEnqueueOutcome(
            workflow_id=claim.workflow_id,
            disposition=PhysicalEnqueueDisposition.ENQUEUED,
            effective_service_priority=ServiceClass.STANDARD.priority,
        ),
    )

    lost = execute_enqueue_claim(
        claim=claim,
        call=call,
        store=losing_store,
        adapter=losing_adapter,
    )

    assert lost.disposition is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    assert losing_store.events == ["call-start-committed"]
    assert losing_adapter.calls == 0


def test_enqueue_adapter_receives_only_allowlisted_context(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    target = _target()
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="allowlisted-context",
                spec={"safe": "label"},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "allowlist-claim",
    ).claims[0]

    call = prepare_enqueue_call(
        item=claimed.item,
        attempt=claimed.attempt_record,
        target=target,
    )

    assert call.workflow_id == claimed.workflow_id
    assert call.service_priority == ServiceClass.STANDARD.priority
    assert call.args == ("allowlisted-context", 0)
    assert call.attributes == {
        "platform.execution_key": claimed.attempt_record.execution_key,
        "platform.workflow_role": "claim-test",
        "platform.attempt": 0,
    }


@pytest.mark.parametrize(
    "outcome",
    [
        pytest.param(
            PhysicalEnqueueOutcome(
                workflow_id="runtime-workflow",
                disposition=PhysicalEnqueueDisposition.ENQUEUED,
                effective_service_priority=ServiceClass.STANDARD.priority,
            ),
            id="enqueued-here",
        ),
        pytest.param(
            PhysicalEnqueueOutcome(
                workflow_id="runtime-workflow",
                disposition=(
                    PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
                ),
                effective_service_priority=7_000,
            ),
            id="workflow-already-present",
        ),
        pytest.param(
            PhysicalEnqueueOutcome(
                workflow_id="runtime-workflow",
                disposition=PhysicalEnqueueDisposition.ENQUEUE_ERROR,
                failure=_physical_failure(),
            ),
            id="enqueue-error",
        ),
    ],
)
def test_physical_enqueue_outcomes_are_recorded_once(
    outcome: PhysicalEnqueueOutcome,
) -> None:
    store = FakeTransitionStore()
    adapter = FakePhysicalAdapter(store=store, outcome=outcome)

    result = execute_enqueue_claim(
        claim=_runtime_claim(),
        call=_runtime_call(),
        store=store,
        adapter=adapter,
    )

    assert (
        result.disposition is EnqueueClaimExecutionDisposition.OUTCOME_RECORDED
    )
    assert result.outcome == outcome
    assert store.events == [
        "call-start-committed",
        "physical-enqueue",
        "outcome-recorded",
    ]
    assert store.compensations == []


def test_lost_success_outcome_creates_exact_compensation_signal() -> None:
    outcome = PhysicalEnqueueOutcome(
        workflow_id="runtime-workflow",
        disposition=PhysicalEnqueueDisposition.ENQUEUED,
        effective_service_priority=ServiceClass.STANDARD.priority,
    )
    store = FakeTransitionStore(outcome_wins=False)
    adapter = FakePhysicalAdapter(store=store, outcome=outcome)

    result = execute_enqueue_claim(
        claim=_runtime_claim(),
        call=_runtime_call(),
        store=store,
        adapter=adapter,
    )

    assert (
        result.disposition is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    )
    assert store.compensations == [outcome]
    assert store.events == [
        "call-start-committed",
        "physical-enqueue",
        "outcome-recorded",
        "compensation-created",
    ]


def test_compensation_workflow_provenance_is_enforced_by_store_and_fk(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="forged-compensation",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        schema=schema,
        claim_id_factory=lambda: "compensation-claim",
    ).claims[0]
    started = start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )
    forged_workflow_id = "forged-workflow"
    forged_claim = started.model_copy(
        update={"workflow_id": forged_workflow_id}
    )
    forged_outcome = PhysicalEnqueueOutcome(
        workflow_id=forged_workflow_id,
        disposition=PhysicalEnqueueDisposition.ENQUEUED,
        effective_service_priority=ServiceClass.STANDARD.priority,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)

    with pytest.raises(
        ClaimConflictError,
        match="workflow provenance changed",
    ):
        store.ensure_lost_outcome_compensation(
            claim=forged_claim,
            outcome=forged_outcome,
        )

    with pytest.raises(IntegrityError), pg_engine.begin() as connection:
        connection.execute(
            insert(schema.enqueue_compensations).values(
                item_id=started.item_id,
                attempt=started.attempt,
                claim_id=started.claim_id,
                workflow_id=forged_workflow_id,
                reason=(
                    EnqueueCompensationReason.INVALIDATED_CALL_STARTED_CLAIM.value
                ),
                cancel_disposition=(
                    EnqueueCompensationDisposition.PENDING.value
                ),
                created_at=func.clock_timestamp(),
            )
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_compensations)
            )
            == 0
        )


def test_uncertain_physical_outcome_remains_unresolved() -> None:
    outcome = PhysicalEnqueueOutcome(
        workflow_id="runtime-workflow",
        disposition=PhysicalEnqueueDisposition.UNCERTAIN,
        failure=_physical_failure(),
    )
    store = FakeTransitionStore()
    adapter = FakePhysicalAdapter(store=store, outcome=outcome)

    result = execute_enqueue_claim(
        claim=_runtime_claim(),
        call=_runtime_call(),
        store=store,
        adapter=adapter,
    )

    assert result.disposition is EnqueueClaimExecutionDisposition.UNCERTAIN
    assert store.events == ["call-start-committed", "physical-enqueue"]
    assert store.compensations == []


def _expired_call_started_claim(
    pg_engine: Engine,
    *,
    schema: PlatformSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[ExecutionTarget, TargetRegistry, str]:
    target = _target()
    _register_items(
        pg_engine,
        schema=schema,
        items=(
            ClaimTestItem(
                item_key="restart-recovery",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        ),
    )
    registry = TargetRegistry()
    registry.register(target)
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        claim_id_factory=lambda: "restart-original-claim",
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )
    return target, registry, claimed.workflow_id


@pytest.mark.parametrize(
    "observation_disposition",
    [
        WorkflowObservationDisposition.EXISTING,
        WorkflowObservationDisposition.ABSENT,
        WorkflowObservationDisposition.UNCERTAIN,
    ],
)
def test_call_started_restart_recovery_observes_before_enqueue(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    observation_disposition: WorkflowObservationDisposition,
) -> None:
    schema = _upgrade_schema(pg_engine)
    _target_value, registry, workflow_id = _expired_call_started_claim(
        pg_engine,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    if observation_disposition is WorkflowObservationDisposition.EXISTING:
        observation = WorkflowObservation(
            workflow_id=workflow_id,
            disposition=observation_disposition,
            outcome=PhysicalEnqueueOutcome(
                workflow_id=workflow_id,
                disposition=(
                    PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
                ),
                effective_service_priority=ServiceClass.STANDARD.priority,
            ),
        )
    elif observation_disposition is WorkflowObservationDisposition.ABSENT:
        observation = WorkflowObservation(
            workflow_id=workflow_id,
            disposition=observation_disposition,
        )
    else:
        observation = WorkflowObservation(
            workflow_id=workflow_id,
            disposition=observation_disposition,
            failure=_physical_failure(),
        )
    observer = StaticWorkflowObserver(observation)
    adapter = CallOutcomeAdapter(PhysicalEnqueueDisposition.ENQUEUED)

    result = recover_call_started_page(
        pg_engine,
        resolver=registry,
        queue_lookup=FakeQueueLookup(
            QueueConfiguration(
                database_backed_queue=True,
                priority_enabled=True,
            )
        ),
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        adapter=adapter,
        observer=observer,
    )

    assert len(result.items) == 1
    assert len(observer.calls) == 1
    if observation_disposition is WorkflowObservationDisposition.UNCERTAIN:
        assert (
            result.items[0].execution.disposition
            is EnqueueClaimExecutionDisposition.UNCERTAIN
        )
        assert adapter.calls == []
    else:
        assert (
            result.items[0].execution.disposition
            is EnqueueClaimExecutionDisposition.OUTCOME_RECORDED
        )
        assert len(adapter.calls) == (
            1
            if observation_disposition is WorkflowObservationDisposition.ABSENT
            else 0
        )
    with pg_engine.connect() as connection:
        claims = tuple(
            connection.execute(select(schema.enqueue_claims)).mappings()
        )
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    if observation_disposition is WorkflowObservationDisposition.EXISTING:
        assert len(claims) == 1
        assert (
            attempt["enqueue_state"]
            == AttemptEnqueueState.WORKFLOW_ALREADY_PRESENT.value
        )
    elif observation_disposition is WorkflowObservationDisposition.ABSENT:
        assert len(claims) == 2
        assert attempt["enqueue_state"] == AttemptEnqueueState.ENQUEUED.value
    else:
        assert len(claims) == 1
        assert attempt["enqueue_state"] == AttemptEnqueueState.CLAIMING.value


@pytest.mark.parametrize("mutation", ["cancelled", "terminal"])
def test_absence_replacement_rechecks_locked_eligibility_before_enqueue(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Literal["cancelled", "terminal"],
) -> None:
    schema = _upgrade_schema(pg_engine)
    _target_value, registry, workflow_id = _expired_call_started_claim(
        pg_engine,
        schema=schema,
        monkeypatch=monkeypatch,
    )
    _install_attempt_eligibility_race(monkeypatch, mutation=mutation)
    observer = StaticWorkflowObserver(
        WorkflowObservation(
            workflow_id=workflow_id,
            disposition=WorkflowObservationDisposition.ABSENT,
        )
    )
    adapter = CallOutcomeAdapter(PhysicalEnqueueDisposition.ENQUEUED)

    result = recover_call_started_page(
        pg_engine,
        resolver=registry,
        queue_lookup=FakeQueueLookup(
            QueueConfiguration(
                database_backed_queue=True,
                priority_enabled=True,
            )
        ),
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        adapter=adapter,
        observer=observer,
    )

    assert len(result.items) == 1
    assert (
        result.items[0].execution.disposition
        is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    )
    assert adapter.calls == []
    with pg_engine.connect() as connection:
        claims = tuple(
            connection.execute(select(schema.enqueue_claims)).mappings()
        )
    assert len(claims) == 1
    assert claims[0]["claim_id"] == "restart-original-claim"


def test_root_submit_enqueues_and_returns_post_enqueue_aggregate(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    target = _target()
    source = ClaimTestSource(
        items=(
            ClaimTestItem(
                item_key="root-submit",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        )
    )
    registry = TargetRegistry()
    registry.register(target)
    adapter = CallOutcomeAdapter(PhysicalEnqueueDisposition.ENQUEUED)

    result = dr_platform.submit(
        operation_key="root-submit-operation",
        workflow_role=target.workflow_role,
        group_key="root-submit-group",
        target=target,
        source=source,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
        queue_lookup=FakeQueueLookup(
            QueueConfiguration(
                database_backed_queue=True,
                priority_enabled=True,
            )
        ),
        enqueue_adapter=adapter,
    )

    assert result.enqueued_count == 1
    assert result.registration_cursor == 1
    assert len(adapter.calls) == 1
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 1
        )


def test_root_submit_missing_queue_creates_no_claim(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    target = _target()
    source = ClaimTestSource(
        items=(
            ClaimTestItem(
                item_key="missing-queue",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        )
    )
    registry = TargetRegistry()
    registry.register(target)

    with pytest.raises(QueueConfigurationError):
        dr_platform.submit(
            operation_key="missing-queue-operation",
            workflow_role=target.workflow_role,
            group_key="missing-queue-group",
            target=target,
            source=source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
            queue_lookup=FakeQueueLookup(None),
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.enqueue_claims)
            )
            == 0
        )


def test_root_submit_runs_reconcile_before_enqueue_without_cut_bump(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_schema(pg_engine)
    target = _target()
    source = ClaimTestSource(
        items=(
            ClaimTestItem(
                item_key="ordered-stages",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        )
    )
    registry = TargetRegistry()
    registry.register(target)
    stage_order: list[str] = []

    def stage(name: str) -> Any:
        def run(*args: Any, **kwargs: Any) -> EnqueuePageResult:
            del args, kwargs
            stage_order.append(name)
            return EnqueuePageResult(items=())

        return run

    queue_lookup = FakeQueueLookup(
        QueueConfiguration(
            database_backed_queue=True,
            priority_enabled=True,
        )
    )
    with patch(
        "dr_platform.reconciliation_runtime.reconcile",
        side_effect=stage("reconcile"),
    ):
        dr_platform.submit(
            operation_key="ordered-stages-operation",
            workflow_role=target.workflow_role,
            group_key="ordered-stages-group",
            target=target,
            source=source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
            queue_lookup=queue_lookup,
        )
        with pg_engine.connect() as connection:
            first_cut = connection.scalar(
                select(schema.operations.c.platform_cut_version)
            )
        stage_order.append("replay-boundary")
        dr_platform.submit(
            operation_key="ordered-stages-operation",
            workflow_role=target.workflow_role,
            group_key="ordered-stages-group",
            target=target,
            source=source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
            queue_lookup=queue_lookup,
        )
        with pg_engine.connect() as connection:
            replay_cut = connection.scalar(
                select(schema.operations.c.platform_cut_version)
            )

    assert stage_order == [
        "reconcile",
        "replay-boundary",
        "reconcile",
    ]
    assert replay_cut == first_cut


def test_exhausted_absence_terminalizes_without_replacement_or_cut_bump(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _upgrade_schema(pg_engine)
    options = SubmitOptions(
        claim_lease_seconds=1,
        retry_policy=RetryPolicy(max_enqueue_tries=1),
    )
    source_items = (
        ClaimTestItem(
            item_key="exhausted-absence",
            spec={},
            service_class=ServiceClass.STANDARD,
        ),
    )
    _register_items(
        pg_engine,
        schema=schema,
        items=source_items,
        options=options,
    )
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    with pg_engine.connect() as connection:
        claimed_at = connection.scalar(select(func.clock_timestamp()))
    assert claimed_at is not None
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at,
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_allow_targets,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        claim_id_factory=lambda: "exhausted-original-claim",
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )
    monkeypatch.setattr(
        coordination_module,
        "database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )
    observer = StaticWorkflowObserver(
        WorkflowObservation(
            workflow_id=claimed.workflow_id,
            disposition=WorkflowObservationDisposition.ABSENT,
        )
    )
    adapter = CallOutcomeAdapter(PhysicalEnqueueDisposition.ENQUEUED)
    queue_lookup = FakeQueueLookup(
        QueueConfiguration(
            database_backed_queue=True,
            priority_enabled=True,
        )
    )

    first = recover_call_started_page(
        pg_engine,
        resolver=registry,
        queue_lookup=queue_lookup,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        adapter=adapter,
        observer=observer,
    )

    assert len(first.items) == 1
    assert (
        first.items[0].execution.disposition
        is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    )
    assert adapter.calls == []
    with pg_engine.connect() as connection:
        claim_row = (
            connection.execute(select(schema.enqueue_claims)).mappings().one()
        )
        attempt_row = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        operation_row = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert claim_row["disposition"] == EnqueueClaimDisposition.EXPIRED.value
    assert (
        attempt_row["enqueue_state"] == AttemptEnqueueState.ENQUEUE_ERROR.value
    )
    assert attempt_row["current_claim_id"] is None
    assert operation_row["status"] == OperationStatus.FAILED.value
    assert operation_row["terminal_reason"] == "enqueue_exhausted"
    assert attempt_row["failure"]["error_type"] == "MaxEnqueueTriesExceeded"
    exhausted_cut = operation_row["platform_cut_version"]

    replay = recover_call_started_page(
        pg_engine,
        resolver=registry,
        queue_lookup=queue_lookup,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        adapter=adapter,
        observer=observer,
    )
    assert replay.items == ()
    with pg_engine.connect() as connection:
        replay_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
        claim_count = connection.scalar(
            select(func.count()).select_from(schema.enqueue_claims)
        )
    assert replay_cut == exhausted_cut
    assert claim_count == 1

    source = ClaimTestSource(items=source_items)
    result = dr_platform.submit(
        operation_key="claim-operation",
        workflow_role=target.workflow_role,
        group_key="claim-group",
        target=target,
        source=source,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
        options=options,
        queue_lookup=queue_lookup,
        enqueue_adapter=adapter,
        workflow_observer=observer,
    )
    assert result.status is OperationStatus.FAILED
    with pg_engine.connect() as connection:
        final_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert final_cut == exhausted_cut
    assert adapter.calls == []
