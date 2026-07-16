"""Focused physical DBOS enqueue and call-start uncertainty tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from sqlalchemy import Engine

import dr_platform.enqueue_runtime as runtime_module
from dr_platform.claims import ClaimedAttempt, ClaimPage
from dr_platform.enqueue_runtime import (
    DbosEnqueueAdapter,
    DbosWorkflowObserver,
    EnqueueClaimExecutionDisposition,
    EnqueuePreparationError,
    PhysicalEnqueueDisposition,
    PhysicalEnqueueOutcome,
    PreparedEnqueueCall,
    QueueConfigurationError,
    WorkflowObservation,
    WorkflowObservationDisposition,
    execute_enqueue_claim,
    prepare_enqueue_call,
    recover_call_started_page,
    validate_priority_queue,
)
from dr_platform.items import item_id
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.records import (
    AttemptRecord,
    EnqueueClaimRecord,
    FailureSnapshot,
    ItemRecord,
)
from dr_platform.status import (
    AttemptEnqueueState,
    EnqueueClaimDisposition,
    FailureClass,
    ServiceClass,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetRegistry,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _target(*, args_for: Any = None) -> ExecutionTarget:
    target: ExecutionTarget
    target = ExecutionTarget(
        target_key="generation",
        target_version=1,
        queue_name="generation-queue",
        workflow_role="generation",
        managed_workflow_name="generation_workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
        workflow=lambda *args: args,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"execution:{item.item_key}:{attempt}",
            workflow_id=f"workflow:{item.item_key}:{attempt}",
        ),
        args_for=args_for or (lambda item, attempt: (item.spec, attempt)),
        recipe_for=lambda item: ExecutionRecipeEnvelope(
            target_ref=target.ref,
            managed_workflow_name=target.managed_workflow_name,
            managed_workflow_version=target.managed_workflow_version,
            argument_recipe_version=target.argument_recipe_version,
            payload={"item_key": item.item_key},
        ),
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.TRANSIENT,
            error_type=type(error).__name__,
            message=str(error),
        ),
    )
    return target


def _item() -> ItemRecord:
    return ItemRecord(
        item_id=item_id(operation_key="operation", item_key="item-1"),
        operation_key="operation",
        item_key="item-1",
        item_index=0,
        shuffle_rank=1,
        service_class=ServiceClass.URGENT,
        service_priority=ServiceClass.URGENT.priority,
        spec={"payload": "safe"},
        current_attempt=0,
        created_at=NOW,
        updated_at=NOW,
        change_seq=1,
    )


def _attempt(
    item: ItemRecord | None = None,
    *,
    claim_id: str = "claim-1",
    enqueue_try: int = 1,
) -> AttemptRecord:
    selected = item or _item()
    return AttemptRecord(
        item_id=selected.item_id,
        attempt=0,
        workflow_role="generation",
        execution_key="execution:item-1:0",
        workflow_id="workflow:item-1:0",
        execution_recipe_digest="recipe-digest",
        enqueue_state=AttemptEnqueueState.CLAIMING,
        enqueue_try=enqueue_try,
        current_claim_id=claim_id,
        source_application_version="test-version",
        requested_service_class=ServiceClass.URGENT,
        requested_service_priority=ServiceClass.URGENT.priority,
        created_at=NOW,
        updated_at=NOW,
        change_seq=1,
    )


def _claim(
    item: ItemRecord | None = None,
    *,
    claim_id: str = "claim-1",
    enqueue_try: int = 1,
    disposition: EnqueueClaimDisposition = EnqueueClaimDisposition.CLAIMED,
) -> EnqueueClaimRecord:
    selected = item or _item()
    return EnqueueClaimRecord(
        item_id=selected.item_id,
        attempt=0,
        claim_id=claim_id,
        workflow_id="workflow:item-1:0",
        enqueue_try=enqueue_try,
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=1),
        enqueue_call_started_at=(
            NOW
            if disposition is EnqueueClaimDisposition.CALL_STARTED
            else None
        ),
        disposition=disposition,
        created_at=NOW,
        change_seq=1,
    )


def _call() -> PreparedEnqueueCall:
    item = _item()
    return prepare_enqueue_call(
        item=item,
        attempt=_attempt(item),
        target=_target(),
    )


@pytest.mark.parametrize(
    "queue",
    [
        None,
        SimpleNamespace(
            database_backed_queue=False,
            priority_enabled=True,
        ),
        SimpleNamespace(
            database_backed_queue=True,
            priority_enabled=False,
        ),
    ],
)
def test_queue_preflight_rejects_invalid_queue(
    queue: object | None,
) -> None:
    class Lookup:
        def retrieve_queue(self, name: str) -> object | None:
            del name
            return queue

    with pytest.raises(QueueConfigurationError):
        validate_priority_queue(queue_lookup=Lookup(), target=_target())


def test_queue_preflight_accepts_valid_database_queue() -> None:
    queue = SimpleNamespace(
        database_backed_queue=True,
        priority_enabled=True,
    )

    class Lookup:
        def retrieve_queue(self, name: str) -> object:
            del name
            return queue

    validate_priority_queue(queue_lookup=Lookup(), target=_target())


def test_prepare_call_accepts_application_owned_workflow_arguments() -> None:
    args = ({"api_key": "application-owned"},)

    call = prepare_enqueue_call(
        item=_item(),
        attempt=_attempt(),
        target=_target(args_for=lambda item, attempt: args),
    )

    assert call.args == args


def test_prepare_call_rejects_unserializable_workflow_arguments() -> None:
    with pytest.raises(
        EnqueuePreparationError,
        match="workflow arguments are not safely serializable",
    ):
        prepare_enqueue_call(
            item=_item(),
            attempt=_attempt(),
            target=_target(args_for=lambda item, attempt: (object(),)),
        )


class _Store:
    def __init__(
        self,
        *,
        start_wins: bool = True,
        outcome_wins: bool = True,
    ) -> None:
        self.start_wins = start_wins
        self.outcome_wins = outcome_wins
        self.events: list[str] = []
        self.compensations: list[
            tuple[EnqueueClaimRecord, PhysicalEnqueueOutcome]
        ] = []

    def mark_enqueue_call_started(self, *, claim: EnqueueClaimRecord) -> bool:
        self.events.append(f"start:{claim.claim_id}")
        return self.start_wins

    def record_enqueue_outcome(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> bool:
        del claim
        self.events.append(f"record:{outcome.disposition.value}")
        return self.outcome_wins

    def ensure_lost_outcome_compensation(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> None:
        self.compensations.append((claim, outcome))
        self.events.append(f"compensate:{outcome.disposition.value}")


class _Adapter:
    def __init__(
        self, outcome: PhysicalEnqueueOutcome, events: list[str]
    ) -> None:
        self.outcome = outcome
        self.events = events

    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome:
        self.events.append(f"dbos:{call.workflow_id}")
        return self.outcome


def _outcome(
    disposition: PhysicalEnqueueDisposition,
) -> PhysicalEnqueueOutcome:
    if disposition in {
        PhysicalEnqueueDisposition.ENQUEUED,
        PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
    }:
        return PhysicalEnqueueOutcome(
            workflow_id="workflow:item-1:0",
            disposition=disposition,
            effective_service_priority=100,
        )
    return PhysicalEnqueueOutcome(
        workflow_id="workflow:item-1:0",
        disposition=disposition,
        failure=FailureSnapshot(
            failure_class=FailureClass.TRANSIENT,
            error_type="RuntimeError",
            message="failed",
        ),
    )


def test_lost_call_start_cas_never_calls_dbos() -> None:
    store = _Store(start_wins=False)
    adapter_events: list[str] = []

    result = execute_enqueue_claim(
        claim=_claim(),
        call=_call(),
        store=store,
        adapter=_Adapter(
            _outcome(PhysicalEnqueueDisposition.ENQUEUED),
            adapter_events,
        ),
    )

    assert (
        result.disposition is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    )
    assert adapter_events == []


@pytest.mark.parametrize(
    "physical_disposition",
    [
        PhysicalEnqueueDisposition.ENQUEUED,
        PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
        PhysicalEnqueueDisposition.ENQUEUE_ERROR,
    ],
)
def test_durable_call_start_precedes_physical_call_and_outcome_record(
    physical_disposition: PhysicalEnqueueDisposition,
) -> None:
    store = _Store()
    adapter = _Adapter(_outcome(physical_disposition), store.events)

    result = execute_enqueue_claim(
        claim=_claim(),
        call=_call(),
        store=store,
        adapter=adapter,
    )

    assert result.disposition is (
        EnqueueClaimExecutionDisposition.OUTCOME_RECORDED
    )
    assert store.events.index("start:claim-1") < store.events.index(
        "dbos:workflow:item-1:0"
    )
    assert store.events.index("dbos:workflow:item-1:0") < store.events.index(
        f"record:{physical_disposition.value}"
    )


def test_uncertain_physical_result_leaves_call_started_unresolved() -> None:
    store = _Store()

    result = execute_enqueue_claim(
        claim=_claim(),
        call=_call(),
        store=store,
        adapter=_Adapter(
            _outcome(PhysicalEnqueueDisposition.UNCERTAIN),
            store.events,
        ),
    )

    assert result.disposition is EnqueueClaimExecutionDisposition.UNCERTAIN
    assert not any(event.startswith("record:") for event in store.events)


@pytest.mark.parametrize(
    "physical_disposition",
    [
        PhysicalEnqueueDisposition.ENQUEUED,
        PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
    ],
)
def test_success_after_lost_outcome_cas_creates_pending_compensation(
    physical_disposition: PhysicalEnqueueDisposition,
) -> None:
    store = _Store(outcome_wins=False)

    result = execute_enqueue_claim(
        claim=_claim(),
        call=_call(),
        store=store,
        adapter=_Adapter(_outcome(physical_disposition), store.events),
    )

    assert (
        result.disposition is EnqueueClaimExecutionDisposition.LOST_AUTHORITY
    )
    assert len(store.compensations) == 1
    compensated_claim, compensated_outcome = store.compensations[0]
    assert compensated_claim.claim_id == "claim-1"
    assert compensated_outcome.disposition is physical_disposition


def test_enqueue_error_after_lost_outcome_cas_never_creates_compensation() -> (
    None
):
    store = _Store(outcome_wins=False)

    execute_enqueue_claim(
        claim=_claim(),
        call=_call(),
        store=store,
        adapter=_Adapter(
            _outcome(PhysicalEnqueueDisposition.ENQUEUE_ERROR),
            store.events,
        ),
    )

    assert not any(event.startswith("compensate:") for event in store.events)


class _Observer:
    def __init__(
        self,
        observation: WorkflowObservation,
        events: list[str],
    ) -> None:
        self.observation = observation
        self.events = events

    def observe(self, call: PreparedEnqueueCall) -> WorkflowObservation:
        self.events.append(f"observe:{call.workflow_id}")
        return self.observation


class _RecoveryStore(_Store):
    def __init__(
        self,
        *,
        replacement: ClaimedAttempt | None = None,
    ) -> None:
        super().__init__()
        self.replacement = replacement

    def replace_call_started_after_absence(
        self,
        *,
        claimed: ClaimedAttempt,
    ) -> ClaimedAttempt | None:
        self.events.append(f"replace:{claimed.claim_id}")
        return self.replacement


def _call_started_receipt(target: ExecutionTarget) -> ClaimedAttempt:
    item = _item()
    return ClaimedAttempt(
        target_ref=target.ref,
        item=item,
        attempt_record=_attempt(item),
        claim=_claim(
            item,
            disposition=EnqueueClaimDisposition.CALL_STARTED,
        ),
    )


def _replacement_receipt(target: ExecutionTarget) -> ClaimedAttempt:
    item = _item()
    return ClaimedAttempt(
        target_ref=target.ref,
        item=item,
        attempt_record=_attempt(
            item,
            claim_id="claim-2",
            enqueue_try=2,
        ),
        claim=_claim(
            item,
            claim_id="claim-2",
            enqueue_try=2,
        ),
    )


def _valid_queue_lookup() -> object:
    class Lookup:
        def retrieve_queue(self, name: str) -> object:
            del name
            return SimpleNamespace(
                database_backed_queue=True,
                priority_enabled=True,
            )

    return Lookup()


def _patch_recovery_claims(
    monkeypatch: pytest.MonkeyPatch,
    *,
    receipt: ClaimedAttempt,
    store: _RecoveryStore,
) -> None:
    import dr_platform.claims as claims_module

    monkeypatch.setattr(
        claims_module,
        "load_call_started_recovery_page",
        lambda engine, **kwargs: ClaimPage(claims=(receipt,)),
    )
    monkeypatch.setattr(
        claims_module,
        "PostgresClaimTransitionStore",
        lambda engine, **kwargs: store,
    )


def test_call_started_recovery_observes_existing_before_recording(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    store = _RecoveryStore()
    receipt = _call_started_receipt(target)
    _patch_recovery_claims(monkeypatch, receipt=receipt, store=store)
    observer = _Observer(
        WorkflowObservation(
            workflow_id=receipt.workflow_id,
            disposition=WorkflowObservationDisposition.EXISTING,
            outcome=_outcome(
                PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
            ),
        ),
        store.events,
    )

    result = recover_call_started_page(
        cast("Engine", object()),
        resolver=registry,
        queue_lookup=cast("Any", _valid_queue_lookup()),
        observer=observer,
    )

    assert result.items[0].execution.disposition is (
        EnqueueClaimExecutionDisposition.OUTCOME_RECORDED
    )
    assert not any(event.startswith("replace:") for event in store.events)


def test_call_started_recovery_absence_replaces_before_same_id_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    receipt = _call_started_receipt(target)
    store = _RecoveryStore(replacement=_replacement_receipt(target))
    _patch_recovery_claims(monkeypatch, receipt=receipt, store=store)
    observer = _Observer(
        WorkflowObservation(
            workflow_id=receipt.workflow_id,
            disposition=WorkflowObservationDisposition.ABSENT,
        ),
        store.events,
    )
    adapter = _Adapter(
        _outcome(PhysicalEnqueueDisposition.ENQUEUED),
        store.events,
    )

    result = recover_call_started_page(
        cast("Engine", object()),
        resolver=registry,
        queue_lookup=cast("Any", _valid_queue_lookup()),
        observer=observer,
        adapter=adapter,
    )

    assert result.items[0].execution.disposition is (
        EnqueueClaimExecutionDisposition.OUTCOME_RECORDED
    )
    assert store.events.index("replace:claim-1") < store.events.index(
        "dbos:workflow:item-1:0"
    )


def test_call_started_recovery_uncertainty_never_replaces_or_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    receipt = _call_started_receipt(target)
    store = _RecoveryStore()
    _patch_recovery_claims(monkeypatch, receipt=receipt, store=store)
    observer = _Observer(
        WorkflowObservation(
            workflow_id=receipt.workflow_id,
            disposition=WorkflowObservationDisposition.UNCERTAIN,
            failure=FailureSnapshot(
                failure_class=FailureClass.UNKNOWN,
                error_type="LookupFailed",
                message="status unavailable",
            ),
        ),
        store.events,
    )

    result = recover_call_started_page(
        cast("Engine", object()),
        resolver=registry,
        queue_lookup=cast("Any", _valid_queue_lookup()),
        observer=observer,
    )

    assert result.items[0].execution.disposition is (
        EnqueueClaimExecutionDisposition.UNCERTAIN
    )
    assert not any(
        event.startswith(("replace:", "record:", "dbos:"))
        for event in store.events
    )


def test_dbos_adapter_links_existing_workflow_and_keeps_existing_priority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def conflict(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("workflow exists")

    monkeypatch.setattr(runtime_module.DBOS, "enqueue_workflow", conflict)
    monkeypatch.setattr(
        runtime_module.DBOS,
        "get_workflow_status",
        lambda workflow_id: SimpleNamespace(
            workflow_id=workflow_id,
            name="generation_workflow",
            queue_name="generation-queue",
            priority=1_000,
            parent_workflow_id=None,
        ),
    )

    outcome = DbosEnqueueAdapter().enqueue(_call())

    assert (
        outcome.disposition
        is PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
    )
    assert outcome.effective_service_priority == 1_000


def test_dbos_adapter_classifies_proven_absent_enqueue_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("database rejected enqueue")

    monkeypatch.setattr(runtime_module.DBOS, "enqueue_workflow", fail)
    monkeypatch.setattr(
        runtime_module.DBOS,
        "get_workflow_status",
        lambda workflow_id: None,
    )

    outcome = DbosEnqueueAdapter().enqueue(_call())

    assert outcome.disposition is PhysicalEnqueueDisposition.ENQUEUE_ERROR
    assert outcome.failure is not None
    assert outcome.failure.failure_class is FailureClass.TRANSIENT


def test_dbos_adapter_keeps_call_uncertain_when_status_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("transport failed")

    def status_fails(workflow_id: str) -> None:
        del workflow_id
        raise RuntimeError("status unavailable")

    monkeypatch.setattr(runtime_module.DBOS, "enqueue_workflow", fail)
    monkeypatch.setattr(
        runtime_module.DBOS, "get_workflow_status", status_fails
    )

    outcome = DbosEnqueueAdapter().enqueue(_call())

    assert outcome.disposition is PhysicalEnqueueDisposition.UNCERTAIN
    assert outcome.failure is not None


def test_dbos_adapter_refuses_to_create_a_child_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    physical_calls = 0

    def forbidden_call(*args: object, **kwargs: object) -> None:
        nonlocal physical_calls
        del args, kwargs
        physical_calls += 1

    monkeypatch.setattr(runtime_module.DBOS, "workflow_id", "parent-workflow")
    monkeypatch.setattr(
        runtime_module.DBOS,
        "enqueue_workflow",
        forbidden_call,
    )

    outcome = DbosEnqueueAdapter().enqueue(_call())

    assert outcome.disposition is PhysicalEnqueueDisposition.ENQUEUE_ERROR
    assert outcome.failure is not None
    assert outcome.failure.failure_class is FailureClass.PERMANENT
    assert physical_calls == 0


def test_dbos_workflow_observer_distinguishes_absence_from_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_module.DBOS,
        "get_workflow_status",
        lambda workflow_id: None,
    )

    absent = DbosWorkflowObserver().observe(_call())

    def lookup_fails(workflow_id: str) -> None:
        del workflow_id
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        runtime_module.DBOS,
        "get_workflow_status",
        lookup_fails,
    )
    uncertain = DbosWorkflowObserver().observe(_call())

    assert absent.disposition is WorkflowObservationDisposition.ABSENT
    assert uncertain.disposition is WorkflowObservationDisposition.UNCERTAIN
