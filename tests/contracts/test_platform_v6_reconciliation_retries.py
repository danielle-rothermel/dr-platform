"""Executable P4 reconciliation, retry, and next-Attempt contract."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, select, text, update

from dr_platform import claims as claims_module
from dr_platform import reconciliation as reconciliation_module
from dr_platform import reconciliation_runtime as runtime_module
from dr_platform import submission as submission_module
from dr_platform.claims import ClaimPageOptions, claim_pending_attempts
from dr_platform.db import PlatformSchema, upgrade_platform_schema
from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.enqueue_runtime import (
    EnqueuePageResult,
    QueueConfigurationError,
)
from dr_platform.manifests import ExecutionTargetRef
from dr_platform.reconciliation import (
    NextAttemptRequest,
    ReconciliationConflictError,
    ReconciliationPersistenceResult,
    apply_reconciliation_observations,
    load_missing_reobservation_page,
    load_reconciliation_page,
    request_next_attempt,
)
from dr_platform.reconciliation_runtime import (
    DbosLifecycleReader,
    ReconcileOptions,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
    reconcile,
)
from dr_platform.records import (
    EligibilityReference,
    FailureSnapshot,
    RetryPolicy,
)
from dr_platform.status import (
    AttemptExecutionState,
    FailureClass,
    NextAttemptDisposition,
    NextAttemptReason,
    OperationStatus,
    ServiceClass,
)
from dr_platform.submission import (
    SubmitOptions,
    SubmitResult,
    prepare_manifest,
    submit,
)
from dr_platform.targets import ExecutionTarget, TargetRegistry
from tests.test_claims import _Item, _register, _Source, _target
from tests.test_submission_pipeline import _manifest


class QueueConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    database_backed_queue: bool = True
    priority_enabled: bool = True


class FakeQueueLookup:
    def retrieve_queue(self, name: str) -> QueueConfiguration:
        del name
        return QueueConfiguration()


class MissingQueueLookup:
    def retrieve_queue(self, name: str) -> None:
        del name


def _registry(target: ExecutionTarget) -> TargetRegistry:
    registry = TargetRegistry()
    registry.register(target)
    return registry


def _register_with_policy(
    engine: Engine,
    *,
    policy: RetryPolicy,
) -> tuple[PlatformSchema, ExecutionTarget]:
    upgrade_platform_schema(str(engine.url))
    schema = PlatformSchema()
    target = _target()
    registry = _registry(target)
    source = _Source(
        items=(
            _Item(
                item_key="policy-item",
                spec={},
                service_class=ServiceClass.STANDARD,
            ),
        )
    )
    options = SubmitOptions(page_size=1, retry_policy=policy)
    manifest = prepare_manifest(
        operation_key="policy-operation",
        workflow_role=target.workflow_role,
        group_key="policy-group",
        target=target,
        source=source,
        options=options,
    )
    with pytest.raises(QueueConfigurationError):
        submit(
            manifest,
            source,
            engine=engine,
            resolver=registry,
            options=options,
            schema=schema,
            queue_lookup=MissingQueueLookup(),
            reconciliation_reader=cast("Any", SimpleNamespace()),
        )
    return schema, target


def _failure(failure_class: FailureClass) -> FailureSnapshot:
    return FailureSnapshot(
        failure_class=failure_class,
        error_type="ContractFailure",
        message="safe diagnostic",
    )


def _admit_targets(target_refs: tuple[ExecutionTargetRef, ...]) -> None:
    del target_refs


def _item_ids(engine: Engine, schema: PlatformSchema) -> tuple[str, ...]:
    with engine.connect() as connection:
        return tuple(
            connection.execute(
                select(schema.items.c.item_id).order_by(
                    schema.items.c.item_index
                )
            ).scalars()
        )


def _confirm_enqueued(
    engine: Engine,
    schema: PlatformSchema,
    *,
    item_ids: tuple[str, ...] | None = None,
) -> None:
    statement = update(schema.item_attempts)
    if item_ids is not None:
        statement = statement.where(
            schema.item_attempts.c.item_id.in_(item_ids)
        )
    with engine.begin() as connection:
        connection.execute(
            statement.values(
                enqueue_state="enqueued",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=ServiceClass.STANDARD.priority,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
        )


def _workflow_id(
    engine: Engine,
    schema: PlatformSchema,
    *,
    item_id: str | None = None,
) -> str:
    statement = select(schema.item_attempts.c.workflow_id)
    if item_id is not None:
        statement = statement.where(schema.item_attempts.c.item_id == item_id)
    with engine.connect() as connection:
        return connection.execute(statement).scalar_one()


def _observation(
    workflow_id: str,
    disposition: ReconciliationObservationDisposition,
    *,
    failure_class: FailureClass = FailureClass.TRANSIENT,
) -> ReconciliationObservation:
    status_by_disposition = {
        ReconciliationObservationDisposition.ACTIVE: (
            DbosWorkflowStatus.ENQUEUED
        ),
        ReconciliationObservationDisposition.SUCCEEDED: (
            DbosWorkflowStatus.SUCCESS
        ),
        ReconciliationObservationDisposition.ERROR: DbosWorkflowStatus.ERROR,
        ReconciliationObservationDisposition.CANCELLED: (
            DbosWorkflowStatus.CANCELLED
        ),
        ReconciliationObservationDisposition.RECOVERY_EXHAUSTED: (
            DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED
        ),
    }
    if disposition is ReconciliationObservationDisposition.ABSENT:
        return ReconciliationObservation(
            workflow_id=workflow_id,
            disposition=disposition,
        )
    if disposition is ReconciliationObservationDisposition.UNCERTAIN:
        return ReconciliationObservation(
            workflow_id=workflow_id,
            disposition=disposition,
            failure=_failure(FailureClass.UNKNOWN),
        )
    return ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=disposition,
        dbos_status=status_by_disposition[disposition],
        failure=(
            _failure(failure_class)
            if disposition is ReconciliationObservationDisposition.ERROR
            else None
        ),
    )


def test_actionable_loader_excludes_terminal_and_claim_owned_states(
    pg_engine: Engine,
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 9,
    )
    item_ids = _item_ids(pg_engine, schema)
    _confirm_enqueued(pg_engine, schema, item_ids=item_ids[:7])
    now = datetime.now(tz=UTC)
    with pg_engine.begin() as connection:
        terminal_states = (
            "succeeded",
            "cancelled",
            "error",
            "recovery_exhausted",
        )
        for item_id, state in zip(item_ids[:4], terminal_states, strict=True):
            values: dict[str, Any] = {
                "execution_state": state,
                "terminal_at": now,
                "updated_at": now,
            }
            if state == "error":
                values.update(
                    retry_disposition="permanent",
                    failure=_failure(FailureClass.PERMANENT).model_dump(
                        mode="json"
                    ),
                )
            connection.execute(
                update(schema.item_attempts)
                .where(schema.item_attempts.c.item_id == item_id)
                .values(**values)
            )
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == item_ids[4])
            .values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=now - timedelta(minutes=2),
                missing_last_observed_at=now,
                terminal_at=now,
                updated_at=now,
            )
        )
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == item_ids[5])
            .values(execution_state="active", updated_at=now)
        )
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == item_ids[6])
            .values(
                enqueue_state="enqueue_error",
                enqueued_at=None,
                effective_service_priority=None,
                priority_source=None,
                failure=_failure(FailureClass.TRANSIENT).model_dump(
                    mode="json"
                ),
                updated_at=now,
            )
        )
    claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        options=ClaimPageOptions(page_size=1),
        schema=schema,
        claim_id_factory=lambda: "claim-owned",
    )

    candidates = load_reconciliation_page(
        pg_engine,
        page_size=9,
        schema=schema,
    )
    missing_candidates = load_missing_reobservation_page(
        pg_engine,
        page_size=9,
        schema=schema,
    )

    candidate_by_id = {
        candidate.item.item_id: candidate for candidate in candidates
    }
    assert set(candidate_by_id) == {
        item_ids[5],
        item_ids[6],
    }
    assert [candidate.item.item_id for candidate in missing_candidates] == [
        item_ids[4]
    ]
    assert (
        candidate_by_id[item_ids[5]].attempt.execution_state
        is AttemptExecutionState.ACTIVE
    )
    assert (
        candidate_by_id[item_ids[6]].attempt.execution_state
        is AttemptExecutionState.NOT_STARTED
    )


def test_leading_missing_cannot_starve_later_actionable_candidate(
    pg_engine: Engine,
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 2,
    )
    with pg_engine.connect() as connection:
        ordered_ids = tuple(
            connection.execute(
                select(schema.items.c.item_id).order_by(
                    schema.items.c.service_priority,
                    schema.items.c.shuffle_rank,
                    schema.items.c.item_id,
                )
            ).scalars()
        )
    leading_missing, later_active = ordered_ids
    _confirm_enqueued(pg_engine, schema)
    now = datetime.now(tz=UTC)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == leading_missing)
            .values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=now - timedelta(minutes=2),
                missing_last_observed_at=now,
                terminal_at=now,
                updated_at=now,
            )
        )
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == later_active)
            .values(execution_state="active", updated_at=now)
        )

    actionable = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )
    missing = load_missing_reobservation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )

    assert [candidate.item.item_id for candidate in actionable] == [
        later_active
    ]
    assert [candidate.item.item_id for candidate in missing] == [
        leading_missing
    ]


def test_terminal_missing_reobservation_rotates_oldest_without_rewrite(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 2,
    )
    _confirm_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        base = connection.scalar(select(text("clock_timestamp()")))
    assert base is not None
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=base - timedelta(minutes=2),
                missing_last_observed_at=base,
                terminal_at=base,
                updated_at=base,
            )
        )
    before = {
        candidate.item.item_id: candidate.attempt
        for candidate in load_missing_reobservation_page(
            pg_engine,
            page_size=2,
            schema=schema,
        )
    }
    selected_ids: list[str] = []
    registry = _registry(target)
    for cycle in range(3):
        selected = load_missing_reobservation_page(
            pg_engine,
            page_size=1,
            schema=schema,
        )
        selected_ids.append(selected[0].item.item_id)
        observed_at = base + timedelta(seconds=cycle + 1)
        monkeypatch.setattr(
            reconciliation_module,
            "_database_now",
            lambda connection, now=observed_at: now,
        )
        apply_reconciliation_observations(
            pg_engine,
            observations={
                selected[0].attempt.workflow_id: _observation(
                    selected[0].attempt.workflow_id,
                    ReconciliationObservationDisposition.SUCCEEDED,
                )
            },
            resolver=registry,
            options=ReconcileOptions(page_size=1),
            schema=schema,
            candidates=selected,
        )

    assert selected_ids[0] != selected_ids[1]
    assert selected_ids[2] == selected_ids[0]
    after = {
        candidate.item.item_id: candidate.attempt
        for candidate in load_missing_reobservation_page(
            pg_engine,
            page_size=2,
            schema=schema,
        )
    }
    assert after == before
    with pg_engine.connect() as connection:
        markers = (
            connection.execute(
                select(schema.missing_reobservations).order_by(
                    schema.missing_reobservations.c.item_id
                )
            )
            .mappings()
            .all()
        )
    assert len(markers) == 2
    marker_by_item = {marker.item_id: marker for marker in markers}
    assert marker_by_item[selected_ids[0]].observation_count == 2
    assert marker_by_item[selected_ids[1]].observation_count == 1
    assert marker_by_item[selected_ids[1]].change_seq > 0
    assert (
        marker_by_item[selected_ids[0]].change_seq
        > marker_by_item[selected_ids[1]].change_seq
    )


def test_missing_requires_confirmed_enqueue_count_and_grace(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 2,
    )
    confirmed_id, unconfirmed_id = _item_ids(pg_engine, schema)
    _confirm_enqueued(pg_engine, schema, item_ids=(confirmed_id,))
    workflow_id = _workflow_id(pg_engine, schema, item_id=confirmed_id)
    with pg_engine.connect() as connection:
        first_now = connection.scalar(select(text("clock_timestamp()")))
    assert first_now is not None
    times = iter((first_now, first_now + timedelta(seconds=2)))
    monkeypatch.setattr(
        reconciliation_module,
        "_database_now",
        lambda connection: next(times),
    )
    options = ReconcileOptions(
        page_size=2,
        missing_grace_seconds=1,
        missing_required_observations=2,
    )
    absent = _observation(
        workflow_id, ReconciliationObservationDisposition.ABSENT
    )

    first = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: absent},
        resolver=_registry(target),
        options=options,
        schema=schema,
    )
    second = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: absent},
        resolver=_registry(target),
        options=options,
        schema=schema,
    )

    assert first.missing_count == 0
    assert second.missing_count == 1
    with pg_engine.connect() as connection:
        rows = {
            row.item_id: row
            for row in connection.execute(
                select(schema.item_attempts)
            ).mappings()
        }
    assert rows[confirmed_id].execution_state == "missing"
    assert rows[confirmed_id].missing_observation_count == 2
    assert rows[unconfirmed_id].execution_state == "not_started"
    assert rows[unconfirmed_id].missing_observation_count == 0


class AmbiguousDbosClient:
    def list_workflows(self, **kwargs: object) -> list[object]:
        del kwargs
        return [
            SimpleNamespace(
                workflow_id="workflow-item-0-0",
                status=DbosWorkflowStatus.SUCCESS.value,
                parent_workflow_id=None,
            ),
            SimpleNamespace(
                workflow_id="workflow-item-0-0",
                status=DbosWorkflowStatus.SUCCESS.value,
                parent_workflow_id=None,
            ),
        ]


def test_ambiguous_lookup_is_uncertain_and_does_not_mutate(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    item_id = _item_ids(pg_engine, schema)[0]
    workflow_id = _workflow_id(pg_engine, schema, item_id=item_id)
    before = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0].attempt
    observation = DbosLifecycleReader(
        cast("Any", AmbiguousDbosClient())
    ).observe(
        workflow_id=workflow_id,
        classify_error=lambda error: _failure(FailureClass.TRANSIENT),
    )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    assert (
        observation.disposition
        is ReconciliationObservationDisposition.UNCERTAIN
    )
    assert result.changed_count == 0
    after = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0].attempt
    assert after == before


@pytest.mark.parametrize(
    ("enqueue_try", "failure_class", "expected_state"),
    [
        (2, FailureClass.TRANSIENT, "pending"),
        (3, FailureClass.TRANSIENT, "enqueue_error"),
        (1, FailureClass.PERMANENT, "enqueue_error"),
    ],
)
def test_enqueue_retry_reuses_attempt_and_respects_bound(
    pg_engine: Engine,
    enqueue_try: int,
    failure_class: FailureClass,
    expected_state: str,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueue_error",
                enqueue_try=enqueue_try,
                failure=_failure(failure_class).model_dump(mode="json"),
                updated_at=text("clock_timestamp()"),
            )
        )
        connection.execute(
            update(schema.operations).values(enqueue_failed_count=1)
        )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={},
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        attempts = (
            connection.execute(select(schema.item_attempts)).mappings().all()
        )
    assert len(attempts) == 1
    assert attempts[0].attempt == 0
    assert attempts[0].enqueue_state == expected_state
    assert result.enqueue_reset_count == int(expected_state == "pending")
    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert operation.enqueue_failed_count == int(
        expected_state == "enqueue_error"
    )
    assert operation.enqueued_count == 0
    assert operation.workflow_already_present_count == 0


@pytest.mark.parametrize(
    (
        "policy",
        "failure_class",
        "expected_retry_disposition",
        "expected_attempts",
    ),
    [
        (RetryPolicy(max_attempts=3), FailureClass.TRANSIENT, "retryable", 2),
        (RetryPolicy(max_attempts=1), FailureClass.TRANSIENT, "exhausted", 1),
        (RetryPolicy(max_attempts=3), FailureClass.PERMANENT, "permanent", 1),
    ],
)
def test_execution_retry_allocates_one_attempt_and_respects_policy(
    pg_engine: Engine,
    policy: RetryPolicy,
    failure_class: FailureClass,
    expected_retry_disposition: str,
    expected_attempts: int,
) -> None:
    schema, target = _register_with_policy(
        pg_engine,
        policy=policy,
    )
    _confirm_enqueued(pg_engine, schema)
    with pg_engine.begin() as connection:
        connection.execute(update(schema.operations).values(enqueued_count=1))
    workflow_id = _workflow_id(pg_engine, schema)
    apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: _observation(
                workflow_id,
                ReconciliationObservationDisposition.ERROR,
                failure_class=failure_class,
            )
        },
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        attempts = (
            connection.execute(
                select(schema.item_attempts).order_by(
                    schema.item_attempts.c.attempt
                )
            )
            .mappings()
            .all()
        )
        current_attempt = connection.scalar(
            select(schema.items.c.current_attempt)
        )
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert len(attempts) == expected_attempts
    assert attempts[0].execution_state == "error"
    assert attempts[0].retry_disposition == expected_retry_disposition
    if expected_attempts == 2:
        assert attempts[1].attempt == 1
        assert attempts[1].source_attempt == 0
        assert attempts[1].enqueue_state == "pending"
        assert current_attempt == 1
        assert operation.enqueued_count == 0
        assert operation.workflow_already_present_count == 0
        assert operation.enqueue_failed_count == 0
    else:
        assert current_attempt == 0
        assert operation.enqueued_count == 1


def test_automatic_retry_locks_successor_workflow_before_domain_rows(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    source_workflow_id = _workflow_id(pg_engine, schema)
    events: list[tuple[str, object]] = []
    original_workflows = (
        reconciliation_module._acquire_workflow_reference_locks
    )
    original_operation = reconciliation_module._lock_operation_for_item
    original_item = reconciliation_module._lock_item
    original_attempt = reconciliation_module._lock_attempt

    def lock_workflows(connection: Any, workflow_ids: Any) -> None:
        identities = tuple(workflow_ids)
        events.append(("workflow", identities))
        original_workflows(connection, identities)

    def lock_operation(connection: Any, **kwargs: Any) -> Any:
        events.append(("operation", kwargs["operation_key"]))
        return original_operation(connection, **kwargs)

    def lock_item(connection: Any, **kwargs: Any) -> Any:
        events.append(("item", kwargs["item_id"]))
        return original_item(connection, **kwargs)

    def lock_attempt(connection: Any, **kwargs: Any) -> Any:
        events.append(("attempt", kwargs["attempt"]))
        return original_attempt(connection, **kwargs)

    monkeypatch.setattr(
        reconciliation_module,
        "_acquire_workflow_reference_locks",
        lock_workflows,
    )
    monkeypatch.setattr(
        reconciliation_module,
        "_lock_operation_for_item",
        lock_operation,
    )
    monkeypatch.setattr(reconciliation_module, "_lock_item", lock_item)
    monkeypatch.setattr(reconciliation_module, "_lock_attempt", lock_attempt)

    apply_reconciliation_observations(
        pg_engine,
        observations={
            source_workflow_id: _observation(
                source_workflow_id,
                ReconciliationObservationDisposition.ERROR,
            )
        },
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    workflow_event = next(
        (index, value)
        for index, (kind, value) in enumerate(events)
        if kind == "workflow"
    )
    first_domain_index = next(
        index
        for index, (kind, _value) in enumerate(events)
        if kind in {"operation", "item", "attempt"}
    )
    successor_workflow_id = "workflow:item-0:1"
    workflow_ids = workflow_event[1]
    assert isinstance(workflow_ids, tuple)
    assert successor_workflow_id in workflow_ids
    assert workflow_event[0] < first_domain_index


def test_workflow_reference_lock_helper_sorts_and_deduplicates() -> None:
    observed: list[str] = []

    class RecordingConnection:
        def execute(
            self,
            statement: object,
            parameters: dict[str, str],
        ) -> None:
            del statement
            observed.append(parameters["id"])

    claims_module._acquire_workflow_reference_locks(
        cast("Any", RecordingConnection()),
        ("workflow-z", "workflow-a", "workflow-z", "workflow-m"),
    )

    assert observed == ["workflow-a", "workflow-m", "workflow-z"]


def test_terminal_success_replay_is_a_noop(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    workflow_id = _workflow_id(pg_engine, schema)
    registry = _registry(target)
    options = ReconcileOptions(page_size=1)
    for disposition in (
        ReconciliationObservationDisposition.ACTIVE,
        ReconciliationObservationDisposition.SUCCEEDED,
    ):
        apply_reconciliation_observations(
            pg_engine,
            observations={workflow_id: _observation(workflow_id, disposition)},
            resolver=registry,
            options=options,
            schema=schema,
        )
    with pg_engine.connect() as connection:
        succeeded = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )

    replay = apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: _observation(
                workflow_id,
                ReconciliationObservationDisposition.SUCCEEDED,
            )
        },
        resolver=registry,
        options=options,
        schema=schema,
    )

    with pg_engine.connect() as connection:
        after = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        after_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert replay.changed_count == 0
    assert after == succeeded
    assert after_cut == cut


def test_identical_active_is_noop_but_active_resets_missing_streak(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    workflow_id = _workflow_id(pg_engine, schema)
    registry = _registry(target)
    options = ReconcileOptions(
        page_size=1,
        missing_grace_seconds=60,
        missing_required_observations=3,
    )
    active = _observation(
        workflow_id,
        ReconciliationObservationDisposition.ACTIVE,
    )
    apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: active},
        resolver=registry,
        options=options,
        schema=schema,
    )
    with pg_engine.connect() as connection:
        stable_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        stable_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert isinstance(stable_cut, int)

    replay = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: active},
        resolver=registry,
        options=options,
        schema=schema,
    )

    with pg_engine.connect() as connection:
        replayed_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        replayed_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
        absent_at = connection.scalar(select(text("clock_timestamp()")))
    assert absent_at is not None
    monkeypatch.setattr(
        reconciliation_module,
        "_database_now",
        lambda connection: absent_at,
    )
    apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: _observation(
                workflow_id,
                ReconciliationObservationDisposition.ABSENT,
            )
        },
        resolver=registry,
        options=options,
        schema=schema,
    )
    reset_at = absent_at + timedelta(seconds=1)
    monkeypatch.setattr(
        reconciliation_module,
        "_database_now",
        lambda connection: reset_at,
    )
    reset = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: active},
        resolver=registry,
        options=options,
        schema=schema,
    )

    with pg_engine.connect() as connection:
        reset_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        reset_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert isinstance(reset_cut, int)
    assert replay.changed_count == 0
    assert replayed_attempt.updated_at == stable_attempt.updated_at
    assert replayed_cut == stable_cut
    assert reset.changed_count == 1
    assert reset_attempt.missing_observation_count == 0
    assert reset_attempt.missing_first_observed_at is None
    assert reset_attempt.missing_last_observed_at is None
    assert reset_attempt.updated_at == reset_at
    assert reset_cut == stable_cut + 2


def test_active_observation_cannot_reverse_cancel_requested(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    workflow_id = _workflow_id(pg_engine, schema)
    now = datetime.now(tz=UTC)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="cancel_requested",
                cancellation_request_id="cancel-request",
                cancellation_requested_at=now,
                cancellation_requested_by="operator",
                cancellation_origin="local_operation",
                updated_at=now,
            )
        )
        connection.execute(
            update(schema.operations).values(
                status="cancelling",
                cancel_requested_at=now,
                updated_at=now,
            )
        )
    with pg_engine.connect() as connection:
        before_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        before_operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: _observation(
                workflow_id,
                ReconciliationObservationDisposition.ACTIVE,
            )
        },
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        after_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        after_operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert result.changed_count == 0
    assert after_attempt == before_attempt
    assert after_operation == before_operation
    assert after_attempt.execution_state == "cancel_requested"
    assert after_operation.status == "cancelling"


def test_late_source_completion_cannot_rewrite_successor(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    source_workflow_id = _workflow_id(pg_engine, schema)
    registry = _registry(target)
    options = ReconcileOptions(page_size=1)
    apply_reconciliation_observations(
        pg_engine,
        observations={
            source_workflow_id: _observation(
                source_workflow_id,
                ReconciliationObservationDisposition.ERROR,
            )
        },
        resolver=registry,
        options=options,
        schema=schema,
    )
    with pg_engine.connect() as connection:
        before_attempts = tuple(
            connection.execute(
                select(schema.item_attempts).order_by(
                    schema.item_attempts.c.attempt
                )
            ).mappings()
        )
        before_current = connection.scalar(
            select(schema.items.c.current_attempt)
        )

    late = apply_reconciliation_observations(
        pg_engine,
        observations={
            source_workflow_id: _observation(
                source_workflow_id,
                ReconciliationObservationDisposition.SUCCEEDED,
            )
        },
        resolver=registry,
        options=options,
        schema=schema,
    )

    with pg_engine.connect() as connection:
        after_attempts = tuple(
            connection.execute(
                select(schema.item_attempts).order_by(
                    schema.item_attempts.c.attempt
                )
            ).mappings()
        )
        after_current = connection.scalar(
            select(schema.items.c.current_attempt)
        )
    assert before_current == after_current == 1
    assert after_attempts == before_attempts
    assert late.changed_count == 0


def test_recovery_exhaustion_is_terminal_and_never_retried(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    workflow_id = _workflow_id(pg_engine, schema)
    registry = _registry(target)
    exhausted = _observation(
        workflow_id,
        ReconciliationObservationDisposition.RECOVERY_EXHAUSTED,
    )

    first = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: exhausted},
        resolver=registry,
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )
    replay = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: exhausted},
        resolver=registry,
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        attempts = (
            connection.execute(select(schema.item_attempts)).mappings().all()
        )
    assert first.changed_count == 1
    assert replay.changed_count == 0
    assert len(attempts) == 1
    assert attempts[0].execution_state == "recovery_exhausted"


def _make_request(
    item_id: str,
    *,
    request_key: str,
    max_attempts: int | None = None,
) -> NextAttemptRequest:
    return NextAttemptRequest(
        item_id=item_id,
        source_attempt=0,
        request_key=request_key,
        reason=NextAttemptReason.DOMAIN_OUTCOME,
        eligibility=EligibilityReference(
            kind="generation_run",
            record_id="run-1",
            digest="run-digest",
        ),
        requested_by="contract-test",
        max_attempts=max_attempts,
    )


def _mark_source_succeeded(
    engine: Engine,
    schema: PlatformSchema,
) -> str:
    item_id = _item_ids(engine, schema)[0]
    with engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=ServiceClass.STANDARD.priority,
                priority_source="enqueued_here",
                execution_state="succeeded",
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
        )
        connection.execute(update(schema.operations).values(enqueued_count=1))
    return item_id


def test_next_attempt_request_replay_is_exact_and_conflicts_on_redefinition(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    item_id = _mark_source_succeeded(pg_engine, schema)
    registry = _registry(target)
    request = _make_request(item_id, request_key="stable")

    first = request_next_attempt(
        request, engine=pg_engine, resolver=registry, schema=schema
    )
    replay = request_next_attempt(
        request, engine=pg_engine, resolver=registry, schema=schema
    )
    unequal = request.model_copy(update={"requested_by": "different-caller"})
    with pytest.raises(ReconciliationConflictError):
        request_next_attempt(
            unequal, engine=pg_engine, resolver=registry, schema=schema
        )

    assert first == replay
    assert first.disposition is NextAttemptDisposition.CREATED
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(text("count(*)")).select_from(
                    schema.next_attempt_requests
                )
            )
            == 1
        )
        assert (
            connection.scalar(
                select(text("count(*)")).select_from(schema.item_attempts)
            )
            == 2
        )
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert operation.enqueued_count == 0
    assert operation.workflow_already_present_count == 0
    assert operation.enqueue_failed_count == 0


@pytest.mark.parametrize("key_mode", ["same", "different"])
def test_concurrent_next_attempt_requests_converge_or_source_advance(
    pg_engine: Engine,
    key_mode: str,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    item_id = _mark_source_succeeded(pg_engine, schema)
    registry = _registry(target)
    requests = (
        _make_request(item_id, request_key="request-a"),
        _make_request(
            item_id,
            request_key=("request-a" if key_mode == "same" else "request-b"),
        ),
    )

    def invoke(request: NextAttemptRequest) -> NextAttemptDisposition:
        return request_next_attempt(
            request,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        ).disposition

    with ThreadPoolExecutor(max_workers=2) as executor:
        dispositions = tuple(executor.map(invoke, requests))

    if key_mode == "same":
        assert dispositions == (
            NextAttemptDisposition.CREATED,
            NextAttemptDisposition.CREATED,
        )
    else:
        assert set(dispositions) == {
            NextAttemptDisposition.CREATED,
            NextAttemptDisposition.SOURCE_ADVANCED,
        }
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(text("count(*)")).select_from(schema.item_attempts)
            )
            == 2
        )


@pytest.mark.parametrize(
    ("source_state", "max_attempts", "expected"),
    [
        ("active", None, NextAttemptDisposition.INELIGIBLE),
        ("missing", None, NextAttemptDisposition.INELIGIBLE),
        ("recovery_exhausted", None, NextAttemptDisposition.INELIGIBLE),
        ("succeeded", 1, NextAttemptDisposition.MAX_ATTEMPTS_EXHAUSTED),
    ],
)
def test_next_attempt_eligibility_and_tightening_bound_fail_closed(
    pg_engine: Engine,
    source_state: str,
    max_attempts: int | None,
    expected: NextAttemptDisposition,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    item_id = _item_ids(pg_engine, schema)[0]
    now = datetime.now(tz=UTC)
    values: dict[str, Any] = {
        "execution_state": source_state,
        "updated_at": now,
    }
    if source_state in {"missing", "recovery_exhausted", "succeeded"}:
        values["terminal_at"] = now
    if source_state == "missing":
        values.update(
            missing_observation_count=3,
            missing_first_observed_at=now - timedelta(minutes=2),
            missing_last_observed_at=now,
        )
    with pg_engine.begin() as connection:
        connection.execute(update(schema.item_attempts).values(**values))
    request = _make_request(
        item_id,
        request_key=f"eligibility-{source_state}",
        max_attempts=max_attempts,
    )

    result = request_next_attempt(
        request,
        engine=pg_engine,
        resolver=_registry(target),
        schema=schema,
    )

    assert result.disposition is expected
    assert result.created_attempt is None
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(text("count(*)")).select_from(schema.item_attempts)
            )
            == 1
        )


def test_terminal_missing_is_periodically_reobserved_without_rewrite(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    workflow_id = _workflow_id(pg_engine, schema)
    now = datetime.now(tz=UTC)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=now - timedelta(minutes=2),
                missing_last_observed_at=now,
                terminal_at=now,
                updated_at=now,
            )
        )
    before = load_missing_reobservation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0].attempt
    late_commit = _observation(
        workflow_id,
        ReconciliationObservationDisposition.SUCCEEDED,
    )

    commit_release = Event()

    def observe_after_delayed_commit() -> ReconciliationObservation:
        assert commit_release.wait(timeout=1)
        return late_commit

    with ThreadPoolExecutor(max_workers=1) as executor:
        delayed = executor.submit(observe_after_delayed_commit)
        assert not delayed.done()
        commit_release.set()
        observed_late_commit = delayed.result()

    results = tuple(
        apply_reconciliation_observations(
            pg_engine,
            observations={workflow_id: observed_late_commit},
            resolver=_registry(target),
            options=ReconcileOptions(page_size=1),
            schema=schema,
        )
        for _ in range(2)
    )

    after = load_missing_reobservation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0].attempt
    assert [result.observed_count for result in results] == [1, 1]
    assert [result.changed_count for result in results] == [0, 0]
    assert after == before


def test_public_reconcile_uses_one_shared_budget_and_enqueues_new_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, int]] = []

    def recover(*args: object, **kwargs: object) -> Any:
        del args
        size = cast("Any", kwargs["options"]).page_size
        events.append(("recover", size))
        return SimpleNamespace(items=(object(),))

    def load(*args: object, **kwargs: object) -> tuple[object, ...]:
        del args
        events.append(("actionable", cast("int", kwargs["page_size"])))
        return (object(), object())

    def load_missing(*args: object, **kwargs: object) -> tuple[object, ...]:
        del args
        events.append(("missing", cast("int", kwargs["page_size"])))
        return ()

    def observe(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        return {}

    def apply(
        *args: object,
        **kwargs: object,
    ) -> ReconciliationPersistenceResult:
        del args, kwargs
        return ReconciliationPersistenceResult(
            observed_count=2,
            changed_count=1,
            enqueue_reset_count=1,
            execution_retry_count=0,
            missing_count=0,
        )

    def replacements(*args: object, **kwargs: object) -> Any:
        del args
        size = cast("Any", kwargs["options"]).page_size
        events.append(("replacement", size))
        return SimpleNamespace(items=(object(),))

    def pending(*args: object, **kwargs: object) -> EnqueuePageResult:
        del args
        size = cast("Any", kwargs["options"]).page_size
        events.append(("pending", size))
        return EnqueuePageResult(items=())

    import dr_platform.enqueue_runtime as enqueue_module

    monkeypatch.setattr(enqueue_module, "recover_call_started_page", recover)
    monkeypatch.setattr(
        enqueue_module,
        "enqueue_replacement_page",
        replacements,
    )
    monkeypatch.setattr(enqueue_module, "enqueue_pending_page", pending)
    monkeypatch.setattr(
        reconciliation_module,
        "load_reconciliation_page",
        load,
    )
    monkeypatch.setattr(
        reconciliation_module,
        "load_missing_reobservation_page",
        load_missing,
    )
    monkeypatch.setattr(
        reconciliation_module, "apply_reconciliation_observations", apply
    )
    monkeypatch.setattr(runtime_module, "_observe_candidates", observe)

    result = reconcile(
        cast("Engine", object()),
        resolver=cast("Any", object()),
        queue_lookup=FakeQueueLookup(),
        options=ReconcileOptions(page_size=5, claim_lease_seconds=17),
        reader=cast("Any", object()),
        recovery_observer=cast("Any", object()),
        enqueue_adapter=cast("Any", object()),
    )

    assert events == [
        ("recover", 5),
        ("actionable", 4),
        ("missing", 2),
        ("replacement", 2),
        ("pending", 1),
    ]
    assert result.enqueue_reset_count == 1
    assert result.replacement_enqueue_count == 1


def test_exact_resubmit_reconciles_before_claim_repair_and_pending_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_ref = ExecutionTargetRef(
        target_key="generation",
        target_version=1,
        target_contract_digest="target-digest",
    )
    manifest = _manifest(target_ref)
    target = SimpleNamespace(ref=target_ref, workflow_role="generation")
    events: list[str] = []
    expected = SubmitResult(
        operation_key="operation",
        status=OperationStatus.RUNNING,
        requested_count=1,
        registration_cursor=1,
        inserted_count=1,
        already_present_count=0,
        enqueued_count=1,
        workflow_already_present_count=0,
        enqueue_failed_count=0,
        total_failure_count=0,
    )

    class Resolver:
        def resolve(self, ref: object) -> object:
            assert ref == target_ref
            return target

    def fake_reconcile(*args: object, **kwargs: object) -> Any:
        del args, kwargs
        events.extend(["reconcile", "claim-repair", "pending-enqueue"])
        return SimpleNamespace()

    monkeypatch.setattr(
        submission_module,
        "_validate_source",
        lambda **kw: None,
    )
    monkeypatch.setattr(
        submission_module, "_create_or_claim_operation", lambda **kw: 1
    )
    monkeypatch.setattr(
        submission_module,
        "_load_submit_result",
        lambda **kw: expected,
    )
    monkeypatch.setattr(runtime_module, "reconcile", fake_reconcile)

    result = submit(
        manifest,
        cast("Any", object()),
        engine=cast("Engine", object()),
        resolver=cast("Any", Resolver()),
        options=SubmitOptions(page_size=1),
        queue_lookup=FakeQueueLookup(),
        reconciliation_reader=cast("Any", object()),
    )

    assert result is expected
    assert events == ["reconcile", "claim-repair", "pending-enqueue"]
