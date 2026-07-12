"""Focused durable reconciliation and next-Attempt persistence tests."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import Engine, event, func, select, text, update

from dr_platform import claims as claims_module
from dr_platform import reconciliation as reconciliation_module
from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.reconciliation import (
    NextAttemptRequest,
    ReconciliationPersistenceResult,
    apply_reconciliation_observations,
    load_missing_reobservation_page,
    load_reconciliation_page,
    request_next_attempt,
)
from dr_platform.reconciliation_runtime import (
    ReconcileOptions,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
)
from dr_platform.records import EligibilityReference, FailureSnapshot
from dr_platform.status import (
    FailureClass,
    NextAttemptDisposition,
    NextAttemptReason,
    ServiceClass,
)
from dr_platform.targets import ExecutionTarget, TargetRegistry
from tests.test_claims import _register

if TYPE_CHECKING:
    from dr_platform.db import PlatformSchema


def _registry(target: ExecutionTarget) -> TargetRegistry:
    registry = TargetRegistry()
    registry.register(target)
    return registry


def test_workflow_reference_lock_helper_sorts_and_deduplicates() -> None:
    observed: list[str] = []

    class Connection:
        def execute(self, statement: object, parameters: object) -> None:
            del statement
            assert isinstance(parameters, Mapping)
            workflow_id = cast("Mapping[str, str]", parameters)["id"]
            observed.append(workflow_id)

    claims_module._acquire_workflow_reference_locks(
        cast("Any", Connection()),
        ["z-source", "a-successor", "z-source"],
    )

    assert observed == ["a-successor", "z-source"]


def _confirm_enqueued(engine: Engine, schema: PlatformSchema) -> str:
    with engine.begin() as connection:
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
        )
    return workflow_id


def test_missing_requires_repeated_observations_and_grace(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        first_now = connection.execute(
            select(text("clock_timestamp()"))
        ).scalar_one()
    times = iter((first_now, first_now + timedelta(seconds=2)))
    monkeypatch.setattr(
        reconciliation_module,
        "_database_now",
        lambda connection: next(times),
    )
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.ABSENT,
    )
    options = ReconcileOptions(
        page_size=1,
        missing_grace_seconds=1,
        missing_required_observations=2,
    )

    first = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=options,
        schema=schema,
    )
    second = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=options,
        schema=schema,
    )

    assert first.missing_count == 0
    assert second.missing_count == 1
    with pg_engine.connect() as connection:
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    assert attempt["execution_state"] == "missing"
    assert attempt["missing_observation_count"] == 2


def test_retryable_execution_error_allocates_one_new_attempt(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.ERROR,
        dbos_status=DbosWorkflowStatus.ERROR,
        failure=FailureSnapshot(
            failure_class=FailureClass.TRANSIENT,
            error_type="TemporaryError",
            message="retry",
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
        result = apply_reconciliation_observations(
            pg_engine,
            observations={workflow_id: observation},
            resolver=_registry(target),
            options=ReconcileOptions(page_size=1),
            schema=schema,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", record_statements)

    assert result.execution_retry_count == 1
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
        item = connection.execute(select(schema.items)).mappings().one()
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert [row["attempt"] for row in attempts] == [0, 1]
    assert attempts[0]["execution_state"] == "error"
    assert attempts[1]["retry_reason"] == "automatic_execution_error"
    assert item["current_attempt"] == 1
    assert operation["enqueued_count"] == 0
    assert operation["workflow_already_present_count"] == 0
    assert operation["enqueue_failed_count"] == 0
    assert operation["status"] == "enqueuing"
    workflow_locks = [
        cast("Mapping[str, object]", parameters).get("id")
        for statement, parameters in observed_statements
        if "hashtextextended" in statement and isinstance(parameters, Mapping)
    ]
    assert "workflow:item-0:1" in workflow_locks
    operation_lock_index = next(
        index
        for index, (statement, _) in enumerate(observed_statements)
        if "FROM platform_operations" in statement
        and "FOR UPDATE" in statement
    )
    successor_lock_index = next(
        index
        for index, (statement, parameters) in enumerate(observed_statements)
        if "hashtextextended" in statement
        and isinstance(parameters, Mapping)
        and parameters.get("id") == "workflow:item-0:1"
    )
    assert successor_lock_index < operation_lock_index


def test_concurrent_automatic_retry_creates_one_successor_attempt(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    candidate = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0]
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.ERROR,
        dbos_status=DbosWorkflowStatus.ERROR,
        failure=FailureSnapshot(
            failure_class=FailureClass.TRANSIENT,
            error_type="TemporaryError",
            message="retry",
        ),
    )
    barrier = Barrier(2)

    def reconcile_same_candidate() -> int:
        barrier.wait()
        result = apply_reconciliation_observations(
            pg_engine,
            observations={workflow_id: observation},
            resolver=_registry(target),
            options=ReconcileOptions(page_size=1),
            schema=schema,
            candidates=(candidate,),
        )
        return result.execution_retry_count

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(reconcile_same_candidate) for _ in range(2)
        )
        counts = tuple(future.result() for future in futures)

    with pg_engine.connect() as connection:
        attempts = tuple(
            connection.scalars(
                select(schema.item_attempts.c.attempt).order_by(
                    schema.item_attempts.c.attempt
                )
            )
        )
    assert sorted(counts) == [0, 1]
    assert attempts == (0, 1)


def test_repeated_identical_active_observation_is_exact_noop(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.ACTIVE,
        dbos_status=DbosWorkflowStatus.PENDING,
    )

    def apply_active() -> ReconciliationPersistenceResult:
        return apply_reconciliation_observations(
            pg_engine,
            observations={workflow_id: observation},
            resolver=_registry(target),
            options=ReconcileOptions(page_size=1),
            schema=schema,
        )

    first = apply_active()
    with pg_engine.connect() as connection:
        first_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        first_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    replay = apply_active()
    with pg_engine.connect() as connection:
        replay_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        replay_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )

    assert first.changed_count == 1
    assert replay.changed_count == 0
    assert replay_attempt["updated_at"] == first_attempt["updated_at"]
    assert replay_cut == first_cut


def test_active_presence_clears_prior_missing_streak_as_real_mutation(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="active",
                dbos_status="PENDING",
                missing_observation_count=1,
                missing_first_observed_at=text(
                    "clock_timestamp() - interval '1 minute'"
                ),
                missing_last_observed_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
        )
        prior_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.ACTIVE,
        dbos_status=DbosWorkflowStatus.PENDING,
    )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        current_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert result.changed_count == 1
    assert attempt["missing_observation_count"] == 0
    assert attempt["missing_first_observed_at"] is None
    assert attempt["missing_last_observed_at"] is None
    assert prior_cut is not None
    assert current_cut == prior_cut + 1


@pytest.mark.parametrize(
    "observation",
    [
        pytest.param(
            ReconciliationObservation(
                workflow_id="workflow:item-0:0",
                disposition=ReconciliationObservationDisposition.ACTIVE,
                dbos_status=DbosWorkflowStatus.PENDING,
            ),
            id="live",
        ),
        pytest.param(
            ReconciliationObservation(
                workflow_id="workflow:item-0:0",
                disposition=ReconciliationObservationDisposition.SUCCEEDED,
                dbos_status=DbosWorkflowStatus.SUCCESS,
            ),
            id="success-without-pre-cancel-proof",
        ),
        pytest.param(
            ReconciliationObservation(
                workflow_id="workflow:item-0:0",
                disposition=ReconciliationObservationDisposition.ERROR,
                dbos_status=DbosWorkflowStatus.ERROR,
                failure=FailureSnapshot(
                    failure_class=FailureClass.TRANSIENT,
                    error_type="TemporaryError",
                    message="late",
                ),
            ),
            id="error-without-pre-cancel-proof",
        ),
    ],
)
def test_non_cancel_observation_cannot_revert_cancel_requested(
    pg_engine: Engine,
    observation: ReconciliationObservation,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    assert workflow_id == observation.workflow_id
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="cancel_requested",
                cancellation_request_id="cancel-request",
                cancellation_requested_at=text("clock_timestamp()"),
                cancellation_requested_by="operator",
                cancellation_origin="local_operation",
                updated_at=text("clock_timestamp()"),
            )
        )
        connection.execute(
            update(schema.operations).values(
                cancel_requested_at=text("clock_timestamp()")
            )
        )
    with pg_engine.connect() as connection:
        before_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        before_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )

    with pg_engine.connect() as connection:
        after_attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        after_cut = connection.scalar(
            select(schema.operations.c.platform_cut_version)
        )
    assert result.changed_count == 0
    assert after_attempt["execution_state"] == "cancel_requested"
    assert after_attempt["updated_at"] == before_attempt["updated_at"]
    assert after_cut == before_cut


def test_domain_outcome_request_is_idempotent(pg_engine: Engine) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.begin() as connection:
        item_id = connection.execute(
            select(schema.items.c.item_id)
        ).scalar_one()
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="succeeded",
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
        )
    request = NextAttemptRequest(
        item_id=item_id,
        source_attempt=0,
        request_key="domain-repeat",
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
        resolver=_registry(target),
        schema=schema,
    )
    replay = request_next_attempt(
        request,
        engine=pg_engine,
        resolver=_registry(target),
        schema=schema,
    )

    assert first == replay
    assert first.disposition is NextAttemptDisposition.CREATED
    with pg_engine.connect() as connection:
        request_count = connection.execute(
            select(text("count(*)")).select_from(schema.next_attempt_requests)
        ).scalar_one()
        attempt_count = connection.execute(
            select(text("count(*)")).select_from(schema.item_attempts)
        ).scalar_one()
    assert request_count == 1
    assert attempt_count == 2


def test_terminal_missing_candidate_is_loaded_but_not_rewritten(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    workflow_id = _confirm_enqueued(pg_engine, schema)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=text(
                    "clock_timestamp() - interval '10 minutes'"
                ),
                missing_last_observed_at=text("clock_timestamp()"),
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
        )
    candidates = load_missing_reobservation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )
    observation = ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.SUCCEEDED,
        dbos_status=DbosWorkflowStatus.SUCCESS,
    )

    result = apply_reconciliation_observations(
        pg_engine,
        observations={workflow_id: observation},
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )
    with pg_engine.connect() as connection:
        marker_before_uncertain = dict(
            connection.execute(select(schema.missing_reobservations))
            .mappings()
            .one()
        )
    uncertain_result = apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.UNCERTAIN,
                failure=FailureSnapshot(
                    failure_class=FailureClass.UNKNOWN,
                    error_type="LookupAmbiguous",
                    message="safe diagnostic",
                ),
            )
        },
        resolver=_registry(target),
        options=ReconcileOptions(page_size=1),
        schema=schema,
    )
    with pg_engine.connect() as connection:
        marker_after_uncertain = dict(
            connection.execute(select(schema.missing_reobservations))
            .mappings()
            .one()
        )

    assert candidates[0].attempt.execution_state.value == "missing"
    assert result.changed_count == 0
    assert uncertain_result.changed_count == 0
    assert (
        marker_after_uncertain["observation_count"]
        == marker_before_uncertain["observation_count"] + 1
    )
    assert (
        marker_after_uncertain["last_reobserved_at"]
        >= marker_before_uncertain["last_reobserved_at"]
    )


def test_terminal_missing_cannot_starve_later_actionable_attempt(
    pg_engine: Engine,
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD, ServiceClass.STANDARD),
    )
    with pg_engine.begin() as connection:
        ordered_item_ids = tuple(
            connection.scalars(
                select(schema.items.c.item_id).order_by(
                    schema.items.c.service_priority,
                    schema.items.c.shuffle_rank,
                    schema.items.c.item_id,
                )
            )
        )
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                execution_state="active",
                dbos_status="PENDING",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
        )
        connection.execute(
            update(schema.item_attempts)
            .where(schema.item_attempts.c.item_id == ordered_item_ids[0])
            .values(
                execution_state="missing",
                missing_observation_count=3,
                missing_first_observed_at=text(
                    "clock_timestamp() - interval '10 minutes'"
                ),
                missing_last_observed_at=text("clock_timestamp()"),
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
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

    assert actionable[0].item.item_id == ordered_item_ids[1]
    assert missing[0].item.item_id == ordered_item_ids[0]


def test_terminal_missing_reobservation_rotates_oldest_marker_first(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD, ServiceClass.STANDARD),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                execution_state="missing",
                dbos_status="PENDING",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                missing_observation_count=3,
                missing_first_observed_at=text(
                    "clock_timestamp() - interval '10 minutes'"
                ),
                missing_last_observed_at=text(
                    "clock_timestamp() - interval '5 minutes'"
                ),
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
            )
        )
    options = ReconcileOptions(page_size=1)
    observed_item_ids: list[str] = []
    for _ in range(2):
        candidate = load_missing_reobservation_page(
            pg_engine,
            page_size=1,
            schema=schema,
        )[0]
        observed_item_ids.append(candidate.item.item_id)
        apply_reconciliation_observations(
            pg_engine,
            observations={
                candidate.attempt.workflow_id: ReconciliationObservation(
                    workflow_id=candidate.attempt.workflow_id,
                    disposition=ReconciliationObservationDisposition.ABSENT,
                )
            },
            resolver=_registry(target),
            options=options,
            schema=schema,
            candidates=(candidate,),
        )
    next_candidate = load_missing_reobservation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )[0]

    assert len(set(observed_item_ids)) == 2
    assert next_candidate.item.item_id == observed_item_ids[0]
    with pg_engine.connect() as connection:
        states = tuple(
            connection.scalars(select(schema.item_attempts.c.execution_state))
        )
        marker_count = connection.scalar(
            select(func.count()).select_from(schema.missing_reobservations)
        )
    assert states == ("missing", "missing")
    assert marker_count == 2


@pytest.mark.parametrize(
    ("execution_state", "extra_values"),
    [
        pytest.param("succeeded", {}, id="succeeded"),
        pytest.param("cancelled", {}, id="cancelled"),
        pytest.param(
            "error",
            {
                "failure": {
                    "failure_class": "permanent",
                    "error_type": "PermanentError",
                    "message": "terminal",
                },
                "retry_disposition": "permanent",
            },
            id="terminal-error",
        ),
        pytest.param(
            "recovery_exhausted",
            {},
            id="recovery-exhausted",
        ),
    ],
)
def test_nonactionable_terminal_attempts_do_not_fill_reconciliation_page(
    pg_engine: Engine,
    execution_state: str,
    extra_values: dict[str, object],
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    _confirm_enqueued(pg_engine, schema)
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                execution_state=execution_state,
                terminal_at=text("clock_timestamp()"),
                updated_at=text("clock_timestamp()"),
                **extra_values,
            )
        )

    candidates = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )

    assert candidates == ()


def test_pending_attempt_does_not_fill_reconciliation_page(
    pg_engine: Engine,
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )

    candidates = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )

    assert candidates == ()


def test_only_retryable_unexhausted_enqueue_error_is_actionable(
    pg_engine: Engine,
) -> None:
    schema, _target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    transient_failure = {
        "failure_class": "transient",
        "error_type": "TemporaryError",
        "message": "retry",
    }
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueue_error",
                enqueue_try=1,
                failure=transient_failure,
                updated_at=text("clock_timestamp()"),
            )
        )

    actionable = load_reconciliation_page(
        pg_engine,
        page_size=1,
        schema=schema,
    )

    assert len(actionable) == 1

    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                failure={
                    "failure_class": "permanent",
                    "error_type": "PermanentError",
                    "message": "stop",
                },
                updated_at=text("clock_timestamp()"),
            )
        )

    assert (
        load_reconciliation_page(
            pg_engine,
            page_size=1,
            schema=schema,
        )
        == ()
    )


def test_enqueue_error_reset_recomputes_current_operation_aggregates(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueue_error",
                enqueue_try=1,
                failure={
                    "failure_class": "transient",
                    "error_type": "TemporaryError",
                    "message": "retry",
                },
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
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert result.enqueue_reset_count == 1
    assert attempt["enqueue_state"] == "pending"
    assert operation["enqueued_count"] == 0
    assert operation["workflow_already_present_count"] == 0
    assert operation["enqueue_failed_count"] == 0
    assert operation["status"] == "enqueuing"
