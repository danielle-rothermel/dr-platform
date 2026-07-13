"""Focused P5a cancellation contract coverage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

import pytest
from sqlalchemy import Engine, func, insert, select, update

from dr_platform import (
    CancellationConflictError,
    CancellationInspection,
    CancellationInspectionDisposition,
    CancellationRequest,
    PlatformSchema,
    cancel_operation,
    repair_late_enqueue_compensations,
    upgrade_platform_schema,
)
from dr_platform.claims import (
    ClaimPageOptions,
    claim_pending_attempts,
    start_enqueue_call,
)
from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.reconciliation import (
    NextAttemptRequest,
    ReconciliationConflictError,
    apply_reconciliation_observations,
    request_next_attempt,
)
from dr_platform.reconciliation_runtime import (
    ReconcileOptions,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
)
from dr_platform.records import EligibilityReference, FailureSnapshot
from dr_platform.status import (
    AttemptEnqueueState,
    AttemptExecutionState,
    CancellationDisposition,
    EnqueueCompensationDisposition,
    FailureClass,
    NextAttemptDisposition,
    NextAttemptReason,
    OperationStatus,
    RetryDisposition,
    ServiceClass,
)
from dr_platform.submission import (
    RegistrationConflictError,
    prepare_manifest,
    submit,
)
from dr_platform.targets import TargetRegistry
from tests.contracts.test_platform_v6_enqueue_claims import (
    ClaimTestItem,
    ClaimTestSource,
    _target,
)


class _Canceller:
    def __init__(
        self,
        *,
        inspections: dict[str, CancellationInspection] | None = None,
        failing: set[str] | None = None,
    ) -> None:
        self.calls: list[tuple[str, bool]] = []
        self.inspections = inspections or {}
        self.failing = failing or set()

    def inspect(self, *, workflow_id: str) -> CancellationInspection:
        return self.inspections.get(
            workflow_id,
            CancellationInspection(
                workflow_id=workflow_id,
                disposition=CancellationInspectionDisposition.ACTIVE,
            ),
        )

    def cancel_workflow(
        self, *, workflow_id: str, cancel_children: bool
    ) -> None:
        self.calls.append((workflow_id, cancel_children))
        if workflow_id in self.failing:
            raise RuntimeError("synthetic DBOS failure")


def _register_operation(
    engine: Engine,
    schema: PlatformSchema,
    *,
    operation_key: str,
    item_keys: tuple[str, ...],
) -> TargetRegistry:
    target = _target()
    source = ClaimTestSource(
        items=tuple(
            ClaimTestItem(
                item_key=item_key,
                spec={},
                service_class=ServiceClass.STANDARD,
            )
            for item_key in item_keys
        )
    )
    manifest = prepare_manifest(
        operation_key=operation_key,
        workflow_role=target.workflow_role,
        group_key="cancel-group",
        target=target,
        source=source,
    )
    registry = TargetRegistry()
    registry.register(target)
    with patch("dr_platform.submission._enqueue_registered_page"):
        submit(
            manifest,
            source,
            engine=engine,
            resolver=registry,
            schema=schema,
        )
    return registry


def _mark_enqueued(
    engine: Engine,
    schema: PlatformSchema,
    *,
    operation_key: str | None = None,
) -> None:
    statement = update(schema.item_attempts)
    if operation_key is not None:
        statement = statement.where(
            schema.item_attempts.c.item_id.in_(
                select(schema.items.c.item_id).where(
                    schema.items.c.operation_key == operation_key
                )
            )
        )
    with engine.begin() as connection:
        connection.execute(
            statement.values(
                enqueue_state=AttemptEnqueueState.ENQUEUED.value,
                enqueued_at=func.clock_timestamp(),
                effective_service_priority=ServiceClass.STANDARD.priority,
                priority_source="enqueued_here",
                updated_at=func.clock_timestamp(),
            )
        )


def test_cancellation_persists_intent_and_exact_replay(
    pg_engine: Any,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="cancel-operation",
        item_keys=("cancel",),
    )
    _mark_enqueued(pg_engine, schema)
    request = CancellationRequest(
        operation_key="cancel-operation",
        request_id="cancel-1",
        requested_by="operator",
    )
    canceller = _Canceller()

    first = cancel_operation(
        request, engine=pg_engine, schema=schema, canceller=canceller
    )
    replay = cancel_operation(
        request, engine=pg_engine, schema=schema, canceller=canceller
    )
    with pytest.raises(CancellationConflictError):
        cancel_operation(
            request.model_copy(update={"requested_by": "other"}),
            engine=pg_engine,
            schema=schema,
            canceller=canceller,
        )
    with pytest.raises(CancellationConflictError):
        cancel_operation(
            request.model_copy(update={"request_id": "later-request"}),
            engine=pg_engine,
            schema=schema,
            canceller=canceller,
        )

    assert first == replay
    assert first.results[0].disposition == "dbos_cancelled"
    assert canceller.calls == [(first.results[0].workflow_id, False)]
    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.item_attempts.c.execution_state)
        ).scalar_one()
    assert state == AttemptExecutionState.CANCELLED.value


def test_cancellation_invalidates_claim_and_finalizes_not_enqueued(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="claim-cancel",
        item_keys=("claimed",),
    )
    claim = claim_pending_attempts(
        pg_engine,
        admit_targets=lambda target_refs: None,
        options=ClaimPageOptions(page_size=1, lease_seconds=60),
        schema=schema,
        claim_id_factory=lambda: "claim-1",
    ).claims[0]

    result = cancel_operation(
        CancellationRequest(
            operation_key="claim-cancel",
            request_id="claim-cancel-request",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=_Canceller(),
    )

    assert (
        result.results[0].disposition is CancellationDisposition.NOT_ENQUEUED
    )
    with pg_engine.connect() as connection:
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        durable_claim = (
            connection.execute(
                select(schema.enqueue_claims).where(
                    schema.enqueue_claims.c.claim_id == claim.claim_id
                )
            )
            .mappings()
            .one()
        )
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert attempt.execution_state == AttemptExecutionState.CANCELLED.value
    assert durable_claim.disposition == "invalidated"
    assert durable_claim.invalidated_by == "claim-cancel-request"
    assert operation.status == OperationStatus.CANCELLED.value


def test_cancellation_repairs_invalidated_call_started_claim(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="late-claim-cancel",
        item_keys=("claimed",),
    )
    claim = claim_pending_attempts(
        pg_engine,
        admit_targets=lambda target_refs: None,
        schema=schema,
        claim_id_factory=lambda: "late-claim-1",
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claim.item_id,
        attempt=claim.attempt,
        claim_id=claim.claim_id,
        schema=schema,
    )
    canceller = _Canceller()

    request = CancellationRequest(
        operation_key="late-claim-cancel",
        request_id="late-claim-cancel-request",
        requested_by="operator",
    )
    cancel_operation(
        request,
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )
    cancel_operation(
        request,
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    with pg_engine.connect() as connection:
        compensation = connection.execute(
            select(schema.enqueue_compensations)
        ).mappings().one()
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    assert compensation["claim_id"] == claim.claim_id
    assert compensation["workflow_id"] == claim.workflow_id
    assert compensation["cancel_disposition"] == "cancelled"
    assert compensation["resolved_at"] is not None
    assert canceller.calls == [(claim.workflow_id, False)]
    assert attempt["execution_state"] == AttemptExecutionState.CANCELLED.value


def test_absent_late_enqueue_hazard_blocks_new_reference(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="hazard-origin",
        item_keys=("shared",),
    )
    claim = claim_pending_attempts(
        pg_engine,
        admit_targets=lambda target_refs: None,
        schema=schema,
        claim_id_factory=lambda: "hazard-claim",
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claim.item_id,
        attempt=claim.attempt,
        claim_id=claim.claim_id,
        schema=schema,
    )
    canceller = _Canceller(
        inspections={
            claim.workflow_id: CancellationInspection(
                workflow_id=claim.workflow_id,
                disposition=CancellationInspectionDisposition.ABSENT,
            )
        }
    )

    cancel_operation(
        CancellationRequest(
            operation_key="hazard-origin",
            request_id="hazard-request",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    with pg_engine.connect() as connection:
        assert connection.scalar(
            select(schema.enqueue_compensations.c.cancel_disposition)
        ) == EnqueueCompensationDisposition.PENDING.value
    with pytest.raises(
        RegistrationConflictError,
        match="unresolved late-enqueue compensation",
    ):
        _register_operation(
            pg_engine,
            schema,
            operation_key="hazard-link",
            item_keys=("shared",),
        )


def test_multiple_compensations_for_one_workflow_converge_without_recancel(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="multi-compensation",
        item_keys=("shared",),
    )
    claim = claim_pending_attempts(
        pg_engine,
        admit_targets=lambda target_refs: None,
        schema=schema,
        claim_id_factory=lambda: "first-claim",
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claim.item_id,
        attempt=claim.attempt,
        claim_id=claim.claim_id,
        schema=schema,
    )
    canceller = _Canceller()
    cancel_operation(
        CancellationRequest(
            operation_key="multi-compensation",
            request_id="multi-cancel",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )
    now = datetime.now(tz=UTC)
    with pg_engine.begin() as connection:
        connection.execute(
            insert(schema.enqueue_claims).values(
                item_id=claim.item_id,
                attempt=claim.attempt,
                claim_id="second-claim",
                workflow_id=claim.workflow_id,
                enqueue_try=2,
                claimed_at=now,
                lease_expires_at=now + timedelta(seconds=60),
                enqueue_call_started_at=now,
                disposition="invalidated",
                invalidated_at=now,
                invalidated_by="multi-cancel",
                resolved_at=now,
                created_at=now,
            )
        )

    repair_late_enqueue_compensations(
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    with pg_engine.connect() as connection:
        dispositions = list(
            connection.scalars(
                select(schema.enqueue_compensations.c.cancel_disposition)
                .order_by(schema.enqueue_compensations.c.claim_id)
            )
        )
    assert dispositions == ["cancelled", "cancelled"]
    assert canceller.calls == [(claim.workflow_id, False)]


def test_partial_external_failure_is_durable_and_replay_retries_only_failure(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine,
        schema,
        operation_key="partial-cancel",
        item_keys=("one", "two"),
    )
    _mark_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        workflow_ids = tuple(
            connection.execute(
                select(schema.item_attempts.c.workflow_id).order_by(
                    schema.item_attempts.c.workflow_id
                )
            ).scalars()
        )
    request = CancellationRequest(
        operation_key="partial-cancel",
        request_id="partial-request",
        requested_by="operator",
    )
    canceller = _Canceller(failing={workflow_ids[1]})

    first = cancel_operation(
        request, engine=pg_engine, schema=schema, canceller=canceller
    )
    assert {result.disposition for result in first.results} == {
        CancellationDisposition.DBOS_CANCELLED,
        CancellationDisposition.FAILED,
    }
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(schema.operations.c.status))
            == "cancelling"
        )

    canceller.failing.clear()
    replay = cancel_operation(
        request, engine=pg_engine, schema=schema, canceller=canceller
    )
    assert all(
        result.disposition is CancellationDisposition.DBOS_CANCELLED
        for result in replay.results
    )
    assert canceller.calls.count((workflow_ids[0], False)) == 1
    assert canceller.calls.count((workflow_ids[1], False)) == 2
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(schema.operations.c.status))
            == "cancelled"
        )


def test_shared_reference_skips_dbos_and_is_locally_sticky(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine, schema, operation_key="owner-a", item_keys=("shared",)
    )
    _register_operation(
        pg_engine, schema, operation_key="owner-b", item_keys=("shared",)
    )
    _mark_enqueued(pg_engine, schema)
    canceller = _Canceller()

    result = cancel_operation(
        CancellationRequest(
            operation_key="owner-a",
            request_id="shared-request",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    assert (
        result.results[0].disposition is CancellationDisposition.SKIPPED_SHARED
    )
    assert canceller.calls == []
    with pg_engine.connect() as connection:
        states = {
            row.operation_key: row.execution_state
            for row in connection.execute(
                select(
                    schema.items.c.operation_key,
                    schema.item_attempts.c.execution_state,
                ).join(
                    schema.item_attempts,
                    schema.item_attempts.c.item_id == schema.items.c.item_id,
                )
            )
        }
    assert states == {"owner-a": "cancelled", "owner-b": "not_started"}


@pytest.mark.parametrize("terminal", ["succeeded", "error"])
def test_terminal_observation_wins_cancellation_race(
    pg_engine: Engine,
    terminal: str,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine, schema, operation_key="terminal-race", item_keys=(terminal,)
    )
    _mark_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
    inspection = CancellationInspection(
        workflow_id=workflow_id,
        disposition=CancellationInspectionDisposition(terminal),
        dbos_status=terminal.upper(),
        failure=(
            FailureSnapshot(
                failure_class=FailureClass.PERMANENT,
                error_type="TerminalError",
                message="safe",
            )
            if terminal == "error"
            else None
        ),
        retry_disposition=(
            RetryDisposition.PERMANENT if terminal == "error" else None
        ),
    )

    result = cancel_operation(
        CancellationRequest(
            operation_key="terminal-race",
            request_id=f"terminal-{terminal}",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=_Canceller(inspections={workflow_id: inspection}),
    )

    assert (
        result.results[0].disposition
        is CancellationDisposition.OBSERVED_TERMINAL
    )
    with pg_engine.connect() as connection:
        attempt = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
    assert attempt.execution_state == terminal


def test_topology_drift_fails_closed_without_recursive_cancel(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine, schema, operation_key="topology", item_keys=("parent",)
    )
    _mark_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
    canceller = _Canceller(
        inspections={
            workflow_id: CancellationInspection(
                workflow_id=workflow_id,
                disposition=CancellationInspectionDisposition.ACTIVE,
                has_children=True,
            )
        }
    )

    result = cancel_operation(
        CancellationRequest(
            operation_key="topology",
            request_id="topology-request",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=canceller,
    )

    assert result.results[0].disposition is CancellationDisposition.FAILED
    assert canceller.calls == []
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(schema.operations.c.status))
            == "cancelling"
        )


def test_cancelled_attempt_is_sticky_against_late_success(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    registry = _register_operation(
        pg_engine, schema, operation_key="sticky", item_keys=("late",)
    )
    _mark_enqueued(pg_engine, schema)
    with pg_engine.connect() as connection:
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
    cancel_operation(
        CancellationRequest(
            operation_key="sticky",
            request_id="sticky-request",
            requested_by="operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=_Canceller(),
    )

    late = apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.SUCCEEDED,
                dbos_status=DbosWorkflowStatus.SUCCESS,
            )
        },
        resolver=registry,
        options=ReconcileOptions(page_size=10),
        schema=schema,
    )

    assert late.changed_count == 0
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(schema.item_attempts.c.execution_state))
            == AttemptExecutionState.CANCELLED.value
        )


def test_foreign_cancellation_provenance_allows_confirmed_retry(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    _register_operation(
        pg_engine, schema, operation_key="origin", item_keys=("shared",)
    )
    registry = _register_operation(
        pg_engine, schema, operation_key="linked", item_keys=("shared",)
    )
    _mark_enqueued(pg_engine, schema)
    cancel_operation(
        CancellationRequest(
            operation_key="origin",
            request_id="origin-request",
            requested_by="origin-operator",
        ),
        engine=pg_engine,
        schema=schema,
        canceller=_Canceller(),
    )
    with pg_engine.connect() as connection:
        linked_item_id, workflow_id = connection.execute(
            select(schema.items.c.item_id, schema.item_attempts.c.workflow_id)
            .join(
                schema.item_attempts,
                schema.item_attempts.c.item_id == schema.items.c.item_id,
            )
            .where(schema.items.c.operation_key == "linked")
        ).one()

    applied = apply_reconciliation_observations(
        pg_engine,
        observations={
            workflow_id: ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.CANCELLED,
                dbos_status=DbosWorkflowStatus.CANCELLED,
            )
        },
        resolver=registry,
        options=ReconcileOptions(page_size=10),
        schema=schema,
    )
    assert applied.changed_count == 1
    with pg_engine.connect() as connection:
        linked = (
            connection.execute(
                select(schema.item_attempts).where(
                    schema.item_attempts.c.item_id == linked_item_id
                )
            )
            .mappings()
            .one()
        )
    assert linked.cancellation_origin == "foreign_operation"
    assert linked.cancellation_origin_operation_key == "origin"
    assert linked.foreign_cancellation_request_id == "origin-request"

    retry = request_next_attempt(
        NextAttemptRequest(
            item_id=linked_item_id,
            source_attempt=0,
            request_key="retry-foreign-cancel",
            reason=NextAttemptReason.OPERATOR_CANCEL_RETRY,
            eligibility=EligibilityReference(
                kind="operator_confirmation",
                record_id="confirmation-1",
                digest="confirmation-digest",
            ),
            requested_by="retry-operator",
            operator_confirmed_at=datetime.now(tz=UTC),
        ),
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )
    assert retry.disposition is NextAttemptDisposition.CREATED


def test_ambiguous_foreign_cancellation_provenance_fails_closed(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    schema = PlatformSchema()
    registry = _register_operation(
        pg_engine, schema, operation_key="linked", item_keys=("shared",)
    )
    _register_operation(
        pg_engine, schema, operation_key="origin-a", item_keys=("shared",)
    )
    _register_operation(
        pg_engine, schema, operation_key="origin-b", item_keys=("shared",)
    )
    _mark_enqueued(pg_engine, schema)
    now = func.clock_timestamp()
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts)
            .where(
                schema.item_attempts.c.item_id.in_(
                    select(schema.items.c.item_id).where(
                        schema.items.c.operation_key.in_(
                            ["origin-a", "origin-b"]
                        )
                    )
                )
            )
            .values(
                execution_state=AttemptExecutionState.CANCELLED.value,
                terminal_at=now,
                cancellation_request_id=func.concat(
                    "request-", schema.item_attempts.c.item_id
                ),
                cancellation_requested_at=now,
                cancellation_requested_by="operator",
                cancellation_disposition=CancellationDisposition.DBOS_CANCELLED.value,
                cancellation_origin="local_operation",
                updated_at=now,
            )
        )
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
            .join(
                schema.items,
                schema.items.c.item_id == schema.item_attempts.c.item_id,
            )
            .where(schema.items.c.operation_key == "linked")
        ).scalar_one()

    with pytest.raises(
        ReconciliationConflictError,
        match="foreign cancellation provenance is ambiguous",
    ):
        apply_reconciliation_observations(
            pg_engine,
            observations={
                workflow_id: ReconciliationObservation(
                    workflow_id=workflow_id,
                    disposition=(
                        ReconciliationObservationDisposition.CANCELLED
                    ),
                    dbos_status=DbosWorkflowStatus.CANCELLED,
                )
            },
            resolver=registry,
            options=ReconcileOptions(page_size=10),
            schema=schema,
        )
