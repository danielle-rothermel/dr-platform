"""Focused PostgreSQL tests for the append-only enqueue Claim ledger."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, and_, insert, select, text, update

from dr_platform import claims as claims_module
from dr_platform.claims import (
    ClaimAuthorityError,
    ClaimConflictError,
    ClaimPageOptions,
    PostgresClaimTransitionStore,
    claim_pending_attempts,
    load_call_started_recovery_page,
    replace_expired_unstarted_claims,
    start_enqueue_call,
)
from dr_platform.db import PlatformSchema, upgrade_platform_schema
from dr_platform.enqueue_runtime import QueueConfigurationError
from dr_platform.manifests import ExecutionRecipeEnvelope, ExecutionTargetRef
from dr_platform.records import EnqueueClaimRecord, FailureSnapshot
from dr_platform.status import (
    AttemptEnqueueState,
    EnqueueClaimDisposition,
    FailureClass,
    ServiceClass,
)
from dr_platform.submission import SubmitOptions, prepare_manifest, submit
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
    TargetRegistry,
)

if TYPE_CHECKING:
    from dr_platform.items import SubmittableItem


class _Item(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: str
    spec: dict[str, Any]
    service_class: ServiceClass


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
    ) -> tuple[SubmittableItem, ...]:
        return self.items[start_index:end_index]


class _Outcome(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    disposition: str
    effective_service_priority: int | None = None
    failure: FailureSnapshot | None = None


class _MissingQueueLookup:
    def retrieve_queue(self, name: str) -> None:
        del name


def _target() -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name="generation-queue",
        workflow_role="generation",
        managed_workflow_name="generation-workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
    )
    ref = declaration.target_ref(target_key="generation", target_version=1)

    def recipe_for(item: SubmittableItem) -> ExecutionRecipeEnvelope:
        return ExecutionRecipeEnvelope(
            target_ref=ref,
            managed_workflow_name=declaration.managed_workflow_name,
            managed_workflow_version=declaration.managed_workflow_version,
            argument_recipe_version=declaration.argument_recipe_version,
            payload={"item_key": item.item_key},
        )

    return ExecutionTarget(
        ref=ref,
        **declaration.model_dump(),
        workflow=lambda: None,
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


def _register(
    engine: Engine,
    *,
    service_classes: tuple[ServiceClass, ...],
) -> tuple[PlatformSchema, ExecutionTarget]:
    upgrade_platform_schema(str(engine.url))
    schema = PlatformSchema()
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    source = _Source(
        items=tuple(
            _Item(
                item_key=f"item-{index}",
                spec={"index": index},
                service_class=service_class,
            )
            for index, service_class in enumerate(service_classes)
        )
    )
    manifest = prepare_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        source=source,
        options=SubmitOptions(page_size=2),
    )
    try:
        submit(
            manifest,
            source,
            engine=engine,
            resolver=registry,
            options=SubmitOptions(page_size=2),
            schema=schema,
            queue_lookup=_MissingQueueLookup(),
        )
    except QueueConfigurationError:
        pass
    else:
        raise AssertionError("missing queue unexpectedly admitted Claims")
    return schema, target


def _claim_ids(*values: str) -> Callable[[], str]:
    iterator: Iterator[str] = iter(values)
    return lambda: next(iterator)


def _admit_targets(target_refs: tuple[ExecutionTargetRef, ...]) -> None:
    del target_refs


def _claim_record(
    engine: Engine,
    schema: PlatformSchema,
    *,
    item_id: str,
    attempt: int,
    claim_id: str,
) -> EnqueueClaimRecord:
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(schema.enqueue_claims).where(
                    and_(
                        schema.enqueue_claims.c.item_id == item_id,
                        schema.enqueue_claims.c.attempt == attempt,
                        schema.enqueue_claims.c.claim_id == claim_id,
                    )
                )
            )
            .mappings()
            .one()
        )
    return EnqueueClaimRecord.model_validate(dict(row))


def test_claim_page_is_bounded_and_uses_kernel_schedule(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(
            ServiceClass.STANDARD,
            ServiceClass.BACKFILL,
            ServiceClass.URGENT,
        ),
    )
    with pg_engine.connect() as connection:
        expected = (
            connection.execute(
                select(schema.items.c.item_id)
                .order_by(
                    schema.items.c.service_priority,
                    schema.items.c.shuffle_rank,
                    schema.items.c.item_id,
                )
                .limit(2)
            )
            .scalars()
            .all()
        )

    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        options=ClaimPageOptions(page_size=2),
        schema=schema,
        claim_id_factory=_claim_ids("claim-a", "claim-b"),
    )

    assert [claim.item_id for claim in page.claims] == expected
    assert [claim.claim_id for claim in page.claims] == ["claim-a", "claim-b"]
    with pg_engine.connect() as connection:
        attempts = connection.execute(
            select(
                schema.item_attempts.c.enqueue_state,
                schema.item_attempts.c.current_claim_id,
                schema.item_attempts.c.enqueue_try,
            ).where(schema.item_attempts.c.item_id.in_(expected))
        ).all()
    assert attempts
    assert all(row.enqueue_state == "claiming" for row in attempts)
    assert all(row.current_claim_id is not None for row in attempts)
    assert all(row.enqueue_try == 1 for row in attempts)


def test_concurrent_claimers_never_claim_one_attempt_twice(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 4,
    )

    def claim(prefix: str) -> tuple[str, ...]:
        counter = iter(range(10))
        page = claim_pending_attempts(
            pg_engine,
            admit_targets=_admit_targets,
            options=ClaimPageOptions(page_size=2),
            schema=schema,
            claim_id_factory=lambda: f"{prefix}-{next(counter)}",
        )
        return tuple(item.item_id for item in page.claims)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(claim, "first")
        second = executor.submit(claim, "second")
        first_ids = set(first.result())
        second_ids = set(second.result())

    assert first_ids.isdisjoint(second_ids)
    assert len(first_ids) == len(second_ids) == 2
    with pg_engine.connect() as connection:
        claim_count = connection.execute(
            select(text("count(*)")).select_from(schema.enqueue_claims)
        ).scalar_one()
    assert claim_count == len(first_ids | second_ids)


def test_composite_claim_key_allows_same_claim_id_on_two_attempts(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 2,
    )
    page = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=lambda: "shared-claim-id",
    )

    first, second = page.claims
    started = start_enqueue_call(
        pg_engine,
        item_id=first.item_id,
        attempt=first.attempt,
        claim_id=first.claim_id,
        schema=schema,
    )

    assert started.disposition is EnqueueClaimDisposition.CALL_STARTED
    with pg_engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.enqueue_claims.c.item_id,
                schema.enqueue_claims.c.disposition,
            ).where(schema.enqueue_claims.c.claim_id == "shared-claim-id")
        ).all()
    disposition_by_item = {row.item_id: row.disposition for row in rows}
    assert disposition_by_item[first.item_id] == "call_started"
    assert disposition_by_item[second.item_id] == "claimed"


def test_target_admission_failure_precedes_claim_mutation(
    pg_engine: Engine,
) -> None:
    schema, target = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    observed: tuple[ExecutionTargetRef, ...] = ()

    def reject(target_refs: tuple[ExecutionTargetRef, ...]) -> None:
        nonlocal observed
        observed = target_refs
        raise RuntimeError("queue priority disabled")

    with pytest.raises(RuntimeError, match="priority disabled"):
        claim_pending_attempts(
            pg_engine,
            admit_targets=reject,
            schema=schema,
            claim_id_factory=_claim_ids("must-not-be-used"),
        )

    assert observed == (target.ref,)
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                select(text("count(*)")).select_from(schema.enqueue_claims)
            ).scalar_one()
            == 0
        )


def test_expired_unstarted_claim_is_replaced_without_reusing_row(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.begin() as connection:
        item_id = connection.execute(
            select(schema.items.c.item_id)
        ).scalar_one()
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
        connection.execute(
            insert(schema.enqueue_claims).values(
                item_id=item_id,
                attempt=0,
                claim_id="old-claim",
                workflow_id=workflow_id,
                enqueue_try=1,
                claimed_at=text("clock_timestamp() - interval '2 minutes'"),
                lease_expires_at=text(
                    "clock_timestamp() - interval '1 minute'"
                ),
                disposition=EnqueueClaimDisposition.CLAIMED.value,
                created_at=text("clock_timestamp() - interval '2 minutes'"),
            )
        )
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state=AttemptEnqueueState.CLAIMING.value,
                enqueue_try=1,
                current_claim_id="old-claim",
                updated_at=text("clock_timestamp()"),
            )
        )

    page = replace_expired_unstarted_claims(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("replacement-claim"),
    )

    assert page.claims[0].claim_id == "replacement-claim"
    assert page.claims[0].enqueue_try == 1
    with pg_engine.connect() as connection:
        claims = connection.execute(
            select(
                schema.enqueue_claims.c.claim_id,
                schema.enqueue_claims.c.disposition,
                schema.enqueue_claims.c.replacement_claim_id,
            ).order_by(schema.enqueue_claims.c.created_at)
        ).all()
    assert claims == [
        ("old-claim", "replaced", "replacement-claim"),
        ("replacement-claim", "claimed", None),
    ]


def test_store_commits_call_start_and_successful_outcome(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    claim = _claim_record(
        pg_engine,
        schema,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)

    assert store.mark_enqueue_call_started(claim=claim)
    assert store.record_enqueue_outcome(
        claim=claim,
        outcome=_Outcome(
            workflow_id=claim.workflow_id,
            disposition="enqueued",
            effective_service_priority=ServiceClass.STANDARD.priority,
        ),
    )
    assert not store.mark_enqueue_call_started(claim=claim)

    with pg_engine.connect() as connection:
        attempt_row = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        claim_row = (
            connection.execute(select(schema.enqueue_claims)).mappings().one()
        )
    assert attempt_row["enqueue_state"] == "enqueued"
    assert attempt_row["current_claim_id"] is None
    assert claim_row["disposition"] == "outcome_recorded"
    assert claim_row["enqueue_call_started_at"] is not None
    assert claim_row["resolved_at"] is not None


def test_store_persists_typed_enqueue_error_without_retry_loop(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    claim = _claim_record(
        pg_engine,
        schema,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)
    failure = FailureSnapshot(
        failure_class=FailureClass.TRANSIENT,
        error_type="TemporaryError",
        message="try later",
    )

    assert store.mark_enqueue_call_started(claim=claim)
    assert store.record_enqueue_outcome(
        claim=claim,
        outcome=_Outcome(
            workflow_id=claim.workflow_id,
            disposition="enqueue_error",
            failure=failure,
        ),
    )

    with pg_engine.connect() as connection:
        attempt_row = (
            connection.execute(select(schema.item_attempts)).mappings().one()
        )
        operation_row = (
            connection.execute(select(schema.operations)).mappings().one()
        )
    assert attempt_row["enqueue_state"] == "enqueue_error"
    assert attempt_row["failure"]["failure_class"] == "transient"
    assert operation_row["status"] == "enqueuing"
    assert operation_row["enqueue_failed_count"] == 1


def test_lost_successful_outcome_creates_exact_pending_compensation(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    claim = _claim_record(
        pg_engine,
        schema,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)
    assert store.mark_enqueue_call_started(claim=claim)
    with pg_engine.begin() as connection:
        now = connection.scalar(select(text("clock_timestamp()")))
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state=AttemptEnqueueState.PENDING.value,
                current_claim_id=None,
                updated_at=now,
            )
        )
        connection.execute(
            update(schema.enqueue_claims).values(
                disposition=EnqueueClaimDisposition.INVALIDATED.value,
                invalidated_at=now,
                invalidated_by="test-cancellation",
                resolved_at=now,
            )
        )
    outcome = _Outcome(
        workflow_id=claim.workflow_id,
        disposition="enqueued",
        effective_service_priority=ServiceClass.STANDARD.priority,
    )

    assert not store.record_enqueue_outcome(claim=claim, outcome=outcome)
    store.ensure_lost_outcome_compensation(claim=claim, outcome=outcome)
    store.ensure_lost_outcome_compensation(claim=claim, outcome=outcome)

    with pg_engine.connect() as connection:
        rows = (
            connection.execute(select(schema.enqueue_compensations))
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["cancel_disposition"] == "pending"


def test_lost_matching_recorded_outcome_is_idempotent(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    claim = _claim_record(
        pg_engine,
        schema,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
    )
    outcome = _Outcome(
        workflow_id=claim.workflow_id,
        disposition="enqueued",
        effective_service_priority=ServiceClass.STANDARD.priority,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)

    assert store.mark_enqueue_call_started(claim=claim)
    assert store.record_enqueue_outcome(claim=claim, outcome=outcome)
    store.ensure_lost_outcome_compensation(claim=claim, outcome=outcome)

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(text("count(*)")).select_from(
                    schema.enqueue_compensations
                )
            )
            == 0
        )


def test_first_compensation_write_rejects_forged_workflow_provenance(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    forged_claim = claimed.claim.model_copy(
        update={"workflow_id": "forged-workflow"}
    )
    forged_outcome = _Outcome(
        workflow_id="forged-workflow",
        disposition="enqueued",
        effective_service_priority=ServiceClass.STANDARD.priority,
    )
    store = PostgresClaimTransitionStore(pg_engine, schema=schema)

    with pytest.raises(ClaimConflictError, match="provenance changed"):
        store.ensure_lost_outcome_compensation(
            claim=forged_claim,
            outcome=forged_outcome,
        )

    with pg_engine.connect() as connection:
        count = connection.execute(
            select(text("count(*)")).select_from(schema.enqueue_compensations)
        ).scalar_one()
    assert count == 0


def test_expired_call_started_absence_appends_next_try_replacement(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.connect() as connection:
        claimed_at = connection.execute(
            select(text("clock_timestamp()"))
        ).scalar_one()
    monkeypatch.setattr(
        claims_module,
        "_database_now",
        lambda connection: claimed_at,
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        claim_id_factory=_claim_ids("original-claim"),
    ).claims[0]
    start_enqueue_call(
        pg_engine,
        item_id=claimed.item_id,
        attempt=claimed.attempt,
        claim_id=claimed.claim_id,
        schema=schema,
    )
    monkeypatch.setattr(
        claims_module,
        "_database_now",
        lambda connection: claimed_at + timedelta(seconds=2),
    )

    recovery = load_call_started_recovery_page(
        pg_engine,
        options=ClaimPageOptions(page_size=1, lease_seconds=1),
        schema=schema,
    )
    store = PostgresClaimTransitionStore(
        pg_engine,
        schema=schema,
        options=ClaimPageOptions(lease_seconds=1),
        claim_id_factory=_claim_ids("replacement-claim"),
        admit_targets=_admit_targets,
    )
    replacement = store.replace_call_started_after_absence(
        claimed=recovery.claims[0]
    )

    assert replacement is not None
    assert replacement.claim_id == "replacement-claim"
    assert replacement.enqueue_try == 2
    assert replacement.workflow_id == claimed.workflow_id
    with pg_engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.enqueue_claims.c.claim_id,
                schema.enqueue_claims.c.disposition,
                schema.enqueue_claims.c.enqueue_try,
            ).order_by(schema.enqueue_claims.c.enqueue_try)
        ).all()
    assert rows == [
        ("original-claim", "replaced", 1),
        ("replacement-claim", "claimed", 2),
    ]


def test_call_started_absence_at_max_tries_resolves_terminally(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    with pg_engine.begin() as connection:
        item_id = connection.execute(
            select(schema.items.c.item_id)
        ).scalar_one()
        workflow_id = connection.execute(
            select(schema.item_attempts.c.workflow_id)
        ).scalar_one()
        connection.execute(
            insert(schema.enqueue_claims).values(
                item_id=item_id,
                attempt=0,
                claim_id="exhausted-claim",
                workflow_id=workflow_id,
                enqueue_try=3,
                claimed_at=text("clock_timestamp() - interval '2 minutes'"),
                lease_expires_at=text(
                    "clock_timestamp() - interval '1 minute'"
                ),
                enqueue_call_started_at=text(
                    "clock_timestamp() - interval '90 seconds'"
                ),
                disposition=EnqueueClaimDisposition.CALL_STARTED.value,
                created_at=text("clock_timestamp() - interval '2 minutes'"),
            )
        )
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state=AttemptEnqueueState.CLAIMING.value,
                enqueue_try=3,
                current_claim_id="exhausted-claim",
                updated_at=text("clock_timestamp()"),
            )
        )
    recovery = load_call_started_recovery_page(
        pg_engine,
        schema=schema,
    ).claims[0]
    store = PostgresClaimTransitionStore(
        pg_engine,
        schema=schema,
        admit_targets=_admit_targets,
    )

    assert store.replace_call_started_after_absence(claimed=recovery) is None
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
    cut_after_resolution = operation_row["platform_cut_version"]

    assert claim_row["disposition"] == "expired"
    assert claim_row["resolved_at"] is not None
    assert attempt_row["enqueue_state"] == "enqueue_error"
    assert attempt_row["current_claim_id"] is None
    assert attempt_row["enqueue_try"] == 3
    assert attempt_row["failure"]["error_type"] == "MaxEnqueueTriesExceeded"
    assert operation_row["status"] == "failed"
    assert operation_row["completed_at"] is not None

    assert store.replace_call_started_after_absence(claimed=recovery) is None
    with pg_engine.connect() as connection:
        replay_cut = connection.execute(
            select(schema.operations.c.platform_cut_version)
        ).scalar_one()
    assert replay_cut == cut_after_resolution


def test_expired_claim_cannot_cross_call_boundary(pg_engine: Engine) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,),
    )
    claimed = claim_pending_attempts(
        pg_engine,
        admit_targets=_admit_targets,
        options=ClaimPageOptions(lease_seconds=1),
        schema=schema,
        claim_id_factory=_claim_ids("claim"),
    ).claims[0]
    with pg_engine.connect() as connection:
        connection.execute(text("SELECT pg_sleep(1.05)"))

    try:
        start_enqueue_call(
            pg_engine,
            item_id=claimed.item_id,
            attempt=claimed.attempt,
            claim_id=claimed.claim_id,
            schema=schema,
        )
    except ClaimAuthorityError:
        pass
    else:
        raise AssertionError("expired Claim crossed the DBOS call boundary")
