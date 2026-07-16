"""One-way public submission pipeline tests for registration into enqueue."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine

import dr_platform.reconciliation_runtime as runtime_module
import dr_platform.submission as submission_module
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.records import FailureSnapshot
from dr_platform.status import FailureClass, OperationStatus, ServiceClass
from dr_platform.submission import SubmitOptions, SubmitResult, submit
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
)


class _Item(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: str
    service_class: ServiceClass
    spec: dict[str, Any]


class _Source:
    item_count = 5

    def __init__(self) -> None:
        self.reads: list[tuple[int, int]] = []

    def read_items(
        self, *, start_index: int, end_index: int
    ) -> tuple[_Item, ...]:
        self.reads.append((start_index, end_index))
        return tuple(
            _Item(
                item_key=f"item-{index}",
                service_class=ServiceClass.STANDARD,
                spec={"value": index},
            )
            for index in range(start_index, end_index)
        )


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
    return ExecutionTarget(
        ref=ref,
        **declaration.model_dump(),
        workflow=lambda: None,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"{item.item_id}:{attempt}",
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


def test_completed_registration_resubmit_repairs_before_new_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target()
    source = _Source()
    repair_stages: list[str] = []
    expected = SubmitResult(
        operation_key="operation",
        status=OperationStatus.RUNNING,
        requested_count=5,
        registration_cursor=3,
        inserted_count=5,
        already_present_count=0,
        enqueued_count=1,
        workflow_already_present_count=0,
        enqueue_failed_count=0,
        total_failure_count=0,
    )

    class Resolver:
        def resolve(self, target_ref: Any) -> ExecutionTarget:
            assert target_ref == target.ref
            return target

    monkeypatch.setattr(
        submission_module,
        "_create_or_claim_operation",
        lambda **kwargs: kwargs["page_count"],
    )

    def capture_reconcile(_engine: Engine, **_kwargs: Any) -> None:
        repair_stages.append("reconcile")

    monkeypatch.setattr(runtime_module, "reconcile", capture_reconcile)
    monkeypatch.setattr(
        submission_module,
        "_load_submit_result",
        lambda **kwargs: expected,
    )

    result = submit(
        operation_key="operation",
        workflow_role="generation",
        group_key="group",
        target=target,
        source=source,
        engine=cast("Engine", object()),
        resolver=Resolver(),
        options=SubmitOptions(page_size=2, claim_lease_seconds=17),
        queue_lookup=cast("Any", object()),
        enqueue_adapter=cast("Any", object()),
        workflow_observer=cast("Any", object()),
    )

    assert result is expected
    assert repair_stages == ["reconcile"]
    assert source.reads == [(0, 2), (2, 4), (4, 5)]


def test_resubmit_routes_lifecycle_through_reconcile_facade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[Engine, dict[str, Any]]] = []
    queue_lookup = cast("Any", object())
    adapter = cast("Any", object())
    observer = cast("Any", object())

    def capture(engine: Engine, **kwargs: Any) -> None:
        calls.append((engine, kwargs))

    monkeypatch.setattr(runtime_module, "reconcile", capture)
    engine = cast("Engine", object())
    resolver = cast("Any", object())
    schema = submission_module.PlatformSchema()

    submission_module._enqueue_registered_page(
        engine=engine,
        resolver=resolver,
        schema=schema,
        options=SubmitOptions(page_size=23, claim_lease_seconds=41),
        queue_lookup=queue_lookup,
        enqueue_adapter=adapter,
        workflow_observer=observer,
    )

    assert len(calls) == 1
    called_engine, kwargs = calls[0]
    assert called_engine is engine
    assert kwargs["resolver"] is resolver
    assert kwargs["queue_lookup"] is queue_lookup
    assert kwargs["enqueue_adapter"] is adapter
    assert kwargs["options"].page_size == 23
    assert kwargs["options"].claim_lease_seconds == 41
    assert kwargs["schema"] is schema
    assert kwargs["recovery_observer"] is observer
