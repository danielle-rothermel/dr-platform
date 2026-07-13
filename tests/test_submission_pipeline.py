"""One-way public submission pipeline tests for registration into enqueue."""

from __future__ import annotations

from typing import Any, cast

import pytest
from sqlalchemy import Engine

import dr_platform.enqueue_runtime as runtime_module
import dr_platform.submission as submission_module
from dr_platform.manifests import (
    ExecutionTargetRef,
    ManifestPage,
    OperationManifest,
)
from dr_platform.status import OperationStatus
from dr_platform.submission import SubmitOptions, SubmitResult, submit


def _manifest(target_ref: ExecutionTargetRef) -> OperationManifest:
    values: dict[str, Any] = {
        "operation_key": "operation",
        "workflow_role": "generation",
        "group_key": "group",
        "target_ref": target_ref,
        "operation_execution_recipe_digest": "recipe-digest",
        "item_count": 1,
        "page_size": 1,
        "items_digest": "items-digest",
        "pages": (
            ManifestPage(
                page_index=0,
                start_index=0,
                end_index=1,
                page_digest="page-digest",
            ),
        ),
    }
    pending = OperationManifest.model_construct(
        **values,
        manifest_digest="pending",
    )
    return OperationManifest(
        **values,
        manifest_digest=pending.expected_manifest_digest(),
    )


def test_completed_registration_resubmit_repairs_before_new_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target_ref = ExecutionTargetRef(
        target_key="generation",
        target_version=1,
        target_contract_digest="target-digest",
    )
    target = type(
        "Target",
        (),
        {"ref": target_ref, "workflow_role": "generation"},
    )()
    manifest = _manifest(target_ref)
    repair_stages: list[str] = []
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
        def resolve(self, target_ref: Any) -> Any:
            assert target_ref == target.ref
            return target

    monkeypatch.setattr(
        submission_module, "_validate_source", lambda **kw: None
    )
    monkeypatch.setattr(
        submission_module,
        "_create_or_claim_operation",
        lambda **kwargs: 1,
    )
    def capture(stage: str) -> Any:
        def fake_stage(_engine: Engine, **_kwargs: Any) -> None:
            repair_stages.append(stage)

        return fake_stage

    monkeypatch.setattr(
        runtime_module,
        "recover_call_started_page",
        capture("call-started-recovery"),
    )
    monkeypatch.setattr(
        runtime_module,
        "enqueue_replacement_page",
        capture("never-started-replacement"),
    )
    monkeypatch.setattr(
        runtime_module,
        "enqueue_pending_page",
        capture("pending-claim"),
    )
    monkeypatch.setattr(
        submission_module,
        "_load_submit_result",
        lambda **kwargs: expected,
    )

    result = submit(
        manifest,
        cast("Any", object()),
        engine=cast("Engine", object()),
        resolver=Resolver(),
        options=SubmitOptions(page_size=1, claim_lease_seconds=17),
        queue_lookup=cast("Any", object()),
        enqueue_adapter=cast("Any", object()),
        workflow_observer=cast("Any", object()),
    )

    assert result is expected
    assert repair_stages == [
        "call-started-recovery",
        "never-started-replacement",
        "pending-claim",
    ]


def test_resubmit_repairs_both_claim_crash_cuts_before_new_claims(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, Engine, dict[str, Any]]] = []
    queue_lookup = cast("Any", object())
    adapter = cast("Any", object())
    observer = cast("Any", object())

    def capture(stage: str) -> Any:
        def fake_stage(engine: Engine, **kwargs: Any) -> None:
            calls.append((stage, engine, kwargs))

        return fake_stage

    monkeypatch.setattr(
        runtime_module,
        "recover_call_started_page",
        capture("call-started-recovery"),
    )
    monkeypatch.setattr(
        runtime_module,
        "enqueue_replacement_page",
        capture("never-started-replacement"),
    )
    monkeypatch.setattr(
        runtime_module,
        "enqueue_pending_page",
        capture("pending-claim"),
    )
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

    assert [stage for stage, _, _ in calls] == [
        "call-started-recovery",
        "never-started-replacement",
        "pending-claim",
    ]
    for _stage, called_engine, kwargs in calls:
        assert called_engine is engine
        assert kwargs["resolver"] is resolver
        assert kwargs["queue_lookup"] is queue_lookup
        assert kwargs["adapter"] is adapter
        assert kwargs["options"].page_size == 23
        assert kwargs["options"].lease_seconds == 41
        assert kwargs["schema"] is schema
    assert calls[0][2]["observer"] is observer
    assert "observer" not in calls[1][2]
    assert "observer" not in calls[2][2]
