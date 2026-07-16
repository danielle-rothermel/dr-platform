"""Startup execution-target registry contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from dr_platform.manifests import (
    ExecutionRecipeEnvelope,
    ExecutionTargetRef,
)
from dr_platform.records import FailureSnapshot, ItemRecord
from dr_platform.status import FailureClass
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetConflictError,
    TargetContractDeclaration,
    TargetRegistry,
    TargetResolutionErrorCode,
    TargetUnavailableError,
)


def _workflow(*args: object) -> object:
    return args


def _execution_for(item: ItemRecord, attempt: int) -> ExecutionIdentity:
    return ExecutionIdentity(
        execution_key=f"{item.item_id}:{attempt}",
        workflow_id=f"workflow:{item.item_id}:{attempt}",
    )


def _args_for(item: ItemRecord, attempt: int) -> tuple[Any, ...]:
    return item.item_id, attempt


def _recipe_for(item: object) -> ExecutionRecipeEnvelope:
    raise AssertionError(f"not called by registry tests: {item!r}")


def _classify_error(error: BaseException) -> FailureSnapshot:
    return FailureSnapshot(
        failure_class=FailureClass.UNKNOWN,
        error_type=type(error).__name__,
        message=str(error),
    )


def _target(
    *,
    target_key: str = "generation",
    target_version: int = 1,
    queue_name: str = "generation-queue",
    managed_workflow_name: str = "generation-workflow-v1",
    workflow: Any = _workflow,
) -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name=queue_name,
        workflow_role="generation",
        managed_workflow_name=managed_workflow_name,
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
    )
    return ExecutionTarget(
        ref=declaration.target_ref(
            target_key=target_key,
            target_version=target_version,
        ),
        **declaration.model_dump(),
        workflow=workflow,
        execution_for=_execution_for,
        args_for=_args_for,
        recipe_for=_recipe_for,
        classify_error=_classify_error,
    )


def test_exact_duplicate_registration_retains_first() -> None:
    first = _target()
    duplicate = _target()
    registry = TargetRegistry()

    assert registry.register(first) is first
    assert registry.register(duplicate) is first
    assert registry.resolve(first.ref) is first


def test_duplicate_registration_rejects_a_different_workflow() -> None:
    first = _target()
    replacement_callable = lambda: None  # noqa: E731
    duplicate = _target(workflow=replacement_callable)
    registry = TargetRegistry()
    registry.register(first)

    with pytest.raises(TargetConflictError):
        registry.register(duplicate)


def test_fresh_registry_resolves_a_serialized_persisted_reference() -> None:
    startup_target = _target()
    persisted_ref = ExecutionTargetRef.model_validate_json(
        startup_target.ref.model_dump_json()
    )
    restarted_registry = TargetRegistry()
    restarted_registry.register(_target())

    assert restarted_registry.resolve(persisted_ref).ref == persisted_ref


def test_missing_target_fails_with_typed_unavailable_error() -> None:
    target_ref = _target().ref

    with pytest.raises(TargetUnavailableError) as caught:
        TargetRegistry().resolve(target_ref)

    assert caught.value.code is (TargetResolutionErrorCode.TARGET_UNAVAILABLE)
    assert caught.value.target_ref == target_ref


def test_digest_mismatch_fails_with_typed_conflict_error() -> None:
    target = _target()
    registry = TargetRegistry()
    registry.register(target)
    conflicting_ref = target.ref.model_copy(
        update={"target_contract_digest": "different-digest"}
    )

    with pytest.raises(TargetConflictError) as caught:
        registry.resolve(conflicting_ref)

    assert caught.value.code is TargetResolutionErrorCode.TARGET_CONFLICT


def test_registration_rejects_ref_that_does_not_match_declaration() -> None:
    target = _target().model_copy(
        update={
            "ref": ExecutionTargetRef(
                target_key="generation",
                target_version=1,
                target_contract_digest="forged-digest",
            )
        }
    )

    with pytest.raises(TargetConflictError) as caught:
        TargetRegistry().register(target)

    assert caught.value.code is TargetResolutionErrorCode.TARGET_CONFLICT


def test_same_key_and_version_rejects_a_changed_declaration() -> None:
    registry = TargetRegistry()
    registry.register(_target())
    changed = _target(queue_name="other-queue")

    with pytest.raises(TargetConflictError):
        registry.register(changed)


def test_managed_workflow_name_cannot_alias_another_target_ref() -> None:
    registry = TargetRegistry()
    registry.register(_target())
    other = _target(target_key="scoring", target_version=2)

    with pytest.raises(TargetConflictError):
        registry.register(other)


def test_register_all_is_atomic_on_conflict() -> None:
    first = _target()
    conflict = _target(
        target_key="scoring",
        target_version=1,
        managed_workflow_name=first.managed_workflow_name,
    )
    registry = TargetRegistry()

    with pytest.raises(TargetConflictError):
        registry.register_all((first, conflict))

    with pytest.raises(TargetUnavailableError):
        registry.resolve(first.ref)
