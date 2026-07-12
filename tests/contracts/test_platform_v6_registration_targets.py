"""Executable P2 Manifest registration and target-resolution contract."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Event, Lock
from typing import Any

import pytest
from dr_serialize import postgres_jsonb_limits, sha256_json_digest
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import event, func, select, text, update
from sqlalchemy.engine import Connection, Engine

from dr_platform import PlatformSchema, upgrade_platform_schema
from dr_platform import submission as submission_module
from dr_platform.items import SubmittableItem, item_id
from dr_platform.manifests import (
    MANIFEST_FORMAT_VERSION,
    ExecutionRecipeEnvelope,
    ExecutionTargetRef,
    ManifestPage,
    ManifestSource,
    OperationManifest,
)
from dr_platform.records import FailureSnapshot, ItemRecord
from dr_platform.status import (
    FailureClass,
    ItemInsertStatus,
    OperationStatus,
    ServiceClass,
    WorkflowTopology,
)
from dr_platform.submission import (
    EMPTY_SUBMISSION_REASON,
    REGISTRATION_ABANDONED_REASON,
    RegistrationAbandonedError,
    RegistrationConflictError,
    RegistrationIneligibleError,
    RegistrationIntegrityError,
    RegistrationItem,
    RegistrationItemResult,
    RegistrationLeaseHeldError,
    RegistrationResult,
    SubmitOptions,
    SubmitResult,
    abandon_registration,
    prepare_manifest,
    submit,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetConflictError,
    TargetContractDeclaration,
    TargetRegistry,
    TargetResolutionErrorCode,
    TargetUnavailableError,
)

TARGET_REF = ExecutionTargetRef(
    target_key="generation",
    target_version=1,
    target_contract_digest="target-contract-digest",
)


class ExampleItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: str
    spec: dict[str, Any]
    service_class: ServiceClass = ServiceClass.STANDARD


class MemoryManifestSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[ExampleItem, ...]

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


def _workflow() -> None:
    return None


def _other_workflow() -> None:
    return None


def _target(
    *,
    target_key: str = "generation",
    target_version: int = 1,
    queue_name: str = "generation-queue",
    managed_workflow_name: str = "generation_workflow",
    workflow: Callable[..., object] = _workflow,
) -> ExecutionTarget:
    declaration = TargetContractDeclaration(
        queue_name=queue_name,
        workflow_role="generation",
        managed_workflow_name=managed_workflow_name,
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
    )
    ref = declaration.target_ref(
        target_key=target_key,
        target_version=target_version,
    )
    return ExecutionTarget(
        ref=ref,
        **declaration.model_dump(),
        workflow=workflow,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"{item.item_key}:{attempt}",
            workflow_id=f"workflow:{item.item_key}:{attempt}",
        ),
        args_for=lambda item, attempt: (item.item_key, attempt),
        recipe_for=_raise_not_used,
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
    )


def _registration_target(
    *,
    registration_hook: Callable[..., RegistrationResult] | None = None,
) -> ExecutionTarget:
    hook_name = "register_predictions" if registration_hook else None
    hook_version = 1 if registration_hook else None
    declaration = TargetContractDeclaration(
        queue_name="generation-queue",
        workflow_role="generation",
        managed_workflow_name="generation_workflow",
        managed_workflow_version=1,
        argument_recipe_version=1,
        classifier_version=1,
        registration_hook_name=hook_name,
        registration_hook_version=hook_version,
    )
    ref = declaration.target_ref(target_key="generation", target_version=1)

    def recipe_for(item: SubmittableItem) -> ExecutionRecipeEnvelope:
        return ExecutionRecipeEnvelope(
            target_ref=ref,
            managed_workflow_name=declaration.managed_workflow_name,
            managed_workflow_version=declaration.managed_workflow_version,
            argument_recipe_version=declaration.argument_recipe_version,
            payload={"item_key": item.item_key, "spec": item.spec},
        )

    return ExecutionTarget(
        ref=ref,
        **declaration.model_dump(),
        workflow=_workflow,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"{item.item_key}:{attempt}",
            workflow_id=f"workflow:{item.item_key}:{attempt}",
        ),
        args_for=lambda item, attempt: (item.item_key, attempt),
        recipe_for=recipe_for,
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
        registration_hook=registration_hook,
    )


def _source(*item_keys: str) -> MemoryManifestSource:
    return MemoryManifestSource(
        items=tuple(
            ExampleItem(item_key=key, spec={"value": key}) for key in item_keys
        )
    )


def _prepare_registration(
    *,
    source: ManifestSource,
    target: ExecutionTarget,
    operation_key: str = "operation-1",
    page_size: int = 2,
) -> OperationManifest:
    return prepare_manifest(
        operation_key=operation_key,
        workflow_role="generation",
        group_key="experiment-1",
        target=target,
        source=source,
        options=SubmitOptions(page_size=page_size),
    )


def _registry(target: ExecutionTarget) -> TargetRegistry:
    registry = TargetRegistry()
    registry.register(target)
    return registry


def _upgrade_scratch_schema(engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(str(engine.url))
    return PlatformSchema()


def _try_operation_registration_lock(
    engine: Engine,
    *,
    operation_key: str,
) -> bool:
    with engine.begin() as connection:
        return bool(
            connection.scalar(
                text(
                    "SELECT pg_try_advisory_xact_lock("
                    "hashtextextended(:operation_key, 1))"
                ),
                {"operation_key": operation_key},
            )
        )


def _successful_hook(
    connection: Connection,
    *,
    operation_key: str,
    items: tuple[RegistrationItem, ...],
) -> RegistrationResult:
    del connection, operation_key
    return RegistrationResult(
        items=tuple(
            RegistrationItemResult(
                item_key=item.item_key,
                insert_status=ItemInsertStatus.INSERTED,
            )
            for item in items
        )
    )


def _raise_not_used(item: object) -> Any:
    raise AssertionError(f"recipe_for was not expected for {item!r}")


def _item(*, operation_key: str, item_key: str) -> ItemRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return ItemRecord(
        item_id=item_id(operation_key=operation_key, item_key=item_key),
        operation_key=operation_key,
        item_key=item_key,
        item_index=0,
        shuffle_rank=1,
        service_class=ServiceClass.STANDARD,
        service_priority=ServiceClass.STANDARD.priority,
        insert_status=ItemInsertStatus.INSERTED,
        created_at=now,
        updated_at=now,
        change_seq=1,
    )


def _build_manifest(
    *,
    operation_key: str = "operation-1",
    item_count: int = 3,
    page_size: int = 2,
    page_digests: tuple[str, ...] = ("page-0", "page-1"),
    items_digest: str = "items-digest",
) -> OperationManifest:
    pages: list[ManifestPage] = []
    for page_index, start_index in enumerate(range(0, item_count, page_size)):
        pages.append(
            ManifestPage(
                page_index=page_index,
                start_index=start_index,
                end_index=min(start_index + page_size, item_count),
                page_digest=page_digests[page_index],
            )
        )
    values: dict[str, Any] = {
        "format_version": MANIFEST_FORMAT_VERSION,
        "operation_key": operation_key,
        "workflow_role": "generation",
        "group_key": "experiment-1",
        "target_ref": TARGET_REF,
        "operation_execution_recipe_digest": "operation-recipe-digest",
        "item_count": item_count,
        "page_size": page_size,
        "items_digest": items_digest,
        "pages": tuple(pages),
    }
    unvalidated = OperationManifest.model_construct(
        **values,
        manifest_digest="pending",
    )
    return OperationManifest(
        **values,
        manifest_digest=unvalidated.expected_manifest_digest(),
    )


def _mutate_manifest(
    manifest: OperationManifest,
    mutate: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    values = manifest.model_dump()
    values["pages"] = list(values["pages"])
    mutate(values)
    return values


def _manifest_digest(values: dict[str, Any]) -> str:
    return sha256_json_digest(
        {
            key: value
            for key, value in values.items()
            if key != "manifest_digest"
        }
    )


def test_exact_manifest_replay_has_equal_identity() -> None:
    manifest = _build_manifest()
    replay = OperationManifest.model_validate(manifest.model_dump())

    assert replay == manifest
    assert replay.manifest_digest == manifest.manifest_digest


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda values: values["pages"].reverse(),
            id="reordered-pages",
        ),
        pytest.param(
            lambda values: values["pages"].pop(),
            id="truncated-pages",
        ),
        pytest.param(
            lambda values: values["pages"].append(
                {
                    "page_index": 2,
                    "start_index": 3,
                    "end_index": 4,
                    "page_digest": "page-2",
                }
            ),
            id="extended-pages",
        ),
        pytest.param(
            lambda values: values.__setitem__(
                "operation_execution_recipe_digest", "different-recipe"
            ),
            id="conflicting-operation-recipe",
        ),
        pytest.param(
            lambda values: values.__setitem__("group_key", "other-group"),
            id="conflicting-operation-field",
        ),
    ],
)
def test_manifest_mutation_cannot_reuse_an_issued_digest(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    manifest = _build_manifest()
    mutated_values = _mutate_manifest(manifest, mutate)

    with pytest.raises(ValidationError):
        OperationManifest.model_validate(mutated_values)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda values: values["pages"][1].__setitem__("start_index", 1),
            id="overlap",
        ),
        pytest.param(
            lambda values: values["pages"][1].__setitem__("start_index", 3),
            id="gap",
        ),
        pytest.param(
            lambda values: values["pages"][1].__setitem__("page_index", 3),
            id="noncontiguous-page-index",
        ),
        pytest.param(
            lambda values: values["pages"][0].__setitem__("end_index", 1),
            id="short-nonfinal-page",
        ),
    ],
)
def test_manifest_rejects_invalid_page_coverage_even_with_a_fresh_digest(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    manifest = _build_manifest()
    mutated_values = _mutate_manifest(manifest, mutate)
    mutated_values["manifest_digest"] = _manifest_digest(mutated_values)

    with pytest.raises(ValidationError):
        OperationManifest.model_validate(mutated_values)


def test_empty_manifest_has_no_page_descriptors() -> None:
    manifest = _build_manifest(
        item_count=0,
        page_size=500,
        page_digests=(),
        items_digest="empty-items-digest",
    )

    assert manifest.item_count == 0
    assert manifest.pages == ()


def test_item_identity_is_operation_local() -> None:
    first = item_id(operation_key="operation-1", item_key="prediction-1")
    replay = item_id(operation_key="operation-1", item_key="prediction-1")
    other_operation = item_id(
        operation_key="operation-2",
        item_key="prediction-1",
    )

    assert replay == first
    assert other_operation != first


def test_target_registration_and_exact_duplicate_are_idempotent() -> None:
    first = _target()
    duplicate_with_reconstructed_callables = _target()
    registry = TargetRegistry()

    assert registry.register(first) is first
    assert registry.register(duplicate_with_reconstructed_callables) is first
    assert registry.resolve(first.ref) is first


def test_fresh_registry_resolves_a_serialized_persisted_reference() -> None:
    original = _target()
    persisted_ref = ExecutionTargetRef.model_validate(
        original.ref.model_dump(mode="json")
    )
    reconstructed = _target()
    restarted_registry = TargetRegistry()

    restarted_registry.register(reconstructed)

    assert restarted_registry.resolve(persisted_ref) is reconstructed


def test_target_key_version_conflict_retains_original_registration() -> None:
    original = _target()
    conflicting = _target(queue_name="different-queue")
    registry = TargetRegistry()
    registry.register(original)

    with pytest.raises(TargetConflictError) as raised:
        registry.register(conflicting)

    assert raised.value.code is TargetResolutionErrorCode.TARGET_CONFLICT
    assert registry.resolve(original.ref) is original


def test_same_declaration_with_different_workflow_callable_conflicts() -> None:
    original = _target()
    conflicting = _target(workflow=_other_workflow)
    registry = TargetRegistry()
    registry.register(original)

    with pytest.raises(TargetConflictError):
        registry.register(conflicting)

    assert registry.resolve(original.ref) is original


def test_managed_workflow_name_cannot_belong_to_two_target_refs() -> None:
    original = _target()
    conflicting = _target(target_key="scoring", target_version=2)
    registry = TargetRegistry()
    registry.register(original)

    with pytest.raises(TargetConflictError):
        registry.register(conflicting)

    assert registry.resolve(original.ref) is original


def test_register_all_is_atomic_on_conflict() -> None:
    first = _target(target_key="first", managed_workflow_name="first_workflow")
    conflicting = _target(
        target_key="second",
        managed_workflow_name="first_workflow",
    )
    registry = TargetRegistry()

    with pytest.raises(TargetConflictError):
        registry.register_all((first, conflicting))

    with pytest.raises(TargetUnavailableError):
        registry.resolve(first.ref)


def test_unavailable_and_digest_mismatch_resolution_fail_closed() -> None:
    target = _target()
    registry = TargetRegistry()

    with pytest.raises(TargetUnavailableError) as unavailable:
        registry.resolve(target.ref)

    assert (
        unavailable.value.code is TargetResolutionErrorCode.TARGET_UNAVAILABLE
    )

    registry.register(target)
    forged_ref = target.ref.model_copy(
        update={"target_contract_digest": "forged-digest"}
    )
    with pytest.raises(TargetConflictError) as conflict:
        registry.resolve(forged_ref)

    assert conflict.value.code is TargetResolutionErrorCode.TARGET_CONFLICT


def test_registration_rejects_a_ref_that_does_not_match_declaration() -> None:
    target = _target()
    forged = target.model_copy(
        update={
            "ref": target.ref.model_copy(
                update={"target_contract_digest": "forged-digest"}
            )
        }
    )

    with pytest.raises(TargetConflictError):
        TargetRegistry().register(forged)


def test_target_declaration_rejects_non_top_level_topology() -> None:
    with pytest.raises(ValidationError):
        TargetContractDeclaration.model_validate(
            {
                "queue_name": "generation-queue",
                "workflow_role": "generation",
                "managed_workflow_name": "generation_workflow",
                "managed_workflow_version": 1,
                "topology": "nested",
                "argument_recipe_version": 1,
                "classifier_version": 1,
            }
        )

    assert tuple(WorkflowTopology) == (WorkflowTopology.TOP_LEVEL_ONLY,)


def test_content_execution_identity_deduplicates_across_operations() -> None:
    target = _target()
    first_item = _item(
        operation_key="operation-1",
        item_key="prediction-1",
    )
    second_item = _item(
        operation_key="operation-2",
        item_key="prediction-1",
    )

    assert first_item.item_id != second_item.item_id
    assert target.execution_for(first_item, 0) == target.execution_for(
        second_item,
        0,
    )
    assert target.execution_for(first_item, 1) != target.execution_for(
        second_item,
        0,
    )


def test_new_registration_and_exact_replay_apply_each_hook_page_once(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    hook_pages: list[tuple[str, ...]] = []

    def recording_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key
        hook_pages.append(tuple(item.item_key for item in items))
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=recording_hook)
    source = _source("item-1", "item-2", "item-3")
    manifest = _prepare_registration(source=source, target=target)
    registry = _registry(target)

    first = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )
    replay = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )

    assert first == replay
    assert first.registration_cursor == 2
    assert first.status is OperationStatus.ENQUEUEING
    assert hook_pages == [("item-1", "item-2"), ("item-3",)]
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 3
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 3
        )


def _successful_hook_result(
    items: tuple[RegistrationItem, ...],
) -> RegistrationResult:
    return RegistrationResult(
        items=tuple(
            RegistrationItemResult(
                item_key=item.item_key,
                insert_status=ItemInsertStatus.INSERTED,
            )
            for item in items
        )
    )


@pytest.mark.parametrize(
    ("source_keys", "conflicting_spec"),
    [
        pytest.param(("item-2", "item-1"), None, id="reordered"),
        pytest.param(("item-1",), None, id="truncated"),
        pytest.param(
            ("item-1", "item-2", "item-3"),
            None,
            id="extended",
        ),
        pytest.param(
            ("item-1", "item-2"),
            {"different": True},
            id="immutable-operation-spec",
        ),
    ],
)
def test_changed_replay_conflicts_before_hook(
    pg_engine: Engine,
    source_keys: tuple[str, ...],
    conflicting_spec: dict[str, Any] | None,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    hook_calls = 0

    def counting_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        nonlocal hook_calls
        del connection, operation_key
        hook_calls += 1
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=counting_hook)
    original_source = _source("item-1", "item-2")
    manifest = _prepare_registration(source=original_source, target=target)
    registry = _registry(target)
    submit(
        manifest,
        original_source,
        engine=pg_engine,
        resolver=registry,
        spec={"stable": True},
        schema=schema,
    )
    initial_hook_calls = hook_calls

    with pytest.raises(RegistrationConflictError):
        submit(
            manifest,
            _source(*source_keys),
            engine=pg_engine,
            resolver=registry,
            spec=conflicting_spec or {"stable": True},
            schema=schema,
        )

    assert hook_calls == initial_hook_calls


def test_empty_manifest_is_failed_without_invoking_hook(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)

    def forbidden_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key, items
        raise AssertionError("empty registration must not invoke its hook")

    target = _registration_target(registration_hook=forbidden_hook)
    source = _source()
    manifest = _prepare_registration(source=source, target=target)

    result = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=_registry(target),
        schema=schema,
    )

    assert result.status is OperationStatus.FAILED
    with pg_engine.connect() as connection:
        row = connection.execute(select(schema.operations)).mappings().one()
        assert row["terminal_reason"] == EMPTY_SUBMISSION_REASON
        assert row["registration_lease_id"] is None
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 0
        )


def test_hook_accounting_conflict_rolls_back_the_complete_page(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)

    def malformed_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key
        return RegistrationResult(
            items=(
                RegistrationItemResult(
                    item_key=items[-1].item_key,
                    insert_status=ItemInsertStatus.INSERTED,
                ),
            )
        )

    target = _registration_target(registration_hook=malformed_hook)
    source = _source("item-1", "item-2")
    manifest = _prepare_registration(source=source, target=target)

    with pytest.raises(RegistrationIntegrityError):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
        assert operation["registration_cursor"] == 0
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 0
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 0
        )


def test_hook_already_present_accounting_commits_idempotently(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)

    def already_present_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key
        return RegistrationResult(
            items=tuple(
                RegistrationItemResult(
                    item_key=item.item_key,
                    insert_status=ItemInsertStatus.ALREADY_PRESENT,
                )
                for item in items
            )
        )

    target = _registration_target(registration_hook=already_present_hook)
    source = _source("item-1", "item-2")
    manifest = _prepare_registration(source=source, target=target)

    result = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=_registry(target),
        schema=schema,
    )

    assert result.inserted_count == 0
    assert result.already_present_count == 2
    assert result.status is OperationStatus.ENQUEUEING


def test_registration_cursor_cas_rejects_authority_lost_during_hook(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)

    def authority_loss_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        # A legitimate hook cannot mutate Platform rows. This adversarial
        # mutation models authority changing between the row check and CAS
        # deterministically, without wall-clock sleeps.
        connection.execute(
            update(schema.operations)
            .where(schema.operations.c.operation_key == operation_key)
            .values(
                registration_lease_expires_at=datetime(
                    2000,
                    1,
                    1,
                    tzinfo=UTC,
                )
            )
        )
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=authority_loss_hook)
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)

    with pytest.raises(
        RegistrationIntegrityError,
        match="cursor CAS lost",
    ):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
        assert operation["registration_cursor"] == 0
        assert operation["registration_lease_expires_at"].year != 2000
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 0
        )


def test_live_lease_blocks_competing_submit_and_abandonment(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)

    def crashing_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key, items
        raise RuntimeError("simulated registrar crash")

    target = _registration_target(registration_hook=crashing_hook)
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    registry = _registry(target)
    with pytest.raises(RuntimeError, match="simulated registrar crash"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )

    with pytest.raises(RegistrationLeaseHeldError):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )
    with pytest.raises(RegistrationLeaseHeldError):
        abandon_registration(
            manifest.operation_key,
            engine=pg_engine,
            abandoned_by="operator",
            reason="stop",
            operator_confirmed=True,
            schema=schema,
        )


def test_expired_lease_resumes_after_a_committed_page(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    hook_call_count = 0

    def second_page_crashes(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        nonlocal hook_call_count
        del connection, operation_key
        hook_call_count += 1
        if hook_call_count == 2:
            raise RuntimeError("simulated second-page crash")
        return _successful_hook_result(items)

    first_target = _registration_target(registration_hook=second_page_crashes)
    source = _source("item-1", "item-2", "item-3")
    manifest = _prepare_registration(source=source, target=first_target)
    with pytest.raises(RuntimeError, match="second-page crash"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(first_target),
            schema=schema,
        )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.operations).values(
                registration_lease_expires_at=datetime(2000, 1, 1, tzinfo=UTC)
            )
        )

    resumed_pages: list[tuple[str, ...]] = []

    def resumed_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del connection, operation_key
        resumed_pages.append(tuple(item.item_key for item in items))
        return _successful_hook_result(items)

    resumed_target = _registration_target(registration_hook=resumed_hook)
    result = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=_registry(resumed_target),
        schema=schema,
    )

    assert result.registration_cursor == 2
    assert resumed_pages == [("item-3",)]
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 3
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 3
        )


def test_abandonment_after_expiry_is_sticky_and_preserves_committed_rows(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    hook_call_count = 0

    def second_page_crashes(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        nonlocal hook_call_count
        del connection, operation_key
        hook_call_count += 1
        if hook_call_count == 2:
            raise RuntimeError("simulated second-page crash")
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=second_page_crashes)
    source = _source("item-1", "item-2", "item-3")
    manifest = _prepare_registration(source=source, target=target)
    registry = _registry(target)
    with pytest.raises(RuntimeError, match="second-page crash"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.operations).values(
                registration_lease_expires_at=datetime(2000, 1, 1, tzinfo=UTC)
            )
        )

    first = abandon_registration(
        manifest.operation_key,
        engine=pg_engine,
        abandoned_by="operator",
        reason="source unavailable",
        operator_confirmed=True,
        schema=schema,
    )
    replay = abandon_registration(
        manifest.operation_key,
        engine=pg_engine,
        abandoned_by="operator",
        reason="source unavailable",
        operator_confirmed=True,
        schema=schema,
    )
    with pytest.raises(RegistrationConflictError):
        abandon_registration(
            manifest.operation_key,
            engine=pg_engine,
            abandoned_by="different-operator",
            reason="different reason",
            operator_confirmed=True,
            schema=schema,
        )

    assert first == replay
    assert first.committed_count == 2
    assert first.remaining_count == 1
    with pytest.raises(RegistrationAbandonedError):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=registry,
            schema=schema,
        )
    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
        assert operation["terminal_reason"] == REGISTRATION_ABANDONED_REASON
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 2
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 2
        )


def test_completed_registration_cannot_be_abandoned(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    target = _registration_target(registration_hook=_successful_hook)
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=_registry(target),
        schema=schema,
    )

    with pytest.raises(RegistrationIneligibleError):
        abandon_registration(
            manifest.operation_key,
            engine=pg_engine,
            abandoned_by="operator",
            reason="too late",
            operator_confirmed=True,
            schema=schema,
        )


def test_concurrent_initial_submitters_serialize_without_unique_violation(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    target = _registration_target()
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    registry = _registry(target)
    absent_row_select_reached = Event()
    release_absent_row_select = Event()
    contender_started = Event()
    select_lock = Lock()
    first_select_blocked = False

    def block_first_operation_select(  # noqa: PLR0913
        connection: Connection,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,  # noqa: FBT001 -- SQLAlchemy event signature
    ) -> None:
        nonlocal first_select_blocked
        del connection, cursor, parameters, context, executemany
        if "FROM platform_operations" not in statement:
            return
        if "FOR UPDATE" not in statement:
            return
        with select_lock:
            should_block = not first_select_blocked
            if should_block:
                first_select_blocked = True
        if should_block:
            absent_row_select_reached.set()
            assert release_absent_row_select.wait(timeout=10)

    def run_submit(*, competitor: bool) -> SubmitResult | BaseException:
        if competitor:
            contender_started.set()
        try:
            return submit(
                manifest,
                source,
                engine=pg_engine,
                resolver=registry,
                schema=schema,
            )
        except RegistrationLeaseHeldError as exc:
            return exc

    executor = ThreadPoolExecutor(max_workers=2)
    event.listen(
        pg_engine, "before_cursor_execute", block_first_operation_select
    )
    try:
        winner_future = executor.submit(run_submit, competitor=False)
        assert absent_row_select_reached.wait(timeout=10)

        operation_lock_available = _try_operation_registration_lock(
            pg_engine,
            operation_key=manifest.operation_key,
        )
        assert operation_lock_available is False

        contender_future = executor.submit(run_submit, competitor=True)
        assert contender_started.wait(timeout=10)
        release_absent_row_select.set()
        winner = winner_future.result(timeout=15)
        contender = contender_future.result(timeout=15)
    finally:
        release_absent_row_select.set()
        executor.shutdown(wait=True)
        event.remove(
            pg_engine,
            "before_cursor_execute",
            block_first_operation_select,
        )

    assert isinstance(winner, SubmitResult)
    assert isinstance(contender, (SubmitResult, RegistrationLeaseHeldError))
    replay = submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=registry,
        schema=schema,
    )
    assert replay.status is OperationStatus.ENQUEUEING
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.operations)
            )
            == 1
        )


def test_jsonb_exact_replay_distinguishes_boolean_from_integer(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    target = _registration_target()
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    registry = _registry(target)
    submit(
        manifest,
        source,
        engine=pg_engine,
        resolver=registry,
        spec={"canonical-axis": True},
        schema=schema,
    )

    with pytest.raises(RegistrationConflictError, match="spec"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=registry,
            spec={"canonical-axis": 1},
            schema=schema,
        )


@pytest.mark.parametrize(
    ("field", "mismatched_value"),
    [
        pytest.param(
            "target_ref",
            ExecutionTargetRef(
                target_key="other",
                target_version=1,
                target_contract_digest="other-contract",
            ),
            id="target-ref",
        ),
        pytest.param(
            "managed_workflow_name",
            "other_workflow",
            id="managed-workflow-name",
        ),
        pytest.param(
            "managed_workflow_version",
            2,
            id="managed-workflow-version",
        ),
        pytest.param("topology", "nested", id="topology"),
        pytest.param(
            "argument_recipe_version",
            2,
            id="argument-recipe-version",
        ),
    ],
)
def test_prepare_manifest_rejects_recipe_envelope_target_mismatch(
    field: str,
    mismatched_value: object,
) -> None:
    target = _registration_target()
    original_recipe_for = target.recipe_for

    def mismatched_recipe_for(
        item: SubmittableItem,
    ) -> ExecutionRecipeEnvelope:
        recipe = original_recipe_for(item)
        return recipe.model_copy(update={field: mismatched_value})

    mismatched_target = target.model_copy(
        update={"recipe_for": mismatched_recipe_for}
    )

    with pytest.raises(RegistrationConflictError, match=field):
        _prepare_registration(
            source=_source("item-1"),
            target=mismatched_target,
        )


def test_submit_options_page_size_defines_manifest_page_descriptors() -> None:
    target = _registration_target()
    source = _source("item-1", "item-2", "item-3", "item-4", "item-5")

    manifest = prepare_manifest(
        operation_key="operation-page-size",
        workflow_role="generation",
        group_key="experiment-1",
        target=target,
        source=source,
        options=SubmitOptions(page_size=2),
    )

    assert manifest.page_size == 2
    assert tuple(
        (page.start_index, page.end_index) for page in manifest.pages
    ) == ((0, 2), (2, 4), (4, 5))


def test_hook_nested_input_mutation_rolls_back_domain_and_kernel_rows(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE registration_hook_domain "
                "(marker TEXT PRIMARY KEY)"
            )
        )

    def mutating_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        del operation_key
        connection.execute(
            text(
                "INSERT INTO registration_hook_domain (marker) "
                "VALUES ('hook-was-here')"
            )
        )
        items[0].spec["nested"]["value"] = "mutated-spec"
        items[0].execution_recipe.payload["spec"]["nested"]["value"] = (
            "mutated-recipe"
        )
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=mutating_hook)
    source = MemoryManifestSource(
        items=(
            ExampleItem(
                item_key="item-1",
                spec={"nested": {"value": "original"}},
            ),
        )
    )
    manifest = _prepare_registration(source=source, target=target)

    with pytest.raises(
        RegistrationIntegrityError,
        match="mutated its frozen page inputs",
    ):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
        assert operation["registration_cursor"] == 0
        assert (
            connection.scalar(
                text("SELECT count(*) FROM registration_hook_domain")
            )
            == 0
        )
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 0
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 0
        )


@pytest.mark.parametrize("field", ["spec", "metadata"])
@pytest.mark.parametrize("failure_mode", ["oversized", "over-depth"])
def test_invalid_operation_payload_fails_before_operation_creation(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    failure_mode: str,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    if failure_mode == "oversized":
        test_max_bytes = 64
        monkeypatch.setattr(
            submission_module,
            "POSTGRES_JSONB_PAYLOAD_MAX_BYTES",
            test_max_bytes,
        )
        invalid_payload: dict[str, Any] = {"blob": "x" * (test_max_bytes + 1)}
    else:
        nested: Any = "leaf"
        for _ in range(postgres_jsonb_limits().max_depth + 2):
            nested = {"nested": nested}
        invalid_payload = {"root": nested}

    target = _registration_target()
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    submit_kwargs: dict[str, Any] = {field: invalid_payload}

    with pytest.raises(ValidationError, match=f"operation {field}"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
            **submit_kwargs,
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.operations)
            )
            == 0
        )


def test_manual_manifest_oversized_item_fails_before_operation_creation(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    test_max_bytes = 64
    monkeypatch.setattr(
        submission_module,
        "POSTGRES_JSONB_PAYLOAD_MAX_BYTES",
        test_max_bytes,
    )
    target = _registration_target()
    source = MemoryManifestSource(
        items=(
            ExampleItem(
                item_key="item-1",
                spec={"blob": "x" * (test_max_bytes + 1)},
            ),
        )
    )
    page = ManifestPage(
        page_index=0,
        start_index=0,
        end_index=1,
        page_digest="manually-issued-page-digest",
    )
    values: dict[str, Any] = {
        "format_version": MANIFEST_FORMAT_VERSION,
        "operation_key": "manual-manifest-operation",
        "workflow_role": target.workflow_role,
        "group_key": "experiment-1",
        "target_ref": target.ref,
        "operation_execution_recipe_digest": "manual-operation-recipe",
        "item_count": 1,
        "page_size": 1,
        "items_digest": "manually-issued-items-digest",
        "pages": (page,),
    }
    pending = OperationManifest.model_construct(
        **values,
        manifest_digest="pending",
    )
    manifest = OperationManifest(
        **values,
        manifest_digest=pending.expected_manifest_digest(),
    )

    with pytest.raises(ValidationError, match="Item spec"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.operations)
            )
            == 0
        )


def test_submit_rejects_options_page_size_different_from_manifest(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    target = _registration_target()
    source = _source("item-1", "item-2", "item-3")
    manifest = _prepare_registration(
        source=source,
        target=target,
        page_size=2,
    )

    with pytest.raises(
        RegistrationConflictError,
        match="page_size must match the frozen Manifest",
    ):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            options=SubmitOptions(page_size=3),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.operations)
            )
            == 0
        )


def test_hook_expired_lease_fails_fresh_clock_cas_and_rolls_back(
    pg_engine: Engine,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    with pg_engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE expiring_hook_domain (marker TEXT PRIMARY KEY)")
        )
        connection.execute(
            text(
                """
                CREATE FUNCTION expire_registration_lease_on_item_insert()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    UPDATE platform_operations
                    SET registration_lease_expires_at = clock_timestamp()
                    WHERE operation_key = NEW.operation_key;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER expire_registration_lease_before_item
                BEFORE INSERT ON platform_items
                FOR EACH ROW
                EXECUTE FUNCTION expire_registration_lease_on_item_insert()
                """
            )
        )

    def expiring_hook(
        connection: Connection,
        *,
        operation_key: str,
        items: tuple[RegistrationItem, ...],
    ) -> RegistrationResult:
        connection.execute(
            text(
                "INSERT INTO expiring_hook_domain (marker) "
                "VALUES ('hook-was-here')"
            )
        )
        del operation_key
        return _successful_hook_result(items)

    target = _registration_target(registration_hook=expiring_hook)
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)

    with pytest.raises(
        RegistrationIntegrityError,
        match="cursor CAS lost",
    ):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        operation = (
            connection.execute(select(schema.operations)).mappings().one()
        )
        assert operation["registration_cursor"] == 0
        assert (
            connection.scalar(
                text("SELECT count(*) FROM expiring_hook_domain")
            )
            == 0
        )
        assert (
            connection.scalar(select(func.count()).select_from(schema.items))
            == 0
        )
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.item_attempts)
            )
            == 0
        )


@pytest.mark.parametrize(
    "invalid_version",
    [pytest.param("", id="empty"), pytest.param(42, id="wrong-type")],
)
def test_invalid_source_application_version_fails_before_operation_creation(
    pg_engine: Engine,
    invalid_version: object,
) -> None:
    schema = _upgrade_scratch_schema(pg_engine)
    target = _registration_target()
    source = _source("item-1")
    manifest = _prepare_registration(source=source, target=target)
    submit_kwargs: dict[str, Any] = {
        "source_application_version": invalid_version
    }

    with pytest.raises(ValidationError, match="source_application_version"):
        submit(
            manifest,
            source,
            engine=pg_engine,
            resolver=_registry(target),
            schema=schema,
            **submit_kwargs,
        )

    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                select(func.count()).select_from(schema.operations)
            )
            == 0
        )
