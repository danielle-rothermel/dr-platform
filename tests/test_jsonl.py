"""JSONL Manifest preflight and adapter contract tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from dr_serialize import Jsonable, canonical_json
from sqlalchemy import Engine

import dr_platform.jsonl as jsonl_module
import dr_platform.submission as submission_module
from dr_platform.jsonl import (
    JsonlFieldNames,
    index_jsonl_manifest_source,
    prepare_jsonl_manifest,
    submit_jsonl,
)
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.records import FailureSnapshot
from dr_platform.status import FailureClass, OperationStatus, ServiceClass
from dr_platform.submission import (
    RegistrationConflictError,
    SubmitOptions,
    SubmitResult,
    prepare_manifest,
    submit,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
    TargetRegistry,
    TargetResolver,
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

    def recipe_for(item: Any) -> ExecutionRecipeEnvelope:
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
        workflow=lambda: None,
        execution_for=lambda item, attempt: ExecutionIdentity(
            execution_key=f"{item.item_id}:{attempt}",
            workflow_id=f"workflow:{item.item_id}:{attempt}",
        ),
        args_for=lambda item, attempt: (item.item_id, attempt),
        recipe_for=recipe_for,
        classify_error=lambda error: FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=type(error).__name__,
            message=str(error),
        ),
    )


def _write_rows(
    path: Path,
    rows: list[Jsonable],
    *,
    prefix: str = "",
) -> None:
    content = prefix + "".join(
        f"{canonical_json(row)}\n" for row in rows
    )
    path.write_text(content, encoding="utf-8")


def _rows() -> list[Jsonable]:
    return [
        {"item_key": "item-a", "group_key": "experiment", "value": 1},
        {"item_key": "item-b", "group_key": "experiment", "value": 2},
    ]


def test_preflight_preserves_original_nonempty_record_indexes(
    tmp_path: Path,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows(), prefix="\n  \n")

    source = index_jsonl_manifest_source(path, group_key="experiment")

    assert [ref.item_index for ref in source.refs] == [0, 1]
    assert source.read_items(start_index=0, end_index=2)[1].item_key == (
        "item-b"
    )


def test_path_and_byte_offsets_do_not_enter_manifest_identity(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "nested" / "second.jsonl"
    second.parent.mkdir()
    _write_rows(first, _rows())
    _write_rows(second, _rows(), prefix="\n\n")
    target = _target()

    first_manifest = prepare_jsonl_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        path=first,
        options=SubmitOptions(page_size=1),
    )
    second_manifest = prepare_jsonl_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        path=second,
        options=SubmitOptions(page_size=1),
    )

    assert first_manifest == second_manifest


def test_custom_fields_map_nested_spec_and_service_class(
    tmp_path: Path,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(
        path,
        [
            {
                "prediction_id": "prediction-1",
                "experiment": "experiment",
                "priority": "urgent",
                "payload": {"prompt": "hello"},
            }
        ],
    )
    fields = JsonlFieldNames(
        item_key="prediction_id",
        group_key="experiment",
        service_class="priority",
        spec="payload",
    )

    item = index_jsonl_manifest_source(
        path,
        group_key="experiment",
        fields=fields,
    ).read_items(start_index=0, end_index=1)[0]

    assert item.item_key == "prediction-1"
    assert item.service_class is ServiceClass.URGENT
    assert item.spec == {"prompt": "hello"}


@pytest.mark.parametrize(
    "row",
    [
        "not-an-object",
        {"group_key": "experiment"},
        {"item_key": "item", "group_key": 3},
    ],
)
def test_preflight_rejects_malformed_item_shape(
    tmp_path: Path,
    row: Jsonable,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, [row])

    with pytest.raises(ValueError, match="JSONL"):
        index_jsonl_manifest_source(path, group_key="experiment")


def test_preflight_rejects_malformed_json(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text('{"item_key":', encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSONL Item JSON"):
        index_jsonl_manifest_source(path, group_key="experiment")


def test_preflight_rejects_multiple_operation_groups(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    rows: list[Jsonable] = [
        {"item_key": "item-a", "group_key": "experiment"},
        {"item_key": "item-b", "group_key": "other"},
    ]
    _write_rows(path, rows)

    with pytest.raises(ValueError, match="must match Operation group_key"):
        index_jsonl_manifest_source(path, group_key="experiment")


def test_reread_rejects_changed_descriptor(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows())
    source = index_jsonl_manifest_source(path, group_key="experiment")
    changed: list[Jsonable] = [
        {"item_key": "changed", "group_key": "experiment", "value": 1},
        {"item_key": "item-b", "group_key": "experiment", "value": 2},
    ]
    _write_rows(path, changed)

    with pytest.raises(RegistrationConflictError, match="source changed"):
        source.read_items(start_index=0, end_index=1)


def test_reread_rejects_record_appended_after_preflight(
    tmp_path: Path,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows())
    source = index_jsonl_manifest_source(path, group_key="experiment")
    with path.open("a", encoding="utf-8") as file:
        file.write(
            canonical_json(
                {"item_key": "item-c", "group_key": "experiment"}
            )
            + "\n"
        )

    with pytest.raises(RegistrationConflictError, match="source changed"):
        source.read_items(start_index=0, end_index=1)


def test_submit_jsonl_delegates_to_submit_with_fresh_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows())
    target = _target()
    manifest = prepare_jsonl_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        path=path,
        options=SubmitOptions(page_size=1),
    )
    captured: dict[str, Any] = {}
    expected = SubmitResult(
        operation_key="operation",
        status=OperationStatus.ENQUEUEING,
        requested_count=2,
        registration_cursor=2,
        inserted_count=2,
        already_present_count=0,
        enqueued_count=0,
        workflow_already_present_count=0,
        enqueue_failed_count=0,
        total_failure_count=0,
    )

    def fake_submit(manifest_arg: Any, source_arg: Any, **kwargs: Any) -> Any:
        captured.update(
            manifest=manifest_arg,
            source=source_arg,
            kwargs=kwargs,
        )
        return expected

    monkeypatch.setattr("dr_platform.jsonl.submit", fake_submit)

    result = submit_jsonl(
        manifest,
        path,
        engine=cast("Engine", object()),
        resolver=cast("TargetResolver", object()),
    )

    assert result is expected
    assert captured["manifest"] is manifest
    assert captured["source"].item_count == 2
    assert captured["kwargs"]["options"].page_size == 1


def test_complete_source_hash_count_is_independent_of_page_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "items.jsonl"
    rows: list[Jsonable] = [
        {
            "item_key": f"item-{index}",
            "group_key": "experiment",
            "value": index,
        }
        for index in range(5)
    ]
    _write_rows(path, rows)
    target = _target()
    source = index_jsonl_manifest_source(path, group_key="experiment")
    manifest = prepare_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        source=source,
        options=SubmitOptions(page_size=1),
    )

    original_source_cut = jsonl_module._source_cut
    source_cut_calls = 0

    def counted_source_cut(path_arg: Path) -> tuple[str, int]:
        nonlocal source_cut_calls
        source_cut_calls += 1
        return original_source_cut(path_arg)

    monkeypatch.setattr(jsonl_module, "_source_cut", counted_source_cut)

    submission_module._validate_source(
        manifest=manifest,
        source=source,
        target=target,
    )

    assert len(manifest.pages) == 5
    assert source_cut_calls == 2


def test_empty_source_append_fails_before_platform_row(tmp_path: Path) -> None:
    path = tmp_path / "empty.jsonl"
    path.write_text("", encoding="utf-8")
    target = _target()
    source = index_jsonl_manifest_source(path, group_key="experiment")
    manifest = prepare_manifest(
        operation_key="empty-operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        source=source,
        options=SubmitOptions(page_size=1),
    )
    registry = TargetRegistry()
    registry.register(target)
    with path.open("a", encoding="utf-8") as file:
        file.write(
            canonical_json(
                {"item_key": "late-item", "group_key": "experiment"}
            )
            + "\n"
        )

    with pytest.raises(RegistrationConflictError, match="source changed"):
        submit(
            manifest,
            source,
            engine=cast("Engine", object()),
            resolver=registry,
            options=SubmitOptions(page_size=1),
        )


def test_submit_jsonl_rejects_page_size_drift(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows())
    manifest = prepare_jsonl_manifest(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=_target(),
        path=path,
        options=SubmitOptions(page_size=1),
    )

    with pytest.raises(RegistrationConflictError, match="page_size"):
        submit_jsonl(
            manifest,
            path,
            engine=cast("Engine", object()),
            resolver=cast("TargetResolver", object()),
            options=SubmitOptions(page_size=2),
        )
