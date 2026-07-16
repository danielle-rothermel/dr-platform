"""Single-read JSONL source and submission adapter tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from dr_serialize import Jsonable, canonical_json
from sqlalchemy import Engine

from dr_platform.jsonl import (
    JsonlFieldNames,
    read_jsonl_source,
    submit_jsonl,
)
from dr_platform.manifests import ExecutionRecipeEnvelope
from dr_platform.records import FailureSnapshot
from dr_platform.status import FailureClass, OperationStatus, ServiceClass
from dr_platform.submission import (
    SubmitOptions,
    SubmitResult,
)
from dr_platform.targets import (
    ExecutionIdentity,
    ExecutionTarget,
    TargetContractDeclaration,
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
    content = prefix + "".join(f"{canonical_json(row)}\n" for row in rows)
    path.write_text(content, encoding="utf-8")


def _rows() -> list[Jsonable]:
    return [
        {"item_key": "item-a", "group_key": "experiment", "value": 1},
        {"item_key": "item-b", "group_key": "experiment", "value": 2},
    ]


def test_read_preserves_nonempty_record_order(
    tmp_path: Path,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows(), prefix="\n  \n")

    source = read_jsonl_source(path, group_key="experiment")

    assert tuple(
        item.item_key for item in source.read_items(start_index=0, end_index=2)
    ) == ("item-a", "item-b")


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

    item = read_jsonl_source(
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
def test_read_rejects_malformed_item_shape(
    tmp_path: Path,
    row: Jsonable,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, [row])

    with pytest.raises(ValueError, match="JSONL"):
        read_jsonl_source(path, group_key="experiment")


def test_read_rejects_malformed_json(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text('{"item_key":', encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSONL Item JSON"):
        read_jsonl_source(path, group_key="experiment")


def test_read_rejects_multiple_operation_groups(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    rows: list[Jsonable] = [
        {"item_key": "item-a", "group_key": "experiment"},
        {"item_key": "item-b", "group_key": "other"},
    ]
    _write_rows(path, rows)

    with pytest.raises(ValueError, match="must match Operation group_key"):
        read_jsonl_source(path, group_key="experiment")


def test_submit_jsonl_materializes_file_once_before_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "items.jsonl"
    _write_rows(path, _rows())
    target = _target()
    file_reads = 0
    submitted_pages: list[tuple[str, ...]] = []
    expected = SubmitResult(
        operation_key="operation",
        status=OperationStatus.ENQUEUEING,
        requested_count=2,
        registration_cursor=2,
        inserted_count=2,
        enqueued_count=0,
        workflow_already_present_count=0,
        enqueue_failed_count=0,
        total_failure_count=0,
    )
    original_open = Path.open

    def tracked_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal file_reads
        if self == path:
            file_reads += 1
        return original_open(self, *args, **kwargs)

    def fake_submit(**kwargs: Any) -> SubmitResult:
        source = kwargs["source"]
        submitted_pages.extend(
            tuple(
                item.item_key
                for item in source.read_items(
                    start_index=start,
                    end_index=min(start + 1, source.item_count),
                )
            )
            for start in range(source.item_count)
        )
        return expected

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr("dr_platform.jsonl.submit", fake_submit)

    result = submit_jsonl(
        operation_key="operation",
        workflow_role="generation",
        group_key="experiment",
        target=target,
        path=path,
        engine=cast("Engine", object()),
        resolver=cast("TargetResolver", object()),
        options=SubmitOptions(page_size=1),
    )

    assert result is expected
    assert file_reads == 1
    assert submitted_pages == [("item-a",), ("item-b",)]
