"""Claim/lease batch submission against a real scratch Postgres."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel
from sqlalchemy import Engine, select, text

from dr_platform import (
    BatchItemEnqueueStatus,
    BatchOperationStatus,
    EnqueueOutcome,
    PlatformSchema,
    batch_item_id,
    submit_batch,
    submit_batch_jsonl,
    upgrade_platform_schema,
)
from dr_platform.submission import (
    claim_pending_batch_item,
    load_batch_operation,
)


class WorkItem(BaseModel):
    name: str
    group: str = "exp"

    @property
    def item_id(self) -> str:
        return f"work-{self.name}"

    @property
    def order_key(self) -> str:
        return f"order-{self.name}"

    @property
    def group_key(self) -> str:
        return self.group


class RecordingEnqueue:
    def __init__(
        self,
        *,
        fail_items: frozenset[str] = frozenset(),
    ) -> None:
        self.calls: list[str] = []
        self.fail_items = fail_items

    def __call__(self, item_id: str) -> EnqueueOutcome:
        self.calls.append(item_id)
        if item_id in self.fail_items:
            raise RuntimeError(f"enqueue exploded for {item_id}")
        return EnqueueOutcome(
            workflow_id=f"wf:{item_id}",
            enqueued=True,
            metadata={"run_id": f"run:{item_id}"},
        )


@pytest.fixture
def schema(pg_engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(str(pg_engine.url))
    return PlatformSchema()


def _items(count: int) -> list[WorkItem]:
    return [WorkItem(name=f"{index:02d}") for index in range(count)]


def test_submit_batch_happy_path(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    enqueue = RecordingEnqueue()
    seeded: list[tuple[str, ...]] = []

    def seed(connection: Any, window: Any) -> set[str]:
        seeded.append(tuple(item.item_id for item in window))
        # Simulate: all domain rows new except work-01.
        return {item.item_id for item in window} - {"work-01"}

    result = submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(5),
        enqueue=enqueue,
        schema=schema,
        seed=seed,
        chunk_size=2,
    )

    assert result.requested_count == 5
    assert result.enqueued_count == 5
    assert result.inserted_count == 4
    assert result.already_present_count == 1
    assert result.failed_count == 0
    # Fair order respected in enqueue order.
    assert enqueue.calls == sorted(f"work-{i:02d}" for i in range(5))
    # Seed hook saw each window inside its registration transaction.
    assert [len(window) for window in seeded] == [2, 2, 1]

    with pg_engine.connect() as connection:
        operation = load_batch_operation(
            connection,
            operation_key="op-1",
            schema=schema,
        )
        rows = (
            connection.execute(
                select(schema.batch_items).order_by(
                    schema.batch_items.c.item_index
                )
            )
            .mappings()
            .all()
        )
    assert operation is not None
    assert operation.status is BatchOperationStatus.COMPLETED
    assert operation.completed_at is not None
    first = dict(rows[0])
    assert first["batch_submit_item_id"] == batch_item_id(
        operation_key="op-1",
        item_id="work-00",
        identity=schema.naming.identity,
    )
    assert first["enqueue_metadata"] == {
        "workflow_id": "wf:work-00",
        "run_id": "run:work-00",
    }


def test_resubmit_is_idempotent_and_skips_terminal_items(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    first_enqueue = RecordingEnqueue()
    submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(3),
        enqueue=first_enqueue,
        schema=schema,
    )

    second_enqueue = RecordingEnqueue()
    result = submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(3),
        enqueue=second_enqueue,
        schema=schema,
    )

    assert second_enqueue.calls == []  # nothing re-bought
    assert result.enqueued_count == 3
    with pg_engine.connect() as connection:
        operation = load_batch_operation(
            connection,
            operation_key="op-1",
            schema=schema,
        )
    assert operation is not None
    assert operation.status is BatchOperationStatus.COMPLETED


def test_failed_enqueues_record_failure_and_retry_on_resubmit(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    failing = RecordingEnqueue(fail_items=frozenset({"work-01"}))
    result = submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(3),
        enqueue=failing,
        schema=schema,
    )
    assert result.failed_count == 1
    failed_item = next(
        item
        for item in result.items
        if item.enqueue_status is BatchItemEnqueueStatus.FAILED
    )
    assert failed_item.failure is not None
    assert failed_item.failure.error_type == "builtins.RuntimeError"

    healthy = RecordingEnqueue()
    retried = submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(3),
        enqueue=healthy,
        schema=schema,
    )
    assert healthy.calls == ["work-01"]  # only the failed item retried
    assert retried.failed_count == 0
    assert retried.enqueued_count == 3


def test_operation_request_mismatch_rejected(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(1),
        enqueue=RecordingEnqueue(),
        schema=schema,
        metadata={"kind": "first"},
    )
    with pytest.raises(ValueError, match="metadata does not match"):
        submit_batch(
            pg_engine,
            operation_key="op-1",
            group_key="exp",
            items=_items(1),
            enqueue=RecordingEnqueue(),
            schema=schema,
            metadata={"kind": "second"},
        )


def test_group_mismatch_and_duplicates_rejected(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pytest.raises(ValueError, match="group_key must match"):
        submit_batch(
            pg_engine,
            operation_key="op-1",
            group_key="other",
            items=_items(1),
            enqueue=RecordingEnqueue(),
            schema=schema,
        )
    duplicated = [*_items(1), *_items(1)]
    with pytest.raises(ValueError, match="duplicate item_id"):
        submit_batch(
            pg_engine,
            operation_key="op-2",
            group_key="exp",
            items=duplicated,
            enqueue=RecordingEnqueue(),
            schema=schema,
        )


def test_claim_cas_loses_second_claim(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    # Register items without enqueueing by making every enqueue fail,
    # then reset one to PENDING manually for the claim race.
    submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=_items(1),
        enqueue=RecordingEnqueue(fail_items=frozenset({"work-00"})),
        schema=schema,
    )
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE dr_platform_batch_submit_items "
                "SET enqueue_status = 'pending', enqueue_metadata = '{}', "
                "failure = NULL"
            )
        )
    with pg_engine.begin() as connection:
        first = claim_pending_batch_item(
            connection,
            operation_key="op-1",
            item_id="work-00",
            schema=schema,
        )
        second = claim_pending_batch_item(
            connection,
            operation_key="op-1",
            item_id="work-00",
            schema=schema,
        )
    assert first is not None
    assert second is None


def test_empty_submission_records_error_operation(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    enqueue = RecordingEnqueue()
    result = submit_batch(
        pg_engine,
        operation_key="op-empty",
        group_key="exp",
        items=[],
        enqueue=enqueue,
        schema=schema,
    )
    assert result.requested_count == 0
    assert enqueue.calls == []
    with pg_engine.connect() as connection:
        operation = load_batch_operation(
            connection,
            operation_key="op-empty",
            schema=schema,
        )
    assert operation is not None
    # Ported whetstone semantics: an empty operation derives ERROR
    # (failed_count >= requested_count at 0/0), not COMPLETED.
    assert operation.status is BatchOperationStatus.ERROR


def test_submit_batch_jsonl_windows_from_file(
    pg_engine: Engine,
    schema: PlatformSchema,
    tmp_path: Any,
) -> None:
    class FileItem(BaseModel):
        item_id: str
        order_key: str
        group_key: str

    items = [
        FileItem(
            item_id=f"work-{index:02d}",
            order_key=f"order-{index:02d}",
            group_key="exp",
        )
        for index in range(5)
    ]
    path = tmp_path / "items.jsonl"
    path.write_text(
        "\n".join(item.model_dump_json() for item in items) + "\n",
        encoding="utf-8",
    )
    enqueue = RecordingEnqueue()

    result = submit_batch_jsonl(
        pg_engine,
        operation_key="op-jsonl",
        group_key="exp",
        items_file=path,
        parse=FileItem.model_validate_json,
        enqueue=enqueue,
        schema=schema,
        chunk_size=2,
    )

    assert result.requested_count == 5
    assert result.enqueued_count == 5
    assert enqueue.calls == sorted(f"work-{i:02d}" for i in range(5))
