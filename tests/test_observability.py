from __future__ import annotations

import pytest
from pydantic import BaseModel
from sqlalchemy import Engine

from dr_platform import (
    AwaitOperationTimeoutError,
    EnqueueOutcome,
    PlatformSchema,
    await_operation,
    load_operation_snapshot,
    operation_workflow_ids,
    submit_batch,
    upgrade_platform_schema,
)


class WorkItem(BaseModel):
    name: str

    @property
    def item_id(self) -> str:
        return f"work-{self.name}"

    @property
    def order_key(self) -> str:
        return f"order-{self.name}"

    @property
    def group_key(self) -> str:
        return "exp"


@pytest.fixture
def schema(pg_engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(str(pg_engine.url))
    return PlatformSchema()


@pytest.fixture
def submitted(pg_engine: Engine, schema: PlatformSchema) -> None:
    submit_batch(
        pg_engine,
        operation_key="op-1",
        group_key="exp",
        items=[WorkItem(name=f"{index}") for index in range(3)],
        enqueue=lambda item_id: EnqueueOutcome(
            workflow_id=f"wf:{item_id}",
            enqueued=True,
        ),
        schema=schema,
    )


@pytest.mark.usefixtures("submitted")
def test_operation_snapshot_and_workflow_ids(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.connect() as connection:
        snapshot = load_operation_snapshot(
            connection,
            operation_key="op-1",
            schema=schema,
        )
        workflow_ids = operation_workflow_ids(
            connection,
            operation_key="op-1",
            schema=schema,
        )
    assert snapshot is not None
    assert snapshot.operation.requested_count == 3
    assert snapshot.enqueue_status_counts == {"enqueued": 3}
    assert workflow_ids == ("wf:work-0", "wf:work-1", "wf:work-2")


@pytest.mark.usefixtures("submitted")
def test_await_operation_returns_when_no_workflows_active(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    statuses = {
        "wf:work-0": iter(["PENDING", "SUCCESS"]),
        "wf:work-1": iter(["SUCCESS", "SUCCESS"]),
        "wf:work-2": iter(["ENQUEUED", "ERROR"]),
    }
    slept: list[float] = []

    breakdown = await_operation(
        pg_engine,
        operation_key="op-1",
        schema=schema,
        poll_interval_seconds=5.0,
        timeout_seconds=60.0,
        status_fn=lambda workflow_id: next(statuses[workflow_id]),
        sleep=slept.append,
        clock=lambda: 0.0,
    )

    assert slept == [5.0]
    assert breakdown.status_counts == {"SUCCESS": 2, "ERROR": 1}
    assert breakdown.active_count == 0


@pytest.mark.usefixtures("submitted")
def test_await_operation_times_out_with_breakdown(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    clock_values = iter([0.0, 0.0, 61.0])

    with pytest.raises(AwaitOperationTimeoutError) as excinfo:
        await_operation(
            pg_engine,
            operation_key="op-1",
            schema=schema,
            poll_interval_seconds=1.0,
            timeout_seconds=60.0,
            status_fn=lambda _workflow_id: "PENDING",
            sleep=lambda _seconds: None,
            clock=lambda: next(clock_values),
        )
    assert excinfo.value.breakdown.status_counts == {"PENDING": 3}


@pytest.mark.usefixtures("submitted")
def test_missing_workflow_status_counts_as_missing(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    breakdown = await_operation(
        pg_engine,
        operation_key="op-1",
        schema=schema,
        poll_interval_seconds=1.0,
        timeout_seconds=60.0,
        status_fn=lambda _workflow_id: None,
        sleep=lambda _seconds: None,
        clock=lambda: 0.0,
    )
    assert breakdown.status_counts == {"MISSING": 3}
