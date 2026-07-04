"""Operation observability: snapshots, recorded workflow ids, and
awaiting work completion.

Workflow ids are deterministic and recorded in ``enqueue_metadata`` at
enqueue time, so the library can watch DBOS workflow statuses with no
domain knowledge. The status callable is injectable; the default reads
``DBOS.get_workflow_status`` (requires a launched DBOS runtime or an
external client wired by the app).
"""

from __future__ import annotations

import time
from collections import Counter
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr
from sqlalchemy import select

from dr_platform.dbos_config import (
    DBOS_ACTIVE_WORKFLOW_STATUSES,
    MISSING_DBOS_WORKFLOW_STATUS,
)
from dr_platform.records import (
    WORKFLOW_ID_METADATA_KEY,
    BatchOperationRecord,
)
from dr_platform.submission import load_batch_operation

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Connection, Engine

    from dr_platform.db.schema import PlatformSchema

type WorkflowStatusFn = Callable[[str], str | None]
"""workflow_id -> DBOS status string, or None if unknown to DBOS."""


class OperationSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation: BatchOperationRecord
    enqueue_status_counts: dict[StrictStr, StrictInt]


class WorkflowStatusBreakdown(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_key: StrictStr
    status_counts: dict[StrictStr, StrictInt]

    @property
    def active_count(self) -> int:
        return sum(
            count
            for status, count in self.status_counts.items()
            if status in DBOS_ACTIVE_WORKFLOW_STATUSES
        )


class AwaitOperationTimeoutError(TimeoutError):
    def __init__(self, breakdown: WorkflowStatusBreakdown) -> None:
        super().__init__(
            f"timed out awaiting operation "
            f"{breakdown.operation_key!r}: {breakdown.status_counts}"
        )
        self.breakdown = breakdown


def load_operation_snapshot(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> OperationSnapshot | None:
    operation = load_batch_operation(
        connection,
        operation_key=operation_key,
        schema=schema,
    )
    if operation is None:
        return None
    rows = connection.execute(
        select(schema.batch_items.c.enqueue_status).where(
            schema.batch_items.c.operation_key == operation_key
        )
    ).all()
    counts = Counter(str(row[0]) for row in rows)
    return OperationSnapshot(
        operation=operation,
        enqueue_status_counts=dict(counts),
    )


def operation_workflow_ids(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> tuple[str, ...]:
    rows = connection.execute(
        select(
            schema.batch_items.c.enqueue_metadata[
                WORKFLOW_ID_METADATA_KEY
            ].astext
        )
        .where(schema.batch_items.c.operation_key == operation_key)
        .order_by(schema.batch_items.c.item_index)
    ).all()
    return tuple(str(row[0]) for row in rows if row[0])


def workflow_status_breakdown(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
    status_fn: WorkflowStatusFn,
) -> WorkflowStatusBreakdown:
    workflow_ids = operation_workflow_ids(
        connection,
        operation_key=operation_key,
        schema=schema,
    )
    counts: Counter[str] = Counter()
    for workflow_id in workflow_ids:
        status = status_fn(workflow_id)
        counts[status or MISSING_DBOS_WORKFLOW_STATUS] += 1
    return WorkflowStatusBreakdown(
        operation_key=operation_key,
        status_counts=dict(counts),
    )


def dbos_workflow_status(workflow_id: str) -> str | None:
    # Lazy: only the default status_fn needs a DBOS runtime.
    from dbos import DBOS  # noqa: PLC0415

    status = DBOS.get_workflow_status(workflow_id)
    if status is None:
        return None
    return str(status.status)


def await_operation(  # noqa: PLR0913 -- poll knobs + injectable clock/sleep
    engine: Engine,
    *,
    operation_key: str,
    schema: PlatformSchema,
    poll_interval_seconds: float,
    timeout_seconds: float,
    status_fn: WorkflowStatusFn = dbos_workflow_status,
    sleep: Callable[[float], Any] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> WorkflowStatusBreakdown:
    """Poll recorded workflow ids until none are active.

    Returns the final status breakdown; raises
    ``AwaitOperationTimeoutError`` (carrying the last breakdown) on
    timeout. Items that never enqueued are the operation summary's
    business, not this poll's.
    """
    deadline = clock() + timeout_seconds
    while True:
        with engine.connect() as connection:
            breakdown = workflow_status_breakdown(
                connection,
                operation_key=operation_key,
                schema=schema,
                status_fn=status_fn,
            )
        if breakdown.active_count == 0:
            return breakdown
        if clock() >= deadline:
            raise AwaitOperationTimeoutError(breakdown)
        sleep(poll_interval_seconds)
