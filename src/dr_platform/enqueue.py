"""Dedup enqueue onto a DBOS queue with race detection.

The mechanism apps build enqueue targets from: deterministic workflow
id + `SetWorkflowID`/`SetEnqueueOptions(deduplication_id=...)` +
status pre-check + race-error absorption. Queue registration and
queue names stay app-side.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbos import DBOS, SetEnqueueOptions, SetWorkflowID
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr

from dr_platform.dbos_config import WORKFLOW_START_RACE_ERRORS

if TYPE_CHECKING:
    from collections.abc import Callable


class EnqueueOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow_id: StrictStr
    enqueued: StrictBool
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)


type EnqueueItem = Callable[[str], EnqueueOutcome]
"""item_id -> starts (or finds) durable work; reports what happened."""


def dedup_enqueue(
    *,
    queue_name: str,
    workflow_id: str,
    workflow: Callable[..., Any],
    args: tuple[Any, ...] = (),
    metadata: dict[str, Any] | None = None,
) -> EnqueueOutcome:
    """Enqueue exactly once per workflow id; races report enqueued=False."""
    if DBOS.get_workflow_status(workflow_id) is not None:
        return EnqueueOutcome(
            workflow_id=workflow_id,
            enqueued=False,
            metadata=dict(metadata or {}),
        )
    try:
        with (
            SetWorkflowID(workflow_id),
            SetEnqueueOptions(deduplication_id=workflow_id),
        ):
            DBOS.enqueue_workflow(queue_name, workflow, *args)
    except WORKFLOW_START_RACE_ERRORS:
        return EnqueueOutcome(
            workflow_id=workflow_id,
            enqueued=False,
            metadata=dict(metadata or {}),
        )
    return EnqueueOutcome(
        workflow_id=workflow_id,
        enqueued=True,
        metadata=dict(metadata or {}),
    )
