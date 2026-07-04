from __future__ import annotations

from typing import Any
from unittest.mock import patch

from dbos._error import DBOSQueueDeduplicatedError

from dr_platform import dedup_enqueue


def _workflow(item_id: str) -> str:
    return item_id


def test_dedup_enqueue_skips_when_status_exists() -> None:
    with (
        patch(
            "dr_platform.enqueue.DBOS.get_workflow_status",
            return_value={"status": "ENQUEUED"},
        ),
        patch("dr_platform.enqueue.DBOS.enqueue_workflow") as enqueue,
    ):
        outcome = dedup_enqueue(
            queue_name="q",
            workflow_id="wf-1",
            workflow=_workflow,
            args=("item-1",),
            metadata={"run_id": "r-1"},
        )
    assert outcome.enqueued is False
    assert outcome.workflow_id == "wf-1"
    assert outcome.metadata == {"run_id": "r-1"}
    enqueue.assert_not_called()


def test_dedup_enqueue_enqueues_fresh_workflow() -> None:
    calls: list[Any] = []
    with (
        patch(
            "dr_platform.enqueue.DBOS.get_workflow_status",
            return_value=None,
        ),
        patch(
            "dr_platform.enqueue.DBOS.enqueue_workflow",
            side_effect=lambda *args: calls.append(args),
        ),
        patch("dr_platform.enqueue.SetWorkflowID"),
        patch("dr_platform.enqueue.SetEnqueueOptions"),
    ):
        outcome = dedup_enqueue(
            queue_name="q",
            workflow_id="wf-1",
            workflow=_workflow,
            args=("item-1",),
        )
    assert outcome.enqueued is True
    assert calls == [("q", _workflow, "item-1")]


def test_dedup_enqueue_absorbs_race_errors() -> None:
    with (
        patch(
            "dr_platform.enqueue.DBOS.get_workflow_status",
            return_value=None,
        ),
        patch(
            "dr_platform.enqueue.DBOS.enqueue_workflow",
            side_effect=DBOSQueueDeduplicatedError("wf-1", "q", "wf-1"),
        ),
        patch("dr_platform.enqueue.SetWorkflowID"),
        patch("dr_platform.enqueue.SetEnqueueOptions"),
    ):
        outcome = dedup_enqueue(
            queue_name="q",
            workflow_id="wf-1",
            workflow=_workflow,
            args=("item-1",),
        )
    assert outcome.enqueued is False
