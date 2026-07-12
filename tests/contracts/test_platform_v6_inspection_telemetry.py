"""Executable P6 inspection, health, and lifecycle-wait contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import Engine, select, update

from dr_platform import inspection as inspection_module
from dr_platform.inspection import (
    OperationWaitOptions,
    OperationWaitTimeoutError,
    health_report,
    inspect_operation,
    list_attempts,
    list_items,
    list_operations,
    wait_operation,
)
from dr_platform.reconciliation_runtime import (
    DbosStepObservation,
    WorkflowMetadataDisposition,
    WorkflowMetadataObservation,
)
from dr_platform.status import ServiceClass
from tests.test_claims import _register


class _StepReader:
    def read_step_history(
        self, *, workflow_id: str, limit: int = 100
    ) -> tuple[DbosStepObservation, ...]:
        assert limit > 0
        return (
            DbosStepObservation(
                workflow_id=workflow_id,
                function_id=1,
                function_name="run",
            ),
        )


class _QueueHealth:
    def configuration_drift_count(self) -> int:
        return 2


class _WorkflowMetadataReader:
    def read_workflow_metadata(
        self, *, workflow_id: str
    ) -> WorkflowMetadataObservation:
        if "item-0" in workflow_id:
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.AVAILABLE,
                application_version="actual-version",
            )
        if "item-1" in workflow_id:
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.UNAVAILABLE,
            )
        return WorkflowMetadataObservation(
            workflow_id=workflow_id,
            disposition=WorkflowMetadataDisposition.AMBIGUOUS,
        )


def test_inspection_pages_are_stable_and_dbos_timeline_is_allowlisted(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD, ServiceClass.URGENT),
    )

    operations = list_operations(engine=pg_engine, schema=schema, limit=1)
    assert len(operations) == 1
    assert operations[0].current_item_count == 2
    assert (
        list_operations(
            engine=pg_engine,
            schema=schema,
            cursor=operations[0].operation.operation_key,
        )
        == ()
    )
    with pytest.raises(ValueError, match="cursor"):
        list_operations(engine=pg_engine, schema=schema, cursor="missing")

    items = list_items("operation", engine=pg_engine, schema=schema, limit=1)
    assert len(items) == 1
    assert items[0].current_attempt is not None
    assert (
        len(
            list_items(
                "operation",
                engine=pg_engine,
                schema=schema,
                cursor=(items[0].item.item_index, items[0].item.item_id),
            )
        )
        == 1
    )

    attempts = list_attempts(
        "operation",
        engine=pg_engine,
        schema=schema,
        reader=cast("Any", _StepReader()),
    )
    assert [
        (entry.attempt.item_id, entry.attempt.attempt) for entry in attempts
    ] == sorted(
        (entry.attempt.item_id, entry.attempt.attempt) for entry in attempts
    )
    assert all(
        entry.dbos_steps[0].function_name == "run" for entry in attempts
    )


def test_health_and_wait_timeout_retain_authoritative_inspection(
    pg_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema, target = _register(
        pg_engine, service_classes=(ServiceClass.STANDARD,)
    )
    now = datetime(2026, 1, 1, tzinfo=UTC)
    report = health_report(
        engine=pg_engine,
        schema=schema,
        now=now,
        no_progress_after_seconds=1,
        queued_age_threshold_seconds=1,
        queue_health=_QueueHealth(),
    )
    assert report.queue_configuration_drift_count == 2
    assert report.oldest_queued_age_seconds is not None

    calls: list[float] = []
    ticks = iter((now, now + timedelta(seconds=2)))
    monkeypatch.setattr(
        inspection_module, "reconcile", lambda *args, **kwargs: None
    )
    options = OperationWaitOptions(
        poll_interval_seconds=1.0,
        timeout_seconds=1.0,
        reconciliation_page_size=1,
        clock=lambda: next(ticks),
        sleeper=calls.append,
    )
    with pytest.raises(OperationWaitTimeoutError) as raised:
        wait_operation(
            "operation",
            engine=pg_engine,
            resolver=cast("Any", target),
            options=options,
            schema=schema,
        )
    assert raised.value.inspection == inspect_operation(
        "operation", engine=pg_engine, schema=schema
    )
    assert calls == []


def test_health_compares_expected_version_with_authoritative_dbos_metadata(
    pg_engine: Engine,
) -> None:
    schema, _ = _register(
        pg_engine,
        service_classes=(ServiceClass.STANDARD,) * 3,
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                source_application_version="expected-version",
                effective_service_priority=(
                    schema.item_attempts.c.requested_service_priority
                ),
                priority_source="enqueued_here",
                enqueued_at=schema.item_attempts.c.created_at,
            )
        )
        assert (
            len(
                tuple(
                    connection.scalars(
                        select(schema.item_attempts.c.workflow_id)
                    )
                )
            )
            == 3
        )

    report = health_report(
        engine=pg_engine,
        schema=schema,
        now=datetime(2026, 1, 1, tzinfo=UTC),
        workflow_metadata_reader=_WorkflowMetadataReader(),
    )

    assert report.application_version_mismatch_count == 1
    assert report.application_version_unavailable_count == 1
    assert report.application_version_ambiguous_count == 1

    unavailable_report = health_report(
        engine=pg_engine,
        schema=schema,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert unavailable_report.application_version_mismatch_count == 0
    assert unavailable_report.application_version_unavailable_count == 3
    assert unavailable_report.application_version_ambiguous_count == 0
