"""Executable P6 inspection, health, and lifecycle-wait contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import Engine

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
from dr_platform.reconciliation_runtime import DbosStepObservation
from dr_platform.status import ServiceClass
from tests.test_claims import _register


class _StepReader:
    def read_step_history(
        self, *, workflow_id: str
    ) -> tuple[DbosStepObservation, ...]:
        return (
            DbosStepObservation(
                workflow_id=workflow_id,
                function_id=1,
                function_name="run",
            ),
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
    )
    assert report.queue_configuration_drift_count == 0
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
