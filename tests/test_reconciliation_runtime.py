"""Focused payload-free DBOS lifecycle observation tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from dbos import DBOSClient
from sqlalchemy import Engine

from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.reconciliation import ReconciliationPersistenceResult
from dr_platform.reconciliation_runtime import (
    DbosLifecycleReader,
    LifecycleObservationReader,
    ReconcileOptions,
    ReconciliationObservationDisposition,
    reconcile,
)
from dr_platform.records import FailureSnapshot
from dr_platform.status import FailureClass

if TYPE_CHECKING:
    from dr_platform.enqueue_runtime import EnqueuePageResult, QueueLookup
    from dr_platform.targets import TargetResolver


class FakeDbosClient:
    def __init__(
        self,
        *,
        matches: list[object] | None = None,
        error: Exception | None = None,
        engine: object | None = None,
    ) -> None:
        self.matches = matches or []
        self.error = error
        self.calls: list[dict[str, object]] = []
        self._sys_db = SimpleNamespace(engine=engine)

    def list_workflows(self, **kwargs: object) -> list[object]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.matches


def _status(
    status: str,
    *,
    workflow_id: str = "workflow-1",
    parent_workflow_id: str | None = None,
) -> object:
    return SimpleNamespace(
        workflow_id=workflow_id,
        status=status,
        parent_workflow_id=parent_workflow_id,
    )


def _classify(error: BaseException) -> FailureSnapshot:
    return FailureSnapshot(
        failure_class=FailureClass.TRANSIENT,
        error_type=type(error).__name__,
        message=str(error),
    )


def test_reconcile_options_match_frozen_missing_defaults() -> None:
    options = ReconcileOptions()

    assert options.missing_grace_seconds == 60
    assert options.missing_required_observations == 3


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        (
            DbosWorkflowStatus.PENDING,
            ReconciliationObservationDisposition.ACTIVE,
        ),
        (
            DbosWorkflowStatus.ENQUEUED,
            ReconciliationObservationDisposition.ACTIVE,
        ),
        (
            DbosWorkflowStatus.DELAYED,
            ReconciliationObservationDisposition.ACTIVE,
        ),
        (
            DbosWorkflowStatus.SUCCESS,
            ReconciliationObservationDisposition.SUCCEEDED,
        ),
        (
            DbosWorkflowStatus.CANCELLED,
            ReconciliationObservationDisposition.CANCELLED,
        ),
        (
            DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED,
            ReconciliationObservationDisposition.RECOVERY_EXHAUSTED,
        ),
    ],
)
def test_status_reader_normalizes_payload_free_workflow_state(
    status: DbosWorkflowStatus,
    disposition: ReconciliationObservationDisposition,
) -> None:
    client = FakeDbosClient(matches=[_status(status.value)])
    reader = DbosLifecycleReader(cast("DBOSClient", client))

    observation = reader.observe(
        workflow_id="workflow-1",
        classify_error=_classify,
    )

    assert observation.disposition is disposition
    assert observation.dbos_status is status
    assert observation.failure is None
    assert client.calls == [
        {
            "workflow_ids": ["workflow-1"],
            "limit": 2,
            "load_input": False,
            "load_output": False,
        }
    ]


def test_error_status_uses_classifier_without_loading_dbos_payload() -> None:
    client = FakeDbosClient(matches=[_status(DbosWorkflowStatus.ERROR.value)])
    classified: list[BaseException] = []

    def classify(error: BaseException) -> FailureSnapshot:
        classified.append(error)
        return _classify(error)

    observation = DbosLifecycleReader(cast("DBOSClient", client)).observe(
        workflow_id="workflow-1",
        classify_error=classify,
    )

    assert (
        observation.disposition is ReconciliationObservationDisposition.ERROR
    )
    assert observation.failure is not None
    assert observation.failure.failure_class is FailureClass.TRANSIENT
    assert len(classified) == 1
    assert "payload" in str(classified[0])


def test_error_status_fails_closed_when_classifier_raises() -> None:
    client = FakeDbosClient(matches=[_status(DbosWorkflowStatus.ERROR.value)])

    def reject_classification(error: BaseException) -> FailureSnapshot:
        del error
        raise ValueError("sensitive classifier detail")

    observation = DbosLifecycleReader(cast("DBOSClient", client)).observe(
        workflow_id="workflow-1",
        classify_error=reject_classification,
    )

    assert (
        observation.disposition is ReconciliationObservationDisposition.ERROR
    )
    assert observation.failure is not None
    assert observation.failure.failure_class is FailureClass.PERMANENT
    assert "sensitive classifier detail" not in observation.failure.message


@pytest.mark.parametrize(
    "client",
    [
        pytest.param(
            FakeDbosClient(error=RuntimeError("secret diagnostic")),
            id="lookup-error",
        ),
        pytest.param(
            FakeDbosClient(
                matches=[
                    _status(DbosWorkflowStatus.SUCCESS.value),
                    _status(DbosWorkflowStatus.SUCCESS.value),
                ]
            ),
            id="ambiguous",
        ),
        pytest.param(
            FakeDbosClient(
                matches=[
                    _status(
                        DbosWorkflowStatus.SUCCESS.value,
                        workflow_id="different-workflow",
                    )
                ]
            ),
            id="identity-mismatch",
        ),
        pytest.param(
            FakeDbosClient(matches=[_status("FUTURE_STATUS")]),
            id="unknown-status",
        ),
        pytest.param(
            FakeDbosClient(
                matches=[
                    _status(
                        DbosWorkflowStatus.SUCCESS.value,
                        parent_workflow_id="parent-workflow",
                    )
                ]
            ),
            id="topology-drift",
        ),
    ],
)
def test_status_reader_fails_closed_on_ambiguous_or_invalid_lookup(
    client: FakeDbosClient,
) -> None:
    observation = DbosLifecycleReader(cast("DBOSClient", client)).observe(
        workflow_id="workflow-1",
        classify_error=_classify,
    )

    assert (
        observation.disposition
        is ReconciliationObservationDisposition.UNCERTAIN
    )
    assert observation.failure is not None
    assert "secret diagnostic" not in observation.failure.message


def test_status_reader_reports_exact_absence_without_failure() -> None:
    observation = DbosLifecycleReader(
        cast("DBOSClient", FakeDbosClient())
    ).observe(
        workflow_id="workflow-1",
        classify_error=_classify,
    )

    assert (
        observation.disposition is ReconciliationObservationDisposition.ABSENT
    )
    assert observation.dbos_status is None
    assert observation.failure is None


class FakeStepResult:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self._rows = rows

    def mappings(self) -> tuple[dict[str, object], ...]:
        return self._rows


class FakeStepConnection:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self._rows = rows
        self.selected_columns: tuple[str, ...] = ()

    def execute(self, statement: Any) -> FakeStepResult:
        self.selected_columns = tuple(statement.selected_columns.keys())
        return FakeStepResult(self._rows)


class FakeStepEngine:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.connection = FakeStepConnection(rows)

    @contextmanager
    def connect(self) -> Iterator[FakeStepConnection]:
        yield self.connection


def test_step_history_selects_only_allowlisted_nonpayload_columns() -> None:
    engine = FakeStepEngine(
        (
            {
                "workflow_uuid": "workflow-1",
                "function_id": 1,
                "function_name": "provider-call",
                "child_workflow_id": None,
                "started_at_epoch_ms": 10,
                "completed_at_epoch_ms": 20,
            },
        )
    )
    client = FakeDbosClient(engine=engine)

    steps = DbosLifecycleReader(cast("DBOSClient", client)).read_step_history(
        workflow_id="workflow-1"
    )

    assert len(steps) == 1
    assert steps[0].function_name == "provider-call"
    assert engine.connection.selected_columns == (
        "workflow_uuid",
        "function_id",
        "function_name",
        "child_workflow_id",
        "started_at_epoch_ms",
        "completed_at_epoch_ms",
    )
    assert not {
        "output",
        "error",
        "serialization",
    } & set(engine.connection.selected_columns)


class UnusedLifecycleReader:
    def observe(self, **kwargs: object) -> Any:
        raise AssertionError(f"unexpected observation: {kwargs}")

    def read_step_history(self, **kwargs: object) -> Any:
        raise AssertionError(f"unexpected step read: {kwargs}")


def test_reconcile_runs_recovery_then_one_bounded_observation_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dr_platform.enqueue_runtime as enqueue_module
    import dr_platform.reconciliation as persistence_module
    import dr_platform.reconciliation_runtime as runtime_module

    events: list[str] = []

    def recover(*args: object, **kwargs: object) -> EnqueuePageResult:
        del args
        events.append("recover")
        assert cast("Any", kwargs["options"]).page_size == 7
        assert kwargs["operation_key"] == "operation"
        return cast(
            "EnqueuePageResult",
            SimpleNamespace(items=(object(), object())),
        )

    def load(*args: object, **kwargs: object) -> Any:
        del args
        events.append("load")
        assert kwargs["page_size"] == 5
        assert kwargs["operation_key"] == "operation"
        return (object(), object())

    def load_missing(*args: object, **kwargs: object) -> Any:
        del args
        events.append("load-missing")
        assert kwargs["page_size"] == 3
        assert kwargs["operation_key"] == "operation"
        return (object(),)

    def observe(*args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        events.append("observe")
        return {}

    def apply(
        *args: object,
        **kwargs: object,
    ) -> ReconciliationPersistenceResult:
        del args
        events.append("apply")
        assert kwargs["observations"] == {}
        assert (
            cast(
                "ReconcileOptions", kwargs["options"]
            ).missing_required_observations
            == 4
        )
        return ReconciliationPersistenceResult(
            observed_count=3,
            changed_count=0,
            enqueue_reset_count=0,
            execution_retry_count=0,
            missing_count=0,
        )

    def replace(*args: object, **kwargs: object) -> EnqueuePageResult:
        del args
        events.append("replace")
        assert cast("Any", kwargs["options"]).page_size == 2
        assert kwargs["operation_key"] == "operation"
        return cast(
            "EnqueuePageResult",
            SimpleNamespace(items=(object(),)),
        )

    def enqueue(*args: object, **kwargs: object) -> EnqueuePageResult:
        del args
        events.append("enqueue")
        assert cast("Any", kwargs["options"]).page_size == 1
        assert kwargs["operation_key"] == "operation"
        return cast("EnqueuePageResult", SimpleNamespace(items=(object(),)))

    monkeypatch.setattr(enqueue_module, "recover_call_started_page", recover)
    monkeypatch.setattr(enqueue_module, "enqueue_replacement_page", replace)
    monkeypatch.setattr(enqueue_module, "enqueue_pending_page", enqueue)
    monkeypatch.setattr(persistence_module, "load_reconciliation_page", load)
    monkeypatch.setattr(
        persistence_module,
        "load_missing_reobservation_page",
        load_missing,
    )
    monkeypatch.setattr(
        persistence_module,
        "apply_reconciliation_observations",
        apply,
    )
    monkeypatch.setattr(runtime_module, "_observe_candidates", observe)

    result = reconcile(
        cast("Engine", object()),
        resolver=cast("TargetResolver", object()),
        queue_lookup=cast("QueueLookup", object()),
        options=ReconcileOptions(
            page_size=7,
            operation_key="operation",
            missing_grace_seconds=11,
            missing_required_observations=4,
        ),
        reader=cast("LifecycleObservationReader", UnusedLifecycleReader()),
        recovery_observer=cast("Any", object()),
    )

    assert events == [
        "recover",
        "load",
        "load-missing",
        "observe",
        "apply",
        "replace",
        "enqueue",
    ]
    assert result.recovered_call_started_count == 2
    assert result.observed_count == 3
    assert result.replacement_enqueue_count == 1
    assert result.pending_enqueue_count == 1
