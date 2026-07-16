"""Focused payload-free DBOS lifecycle observation tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest
from dbos import DBOSClient

from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.reconciliation_runtime import (
    DbosLifecycleReader,
    ReconciliationObservationDisposition,
    WorkflowMetadataDisposition,
)
from dr_platform.status import FailureClass


class FakeDbosClient:
    def __init__(
        self,
        *,
        matches: list[object] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.matches = matches or []
        self.error = error

    def list_workflows(self, **kwargs: object) -> list[object]:
        del kwargs
        if self.error is not None:
            raise self.error
        return self.matches


def _status(
    status: str,
    *,
    workflow_id: str = "workflow-1",
    parent_workflow_id: str | None = None,
    application_version: str | None = "application-v1",
) -> object:
    return SimpleNamespace(
        workflow_id=workflow_id,
        status=status,
        parent_workflow_id=parent_workflow_id,
        application_version=application_version,
    )


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

    observation = reader.observe(workflow_id="workflow-1")

    assert observation.disposition is disposition
    assert observation.dbos_status is status
    assert observation.failure is None


def test_error_status_fails_closed_without_authoritative_classification() -> (
    None
):
    client = FakeDbosClient(matches=[_status(DbosWorkflowStatus.ERROR.value)])
    observation = DbosLifecycleReader(cast("DBOSClient", client)).observe(
        workflow_id="workflow-1",
    )

    assert (
        observation.disposition is ReconciliationObservationDisposition.ERROR
    )
    assert observation.failure is not None
    assert observation.failure.failure_class is FailureClass.PERMANENT
    assert observation.failure.error_type == "DbosWorkflowErrorUnclassifiable"
    assert "unavailable" in observation.failure.message


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
    )

    assert (
        observation.disposition is ReconciliationObservationDisposition.ABSENT
    )
    assert observation.dbos_status is None
    assert observation.failure is None


@pytest.mark.parametrize(
    ("client", "disposition", "application_version"),
    [
        pytest.param(
            FakeDbosClient(
                matches=[_status(DbosWorkflowStatus.SUCCESS.value)]
            ),
            WorkflowMetadataDisposition.AVAILABLE,
            "application-v1",
            id="available",
        ),
        pytest.param(
            FakeDbosClient(),
            WorkflowMetadataDisposition.UNAVAILABLE,
            None,
            id="absent",
        ),
        pytest.param(
            FakeDbosClient(error=RuntimeError("secret")),
            WorkflowMetadataDisposition.UNAVAILABLE,
            None,
            id="lookup-unavailable",
        ),
        pytest.param(
            FakeDbosClient(
                matches=[
                    _status(DbosWorkflowStatus.SUCCESS.value),
                    _status(DbosWorkflowStatus.SUCCESS.value),
                ]
            ),
            WorkflowMetadataDisposition.AMBIGUOUS,
            None,
            id="ambiguous",
        ),
    ],
)
def test_workflow_metadata_reader_returns_typed_payload_free_outcome(
    client: FakeDbosClient,
    disposition: WorkflowMetadataDisposition,
    application_version: str | None,
) -> None:
    metadata = DbosLifecycleReader(
        cast("DBOSClient", client)
    ).read_workflow_metadata(workflow_id="workflow-1")

    assert metadata.disposition is disposition
    assert metadata.application_version == application_version
