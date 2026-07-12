from __future__ import annotations

import inspect
import os
from collections.abc import Iterator
from importlib.metadata import version
from typing import Any

import pytest
import sqlalchemy as sa
from dbos import DBOSClient, Queue, SetEnqueueOptions
from dbos._client import EnqueueOptions
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer, Serializer
from dbos._sys_db import (
    SystemDatabase,
    WorkflowStatusString,
)
from sqlalchemy import create_engine, text

DBOS_VERSION = "2.26.0"
CONTRACT_SCHEMA_PREFIX = "dbos_contract_preflight"
QUEUE_NAME = "contract-priority"

WORKFLOW_STATUS_COLUMNS = (
    "workflow_uuid",
    "status",
    "name",
    "authenticated_user",
    "assumed_role",
    "authenticated_roles",
    "output",
    "error",
    "executor_id",
    "created_at",
    "updated_at",
    "application_version",
    "application_id",
    "class_name",
    "config_name",
    "recovery_attempts",
    "queue_name",
    "workflow_timeout_ms",
    "workflow_deadline_epoch_ms",
    "started_at_epoch_ms",
    "deduplication_id",
    "inputs",
    "priority",
    "queue_partition_key",
    "forked_from",
    "was_forked_from",
    "owner_xid",
    "parent_workflow_id",
    "serialization",
    "delay_until_epoch_ms",
    "rate_limited",
    "completed_at",
    "attributes",
    "schedule_name",
)
OPERATION_OUTPUT_COLUMNS = (
    "workflow_uuid",
    "function_id",
    "function_name",
    "output",
    "error",
    "child_workflow_id",
    "started_at_epoch_ms",
    "completed_at_epoch_ms",
    "serialization",
)
QUEUE_COLUMNS = (
    "queue_id",
    "name",
    "concurrency",
    "worker_concurrency",
    "rate_limit_max",
    "rate_limit_period_sec",
    "priority_enabled",
    "partition_queue",
    "polling_interval_sec",
    "created_at",
    "updated_at",
)
APPLICATION_VERSION_COLUMNS = (
    "version_id",
    "version_name",
    "version_timestamp",
    "created_at",
)
WORKFLOW_STATUSES = {
    "PENDING",
    "SUCCESS",
    "ERROR",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
    "CANCELLED",
    "ENQUEUED",
    "DELAYED",
}
SAFE_STEP_TIMELINE_COLUMNS = (
    "workflow_uuid",
    "function_id",
    "function_name",
    "child_workflow_id",
    "started_at_epoch_ms",
    "completed_at_epoch_ms",
)
STEP_PAYLOAD_COLUMNS = {"output", "error", "serialization"}
SAFE_WORKFLOW_TELEMETRY_COLUMNS = (
    "workflow_uuid",
    "status",
    "name",
    "executor_id",
    "created_at",
    "updated_at",
    "application_version",
    "application_id",
    "class_name",
    "config_name",
    "recovery_attempts",
    "queue_name",
    "workflow_timeout_ms",
    "workflow_deadline_epoch_ms",
    "started_at_epoch_ms",
    "priority",
    "queue_partition_key",
    "forked_from",
    "was_forked_from",
    "parent_workflow_id",
    "delay_until_epoch_ms",
    "rate_limited",
    "completed_at",
    "attributes",
    "schedule_name",
)
WORKFLOW_PAYLOAD_COLUMNS = {
    "authenticated_user",
    "assumed_role",
    "authenticated_roles",
    "inputs",
    "output",
    "error",
    "serialization",
}
SAFE_QUEUE_TELEMETRY_COLUMNS = (
    "name",
    "concurrency",
    "worker_concurrency",
    "rate_limit_max",
    "rate_limit_period_sec",
    "priority_enabled",
    "partition_queue",
    "polling_interval_sec",
    "created_at",
    "updated_at",
)


class PayloadRejectingSerializer(Serializer):
    """Detect any accidental payload deserialization in contract probes."""

    def __init__(self) -> None:
        self.deserialize_calls = 0

    def serialize(self, data: Any) -> str:
        del data
        raise AssertionError("contract reader must not serialize payloads")

    def deserialize(self, serialized_data: str) -> Any:
        del serialized_data
        self.deserialize_calls += 1
        raise AssertionError("contract reader must not deserialize payloads")

    def name(self) -> str:
        return "payload_rejecting"


@pytest.fixture(scope="module")
def dbos_client(pg_url: str) -> Iterator[DBOSClient]:
    schema = f"{CONTRACT_SCHEMA_PREFIX}_{os.getpid()}"
    admin_engine = create_engine(pg_url)
    with admin_engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))

    system_database = SystemDatabase.create(
        system_database_url=pg_url,
        engine_kwargs={},
        engine=None,
        schema=schema,
        serializer=DefaultSerializer(),
        executor_id=None,
        use_listen_notify=False,
    )
    system_database.run_migrations()
    system_database.destroy()

    client = DBOSClient(
        system_database_url=pg_url,
        dbos_system_schema=schema,
    )
    yield client

    client.destroy()
    with admin_engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    admin_engine.dispose()


def test_dbos_and_otel_extra_are_pinned_and_installed() -> None:
    assert version("dbos") == DBOS_VERSION
    assert version("opentelemetry-sdk")
    assert version("opentelemetry-exporter-otlp-proto-http")


def test_public_signatures_match_the_pinned_contract() -> None:
    client_parameters = inspect.signature(DBOSClient).parameters
    assert tuple(client_parameters) == (
        "database_url",
        "system_database_url",
        "system_database_engine",
        "application_database_url",
        "dbos_system_schema",
        "serializer",
    )

    list_parameters = inspect.signature(DBOSClient.list_workflows).parameters
    assert tuple(list_parameters) == (
        "self",
        "workflow_ids",
        "status",
        "start_time",
        "end_time",
        "completed_after",
        "completed_before",
        "dequeued_after",
        "dequeued_before",
        "name",
        "app_version",
        "forked_from",
        "parent_workflow_id",
        "user",
        "queue_name",
        "limit",
        "offset",
        "sort_desc",
        "workflow_id_prefix",
        "load_input",
        "load_output",
        "executor_id",
        "queues_only",
        "was_forked_from",
        "has_parent",
        "attributes",
        "schedule_name",
    )
    assert list_parameters["load_input"].default is True
    assert list_parameters["load_output"].default is True

    step_parameters = inspect.signature(
        DBOSClient.list_workflow_steps
    ).parameters
    assert tuple(step_parameters) == ("self", "workflow_id", "limit", "offset")
    assert "load_output" not in step_parameters

    cancel_parameters = inspect.signature(
        DBOSClient.cancel_workflow
    ).parameters
    assert tuple(cancel_parameters) == (
        "self",
        "workflow_id",
        "cancel_children",
    )
    assert cancel_parameters["cancel_children"].default is False

    queue_parameters = inspect.signature(Queue).parameters
    assert queue_parameters["priority_enabled"].default is False
    assert queue_parameters["database_backed_queue"].default is False

    enqueue_parameters = inspect.signature(SetEnqueueOptions).parameters
    assert tuple(enqueue_parameters) == (
        "deduplication_id",
        "priority",
        "app_version",
        "queue_partition_key",
        "delay_seconds",
    )


def test_system_schema_and_statuses_match_the_pinned_contract() -> None:
    assert (
        tuple(SystemSchema.workflow_status.c.keys()) == WORKFLOW_STATUS_COLUMNS
    )
    assert (
        tuple(SystemSchema.operation_outputs.c.keys())
        == OPERATION_OUTPUT_COLUMNS
    )
    assert tuple(SystemSchema.queues.c.keys()) == QUEUE_COLUMNS
    assert (
        tuple(SystemSchema.application_versions.c.keys())
        == APPLICATION_VERSION_COLUMNS
    )
    assert {
        status.value for status in WorkflowStatusString
    } == WORKFLOW_STATUSES


def test_reviewed_telemetry_allowlists_are_complete_and_payload_free() -> None:
    assert set(SAFE_WORKFLOW_TELEMETRY_COLUMNS) <= set(WORKFLOW_STATUS_COLUMNS)
    assert not (
        set(SAFE_WORKFLOW_TELEMETRY_COLUMNS) & WORKFLOW_PAYLOAD_COLUMNS
    )
    assert set(SAFE_STEP_TIMELINE_COLUMNS) <= set(OPERATION_OUTPUT_COLUMNS)
    assert not (set(SAFE_STEP_TIMELINE_COLUMNS) & STEP_PAYLOAD_COLUMNS)
    assert set(SAFE_QUEUE_TELEMETRY_COLUMNS) <= set(QUEUE_COLUMNS)
    assert set(APPLICATION_VERSION_COLUMNS) == set(
        SystemSchema.application_versions.c.keys()
    )


def test_database_queue_registration_and_retrieval_preserve_operator_config(
    dbos_client: DBOSClient,
) -> None:
    queue_name = "contract-registration"
    registered = dbos_client.register_queue(
        queue_name,
        concurrency=3,
        worker_concurrency=2,
        limiter={"limit": 5, "period": 1.5},
        priority_enabled=True,
        polling_interval_sec=0.25,
        on_conflict="always_update",
    )
    assert registered.name == queue_name
    assert registered.priority_enabled is True

    retrieved = dbos_client.retrieve_queue(queue_name)
    assert retrieved is not None
    assert retrieved.name == queue_name
    assert retrieved.priority_enabled is True
    assert retrieved.concurrency == 3
    assert retrieved.worker_concurrency == 2
    assert retrieved.limiter == {"limit": 5, "period": 1.5}

    dbos_client.register_queue(
        queue_name,
        concurrency=99,
        priority_enabled=False,
        on_conflict="never_update",
    )
    preserved = dbos_client.retrieve_queue(queue_name)
    assert preserved is not None
    assert preserved.concurrency == 3
    assert preserved.priority_enabled is True


def test_enqueue_identity_options_attributes_and_filtering(
    dbos_client: DBOSClient,
) -> None:
    queue_name = "contract-options"
    dbos_client.register_queue(
        queue_name,
        priority_enabled=True,
        on_conflict="always_update",
    )
    attributes = {
        "platform.execution_key": "execution-1",
        "platform.workflow_role": "generation",
    }
    options: EnqueueOptions = {
        "workflow_name": "contract-options-workflow",
        "queue_name": queue_name,
        "workflow_id": "contract-options-id",
        "app_version": "contract-app-version",
        "deduplication_id": "contract-deduplication-id",
        "priority": 7,
        "delay_seconds": 60,
        "attributes": attributes,
    }
    handle = dbos_client.enqueue(options, "sensitive-input")

    assert handle.get_workflow_id() == "contract-options-id"
    matches = dbos_client.list_workflows(
        attributes={"platform.execution_key": "execution-1"},
        load_input=False,
        load_output=False,
    )
    assert [match.workflow_id for match in matches] == ["contract-options-id"]
    status = matches[0]
    assert status.name == "contract-options-workflow"
    assert status.status == "DELAYED"
    assert status.queue_name == queue_name
    assert status.app_version == "contract-app-version"
    assert status.deduplication_id == "contract-deduplication-id"
    assert status.priority == 7
    assert status.attributes == attributes
    assert status.input is None
    assert status.output is None


def test_queue_dequeues_by_priority_then_creation_time(
    dbos_client: DBOSClient,
) -> None:
    queue = dbos_client.register_queue(
        QUEUE_NAME,
        priority_enabled=True,
        on_conflict="always_update",
    )
    workflows = (
        ("contract-later", 2, 2),
        ("contract-earlier", 2, 1),
        ("contract-urgent", 1, 3),
    )
    for workflow_id, priority, _created_at in workflows:
        options: EnqueueOptions = {
            "workflow_name": "contract-workflow",
            "queue_name": QUEUE_NAME,
            "workflow_id": workflow_id,
            "priority": priority,
        }
        dbos_client.enqueue(options)

    with dbos_client._sys_db.engine.begin() as connection:
        for workflow_id, _priority, created_at in workflows:
            connection.execute(
                sa.update(SystemSchema.workflow_status)
                .where(
                    SystemSchema.workflow_status.c.workflow_uuid == workflow_id
                )
                .values(created_at=created_at)
            )

    dequeued = dbos_client._sys_db.start_queued_workflows(
        queue,
        executor_id="contract-executor",
        app_version="contract-version",
        queue_partition_key=None,
    )

    assert dequeued == [
        "contract-urgent",
        "contract-earlier",
        "contract-later",
    ]


def test_same_millisecond_equal_priority_order_is_intentionally_unspecified(
    dbos_client: DBOSClient,
) -> None:
    queue_name = "contract-tie"
    queue = dbos_client.register_queue(
        queue_name,
        priority_enabled=True,
        on_conflict="always_update",
    )
    workflow_ids = (
        "contract-tie-a",
        "contract-tie-b",
        "contract-tie-c",
    )
    for workflow_id in workflow_ids:
        options: EnqueueOptions = {
            "workflow_name": "contract-tie-workflow",
            "queue_name": queue_name,
            "workflow_id": workflow_id,
            "priority": 5,
        }
        dbos_client.enqueue(options)

    with dbos_client._sys_db.engine.begin() as connection:
        connection.execute(
            sa.update(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.workflow_uuid.in_(workflow_ids)
            )
            .values(created_at=1_000)
        )

    dequeued = dbos_client._sys_db.start_queued_workflows(
        queue,
        executor_id="contract-tie-executor",
        app_version="contract-version",
        queue_partition_key=None,
    )

    # DBOS has no final tie-break after (priority, created_at). The contract is
    # set completeness, never a particular order for an exact timestamp tie.
    assert set(dequeued) == set(workflow_ids)


def test_cancellation_is_nonrecursive_and_missing_rows_are_not_tombstoned(
    dbos_client: DBOSClient,
) -> None:
    dbos_client.register_queue(
        QUEUE_NAME,
        priority_enabled=True,
        on_conflict="always_update",
    )
    parent_options: EnqueueOptions = {
        "workflow_name": "contract-workflow",
        "queue_name": QUEUE_NAME,
        "workflow_id": "contract-parent",
    }
    child_options: EnqueueOptions = {
        "workflow_name": "contract-workflow",
        "queue_name": QUEUE_NAME,
        "workflow_id": "contract-child",
    }
    dbos_client.enqueue(parent_options)
    dbos_client.enqueue(child_options)
    with dbos_client._sys_db.engine.begin() as connection:
        connection.execute(
            sa.update(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == "contract-child"
            )
            .values(parent_workflow_id="contract-parent")
        )

    dbos_client.cancel_workflow("contract-parent", cancel_children=False)
    statuses = {
        status.workflow_id: status.status
        for status in dbos_client.list_workflows(
            workflow_ids=["contract-parent", "contract-child"],
            load_input=False,
            load_output=False,
        )
    }
    assert statuses == {
        "contract-parent": "CANCELLED",
        "contract-child": "ENQUEUED",
    }

    dbos_client.cancel_workflow("contract-missing", cancel_children=False)
    assert (
        dbos_client.list_workflows(
            workflow_ids=["contract-missing"],
            load_input=False,
            load_output=False,
        )
        == []
    )


def test_recursive_cancellation_traverses_all_descendants(
    dbos_client: DBOSClient,
) -> None:
    queue_name = "contract-recursive-cancel"
    dbos_client.register_queue(
        queue_name,
        priority_enabled=True,
        on_conflict="always_update",
    )
    workflow_ids = (
        "contract-recursive-parent",
        "contract-recursive-child",
        "contract-recursive-grandchild",
    )
    for workflow_id in workflow_ids:
        options: EnqueueOptions = {
            "workflow_name": "contract-cancel-workflow",
            "queue_name": queue_name,
            "workflow_id": workflow_id,
        }
        dbos_client.enqueue(options)

    with dbos_client._sys_db.engine.begin() as connection:
        connection.execute(
            sa.update(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == "contract-recursive-child"
            )
            .values(parent_workflow_id="contract-recursive-parent")
        )
        connection.execute(
            sa.update(SystemSchema.workflow_status)
            .where(
                SystemSchema.workflow_status.c.workflow_uuid
                == "contract-recursive-grandchild"
            )
            .values(parent_workflow_id="contract-recursive-child")
        )

    dbos_client.cancel_workflow(
        "contract-recursive-parent",
        cancel_children=True,
    )
    statuses = dbos_client.list_workflows(
        workflow_ids=list(workflow_ids),
        load_input=False,
        load_output=False,
    )
    assert {status.status for status in statuses} == {"CANCELLED"}


def test_allowlisted_step_timeline_never_selects_or_deserializes_payloads(
    dbos_client: DBOSClient,
    pg_url: str,
) -> None:
    dbos_client.register_queue(
        QUEUE_NAME,
        priority_enabled=True,
        on_conflict="always_update",
    )
    options: EnqueueOptions = {
        "workflow_name": "contract-workflow",
        "queue_name": QUEUE_NAME,
        "workflow_id": "contract-payload",
    }
    dbos_client.enqueue(options, "sensitive-input")
    with dbos_client._sys_db.engine.begin() as connection:
        connection.execute(
            sa.insert(SystemSchema.operation_outputs).values(
                workflow_uuid="contract-payload",
                function_id=1,
                function_name="provider-call",
                output="sensitive-output",
                error=None,
                child_workflow_id=None,
                started_at_epoch_ms=1,
                completed_at_epoch_ms=2,
                serialization="payload_rejecting",
            )
        )

    serializer = PayloadRejectingSerializer()
    payload_rejecting_client = DBOSClient(
        system_database_url=pg_url,
        dbos_system_schema=dbos_client._sys_db.schema,
        serializer=serializer,
    )
    try:
        payload_rejecting_client.list_workflow_steps("contract-payload")
        assert serializer.deserialize_calls == 1

        columns = tuple(
            SystemSchema.operation_outputs.c[name]
            for name in SAFE_STEP_TIMELINE_COLUMNS
        )
        assert not (set(SAFE_STEP_TIMELINE_COLUMNS) & STEP_PAYLOAD_COLUMNS)
        with payload_rejecting_client._sys_db.engine.begin() as connection:
            row = connection.execute(
                sa.select(*columns).where(
                    SystemSchema.operation_outputs.c.workflow_uuid
                    == "contract-payload"
                )
            ).one()

        assert tuple(row) == (
            "contract-payload",
            1,
            "provider-call",
            None,
            1,
            2,
        )
        assert serializer.deserialize_calls == 1
    finally:
        payload_rejecting_client.destroy()
