from __future__ import annotations

from uuid import uuid4

import pytest
from dbos import DBOS, Queue
from sqlalchemy import Engine, select, text

from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.live_identity import (
    LOCAL_EXECUTOR_SENTINEL,
    LiveDbosIdentity,
)
from dr_platform.recovery.sweep import (
    DbosWorkflowStatus,
    sweep_abandoned_stages,
)
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    initialize_dbos_runtime,
)
from dr_platform.runtime.dispatcher import register_scheduled_dispatcher
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    _migrate,
    set_live_dbos_identity,
)
from tests.execution.test_handoff import (
    _configure_controls,
    _pipeline,
    _submit_items,
    _utc_now,
)


def test_sweep_does_not_stale_project_default_path_pending_after_launch(
    clean_pg: str,
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    suffix = uuid4().hex[:10]
    _migrate(pg_engine)
    pipeline = _pipeline(
        key=f"sweep-live-version-{suffix}",
        stage_logic=(("execute", lambda input_reference: input_reference),),
    )
    wrapped = wrap_pipeline_workflows(pipeline, max_recovery_attempts=1)
    registry = PipelineRegistry()
    registry.register(wrapped)
    Queue(wrapped.stages[0].queue_name, concurrency=1)
    _configure_controls(pg_engine, wrapped, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        wrapped,
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        items=(
            WorkInput(
                work_key=f"work-{suffix}",
                input_reference="input",
                labels={},
            ),
        ),
    )
    config = PlatformDbosConfig(
        database_url=clean_pg,
        system_database_url=clean_pg,
        max_recovery_attempts=1,
    )
    initialize_dbos_runtime(config, app_name=f"drp-sweep-live-{suffix}")
    live_identity = LiveDbosIdentity(
        executor_ids=frozenset({"reconciler-local"}),
    )
    registration = register_scheduled_dispatcher(
        live_dbos_identity=live_identity,
        config=config,
        engine=pg_engine,
        registry=registry,
    )
    client = registration.client
    schema = LedgerSchema()
    try:
        DBOS.launch()
        live_app_version = DBOS.application_version
        assert live_app_version
        DBOS.set_latest_application_version(live_app_version)
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        with pg_engine.connect() as connection:
            workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
            ).scalar_one()
            row = connection.execute(
                text(
                    """
                    SELECT status, application_version, executor_id
                    FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": workflow_id},
            ).one()
        assert row.status == DbosWorkflowStatus.ENQUEUED.value
        assert row.application_version is None

        enqueued_summary = sweep_abandoned_stages(
            pg_engine,
            client=client,
            live_identity=live_identity,
            clock=_utc_now,
        )
        assert enqueued_summary.projected_count == 0

        with pg_engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE dbos.workflow_status
                    SET application_version = :application_version,
                        executor_id = :executor_id,
                        status = :status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {
                    "application_version": live_app_version,
                    "executor_id": "reconciler-local",
                    "status": DbosWorkflowStatus.PENDING.value,
                    "workflow_id": workflow_id,
                },
            )
        statuses = client.list_workflows(
            workflow_ids=[workflow_id],
            load_input=False,
            load_output=False,
        )
        assert len(statuses) == 1
        status = statuses[0]
        assert status.status == DbosWorkflowStatus.PENDING.value
        assert status.app_version == live_app_version
        assert status.executor_id == "reconciler-local"

        summary = sweep_abandoned_stages(
            pg_engine,
            client=client,
            live_identity=live_identity,
            clock=_utc_now,
        )
        assert summary.projected_count == 0
        assert summary.identity_unavailable is False

        set_live_dbos_identity(
            monkeypatch, app_version="", executor_id=LOCAL_EXECUTOR_SENTINEL
        )
        summary_after_empty_version = sweep_abandoned_stages(
            pg_engine,
            client=client,
            live_identity=live_identity,
            clock=_utc_now,
        )
        assert summary_after_empty_version.projected_count == 0
        assert summary_after_empty_version.identity_unavailable is True
    finally:
        registration.close()
        DBOS.destroy(destroy_registry=True)

    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
    assert state == StageExecutionState.ADMITTED.value
