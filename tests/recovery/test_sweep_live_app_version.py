from __future__ import annotations

from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import Engine, select

from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.sweep import sweep_abandoned_stages
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    initialize_dbos_runtime,
)
from dr_platform.runtime.dispatcher import register_scheduled_dispatcher
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    _as_dbos_client,
    _migrate,
    _RecordingClient,
    default_live_dbos_identity,
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
    registration = register_scheduled_dispatcher(
        live_dbos_identity=default_live_dbos_identity(),
        config=config,
        engine=pg_engine,
        registry=registry,
    )
    client = registration.client
    try:
        DBOS.launch()
        DBOS.set_latest_application_version(DBOS.application_version)
        admission_client = _RecordingClient()
        assert (
            run_admission_pass(
                pg_engine,
                client=_as_dbos_client(admission_client),
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        assert DBOS.application_version

        summary = sweep_abandoned_stages(
            pg_engine,
            client=client,
            live_identity=default_live_dbos_identity(),
            clock=_utc_now,
        )
    finally:
        registration.close()
        DBOS.destroy(destroy_registry=True)

    assert summary.projected_count == 0
    schema = LedgerSchema()
    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
    assert state == StageExecutionState.ADMITTED.value
