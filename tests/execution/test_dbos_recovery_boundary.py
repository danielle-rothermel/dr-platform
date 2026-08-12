from __future__ import annotations

import os
import time
from multiprocessing import get_context
from uuid import uuid4

from dbos import DBOS, DBOSClient, DBOSConfig, Queue
from sqlalchemy import Engine, create_engine, func, select, text

from dr_platform._core.identities import PipelineKey, StageKey
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import TerminalSummaryField
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.retry import retry_stage
from dr_platform.recovery.sweep import sweep_abandoned_stages
from dr_platform.runtime.database.migrate import upgrade_platform_schema
from dr_platform.runtime.dbos import PlatformDbosConfig
from dr_platform.runtime.dispatcher import register_scheduled_dispatcher
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    _args_for,
    dbos_config,
    default_live_dbos_identity,
    initialize_dbos_schema,
    submit_items,
)

_HARD_EXIT_CODE = 86
_WORKER_TIMEOUT_SECONDS = 10
_WORKER_JOIN_TIMEOUT_SECONDS = 20
_PROBE_ROW_ID = 1
_MAX_RECOVERY_ATTEMPTS = 1


async def _recovery_probe_stage(database_url: str) -> str:
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE dbos_recovery_probe
                    SET call_count = call_count + 1
                    WHERE id = :id
                    """
                ),
                {"id": _PROBE_ROW_ID},
            )
    finally:
        engine.dispose()

    os._exit(_HARD_EXIT_CODE)


def _recovery_pipeline(
    *,
    pipeline_key: str,
    queue_name: str,
) -> PipelineDefinition:
    declared = PipelineDefinition(
        key=PipelineKey(pipeline_key),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=queue_name,
                workflow=_recovery_probe_stage,
                args_for=_args_for,
            ),
        ),
    )
    return wrap_pipeline_workflows(
        declared,
        max_recovery_attempts=_MAX_RECOVERY_ATTEMPTS,
    )


def _dbos_config(
    *,
    database_url: str,
    application_name: str,
    application_version: str,
) -> DBOSConfig:
    return dbos_config(
        name=application_name,
        system_database_url=database_url,
        application_database_url=database_url,
        application_version=application_version,
        notification_listener_polling_interval_sec=0.01,
    )


def _initialize_dbos_schema(
    *,
    database_url: str,
    application_name: str,
    application_version: str,
) -> None:
    initialize_dbos_schema(
        _dbos_config(
            database_url=database_url,
            application_name=application_name,
            application_version=application_version,
        )
    )


def _run_recovery_worker(
    database_url: str,
    pipeline_key: str,
    queue_name: str,
    application_name: str,
    application_version: str,
) -> None:
    pipeline = _recovery_pipeline(
        pipeline_key=pipeline_key,
        queue_name=queue_name,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    Queue(queue_name, polling_interval_sec=0.01)
    engine = create_engine(database_url)
    registration = None
    try:
        DBOS(
            config=_dbos_config(
                database_url=database_url,
                application_name=application_name,
                application_version=application_version,
            )
        )
        registration = register_scheduled_dispatcher(
            live_dbos_identity=default_live_dbos_identity(
                app_version=application_version
            ),
            config=PlatformDbosConfig(
                database_url=database_url,
                system_database_url=database_url,
                max_recovery_attempts=_MAX_RECOVERY_ATTEMPTS,
            ),
            engine=engine,
            registry=registry,
            batch_size=1,
            barrier_batch_size=1,
            sweep_cron="*/1 * * * * *",
        )
        DBOS.launch()
        deadline = time.monotonic() + _WORKER_TIMEOUT_SECONDS
        schema = LedgerSchema()
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                state = connection.execute(
                    select(schema.stage_executions.c.state)
                ).scalar_one()
            if state in {
                StageExecutionState.FAILED.value,
                StageExecutionState.SUCCEEDED.value,
            }:
                return
            time.sleep(0.02)
        raise TimeoutError("recovery worker did not settle")
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)
        engine.dispose()


def _run_worker_process(
    *,
    database_url: str,
    pipeline_key: str,
    queue_name: str,
    application_name: str,
    application_version: str,
) -> int:
    process = get_context("spawn").Process(
        target=_run_recovery_worker,
        args=(
            database_url,
            pipeline_key,
            queue_name,
            application_name,
            application_version,
        ),
    )
    process.start()
    process.join(_WORKER_JOIN_TIMEOUT_SECONDS)
    if process.is_alive():
        process.terminate()
        process.join()
        raise AssertionError("recovery worker timed out")
    assert process.exitcode is not None
    return process.exitcode


def _create_probe_table(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE dbos_recovery_probe (
                    id INTEGER PRIMARY KEY,
                    call_count INTEGER NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO dbos_recovery_probe (id, call_count)
                VALUES (:id, 0)
                """
            ),
            {"id": _PROBE_ROW_ID},
        )


def test_same_identity_crash_recovers_to_failed_and_retry_stage(  # noqa: PLR0915
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:10]
    pipeline_key = f"recovery-{suffix}"
    queue_name = f"recovery-{suffix}"
    application_name = f"drp-recovery-{suffix}"
    application_version = f"recovery-{suffix}"

    upgrade_platform_schema(clean_pg)
    _create_probe_table(pg_engine)
    _initialize_dbos_schema(
        database_url=clean_pg,
        application_name=application_name,
        application_version=application_version,
    )

    pipeline = _recovery_pipeline(
        pipeline_key=pipeline_key,
        queue_name=queue_name,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
    )
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:recovery",
        items=(
            WorkInput(
                work_key=f"work-{suffix}",
                input_reference=clean_pg,
                labels={},
            ),
        ),
        registry=registry,
        engine=pg_engine,
    )

    client = DBOSClient(system_database_url=clean_pg)
    try:
        summary = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
        )
    finally:
        client.destroy()
        DBOS.destroy(destroy_registry=True)
    assert summary.admitted_total == 1

    schema = LedgerSchema()
    with pg_engine.connect() as connection:
        original_attempt = connection.execute(
            select(
                schema.stage_attempts.c.stage_attempt_id,
                schema.stage_attempts.c.workflow_id,
                schema.stage_executions.c.stage_execution_id,
            ).select_from(
                schema.stage_attempts.join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
            )
        ).one()

    first_exit = _run_worker_process(
        database_url=clean_pg,
        pipeline_key=pipeline_key,
        queue_name=queue_name,
        application_name=application_name,
        application_version=application_version,
    )
    assert first_exit == _HARD_EXIT_CODE

    with pg_engine.connect() as connection:
        state_after_crash = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
        dbos_status_after_crash = connection.execute(
            text(
                """
                SELECT status
                FROM dbos.workflow_status
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"workflow_id": original_attempt.workflow_id},
        ).scalar_one()

    assert state_after_crash == StageExecutionState.ADMITTED.value
    assert dbos_status_after_crash == "PENDING"

    second_exit = _run_worker_process(
        database_url=clean_pg,
        pipeline_key=pipeline_key,
        queue_name=queue_name,
        application_name=application_name,
        application_version=application_version,
    )
    assert second_exit == _HARD_EXIT_CODE

    third_exit = _run_worker_process(
        database_url=clean_pg,
        pipeline_key=pipeline_key,
        queue_name=queue_name,
        application_name=application_name,
        application_version=application_version,
    )
    assert third_exit == 0

    with pg_engine.connect() as connection:
        dbos_status_before_sweep = connection.execute(
            text(
                """
                SELECT status
                FROM dbos.workflow_status
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"workflow_id": original_attempt.workflow_id},
        ).scalar_one()
        platform_state_before_sweep = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()

    assert platform_state_before_sweep == StageExecutionState.FAILED.value
    assert dbos_status_before_sweep == "MAX_RECOVERY_ATTEMPTS_EXCEEDED"

    client = DBOSClient(system_database_url=clean_pg)
    try:
        sweep = sweep_abandoned_stages(
            pg_engine,
            client=client,
            live_identity=default_live_dbos_identity(
                app_version=application_version
            ),
        )
    finally:
        client.destroy()

    assert sweep.projected_count == 0
    with pg_engine.connect() as connection:
        failed = connection.execute(
            select(
                schema.stage_executions.c.state,
                schema.stage_attempts.c.terminal_summary,
            ).select_from(
                schema.stage_attempts.join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
            )
        ).one()
        dbos_status = connection.execute(
            text(
                """
                SELECT status
                FROM dbos.workflow_status
                WHERE workflow_uuid = :workflow_id
                """
            ),
            {"workflow_id": original_attempt.workflow_id},
        ).scalar_one()
        attempts_after = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()
        calls_after = connection.execute(
            text(
                """
                SELECT call_count
                FROM dbos_recovery_probe
                WHERE id = :id
                """
            ),
            {"id": _PROBE_ROW_ID},
        ).scalar_one()

    assert failed.state == StageExecutionState.FAILED.value
    assert failed.terminal_summary == {
        TerminalSummaryField.OUTCOME.value: StageExecutionState.FAILED.value,
        TerminalSummaryField.PRODUCER.value: "abandonment",
        TerminalSummaryField.DBOS_STATUS.value: (
            "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
        ),
    }
    assert dbos_status == "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    assert attempts_after == 1
    assert calls_after == 2

    retried = retry_stage(
        original_attempt.stage_execution_id,
        engine=pg_engine,
    )
    assert retried.stage_execution.state is StageExecutionState.READY
    assert retried.new_attempt.attempt_number == 2


def test_stale_app_version_pending_projects_without_body_rerun(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:10]
    pipeline_key = f"stale-{suffix}"
    queue_name = f"stale-{suffix}"
    application_name = f"drp-stale-{suffix}"
    old_version = f"old-{suffix}"
    new_version = f"new-{suffix}"

    upgrade_platform_schema(clean_pg)
    _create_probe_table(pg_engine)
    _initialize_dbos_schema(
        database_url=clean_pg,
        application_name=application_name,
        application_version=old_version,
    )

    pipeline = _recovery_pipeline(
        pipeline_key=pipeline_key,
        queue_name=queue_name,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
    )
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:recovery",
        items=(
            WorkInput(
                work_key=f"work-{suffix}",
                input_reference="input",
                labels={},
            ),
        ),
        registry=registry,
        engine=pg_engine,
    )

    client = DBOSClient(system_database_url=clean_pg)
    try:
        summary = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
        )
    finally:
        client.destroy()
        DBOS.destroy(destroy_registry=True)
    assert summary.admitted_total == 1

    schema = LedgerSchema()
    with pg_engine.connect() as connection:
        workflow_id = connection.execute(
            select(schema.stage_attempts.c.workflow_id)
        ).scalar_one()

    class _StaleStatus:
        status = "PENDING"
        error = None
        app_version = old_version
        executor_id = "local"

        def __init__(self, workflow_id: str) -> None:
            self.workflow_id = workflow_id

    stale_client = type(
        "StaleClient",
        (),
        {"list_workflows": lambda self, **kwargs: [_StaleStatus(workflow_id)]},
    )()

    sweep = sweep_abandoned_stages(
        pg_engine,
        client=stale_client,  # ty: ignore[invalid-argument-type]
        live_identity=default_live_dbos_identity(app_version=new_version),
    )
    assert sweep.projected_count == 1

    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
        terminal_summary = connection.execute(
            select(schema.stage_attempts.c.terminal_summary)
        ).scalar_one()
        calls_after_sweep = connection.execute(
            text("SELECT call_count FROM dbos_recovery_probe WHERE id = :id"),
            {"id": _PROBE_ROW_ID},
        ).scalar_one()

    assert state == StageExecutionState.FAILED.value
    assert terminal_summary[TerminalSummaryField.REASON.value] == (
        "stale_app_version"
    )
    assert calls_after_sweep == 0
