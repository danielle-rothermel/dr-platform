from __future__ import annotations

import os
import time
from multiprocessing import get_context
from uuid import uuid4

from dbos import DBOS, DBOSClient, DBOSConfig, Queue
from sqlalchemy import Engine, create_engine, func, select, text

from dr_platform._core.identities import PipelineKey, StageKey
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.runtime.database.migrate import upgrade_platform_schema
from dr_platform.runtime.dbos import PlatformDbosConfig
from dr_platform.runtime.dispatcher import register_scheduled_dispatcher
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    _args_for,
    dbos_config,
    initialize_dbos_schema,
    submit_items,
)

_HARD_EXIT_CODE = 86
_WORKER_TIMEOUT_SECONDS = 10
_WORKER_JOIN_TIMEOUT_SECONDS = 20
_PROBE_ROW_ID = 1


async def _recovery_probe_stage(database_url: str) -> str:
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            call_count = connection.execute(
                text(
                    """
                    UPDATE dbos_recovery_probe
                    SET call_count = call_count + 1
                    WHERE id = :id
                    RETURNING call_count
                    """
                ),
                {"id": _PROBE_ROW_ID},
            ).scalar_one()
    finally:
        engine.dispose()

    if call_count == 1:
        os._exit(_HARD_EXIT_CODE)
    return "output:recovered"


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
    return wrap_pipeline_workflows(declared)


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
            config=PlatformDbosConfig(
                database_url=database_url,
                system_database_url=database_url,
            ),
            engine=engine,
            registry=registry,
            batch_size=1,
            barrier_batch_size=1,
        )
        DBOS.launch()
        deadline = time.monotonic() + _WORKER_TIMEOUT_SECONDS
        schema = StagingSchema()
        while time.monotonic() < deadline:
            with engine.connect() as connection:
                state = connection.execute(
                    select(schema.stage_executions.c.state)
                ).scalar_one()
            if state == StageExecutionState.SUCCEEDED.value:
                return
            time.sleep(0.02)
        raise TimeoutError("recovered stage did not reach SUCCEEDED")
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


def test_dbos_recovery_reuses_the_platform_stage_attempt(
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

    schema = StagingSchema()
    with pg_engine.connect() as connection:
        original_attempt = connection.execute(
            select(
                schema.stage_attempts.c.stage_attempt_id,
                schema.stage_attempts.c.workflow_id,
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
        attempts_after_crash = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()
        calls_after_crash = connection.execute(
            text(
                """
                SELECT call_count
                FROM dbos_recovery_probe
                WHERE id = :id
                """
            ),
            {"id": _PROBE_ROW_ID},
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
    assert attempts_after_crash == 1
    assert calls_after_crash == 1
    assert dbos_status_after_crash == "PENDING"

    recovered_exit = _run_worker_process(
        database_url=clean_pg,
        pipeline_key=pipeline_key,
        queue_name=queue_name,
        application_name=application_name,
        application_version=application_version,
    )
    assert recovered_exit == 0

    with pg_engine.connect() as connection:
        recovered_execution = connection.execute(
            select(
                schema.stage_executions.c.state,
                schema.stage_executions.c.current_attempt,
                schema.stage_executions.c.output_reference,
            )
        ).one()
        recovered_attempts = connection.execute(
            select(
                schema.stage_attempts.c.stage_attempt_id,
                schema.stage_attempts.c.workflow_id,
            )
        ).all()
        recovered_call_count = connection.execute(
            text(
                """
                SELECT call_count
                FROM dbos_recovery_probe
                WHERE id = :id
                """
            ),
            {"id": _PROBE_ROW_ID},
        ).scalar_one()

    assert recovered_execution.state == StageExecutionState.SUCCEEDED.value
    assert recovered_execution.current_attempt == 1
    assert recovered_execution.output_reference == "output:recovered"
    assert recovered_attempts == [original_attempt]
    assert recovered_call_count >= 2
