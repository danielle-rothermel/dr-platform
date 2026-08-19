from __future__ import annotations

import asyncio
import threading
from datetime import timedelta
from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import select

from dr_platform._core.identities import PipelineKey, StageKey
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.admission.runner import AdmissionPayload, run_admission_pass
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.cancellation import cancel_work
from dr_platform.submission.stream import WorkInput
from tests.conftest import NOW, _migrate, submit_items, wait_for_handoff
from tests.execution.test_handoff import _launch_dbos

_body_started = threading.Event()
_body_cancelled = threading.Event()


async def _long_running_body(input_reference: str) -> str:
    _body_started.set()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        _body_cancelled.set()
        raise
    return f"done:{input_reference}"


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_reference,)


def test_cancel_work_reaches_running_preemptible_stage_body(
    clean_pg: str,
    pg_engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = wrap_pipeline_workflows(
        PipelineDefinition(
            key=PipelineKey(f"preempt-{suffix}"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name=f"preempt-{suffix}",
                    workflow=_long_running_body,
                    args_for=_args_for,
                ),
            ),
        ),
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:preempt",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)
    _body_started.clear()
    _body_cancelled.clear()
    registration = _launch_dbos(
        clean_pg,
        suffix=suffix,
        engine=pg_engine,
        registry=registry,
    )
    try:
        summary = run_admission_pass(
            pg_engine,
            client=registration.client,
            registry=registry,
            clock=lambda: NOW,
        )
        assert summary.admitted_total == 1
        wait_for_handoff(_body_started.is_set, timeout_seconds=10)
        cancel_work(
            engine=pg_engine,
            client=registration.client,
            campaign_key=f"campaign-{suffix}",
            work_key="work",
            clock=lambda: NOW + timedelta(seconds=1),
        )
        wait_for_handoff(_body_cancelled.is_set, timeout_seconds=5)
        with pg_engine.connect() as connection:
            state = connection.execute(
                select(schema.stage_executions.c.state).where(
                    schema.stage_executions.c.stage_index == 0
                )
            ).scalar_one()
        assert state == StageExecutionState.CANCELLED.value
    finally:
        registration.close()
        DBOS.destroy()
    del schema
