from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dbos import DBOS, Queue
from sqlalchemy import Engine, select

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.inspection.statuses import bulk_work_statuses
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    initialize_dbos_runtime,
)
from dr_platform.runtime.dispatcher import register_scheduled_dispatcher
from dr_platform.submission.stream import (
    RunMemberInput,
    RunRegistrationDeclaration,
    WorkInput,
    submit,
)
from tests.conftest import NOW, _migrate, default_live_dbos_identity

if TYPE_CHECKING:
    from collections.abc import Callable

    from dr_platform.admission.runner import AdmissionPayload


def _await_dbos_result(
    workflow_id: str,
    *,
    registration,
    timeout: float = 10,
):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            registration.client.retrieve_workflow(workflow_id).get_result,
            polling_interval_sec=0.01,
        )
        return future.result(timeout=timeout)


def _workflow_ids(engine: Engine) -> tuple[str, ...]:
    schema = LedgerSchema()
    with engine.connect() as connection:
        return tuple(
            connection.execute(
                select(schema.stage_attempts.c.workflow_id).order_by(
                    schema.stage_attempts.c.stage_attempt_id
                )
            ).scalars()
        )


def test_checkpoint_transactions_register_read_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolation_levels: list[str] = []

    def transaction(
        isolation_level: str = "SERIALIZABLE",
        *,
        name: str | None = None,
    ):
        assert name is not None
        isolation_levels.append(isolation_level)
        return lambda function: function

    def workflow(*, name: str | None = None, **kwargs: object):
        del kwargs
        assert name is not None
        return lambda function: function

    monkeypatch.setattr(DBOS, "transaction", transaction)
    monkeypatch.setattr(DBOS, "workflow", workflow)

    async def stage(value: str) -> str:
        return value

    async def completion(value: str) -> str:
        return value

    declared = PipelineDefinition(
        key=PipelineKey("read-committed-checkpoints"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="read-committed-stage",
                workflow=stage,
                args_for=lambda payload: (payload.input_reference,),
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name="read-committed-completion",
            workflow=completion,
            args_for=lambda payload: (payload.manifest_reference,),
        ),
    )

    wrap_pipeline_workflows(declared, max_recovery_attempts=1)

    assert isolation_levels == ["READ COMMITTED", "READ COMMITTED"]


def _run_pipeline(  # noqa: PLR0913 -- explicit integration wiring
    clean_pg: str,
    engine: Engine,
    *,
    suffix: str,
    workflow,
    args_for: Callable[[AdmissionPayload], tuple[object, ...]],
    member_count: int,
):
    _migrate(engine)
    declared = PipelineDefinition(
        key=PipelineKey(f"async-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"async-{suffix}",
                workflow=workflow,
                args_for=args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, max_recovery_attempts=1)
    registry = PipelineRegistry()
    registry.register(pipeline)
    Queue(pipeline.stages[0].queue_name, concurrency=member_count)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=member_count,
        engine=engine,
        clock=lambda: NOW,
    )
    submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(member_count),
        members=(
            RunMemberInput(
                ordinal=index,
                work=WorkInput(
                    work_key=f"work-{index}",
                    input_reference=f"input:{index}",
                    labels={},
                ),
            )
            for index in range(member_count)
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )
    config = PlatformDbosConfig(
        database_url=clean_pg,
        system_database_url=clean_pg,
        max_recovery_attempts=1,
    )
    initialize_dbos_runtime(config, app_name=f"drp-async-{suffix}")
    registration = register_scheduled_dispatcher(
        live_dbos_identity=default_live_dbos_identity(app_version="test"),
        config=config,
        engine=engine,
        registry=registry,
        batch_size=member_count,
        sweep_cron=None,
    )
    DBOS.launch()
    DBOS.set_latest_application_version(DBOS.application_version)
    registration.workflow(NOW, NOW)
    return registration


def test_async_stages_share_one_loop_affine_resource_concurrently(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:10]

    class _LoopResource:
        def __init__(self) -> None:
            self.loop: asyncio.AbstractEventLoop | None = None
            self.entered = 0
            self.all_entered = asyncio.Event()

        async def use(self, value: str) -> str:
            loop = asyncio.get_running_loop()
            if self.loop is None:
                self.loop = loop
            assert self.loop is loop
            self.entered += 1
            if self.entered == 2:
                self.all_entered.set()
            await self.all_entered.wait()
            return f"output:{value}"

    resource = _LoopResource()

    async def workflow(value: str) -> str:
        return await resource.use(value)

    def args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload.input_reference,)

    registration = None
    try:
        registration = _run_pipeline(
            clean_pg,
            pg_engine,
            suffix=suffix,
            workflow=workflow,
            args_for=args_for,
            member_count=2,
        )
        workflow_ids = _workflow_ids(pg_engine)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = tuple(
                executor.submit(
                    _await_dbos_result,
                    workflow_id,
                    registration=registration,
                )
                for workflow_id in workflow_ids
            )
            results = tuple(future.result(timeout=10) for future in futures)
        statuses = bulk_work_statuses(
            "campaign-1", ("work-0", "work-1"), engine=pg_engine
        ).statuses
        assert set(results) == {"output:input:0", "output:input:1"}
        assert resource.entered == 2
        assert all(
            status.state is StageExecutionState.SUCCEEDED
            for status in statuses.values()
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_args_for_failure_is_recorded_by_the_durable_wrapper(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:10]
    workflow_called = False

    async def workflow(_value: str) -> str:
        nonlocal workflow_called
        workflow_called = True
        return "output:unexpected"

    def args_for(_payload: AdmissionPayload) -> tuple[object, ...]:
        raise ValueError("cannot derive arguments")

    registration = None
    try:
        registration = _run_pipeline(
            clean_pg,
            pg_engine,
            suffix=suffix,
            workflow=workflow,
            args_for=args_for,
            member_count=1,
        )
        workflow_id = _workflow_ids(pg_engine)[0]
        assert (
            _await_dbos_result(workflow_id, registration=registration) is None
        )
        status = bulk_work_statuses(
            "campaign-1", ("work-0",), engine=pg_engine
        ).statuses[WorkKey("work-0")]
        assert workflow_called is False
        assert status.state is StageExecutionState.FAILED
        with pg_engine.connect() as connection:
            summary = connection.execute(
                select(LedgerSchema().stage_attempts.c.terminal_summary)
            ).scalar_one()
        assert summary["error_type"] == "builtins.ValueError"
        assert summary["message"] == "cannot derive arguments"
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)
