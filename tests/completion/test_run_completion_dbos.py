from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from dbos import DBOS, Queue
from sqlalchemy import Engine, select

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.completion.execution import (
    RunCompletionPayload,
    inspect_run_completion,
)
from dr_platform.execution.handoff import wrap_pipeline_workflows
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
    compute_run_membership_digest,
    submit,
)
from tests.conftest import NOW, _migrate
from tests.execution.test_async_workflow import _await_dbos_result

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dr_platform.admission.runner import AdmissionPayload


def test_run_completion_payload_executes_through_dbos(
    clean_pg: str,
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    application_outputs: dict[str, str] = {}
    aggregate_artifact: dict[str, object] = {}

    async def stage(input_reference: str) -> str:
        output = f"output:{input_reference}"
        application_outputs[input_reference] = output
        return output

    def stage_args(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload.input_reference,)

    members = tuple(
        RunMemberInput(
            ordinal=index,
            work=WorkInput(
                work_key=f"work-{index}",
                input_reference=f"input:{index}",
                labels={},
            ),
        )
        for index in range(2)
    )
    digest = compute_run_membership_digest(members, expected_member_count=2)
    manifests: Mapping[str, str] = {"manifest:run-1": digest}

    def completion_args(
        payload: RunCompletionPayload,
    ) -> tuple[object, ...]:
        return (payload,)

    async def completion(payload: RunCompletionPayload) -> str:
        assert (
            manifests[payload.manifest_reference] == payload.membership_digest
        )
        aggregate_artifact.update(
            {
                "membership_digest": payload.membership_digest,
                "consumed": tuple(sorted(application_outputs.items())),
            }
        )
        return "aggregate:run-1"

    declared = PipelineDefinition(
        key=PipelineKey(f"completion-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"execute-{suffix}",
                workflow=stage,
                args_for=stage_args,
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name=f"aggregate-{suffix}",
            workflow=completion,
            args_for=completion_args,
        ),
    )
    pipeline = wrap_pipeline_workflows(declared)
    registry = PipelineRegistry()
    registry.register(pipeline)
    Queue(pipeline.stages[0].queue_name, concurrency=2)
    assert pipeline.run_completion is not None
    Queue(pipeline.run_completion.queue_name, concurrency=1)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=2,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(2, "manifest:run-1", digest),
        members=members,
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    config = PlatformDbosConfig(
        database_url=clean_pg, system_database_url=clean_pg
    )
    initialize_dbos_runtime(config, app_name=f"drp-completion-{suffix}")
    registration = None
    try:
        # Keep DBOS workflow registration without competing scheduler pollers.
        monkeypatch.setattr(
            DBOS, "scheduled", lambda _cron: lambda workflow: workflow
        )
        registration = register_scheduled_dispatcher(
            config=config,
            engine=pg_engine,
            registry=registry,
            batch_size=2,
            barrier_batch_size=1,
        )
        DBOS.launch()
        DBOS.set_latest_application_version(DBOS.application_version)
        registration.workflow(NOW, NOW)
        schema = StagingSchema()
        with pg_engine.connect() as connection:
            stage_workflow_ids = tuple(
                connection.execute(
                    select(schema.stage_attempts.c.workflow_id).order_by(
                        schema.stage_attempts.c.stage_attempt_id
                    )
                ).scalars()
            )
        assert {
            _await_dbos_result(workflow_id, registration=registration)
            for workflow_id in stage_workflow_ids
        } == {"output:input:0", "output:input:1"}

        registration.barrier_workflow(NOW, NOW)
        with pg_engine.connect() as connection:
            assert (
                connection.execute(
                    select(schema.pipeline_runs.c.released_at).where(
                        schema.pipeline_runs.c.run_key == "run-1"
                    )
                ).scalar_one()
                is not None
            )
        execution = inspect_run_completion("run-1", engine=pg_engine)
        assert (
            _await_dbos_result(
                execution.workflow_id, registration=registration
            )
            == "aggregate:run-1"
        )
        completed = inspect_run_completion("run-1", engine=pg_engine)
        assert completed.output_reference == "aggregate:run-1"
        assert aggregate_artifact == {
            "membership_digest": digest,
            "consumed": (
                ("input:0", "output:input:0"),
                ("input:1", "output:input:1"),
            ),
        }
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)
