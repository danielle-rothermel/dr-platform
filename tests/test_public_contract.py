from __future__ import annotations

import time
from datetime import UTC, datetime
from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import Engine

from dr_platform import (
    AdmissionPayload,
    LiveDbosIdentity,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    PlatformDbosConfig,
    RunMemberInput,
    RunRegistrationDeclaration,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkInput,
    bulk_work_statuses,
    initialize_dbos_runtime,
    inspect_campaign,
    register_scheduled_dispatcher,
    set_stage_capacity,
    submit,
    upgrade_platform_schema,
    wrap_pipeline_workflows,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _wait_until_stage_finished(
    campaign_key: str,
    work_keys: tuple[str, ...],
    *,
    stage_index: int,
    terminal_stage: bool,
    engine: Engine,
) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        statuses = bulk_work_statuses(
            campaign_key,
            work_keys,
            engine=engine,
        ).statuses.values()
        if all(
            (
                status.current_stage_index is not None
                and status.current_stage_index > stage_index
            )
            or (
                terminal_stage
                and status.state is StageExecutionState.SUCCEEDED
            )
            for status in statuses
        ):
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for the public staged-work flow")


def test_root_contract_defines_submits_executes_and_inspects(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(clean_pg)
    suffix = uuid4().hex[:10]
    campaign_key = f"campaign-{suffix}"
    work_keys = ("work-a", "work-b")

    def args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload.input_reference,)

    async def prepare(input_reference: str) -> str:
        return f"prepared:{input_reference}"

    async def execute(input_reference: str) -> str:
        return f"executed:{input_reference}"

    async def score(input_reference: str) -> str:
        return f"scored:{input_reference}"

    declared = PipelineDefinition(
        key=PipelineKey(f"public-contract-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("prepare"),
                queue_name=f"prepare-{suffix}",
                workflow=prepare,
                args_for=args_for,
            ),
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"execute-{suffix}",
                workflow=execute,
                args_for=args_for,
            ),
            StageDefinition(
                key=StageKey("score"),
                queue_name=f"score-{suffix}",
                workflow=score,
                args_for=args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, max_recovery_attempts=1)
    registry = PipelineRegistry()
    registry.register(pipeline)
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)
        set_stage_capacity(
            pipeline=pipeline.identity,
            stage_key=stage.key,
            capacity=2,
            engine=pg_engine,
        )

    yielded: list[str] = []

    def items():
        for work_key in work_keys:
            yielded.append(work_key)
            yield WorkInput(
                work_key=work_key,
                input_reference=f"input:{work_key}",
                labels={"kind": "neutral"},
            )

    receipt = submit(
        campaign_key=campaign_key,
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:public-contract",
        declaration=RunRegistrationDeclaration(len(work_keys)),
        members=(
            RunMemberInput(ordinal=index, work=item)
            for index, item in enumerate(items())
        ),
        registry=registry,
        engine=pg_engine,
        chunk_size=1,
    )
    assert yielded == list(work_keys)
    assert receipt.created_work_count == 2

    platform_config = PlatformDbosConfig(
        database_url=clean_pg,
        system_database_url=clean_pg,
        max_recovery_attempts=1,
    )
    registration = None
    try:
        initialize_dbos_runtime(
            platform_config,
            app_name=f"drp-public-{suffix}",
        )
        registration = register_scheduled_dispatcher(
            live_dbos_identity=LiveDbosIdentity(
                app_version=DBOS.application_version,
                executor_ids=frozenset({DBOS.executor_id}),
            ),
            config=platform_config,
            engine=pg_engine,
            registry=registry,
        )
        DBOS.launch()
        # DBOSClient enqueues versionless work for the latest-version worker.
        DBOS.set_latest_application_version(DBOS.application_version)
        last_stage_index = len(pipeline.stages) - 1
        for stage_index in range(len(pipeline.stages)):
            registration.workflow(_utc_now(), _utc_now())
            _wait_until_stage_finished(
                campaign_key,
                work_keys,
                stage_index=stage_index,
                terminal_stage=stage_index == last_stage_index,
                engine=pg_engine,
            )

        campaign = inspect_campaign(campaign_key, engine=pg_engine)
        statuses = bulk_work_statuses(
            campaign_key,
            work_keys,
            engine=pg_engine,
        )
        assert campaign.run_count == 1
        assert campaign.work_item_count == 2
        assert all(
            status.present
            and str(status.current_stage_key) == "score"
            and status.state is StageExecutionState.SUCCEEDED
            for status in statuses.statuses.values()
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)
