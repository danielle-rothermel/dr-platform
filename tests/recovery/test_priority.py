from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import Engine, select, text

from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import set_stage_capacity
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import (
    StageSuccessor,
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.priority import set_work_priority
from dr_platform.runtime.dbos import DBOS_WORKFLOW_STATUS_TABLE
from dr_platform.submission.stream import WorkInput
from tests.admission.test_runner import (
    _as_dbos_client,
    _pipeline_control,
    _RecordingClient,
    _starvation_registry,
    _submit_two_stage_backlog,
)
from tests.conftest import NOW, _migrate, submit_items
from tests.conftest import recorded_workflow_id as _recorded_workflow_id
from tests.execution.test_handoff import _launch_dbos


async def _workflow(*args: object) -> str:
    return repr(args)


def _args_for(*args: object) -> tuple[object, ...]:
    return args


def test_submission_pins_work_priority(pg_engine) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="execute-queue",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work-a",
                input_reference="input:a",
                labels={},
                priority=5,
            ),
        ),
        registry=registry,
        engine=pg_engine,
    )
    with pg_engine.connect() as connection:
        priority = connection.execute(
            select(schema.work_items.c.priority).where(
                schema.work_items.c.work_key == "work-a"
            )
        ).scalar_one()
    assert priority == 5


def test_admission_prefers_lower_priority_before_rank(pg_engine) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-admit-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="execute-queue",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
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
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work-high",
                input_reference="input:high",
                labels={},
                priority=9,
            ),
            WorkInput(
                work_key="work-low",
                input_reference="input:low",
                labels={},
                priority=1,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        expected_member_count=2,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )
    with pg_engine.connect() as connection:
        admitted_work_key = connection.execute(
            select(schema.work_items.c.work_key)
            .select_from(schema.stage_executions.join(schema.work_items))
            .where(
                schema.stage_executions.c.state
                == StageExecutionState.ADMITTED.value
            )
        ).scalar_one()
    assert admitted_work_key == "work-low"
    assert client.enqueued[0]["priority"] == 1


def test_set_work_priority_reorders_ready_executions(pg_engine) -> None:
    schema = _migrate(pg_engine)
    registry = _starvation_registry()
    _submit_two_stage_backlog(pg_engine, registry)
    _pipeline_control(pg_engine, suffix="a", capacity=5, paused=False)
    result = set_work_priority(
        campaign_key=CampaignKey("campaign-two-stage"),
        work_key=WorkKey("work-b"),
        priority=0,
        engine=pg_engine,
    )
    assert result.priority == 0
    with pg_engine.connect() as connection:
        priorities = dict(
            connection.execute(
                select(
                    schema.work_items.c.work_key,
                    schema.stage_executions.c.priority,
                )
                .select_from(schema.stage_executions.join(schema.work_items))
                .where(schema.stage_executions.c.state == "ready")
            )
            .tuples()
            .all()
        )
    assert priorities["work-b"] == 0


def test_set_work_priority_updates_dbos_workflow_status(
    clean_pg: str,
    pg_engine,
) -> None:
    _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = wrap_pipeline_workflows(
        PipelineDefinition(
            key=PipelineKey(f"priority-boost-{suffix}"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name=f"boost-{suffix}",
                    workflow=_workflow,
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
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
                priority=9,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    Queue(pipeline.stages[0].queue_name, priority_enabled=True)
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
        result = set_work_priority(
            campaign_key=f"campaign-{suffix}",
            work_key="work",
            priority=1,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=1),
        )
        assert result.updated_workflow_ids
        with pg_engine.connect() as connection:
            dbos_priority = connection.execute(
                text(
                    """
                    SELECT priority FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": result.updated_workflow_ids[0]},
            ).scalar_one()
        assert DBOS_WORKFLOW_STATUS_TABLE == "dbos.workflow_status"
        assert dbos_priority == 1
    finally:
        registration.close()
        DBOS.destroy()


def test_set_work_priority_does_not_deadlock_concurrent_handoff(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-handoff-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("prepare"),
                queue_name=f"prepare-{suffix}",
                workflow=_workflow,
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"execute-{suffix}",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    campaign_key = f"campaign-{suffix}"
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("prepare"),
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=campaign_key,
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority-handoff",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
                priority=3,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=lambda: NOW,
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0])

    def complete_handoff() -> None:
        with pg_engine.begin() as connection:
            _complete_stage_in_transaction(
                connection,
                workflow_id=workflow_id,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key="prepare",
                stage_index=0,
                succeeded=True,
                output_reference="output:prepare",
                terminal_summary={"outcome": "succeeded"},
                terminal_reference="output:prepare",
                evidence=None,
                successors=(
                    StageSuccessor(
                        stage_key=StageKey("execute"),
                        stage_index=1,
                        input_reference="output:prepare",
                    ),
                ),
                completed_at=NOW + timedelta(seconds=10),
            )

    def boost_priority() -> None:
        set_work_priority(
            campaign_key=campaign_key,
            work_key="work",
            priority=0,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=2),
        )

    start = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        handoff_future = executor.submit(
            lambda: (start.wait(timeout=10), complete_handoff())[1]
        )
        priority_future = executor.submit(
            lambda: (start.wait(timeout=10), boost_priority())[1]
        )
        handoff_future.result(timeout=10)
        priority_future.result(timeout=10)

    with pg_engine.connect() as connection:
        work_priority = connection.execute(
            select(schema.work_items.c.priority).where(
                schema.work_items.c.work_key == "work"
            )
        ).scalar_one()
        execute_priority = connection.execute(
            select(schema.stage_executions.c.priority)
            .where(
                schema.stage_executions.c.stage_key == "execute",
                schema.stage_executions.c.state
                == StageExecutionState.READY.value,
            )
            .limit(1)
        ).scalar_one()
    assert work_priority == 0
    assert execute_priority == 0


def test_set_work_priority_clamps_updated_at_after_handoff_successor(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-clamp-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("prepare"),
                queue_name=f"prepare-{suffix}",
                workflow=_workflow,
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"execute-{suffix}",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    campaign_key = f"campaign-{suffix}"
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("prepare"),
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=campaign_key,
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority-clamp",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
                priority=9,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=lambda: NOW,
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("execute"),
                    stage_index=1,
                    input_reference="output:prepare",
                ),
            ),
            completed_at=NOW + timedelta(seconds=10),
        )
    set_work_priority(
        campaign_key=campaign_key,
        work_key="work",
        priority=0,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    with pg_engine.connect() as connection:
        created_at, updated_at, priority = connection.execute(
            select(
                schema.stage_executions.c.created_at,
                schema.stage_executions.c.updated_at,
                schema.stage_executions.c.priority,
            ).where(
                schema.stage_executions.c.stage_key == "execute",
                schema.stage_executions.c.state
                == StageExecutionState.READY.value,
            )
        ).one()
    assert priority == 0
    assert updated_at >= created_at
    assert updated_at == NOW + timedelta(seconds=10)


def test_reuse_syncs_dbos_priority_for_admitted_work(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = wrap_pipeline_workflows(
        PipelineDefinition(
            key=PipelineKey(f"priority-reuse-{suffix}"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name=f"reuse-{suffix}",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        ),
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    campaign_key = f"campaign-{suffix}"
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=campaign_key,
        run_key=f"run-a-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
                priority=9,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    Queue(pipeline.stages[0].queue_name, priority_enabled=True)
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
        with pg_engine.connect() as connection:
            workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .select_from(
                    schema.stage_attempts.join(schema.stage_executions).join(
                        schema.work_items
                    )
                )
                .where(
                    schema.work_items.c.work_key == "work",
                    schema.stage_executions.c.state
                    == StageExecutionState.ADMITTED.value,
                )
            ).scalar_one()
        submit_items(
            campaign_key=campaign_key,
            run_key=f"run-b-{suffix}",
            pipeline=pipeline.identity,
            execution_config_reference="config:priority",
            items=(
                WorkInput(
                    work_key="work",
                    input_reference="input",
                    labels={},
                    priority=1,
                ),
            ),
            registry=registry,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=1),
        )
        with pg_engine.connect() as connection:
            dbos_priority = connection.execute(
                text(
                    """
                    SELECT priority FROM dbos.workflow_status
                    WHERE workflow_uuid = :workflow_id
                    """
                ),
                {"workflow_id": workflow_id},
            ).scalar_one()
        assert dbos_priority == 1
    finally:
        registration.close()
        DBOS.destroy()


def test_reuse_priority_sync_does_not_deadlock_set_work_priority(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-reuse-race-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"reuse-race-{suffix}",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    campaign_key = f"campaign-{suffix}"
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=campaign_key,
        run_key=f"run-a-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
                priority=9,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW,
    )

    def reuse_work() -> None:
        submit_items(
            campaign_key=campaign_key,
            run_key=f"run-b-{suffix}",
            pipeline=pipeline.identity,
            execution_config_reference="config:priority",
            items=(
                WorkInput(
                    work_key="work",
                    input_reference="input",
                    labels={},
                    priority=0,
                ),
            ),
            registry=registry,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=2),
        )

    def boost_priority() -> None:
        set_work_priority(
            campaign_key=campaign_key,
            work_key="work",
            priority=1,
            engine=pg_engine,
            clock=lambda: NOW + timedelta(seconds=2),
        )

    start = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as executor:
        reuse_future = executor.submit(
            lambda: (start.wait(timeout=10), reuse_work())[1]
        )
        priority_future = executor.submit(
            lambda: (start.wait(timeout=10), boost_priority())[1]
        )
        reuse_future.result(timeout=10)
        priority_future.result(timeout=10)
