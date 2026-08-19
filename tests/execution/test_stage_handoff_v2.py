from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING
from uuid import uuid4

from dbos import DBOS, Queue
from sqlalchemy import Engine, select

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)
from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    get_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    get_stage_execution,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.runner import AdmissionPayload, run_admission_pass
from dr_platform.execution.failures import StageApplicationFailure
from dr_platform.execution.handoff import (
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.execution.stage_completion import (
    StageCompletion,
    StageSuccessor,
    parse_stage_workflow_result,
)
from dr_platform.inspection.work_items import list_predecessor_stage_outputs
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.retry import retry_stage
from dr_platform.runtime.dispatcher import (
    DispatcherRegistration,  # noqa: TC001
)
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    NOW,
    _args_for,
    _as_dbos_client,
    _migrate,
    _RecordingClient,
    submit_items,
)
from tests.conftest import (
    configure_stage_controls as _configure_controls,
)
from tests.conftest import (
    handoff_utc_now as _utc_now,
)
from tests.conftest import (
    launch_handoff_dbos as _launch_dbos,
)
from tests.conftest import (
    recorded_workflow_id as _recorded_workflow_id,
)
from tests.conftest import (
    stage_state_count as _stage_state_count,
)
from tests.conftest import (
    wait_for_handoff as _wait_for,
)

if TYPE_CHECKING:
    from datetime import datetime

    from dr_platform._core.ledger.schema import LedgerSchema


def _mark_succeeded(
    connection,
    *,
    stage_execution_id: int,
    output_reference: str,
    at: datetime,
) -> None:
    current = get_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
    )
    assert current is not None
    if current.state is StageExecutionState.READY:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=at,
        )
    elif current.state is not StageExecutionState.ADMITTED:
        raise AssertionError(
            f"expected READY or ADMITTED before marking succeeded, "
            f"found {current.state.name}"
        )
    transition_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        new_state=StageExecutionState.SUCCEEDED,
        output_reference=output_reference,
        updated_at=at,
    )


def _mark_failed(
    connection,
    *,
    stage_execution_id: int,
    at: datetime,
) -> None:
    current = get_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
    )
    assert current is not None
    if current.current_attempt == 0:
        append_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            created_at=at,
            admitted_at=at,
        )
    if current.state is StageExecutionState.READY:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=at,
        )
    transition_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        new_state=StageExecutionState.FAILED,
        updated_at=at,
    )
    failed = get_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
    )
    assert failed is not None
    record_stage_attempt_terminal(
        connection,
        stage_execution_id=stage_execution_id,
        attempt_number=failed.current_attempt,
        terminal_at=at,
        terminal_summary={"outcome": "failed"},
    )


def _as_async(logic: Callable[..., object]):
    async def run(*args: object) -> object:
        result = logic(*args)
        if inspect.isawaitable(result):
            result = await result
        return result

    return run


def _pipeline_with_completion(
    *,
    key: str,
    stages: tuple[tuple[str, Callable[[str], object]], ...],
) -> PipelineDefinition:
    def as_async(logic: Callable[[str], object]):
        return _as_async(logic)

    return PipelineDefinition(
        key=PipelineKey(key),
        version=1,
        stages=tuple(
            StageDefinition(
                key=StageKey(stage_key),
                queue_name=f"{key}-{stage_key}-queue",
                workflow=as_async(logic),
                args_for=_args_for,
            )
            for stage_key, logic in stages
        ),
    )


def test_linear_str_return_forwards_output_as_next_input(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    pipeline = _pipeline_with_completion(
        key="linear-compat",
        stages=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    submit_items(
        campaign_key="campaign-linear",
        run_key="run-linear",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input:a", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    client = _RecordingClient()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=_utc_now,
        ).admitted_total
        == 1
    )
    workflow_id = _recorded_workflow_id(client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=True,
            output_reference="prepared:input:a",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="prepared:input:a",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("execute"),
                    stage_index=1,
                    input_reference="prepared:input:a",
                ),
            ),
            completed_at=_utc_now(),
        )

    client.enqueued.clear()
    client.enqueued_args.clear()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=_utc_now,
        ).admitted_total
        == 1
    )
    assert client.enqueued_payloads[0]["input_reference"] == "prepared:input:a"


def test_fan_out_inserts_multiple_successor_rows(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline_with_completion(
        key="fan-out",
        stages=(
            ("split", lambda input_reference: input_reference),
            ("branch_a", lambda input_reference: input_reference),
            ("branch_b", lambda input_reference: input_reference),
            ("join", lambda input_reference: input_reference),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=4)
    submit_items(
        campaign_key="campaign-fanout",
        run_key="run-fanout",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="split",
            stage_index=0,
            succeeded=True,
            output_reference="split:output",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="split:output",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=1,
                    input_reference="row:a",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch_b"),
                    stage_index=2,
                    input_reference="row:b",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
            completed_at=_utc_now(),
        )

    with pg_engine.connect() as connection:
        rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.input_reference,
                    schema.stage_executions.c.barrier,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .mappings()
            .all()
        )

    assert len(rows) == 4
    assert rows[0]["stage_key"] == "split"
    assert rows[0]["state"] == StageExecutionState.SUCCEEDED.value
    assert {row["stage_index"] for row in rows[1:]} == {1, 2, 3}
    assert all(
        row["state"] == StageExecutionState.READY.value for row in rows[1:]
    )
    assert rows[1]["input_reference"] == "row:a"
    assert rows[2]["input_reference"] == "row:b"
    assert rows[3]["barrier"] is True


def test_same_key_fan_out_inserts_sibling_rows(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline_with_completion(
        key="same-key-fan-out",
        stages=(
            ("split", lambda input_reference: input_reference),
            ("branch", lambda input_reference: input_reference),
            ("join", lambda input_reference: input_reference),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=4)
    submit_items(
        campaign_key="campaign-same-key-fanout",
        run_key="run-same-key-fanout",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="split",
            stage_index=0,
            succeeded=True,
            output_reference="split:output",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="split:output",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch"),
                    stage_index=1,
                    input_reference="row:1",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch"),
                    stage_index=2,
                    input_reference="row:2",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
            completed_at=_utc_now(),
        )

    with pg_engine.connect() as connection:
        rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.stage_key,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .mappings()
            .all()
        )

    assert [row["stage_key"] for row in rows] == [
        "split",
        "branch",
        "branch",
        "join",
    ]


def _fan_out_split_handoff(
    pg_engine: Engine,
    *,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
) -> tuple[LedgerSchema, _RecordingClient]:
    schema = _migrate(pg_engine)
    _configure_controls(pg_engine, pipeline, capacity=4)
    submit_items(
        campaign_key="campaign-fanout",
        run_key="run-fanout",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="split",
            stage_index=0,
            succeeded=True,
            output_reference="split:output",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="split:output",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=1,
                    input_reference="row:a",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch_b"),
                    stage_index=2,
                    input_reference="row:b",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
            completed_at=_utc_now(),
        )
    return schema, client


def test_barrier_join_not_admitted_until_branches_succeed(
    pg_engine: Engine,
) -> None:
    pipeline = _pipeline_with_completion(
        key="barrier-join",
        stages=(
            ("split", lambda input_reference: input_reference),
            ("branch_a", lambda input_reference: input_reference),
            ("branch_b", lambda input_reference: input_reference),
            ("join", lambda input_reference: input_reference),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    schema, _client = _fan_out_split_handoff(
        pg_engine, registry=registry, pipeline=pipeline
    )

    branch_client = _RecordingClient()
    first_pass = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(branch_client),
        registry=registry,
        clock=_utc_now,
    )
    assert first_pass.admitted_total == 2
    assert first_pass.skipped_for_barrier >= 1
    assert (
        _stage_state_count(
            pg_engine,
            schema,
            stage_index=3,
            state=StageExecutionState.READY,
        )
        == 1
    )

    with pg_engine.connect() as connection:
        branch_ids = (
            connection.execute(
                select(schema.stage_executions.c.stage_execution_id).where(
                    schema.stage_executions.c.stage_index.in_((1, 2))
                )
            )
            .scalars()
            .all()
        )
    with pg_engine.begin() as connection:
        for stage_execution_id in branch_ids:
            _mark_succeeded(
                connection,
                stage_execution_id=stage_execution_id,
                output_reference=f"out:{stage_execution_id}",
                at=_utc_now(),
            )

    join_client = _RecordingClient()
    second_pass = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(join_client),
        registry=registry,
        clock=_utc_now,
    )
    assert second_pass.admitted_total == 1
    assert join_client.enqueued_payloads[0]["stage_key"] == "join"


def test_barrier_failed_sibling_blocks_join(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline_with_completion(
        key="barrier-failed-sibling",
        stages=(
            ("branch_a", lambda input_reference: input_reference),
            ("join", lambda input_reference: input_reference),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=4)
    branch_execution_id: int
    with pg_engine.begin() as connection:
        run_key = "run-barrier-failed"
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key=run_key,
                campaign_key="campaign-barrier-failed",
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                execution_config_reference="config:1",
                expected_member_count=1,
                created_at=NOW,
            )
        )
        work_item_id = connection.execute(
            schema.work_items.insert()
            .values(
                campaign_key="campaign-barrier-failed",
                work_key="work-barrier-failed",
                origin_run_key=run_key,
                input_reference="seed",
                labels={},
                rank=1,
            )
            .returning(schema.work_items.c.work_item_id)
        ).scalar_one()
        split = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="split",
            stage_index=0,
            input_reference="seed",
            created_at=NOW,
        )
        _mark_succeeded(
            connection,
            stage_execution_id=split.stage_execution_id,
            output_reference="split:out",
            at=NOW,
        )
        branch = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="branch_a",
            stage_index=1,
            input_reference="row:a",
            created_at=NOW,
        )
        _mark_failed(
            connection,
            stage_execution_id=branch.stage_execution_id,
            at=NOW,
        )
        branch_execution_id = branch.stage_execution_id
        insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="join",
            stage_index=3,
            input_reference="join:pending",
            barrier=True,
            created_at=NOW,
        )

    blocked = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=_utc_now,
    )
    assert blocked.admitted_total == 0
    assert blocked.skipped_for_barrier >= 1

    retry_stage(
        stage_execution_id=branch_execution_id,
        engine=pg_engine,
        clock=_utc_now,
    )
    with pg_engine.begin() as connection:
        _mark_succeeded(
            connection,
            stage_execution_id=branch_execution_id,
            output_reference="branch:out",
            at=_utc_now(),
        )

    join_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(join_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    assert join_client.enqueued_payloads[0]["stage_key"] == "join"


def test_run_barrier_waits_for_barrier_join(pg_engine: Engine) -> None:
    from dr_platform.completion.barrier import run_barrier_pass
    from dr_platform.submission.stream import (
        RunMemberInput,
        RunRegistrationDeclaration,
        compute_run_membership_digest,
        submit,
    )

    async def _completion(_payload: object) -> str:
        return "aggregate:done"

    declared = PipelineDefinition(
        key=PipelineKey("barrier-run"),
        version=1,
        stages=_pipeline_with_completion(
            key="barrier-run-stages",
            stages=(
                ("split", lambda input_reference: input_reference),
                ("branch_a", lambda input_reference: input_reference),
                ("branch_b", lambda input_reference: input_reference),
                ("join", lambda input_reference: input_reference),
            ),
        ).stages,
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name="barrier-run-aggregate",
            workflow=_completion,
            args_for=_args_for,
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        max_recovery_attempts=1,
        clock=_utc_now,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    schema = _migrate(pg_engine)
    _configure_controls(pg_engine, pipeline, capacity=4)
    members = (
        RunMemberInput(
            ordinal=0,
            work=WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
            ),
        ),
    )
    digest = compute_run_membership_digest(
        members,
        expected_member_count=len(members),
    )
    submit(
        campaign_key="campaign-barrier-run",
        run_key="run-barrier-join",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(
            len(members),
            "manifest:run-barrier-join",
            digest,
        ),
        members=members,
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    split_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(split_client),
        registry=registry,
        clock=_utc_now,
    )
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=_recorded_workflow_id(split_client.enqueued[0]),
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="split",
            stage_index=0,
            succeeded=True,
            output_reference="split:output",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="split:output",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=1,
                    input_reference="row:a",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch_b"),
                    stage_index=2,
                    input_reference="row:b",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
            completed_at=_utc_now(),
        )

    waiting = run_barrier_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=_utc_now,
    )
    assert waiting.releases == ()

    with pg_engine.connect() as connection:
        branch_ids = (
            connection.execute(
                select(schema.stage_executions.c.stage_execution_id).where(
                    schema.stage_executions.c.stage_index.in_((1, 2))
                )
            )
            .scalars()
            .all()
        )
    with pg_engine.begin() as connection:
        for stage_execution_id in branch_ids:
            _mark_succeeded(
                connection,
                stage_execution_id=stage_execution_id,
                output_reference=f"out:{stage_execution_id}",
                at=_utc_now(),
            )

    join_client = _RecordingClient()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(join_client),
            registry=registry,
            clock=_utc_now,
        ).admitted_total
        == 1
    )
    with pg_engine.connect() as connection:
        join_id = connection.execute(
            select(schema.stage_executions.c.stage_execution_id).where(
                schema.stage_executions.c.stage_index == 3
            )
        ).scalar_one()
    with pg_engine.begin() as connection:
        _mark_succeeded(
            connection,
            stage_execution_id=join_id,
            output_reference="join:out",
            at=_utc_now(),
        )

    released = run_barrier_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=_utc_now,
    )
    assert len(released.releases) == 1


def test_loop_allows_same_stage_key_at_higher_index(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        run_key = "run-loop"
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key=run_key,
                campaign_key="campaign-loop",
                pipeline_key="optimizer",
                pipeline_version=1,
                execution_config_reference="config:1",
                expected_member_count=1,
                created_at=NOW,
            )
        )
        work_item_id = connection.execute(
            schema.work_items.insert()
            .values(
                campaign_key="campaign-loop",
                work_key="work-loop",
                origin_run_key=run_key,
                input_reference="seed",
                labels={},
                rank=1,
            )
            .returning(schema.work_items.c.work_item_id)
        ).scalar_one()
        first = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="optim_step",
            stage_index=0,
            input_reference="seed",
            created_at=NOW,
        )
        _mark_succeeded(
            connection,
            stage_execution_id=first.stage_execution_id,
            output_reference="optim:0",
            at=NOW,
        )
        loop = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="optim_step",
            stage_index=3,
            input_reference="optim:0",
            created_at=NOW,
        )

    assert loop.stage_key.value == "optim_step"
    assert loop.stage_index == 3
    with pg_engine.connect() as connection:
        keys = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.stage_index,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .tuples()
            .all()
        )
    assert keys == [("optim_step", 0), ("optim_step", 3)]


def test_loop_iteration_admits_with_unique_workflow_id(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline_with_completion(
        key="optimizer",
        stages=(("optim_step", lambda input_reference: input_reference),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    submit_items(
        campaign_key="campaign-loop-admit",
        run_key="run-loop-admit",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(WorkInput(work_key="work", input_reference="seed", labels={}),),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    client = _RecordingClient()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(client),
            registry=registry,
            clock=_utc_now,
        ).admitted_total
        == 1
    )
    first_workflow_id = _recorded_workflow_id(client.enqueued[0])
    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=first_workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="optim_step",
            stage_index=0,
            succeeded=True,
            output_reference="optim:0",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="optim:0",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("optim_step"),
                    stage_index=3,
                    input_reference="optim:0",
                ),
            ),
            completed_at=_utc_now(),
        )

    loop_client = _RecordingClient()
    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(loop_client),
        registry=registry,
        clock=_utc_now,
    )
    loop_workflow_id = _recorded_workflow_id(loop_client.enqueued[0])
    with pg_engine.connect() as connection:
        loop_row = connection.execute(
            select(
                schema.stage_executions.c.state,
                schema.stage_executions.c.current_attempt,
            ).where(schema.stage_executions.c.stage_index == 3)
        ).one()

    assert summary.admitted_total == 1
    assert loop_client.enqueued_payloads[0]["stage_index"] == 3
    assert loop_row.state == StageExecutionState.ADMITTED.value
    assert loop_row.current_attempt == 1
    assert loop_workflow_id != first_workflow_id


def test_loop_iteration_completes_through_wrapped_workflow(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def optim_step(input_reference: str) -> object:
        if input_reference == "seed":
            return StageCompletion(
                output_reference="optim:0",
                successors=(
                    StageSuccessor(
                        stage_key=StageKey("optim_step"),
                        stage_index=3,
                        input_reference="optim:0",
                    ),
                ),
            )
        return StageCompletion(
            output_reference="optim:loop:done",
            successors=(),
        )

    declared = _pipeline_with_completion(
        key=f"optimizer-{suffix}",
        stages=(("optim_step", optim_step),),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        clock=_utc_now,
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    submit_items(
        campaign_key=f"campaign-loop-e2e-{suffix}",
        run_key=f"run-loop-e2e-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(WorkInput(work_key="work", input_reference="seed", labels={}),),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )

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
            loop_row = connection.execute(
                select(
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.input_reference,
                ).where(schema.stage_executions.c.stage_index == 3)
            ).one()
        assert loop_row.state == StageExecutionState.ADMITTED.value
        assert loop_row.input_reference == "optim:0"

        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=3,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        with pg_engine.connect() as connection:
            output_reference = connection.execute(
                select(schema.stage_executions.c.output_reference).where(
                    schema.stage_executions.c.stage_index == 3
                )
            ).scalar_one()
        assert output_reference == "optim:loop:done"
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_fan_out_barrier_join_streams_end_to_end_through_wrapped_workflows(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def split(_input_reference: str) -> StageCompletion:
        return StageCompletion(
            output_reference="split:output",
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=1,
                    input_reference="row:a",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch_b"),
                    stage_index=2,
                    input_reference="row:b",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
        )

    def branch_a(input_reference: str) -> StageCompletion:
        return StageCompletion(output_reference=f"branch:{input_reference}")

    def branch_b(input_reference: str) -> StageCompletion:
        return StageCompletion(output_reference=f"branch:{input_reference}")

    def join(payload: AdmissionPayload) -> str:
        outputs = list_predecessor_stage_outputs(
            payload.work_item_id,
            payload.stage_index,
            engine=pg_engine,
        )
        refs = "|".join(item.output_reference for item in outputs)
        return f"join:{payload.input_reference}:{refs}"

    def join_args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload,)

    declared = PipelineDefinition(
        key=PipelineKey(f"fanout-barrier-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("split"),
                queue_name=f"fanout-barrier-{suffix}-split-queue",
                workflow=_as_async(split),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("branch_a"),
                queue_name=f"fanout-barrier-{suffix}-branch_a-queue",
                workflow=_as_async(branch_a),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("branch_b"),
                queue_name=f"fanout-barrier-{suffix}-branch_b-queue",
                workflow=_as_async(branch_b),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("join"),
                queue_name=f"fanout-barrier-{suffix}-join-queue",
                workflow=_as_async(join),
                args_for=join_args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        clock=_utc_now,
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=4)
    submit_items(
        campaign_key=f"campaign-fanout-e2e-{suffix}",
        run_key=f"run-fanout-e2e-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )

        branch_pass = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert branch_pass.admitted_total == 2
        assert branch_pass.skipped_for_barrier == 1
        assert (
            _stage_state_count(
                pg_engine,
                schema,
                stage_index=3,
                state=StageExecutionState.READY,
            )
            == 1
        )

        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=1,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
                and _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=2,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )

        join_pass = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert join_pass.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=3,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        with pg_engine.connect() as connection:
            output_reference = connection.execute(
                select(schema.stage_executions.c.output_reference).where(
                    schema.stage_executions.c.stage_index == 3
                )
            ).scalar_one()
        assert output_reference == (
            "join:join:pending:split:output|branch:row:a|branch:row:b"
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_failed_sibling_blocks_barrier_join_until_retry_e2e(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    branch_attempts = {"row:a": 0}

    def split(_input_reference: str) -> StageCompletion:
        return StageCompletion(
            output_reference="split:output",
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=1,
                    input_reference="row:a",
                ),
                StageSuccessor(
                    stage_key=StageKey("branch_b"),
                    stage_index=2,
                    input_reference="row:b",
                ),
                StageSuccessor(
                    stage_key=StageKey("join"),
                    stage_index=3,
                    input_reference="join:pending",
                    barrier=True,
                ),
            ),
        )

    def branch_a(input_reference: str) -> StageCompletion:
        if input_reference == "row:a":
            branch_attempts[input_reference] += 1
            if branch_attempts[input_reference] == 1:
                raise StageApplicationFailure("fail once")
        return StageCompletion(output_reference=f"branch:{input_reference}")

    def branch_b(input_reference: str) -> StageCompletion:
        return StageCompletion(output_reference=f"branch:{input_reference}")

    def join(payload: AdmissionPayload) -> str:
        outputs = list_predecessor_stage_outputs(
            payload.work_item_id,
            payload.stage_index,
            engine=pg_engine,
        )
        refs = "|".join(item.output_reference for item in outputs)
        return f"join:{payload.input_reference}:{refs}"

    def join_args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload,)

    declared = PipelineDefinition(
        key=PipelineKey(f"fanout-retry-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("split"),
                queue_name=f"fanout-retry-{suffix}-split-queue",
                workflow=_as_async(split),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("branch_a"),
                queue_name=f"fanout-retry-{suffix}-branch_a-queue",
                workflow=_as_async(branch_a),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("branch_b"),
                queue_name=f"fanout-retry-{suffix}-branch_b-queue",
                workflow=_as_async(branch_b),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("join"),
                queue_name=f"fanout-retry-{suffix}-join-queue",
                workflow=_as_async(join),
                args_for=join_args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        clock=_utc_now,
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=4)
    submit_items(
        campaign_key=f"campaign-fanout-retry-{suffix}",
        run_key=f"run-fanout-retry-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=1,
                    state=StageExecutionState.FAILED,
                )
                == 1
                and _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=2,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )

        blocked = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert blocked.admitted_total == 0
        assert blocked.skipped_for_barrier == 1
        assert (
            _stage_state_count(
                pg_engine,
                schema,
                stage_index=3,
                state=StageExecutionState.READY,
            )
            == 1
        )

        with pg_engine.connect() as connection:
            failed_branch_id = connection.execute(
                select(schema.stage_executions.c.stage_execution_id).where(
                    schema.stage_executions.c.stage_index == 1
                )
            ).scalar_one()
        retry_stage(
            stage_execution_id=failed_branch_id,
            engine=pg_engine,
            clock=_utc_now,
        )
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=1,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=3,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_str_return_at_non_registration_index_raises_in_wrapped_workflow(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def only(input_reference: str) -> object:
        if input_reference == "seed":
            return StageCompletion(
                output_reference="split:output",
                successors=(
                    StageSuccessor(
                        stage_key=StageKey("only"),
                        stage_index=1,
                        input_reference="row:only",
                    ),
                ),
            )
        return "leaf:output"

    declared = PipelineDefinition(
        key=PipelineKey(f"str-pin-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("only"),
                queue_name=f"str-pin-{suffix}-only-queue",
                workflow=_as_async(only),
                args_for=_args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        clock=_utc_now,
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    submit_items(
        campaign_key=f"campaign-str-pin-{suffix}",
        run_key=f"run-str-pin-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(WorkInput(work_key="work", input_reference="seed", labels={}),),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=1,
                    state=StageExecutionState.FAILED,
                )
                == 1
            ),
            timeout_seconds=20,
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_str_return_at_non_registration_fan_out_leaf_fails(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def split(_input_reference: str) -> StageCompletion:
        return StageCompletion(
            output_reference="split:output",
            successors=(
                StageSuccessor(
                    stage_key=StageKey("branch_a"),
                    stage_index=2,
                    input_reference="row:a",
                ),
            ),
        )

    def branch_a(_input_reference: str) -> str:
        return "leaf:output"

    declared = PipelineDefinition(
        key=PipelineKey(f"str-fanout-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("split"),
                queue_name=f"str-fanout-{suffix}-split-queue",
                workflow=_as_async(split),
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("branch_a"),
                queue_name=f"str-fanout-{suffix}-branch-queue",
                workflow=_as_async(branch_a),
                args_for=_args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared,
        clock=_utc_now,
        max_recovery_attempts=1,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    submit_items(
        campaign_key=f"campaign-str-fanout-{suffix}",
        run_key=f"run-str-fanout-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=(WorkInput(work_key="work", input_reference="seed", labels={}),),
        registry=registry,
        engine=pg_engine,
        clock=_utc_now,
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
        assert (
            run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            ).admitted_total
            == 1
        )
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=2,
                    state=StageExecutionState.FAILED,
                )
                == 1
            ),
            timeout_seconds=20,
        )
        with pg_engine.connect() as connection:
            stage_execution_id = connection.execute(
                select(schema.stage_executions.c.stage_execution_id).where(
                    schema.stage_executions.c.stage_index == 2,
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value,
                )
            ).scalar_one()
            attempt = get_stage_attempt(
                connection,
                stage_execution_id=stage_execution_id,
                attempt_number=1,
            )
        assert attempt is not None
        assert attempt.terminal_summary is not None
        message = attempt.terminal_summary.get("message")
        assert isinstance(message, str)
        assert "non-registration index" in message
        assert "StageCompletion, not str" in message
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_stage_completion_return_type_is_accepted_by_parser() -> None:
    pipeline = _pipeline_with_completion(
        key="typed",
        stages=(("only", lambda input_reference: input_reference),),
    )
    completion = StageCompletion(
        output_reference="done",
        successors=(
            StageSuccessor(
                stage_key=StageKey("only"),
                stage_index=5,
                input_reference="next",
            ),
        ),
    )
    parsed = parse_stage_workflow_result(
        completion,
        pipeline=pipeline,
        current_stage_index=2,
    )
    assert parsed.output_reference == "done"
    assert len(parsed.successors) == 1
