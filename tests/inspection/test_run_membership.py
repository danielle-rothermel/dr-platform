from __future__ import annotations

from sqlalchemy import Engine, event, update

from dr_platform._core.identities import PipelineKey, RunKey, StageKey
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.inspection.campaigns import list_runs
from dr_platform.inspection.statuses import (
    StateCount,
    bulk_run_state_counts,
    run_state_counts,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.submission.stream import (
    RunMemberInput,
    RunRegistrationDeclaration,
    WorkInput,
    submit,
)
from tests.conftest import NOW, _migrate


async def _workflow(value: str) -> str:
    return f"output:{value}"


def _args_for(payload: object) -> tuple[object, ...]:
    return (payload,)


def _registry() -> tuple[PipelineRegistry, PipelineDefinition]:
    pipeline = PipelineDefinition(
        key=PipelineKey("inspection-membership"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="execute",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    return registry, pipeline


def _submit_run(
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    run_key: str,
    work_indexes: tuple[int, ...],
) -> None:
    submit(
        campaign_key="campaign-1",
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(len(work_indexes)),
        members=(
            RunMemberInput(
                ordinal=ordinal,
                work=WorkInput(
                    work_key=f"work-{work_index}",
                    input_reference=f"input:{work_index}",
                    labels={},
                ),
            )
            for ordinal, work_index in enumerate(work_indexes)
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


def test_run_counts_follow_overlapping_membership(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-a",
        work_indexes=(0, 1),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-b",
        work_indexes=(1, 2),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.stage_executions)
            .where(schema.stage_executions.c.work_item_id == 2)
            .values(
                state=StageExecutionState.SUCCEEDED.value,
                output_reference="output:1",
            )
        )
    expected = (
        StateCount(state=StageExecutionState.READY, count=1),
        StateCount(state=StageExecutionState.SUCCEEDED, count=1),
    )
    assert run_state_counts("run-a", engine=pg_engine) == expected
    assert run_state_counts("run-b", engine=pg_engine) == expected
    assert [
        run.registered_member_count
        for run in list_runs("campaign-1", engine=pg_engine)
    ] == [2, 2]


def test_bulk_run_counts_distinguish_missing_and_present_empty(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-empty",
        work_indexes=(),
    )
    counts = bulk_run_state_counts(
        ("run-empty", "run-missing"), engine=pg_engine
    )
    assert counts == {
        RunKey("run-empty"): (),
        RunKey("run-missing"): None,
    }


def test_bulk_run_counts_execute_one_query_per_input_chunk(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    for index in range(5):
        _submit_run(
            pg_engine,
            registry,
            pipeline,
            run_key=f"run-{index}",
            work_indexes=(index,),
        )
    statements = 0

    def before_cursor_execute(*_args: object) -> None:
        nonlocal statements
        statements += 1

    event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
    try:
        counts = bulk_run_state_counts(
            tuple(f"run-{index}" for index in range(5)),
            engine=pg_engine,
            chunk_size=2,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", before_cursor_execute)
    assert len(counts) == 5
    assert statements == 3


def test_paged_run_count_consumer_has_no_per_run_queries(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    for index in range(5):
        _submit_run(
            pg_engine,
            registry,
            pipeline,
            run_key=f"run-{index}",
            work_indexes=(index,),
        )
    statements = 0

    def before_cursor_execute(*_args: object) -> None:
        nonlocal statements
        statements += 1

    event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
    try:
        page = list_runs("campaign-1", engine=pg_engine, limit=5)
        counts = bulk_run_state_counts(
            tuple(run.run_key for run in page), engine=pg_engine
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", before_cursor_execute)
    assert len(counts) == 5
    assert statements == 3
