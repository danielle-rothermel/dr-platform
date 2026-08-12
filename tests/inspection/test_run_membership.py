from __future__ import annotations

from sqlalchemy import Engine, event, text, update

from dr_platform._core.identities import PipelineKey, RunKey, StageKey
from dr_platform._core.ledger.states import StageExecutionState, StateCount
from dr_platform.inspection.campaigns import _run_summary_statement, list_runs
from dr_platform.inspection.statuses import (
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


def test_list_runs_bounds_planner_work_to_selected_page(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    page_size = 5
    page_members_per_run = 2
    history_run_count = 2_000
    history_members_per_run = 50
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    created_at
                )
                SELECT
                    'page-' || lpad(run_index::text, 4, '0'),
                    'campaign-plan', 'inspection-membership', 1,
                    'config:plan', :members_per_run, :created_at
                FROM generate_series(0, :last_run_index) AS run_index
                """
            ),
            {
                "created_at": NOW,
                "last_run_index": page_size - 1,
                "members_per_run": page_members_per_run,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    created_at
                )
                SELECT
                    'history-' || lpad(run_index::text, 4, '0'),
                    'campaign-plan', 'inspection-membership', 1,
                    'config:plan', :members_per_run, :created_at
                FROM generate_series(0, :last_run_index) AS run_index
                """
            ),
            {
                "created_at": NOW.replace(year=NOW.year + 1),
                "last_run_index": history_run_count - 1,
                "members_per_run": history_members_per_run,
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'campaign-plan',
                        'page-work-' || member_index::text,
                        'page-' || lpad(
                            (member_index / :members_per_run)::text,
                            4,
                            '0'
                        ),
                        'input:page-' || member_index::text,
                        '{}'::jsonb,
                        member_index + 1
                    FROM generate_series(0, :last_member_index)
                        AS member_index
                    RETURNING work_item_id, origin_run_key, rank
                )
                INSERT INTO platform_run_memberships (
                    run_key, member_ordinal, work_item_id
                )
                SELECT
                    origin_run_key,
                    (rank - 1) % :members_per_run,
                    work_item_id
                FROM inserted_work
                """
            ),
            {
                "last_member_index": (page_size * page_members_per_run - 1),
                "members_per_run": page_members_per_run,
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'campaign-plan',
                        'history-work-' || member_index::text,
                        'history-' || lpad(
                            (member_index / :members_per_run)::text,
                            4,
                            '0'
                        ),
                        'input:history-' || member_index::text,
                        '{}'::jsonb,
                        member_index + :first_rank
                    FROM generate_series(0, :last_member_index)
                        AS member_index
                    RETURNING work_item_id, origin_run_key, rank
                )
                INSERT INTO platform_run_memberships (
                    run_key, member_ordinal, work_item_id
                )
                SELECT
                    origin_run_key,
                    (rank - :first_rank) % :members_per_run,
                    work_item_id
                FROM inserted_work
                """
            ),
            {
                "first_rank": page_size * page_members_per_run + 1,
                "last_member_index": (
                    history_run_count * history_members_per_run - 1
                ),
                "members_per_run": history_members_per_run,
            },
        )

    page = list_runs("campaign-plan", engine=pg_engine, limit=page_size)
    assert tuple(str(run.run_key) for run in page) == tuple(
        f"page-{index:04d}" for index in range(page_size)
    )
    assert all(
        run.registered_member_count == page_members_per_run for run in page
    )

    statement = _run_summary_statement(
        schema,
        campaign_key="campaign-plan",
        limit=page_size,
        after=None,
    )
    sql = str(
        statement.compile(
            dialect=pg_engine.dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    with pg_engine.begin() as connection:
        connection.execute(
            text("ANALYZE platform_pipeline_runs, platform_run_memberships")
        )
        plan = connection.execute(
            text(
                "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, "
                f"TIMING OFF, SUMMARY OFF) {sql}"
            )
        ).scalar_one()

    visited_rows: dict[str, int] = {
        "platform_pipeline_runs": 0,
        "platform_run_memberships": 0,
    }

    def collect(value: object) -> None:
        if isinstance(value, dict):
            relation_name = value.get("Relation Name")
            if (
                isinstance(relation_name, str)
                and relation_name in visited_rows
            ):
                actual_rows = value.get("Actual Rows")
                actual_loops = value.get("Actual Loops")
                assert isinstance(actual_rows, int)
                assert isinstance(actual_loops, int)
                visited_rows[relation_name] += actual_rows * actual_loops
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(plan)
    assert visited_rows["platform_pipeline_runs"] <= page_size
    assert visited_rows["platform_run_memberships"] <= (
        page_size * page_members_per_run
    )
