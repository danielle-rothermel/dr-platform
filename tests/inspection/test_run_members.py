from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import Engine, event, select, text

from dr_platform._core.identities import PipelineKey, StageKey, WorkKey
from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryProducer,
    build_terminal_summary,
)
from dr_platform.inspection.run_members import list_run_members
from dr_platform.inspection.statuses import bulk_work_terminal_statuses
from dr_platform.inspection.terminal_filters import TerminalSummaryFilter
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


def _fail_member(
    engine: Engine,
    *,
    work_key: str,
    producer: TerminalSummaryProducer,
    at,
) -> None:
    from dr_platform._core.ledger.schema import StagingSchema

    schema = StagingSchema()
    with engine.connect() as connection:
        row = connection.execute(
            select(
                schema.stage_executions.c.stage_execution_id,
            )
            .select_from(
                schema.work_items.join(
                    schema.stage_executions,
                    schema.work_items.c.work_item_id
                    == schema.stage_executions.c.work_item_id,
                )
            )
            .where(schema.work_items.c.work_key == work_key)
        ).one()
    with engine.begin() as connection:
        attempt = append_stage_attempt(
            connection,
            stage_execution_id=row.stage_execution_id,
            created_at=at,
            admitted_at=at,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=row.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=at,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=row.stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=at + timedelta(seconds=1),
        )
        record_stage_attempt_terminal(
            connection,
            stage_execution_id=row.stage_execution_id,
            attempt_number=attempt.attempt_number,
            terminal_at=at + timedelta(seconds=1),
            terminal_summary=build_terminal_summary(
                outcome=StageExecutionState.FAILED.value,
                producer=producer,
                error_type="builtins.RuntimeError",
            ),
        )


async def _workflow(value: str) -> str:
    return f"output:{value}"


def _args_for(payload: object) -> tuple[object, ...]:
    return (payload,)


def _registry() -> tuple[PipelineRegistry, PipelineDefinition]:
    pipeline = PipelineDefinition(
        key=PipelineKey("run-members"),
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


def test_list_run_members_returns_ordered_membership_scope(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-a",
        work_indexes=(1, 2),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-b",
        work_indexes=(2, 3),
    )

    members = list_run_members("run-a", engine=pg_engine, limit=10)
    assert [member.work_key.value for member in members] == [
        "work-1",
        "work-2",
    ]
    assert members[0].member_ordinal == 0
    assert members[0].input_reference == "input:1"
    assert members[0].state is StageExecutionState.READY


def test_list_run_members_paginates_by_member_ordinal(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-page",
        work_indexes=(1, 2, 3),
    )

    first_page = list_run_members("run-page", engine=pg_engine, limit=2)
    second_page = list_run_members(
        "run-page",
        engine=pg_engine,
        cursor=first_page[-1].member_ordinal,
        limit=2,
    )
    assert [member.member_ordinal for member in first_page] == [0, 1]
    assert [member.member_ordinal for member in second_page] == [2]


def test_list_run_members_rejects_unknown_run(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    with pytest.raises(LookupError, match="run is unknown"):
        list_run_members("missing-run", engine=pg_engine)


def test_list_run_members_rejects_unknown_cursor(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-cursor",
        work_indexes=(1,),
    )
    with pytest.raises(ValueError, match="run member cursor is unknown"):
        list_run_members(
            "run-cursor",
            engine=pg_engine,
            cursor=99,
        )


def test_list_run_members_uses_one_query_per_page(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-planner",
        work_indexes=tuple(range(20)),
    )

    list_queries = 0

    def before_cursor_execute(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal list_queries
        normalized = " ".join(str(statement).split())
        if (
            "platform_run_memberships" in normalized
            and "member_ordinal" in normalized
        ):
            list_queries += 1

    event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
    try:
        with pg_engine.connect() as connection:
            connection.execute(text("ANALYZE platform_run_memberships"))
            connection.execute(text("ANALYZE platform_work_items"))
            connection.execute(text("ANALYZE platform_stage_executions"))
            connection.commit()
        page = list_run_members("run-planner", engine=pg_engine, limit=5)
    finally:
        event.remove(pg_engine, "before_cursor_execute", before_cursor_execute)

    assert len(page) == 5
    assert list_queries == 1


def test_filtered_run_members_and_bulk_terminal_agree_on_ready_state(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-ready-filter",
        work_indexes=(1, 2),
    )
    ready_filter = TerminalSummaryFilter(state=StageExecutionState.READY)

    members = list_run_members(
        "run-ready-filter",
        engine=pg_engine,
        terminal_filter=ready_filter,
        limit=10,
    )
    bulk = bulk_work_terminal_statuses(
        "campaign-1",
        ("work-1", "work-2"),
        engine=pg_engine,
        terminal_filter=ready_filter,
    ).statuses

    assert {member.work_key.value for member in members} == {
        "work-1",
        "work-2",
    }
    assert all(member.state is StageExecutionState.READY for member in members)
    assert bulk[WorkKey("work-1")].present
    assert bulk[WorkKey("work-2")].present
    assert bulk[WorkKey("work-1")].state is StageExecutionState.READY
    assert bulk[WorkKey("work-2")].state is StageExecutionState.READY


def test_list_run_members_filters_by_terminal_producer(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-filter",
        work_indexes=(1, 2, 3),
    )
    _fail_member(
        pg_engine,
        work_key="work-1",
        producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        at=NOW,
    )
    _fail_member(
        pg_engine,
        work_key="work-2",
        producer=TerminalSummaryProducer.ABANDONMENT,
        at=NOW + timedelta(seconds=1),
    )

    members = list_run_members(
        "run-filter",
        engine=pg_engine,
        terminal_filter=TerminalSummaryFilter(
            producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        ),
        limit=10,
    )

    assert [member.work_key.value for member in members] == ["work-1"]
    assert members[0].member_ordinal == 0
    assert members[0].stage_execution_id is not None
    assert members[0].terminal_summary is not None
    assert (
        members[0].terminal_summary["producer"]
        == TerminalSummaryProducer.APPLICATION_FAILURE.value
    )


def test_list_run_members_filter_respects_membership_scope(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-scope-a",
        work_indexes=(1, 2),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-scope-b",
        work_indexes=(2, 3),
    )
    _fail_member(
        pg_engine,
        work_key="work-1",
        producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        at=NOW,
    )
    _fail_member(
        pg_engine,
        work_key="work-3",
        producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        at=NOW + timedelta(seconds=1),
    )

    members = list_run_members(
        "run-scope-a",
        engine=pg_engine,
        terminal_filter=TerminalSummaryFilter(
            producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        ),
        limit=10,
    )

    assert [member.work_key.value for member in members] == ["work-1"]


def test_list_run_members_uses_one_query_per_filtered_page(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-filter-planner",
        work_indexes=tuple(range(20)),
    )
    for index in range(10):
        _fail_member(
            pg_engine,
            work_key=f"work-{index}",
            producer=TerminalSummaryProducer.APPLICATION_FAILURE,
            at=NOW + timedelta(seconds=index),
        )

    list_queries = 0

    def before_cursor_execute(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal list_queries
        normalized = " ".join(str(statement).split())
        if (
            "platform_run_memberships" in normalized
            and "member_ordinal" in normalized
            and "terminal_summary" in normalized
        ):
            list_queries += 1

    event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
    try:
        with pg_engine.connect() as connection:
            connection.execute(text("ANALYZE platform_run_memberships"))
            connection.execute(text("ANALYZE platform_work_items"))
            connection.execute(text("ANALYZE platform_stage_executions"))
            connection.execute(text("ANALYZE platform_stage_attempts"))
            connection.commit()
        page = list_run_members(
            "run-filter-planner",
            engine=pg_engine,
            terminal_filter=TerminalSummaryFilter(
                producer=TerminalSummaryProducer.APPLICATION_FAILURE,
            ),
            limit=5,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", before_cursor_execute)

    assert len(page) == 5
    assert list_queries == 1
