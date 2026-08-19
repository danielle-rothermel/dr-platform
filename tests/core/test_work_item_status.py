from __future__ import annotations

from sqlalchemy import Connection, Engine, func, select

from dr_platform._core.ledger.executions import (
    StageExecutionRecord,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.work_item_status import work_item_status_rows
from dr_platform.submission.runs import insert_pipeline_run
from tests.conftest import NOW, _migrate


def _create_work_item(connection: Connection) -> int:
    insert_pipeline_run(
        connection,
        run_key="run-status",
        campaign_key="campaign-status",
        pipeline_key="pipeline",
        pipeline_version=1,
        execution_config_reference="config:1",
        expected_member_count=0,
        created_at=NOW,
    )
    work_items = LedgerSchema().work_items
    return connection.execute(
        work_items.insert()
        .values(
            campaign_key="campaign-status",
            work_key="work-status",
            origin_run_key="run-status",
            input_reference="input:1",
            labels={},
            rank=1,
        )
        .returning(work_items.c.work_item_id)
    ).scalar_one()


def _insert_execution(
    connection: Connection,
    *,
    work_item_id: int,
    stage_key: str,
    stage_index: int,
    state: StageExecutionState,
) -> StageExecutionRecord:
    execution = insert_stage_execution(
        connection,
        work_item_id=work_item_id,
        stage_key=stage_key,
        stage_index=stage_index,
        input_reference=f"input:{stage_index}",
        created_at=NOW,
    )
    if state is StageExecutionState.READY:
        return execution
    if state is StageExecutionState.ADMITTED:
        return transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
    execution = transition_stage_execution(
        connection,
        stage_execution_id=execution.stage_execution_id,
        new_state=StageExecutionState.ADMITTED,
        updated_at=NOW,
    )
    if state is StageExecutionState.SUCCEEDED:
        return transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            updated_at=NOW,
            output_reference=f"output:{stage_index}",
        )
    if state is StageExecutionState.FAILED:
        return transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=NOW,
        )
    if state is StageExecutionState.CANCELLED:
        return transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.CANCELLED,
            updated_at=NOW,
        )
    raise ValueError(f"unsupported test state: {state}")


def _derived_status(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> tuple[StageExecutionState, int, int]:
    status = work_item_status_rows(
        schema,
        select(schema.work_items.c.work_item_id).where(
            schema.work_items.c.work_item_id == work_item_id
        ),
    )
    row = connection.execute(
        select(
            status.c.state,
            status.c.stage_index,
            status.c.stage_execution_id,
        )
    ).one()
    return (
        StageExecutionState(row.state),
        row.stage_index,
        row.stage_execution_id,
    )


def _max_index_status(
    connection: Connection,
    *,
    work_item_id: int,
    schema: LedgerSchema,
) -> tuple[StageExecutionState, int, int]:
    executions = schema.stage_executions
    max_index = (
        select(
            executions.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .where(executions.c.work_item_id == work_item_id)
        .group_by(executions.c.work_item_id)
        .subquery()
    )
    row = connection.execute(
        select(
            executions.c.state,
            executions.c.stage_index,
            executions.c.stage_execution_id,
        ).select_from(
            executions.join(
                max_index,
                (executions.c.work_item_id == max_index.c.work_item_id)
                & (executions.c.stage_index == max_index.c.stage_index),
            )
        )
    ).one()
    return (
        StageExecutionState(row.state),
        row.stage_index,
        row.stage_execution_id,
    )


def test_failed_and_ready_selects_failed_at_lowest_index(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _create_work_item(connection)
        failed = _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="branch_a",
            stage_index=1,
            state=StageExecutionState.FAILED,
        )
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="join",
            stage_index=3,
            state=StageExecutionState.READY,
        )
        state, stage_index, stage_execution_id = _derived_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
    assert state is StageExecutionState.FAILED
    assert stage_index == 1
    assert stage_execution_id == failed.stage_execution_id


def test_all_succeeded_selects_highest_index(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _create_work_item(connection)
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="stage_a",
            stage_index=0,
            state=StageExecutionState.SUCCEEDED,
        )
        highest = _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="stage_b",
            stage_index=2,
            state=StageExecutionState.SUCCEEDED,
        )
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="stage_c",
            stage_index=1,
            state=StageExecutionState.SUCCEEDED,
        )
        state, stage_index, stage_execution_id = _derived_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
    assert state is StageExecutionState.SUCCEEDED
    assert stage_index == 2
    assert stage_execution_id == highest.stage_execution_id


def test_admitted_and_ready_selects_admitted(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _create_work_item(connection)
        admitted = _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="branch",
            stage_index=1,
            state=StageExecutionState.ADMITTED,
        )
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="join",
            stage_index=2,
            state=StageExecutionState.READY,
        )
        state, stage_index, stage_execution_id = _derived_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
    assert state is StageExecutionState.ADMITTED
    assert stage_index == 1
    assert stage_execution_id == admitted.stage_execution_id


def test_cancelled_and_failed_selects_failed(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _create_work_item(connection)
        failed = _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="branch",
            stage_index=1,
            state=StageExecutionState.FAILED,
        )
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="other",
            stage_index=2,
            state=StageExecutionState.CANCELLED,
        )
        state, stage_index, stage_execution_id = _derived_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
    assert state is StageExecutionState.FAILED
    assert stage_index == 1
    assert stage_execution_id == failed.stage_execution_id


def test_linear_pipeline_matches_max_index_query(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _create_work_item(connection)
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="stage_a",
            stage_index=0,
            state=StageExecutionState.SUCCEEDED,
        )
        _insert_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="stage_b",
            stage_index=1,
            state=StageExecutionState.ADMITTED,
        )
        derived = _derived_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
        legacy = _max_index_status(
            connection,
            work_item_id=work_item_id,
            schema=schema,
        )
    assert derived == legacy
