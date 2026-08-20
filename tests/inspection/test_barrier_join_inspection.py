from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import Engine

from dr_platform._core.identities import StageKey
from dr_platform._core.ledger.executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.inspection.barrier_join import resolve_barrier_join_cluster
from dr_platform.inspection.work_items import (
    PredecessorStageOutput,
    list_episode_predecessor_outputs,
    list_predecessor_stage_outputs,
    list_stage_executions,
)
from tests.conftest import NOW, _migrate

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import LedgerSchema


def _seed_work_item(connection: Connection, schema: LedgerSchema) -> int:
    run_key = "run-barrier-join-inspection"
    connection.execute(
        schema.pipeline_runs.insert().values(
            run_key=run_key,
            campaign_key="campaign-barrier-join-inspection",
            pipeline_key="evaluation",
            pipeline_version=1,
            execution_config_reference="config:1",
            expected_member_count=1,
            created_at=NOW,
        )
    )
    return connection.execute(
        schema.work_items.insert()
        .values(
            campaign_key="campaign-barrier-join-inspection",
            work_key="work-barrier-join-inspection",
            origin_run_key=run_key,
            input_reference="seed",
            labels={},
            rank=1,
        )
        .returning(schema.work_items.c.work_item_id)
    ).scalar_one()


def _succeed_stage(  # noqa: PLR0913 -- explicit ledger seed facts
    connection: Connection,
    *,
    work_item_id: int,
    stage_key: str,
    stage_index: int,
    input_reference: str,
    output_reference: str,
    barrier: bool = False,
) -> None:
    execution = insert_stage_execution(
        connection,
        work_item_id=work_item_id,
        stage_key=stage_key,
        stage_index=stage_index,
        input_reference=input_reference,
        barrier=barrier,
        created_at=NOW,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=execution.stage_execution_id,
        new_state=StageExecutionState.ADMITTED,
        updated_at=NOW,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=execution.stage_execution_id,
        new_state=StageExecutionState.SUCCEEDED,
        output_reference=output_reference,
        updated_at=NOW,
    )


def _seed_single_deferral_episode(
    connection: Connection,
    schema: LedgerSchema,
    *,
    row_count: int = 2,
) -> int:
    work_item_id = _seed_work_item(connection, schema)
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="optim_step",
        stage_index=0,
        input_reference="optim:in:0",
        output_reference="optim:out:0",
    )
    for offset in range(1, row_count + 1):
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="eval_row",
            stage_index=offset,
            input_reference=f"row:in:{offset}",
            output_reference=f"row:out:{offset}",
        )
    fanin_index = row_count + 1
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_fanin",
        stage_index=fanin_index,
        input_reference="fanin:in:1",
        output_reference="fanin:out:1",
        barrier=True,
    )
    return work_item_id


def _seed_double_deferral_episode(
    connection: Connection,
    schema: LedgerSchema,
) -> tuple[int, int, int]:
    work_item_id = _seed_work_item(connection, schema)

    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="optim_step",
        stage_index=0,
        input_reference="optim:in:0",
        output_reference="optim:out:0",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_row",
        stage_index=1,
        input_reference="row:in:1",
        output_reference="row:out:1",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_row",
        stage_index=2,
        input_reference="row:in:2",
        output_reference="row:out:2",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_fanin",
        stage_index=3,
        input_reference="fanin:in:1",
        output_reference="fanin:out:1",
        barrier=True,
    )

    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="optim_step",
        stage_index=4,
        input_reference="optim:in:4",
        output_reference="optim:out:4",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_row",
        stage_index=5,
        input_reference="row:in:5",
        output_reference="row:out:5",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_row",
        stage_index=6,
        input_reference="row:in:6",
        output_reference="row:out:6",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="eval_fanin",
        stage_index=7,
        input_reference="fanin:in:2",
        output_reference="fanin:out:2",
        barrier=True,
    )
    return work_item_id, 4, 7


def test_filtered_predecessors_return_single_episode_rows(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    outputs = list_predecessor_stage_outputs(
        work_item_id,
        3,
        stage_key=StageKey("eval_row"),
        min_stage_index=0,
        engine=pg_engine,
    )

    assert outputs == (
        PredecessorStageOutput(
            stage_index=1,
            stage_key=StageKey("eval_row"),
            input_reference="row:in:1",
            output_reference="row:out:1",
        ),
        PredecessorStageOutput(
            stage_index=2,
            stage_key=StageKey("eval_row"),
            input_reference="row:in:2",
            output_reference="row:out:2",
        ),
    )


def test_filtered_predecessors_exclude_other_episode_rows(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = _seed_double_deferral_episode(
            connection,
            schema,
        )

    outputs = list_predecessor_stage_outputs(
        work_item_id,
        fanin_index,
        stage_key=StageKey("eval_row"),
        min_stage_index=optim_index,
        engine=pg_engine,
    )

    assert [item.stage_index for item in outputs] == [5, 6]
    assert all(item.stage_key == StageKey("eval_row") for item in outputs)


def test_filtered_predecessors_exclude_stale_rows_outside_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = _seed_double_deferral_episode(
            connection,
            schema,
        )

    unfiltered = list_predecessor_stage_outputs(
        work_item_id,
        fanin_index,
        stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )
    filtered = list_predecessor_stage_outputs(
        work_item_id,
        fanin_index,
        stage_key=StageKey("eval_row"),
        min_stage_index=optim_index,
        engine=pg_engine,
    )

    assert [item.stage_index for item in unfiltered] == [1, 2, 5, 6]
    assert [item.stage_index for item in filtered] == [5, 6]


def test_filtered_predecessors_return_empty_for_empty_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    assert (
        list_predecessor_stage_outputs(
            work_item_id,
            1,
            stage_key=StageKey("eval_row"),
            min_stage_index=0,
            engine=pg_engine,
        )
        == ()
    )


def test_filtered_predecessors_reject_invalid_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    with pytest.raises(ValueError, match="exclusive range"):
        list_predecessor_stage_outputs(
            work_item_id,
            3,
            min_stage_index=2,
            max_stage_index=2,
            engine=pg_engine,
        )


def test_episode_predecessors_return_single_episode_rows(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    outputs = list_episode_predecessor_outputs(
        work_item_id,
        3,
        origin_stage_index=0,
        stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )

    assert outputs == (
        PredecessorStageOutput(
            stage_index=1,
            stage_key=StageKey("eval_row"),
            input_reference="row:in:1",
            output_reference="row:out:1",
        ),
        PredecessorStageOutput(
            stage_index=2,
            stage_key=StageKey("eval_row"),
            input_reference="row:in:2",
            output_reference="row:out:2",
        ),
    )


def test_episode_predecessors_exclude_other_episode_rows(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = _seed_double_deferral_episode(
            connection,
            schema,
        )

    outputs = list_episode_predecessor_outputs(
        work_item_id,
        fanin_index,
        origin_stage_index=optim_index,
        stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )

    assert [item.stage_index for item in outputs] == [5, 6]
    assert all(item.stage_key == StageKey("eval_row") for item in outputs)


def test_episode_predecessors_return_empty_for_empty_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    assert (
        list_episode_predecessor_outputs(
            work_item_id,
            1,
            origin_stage_index=0,
            stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )
        == ()
    )


def test_episode_predecessors_reject_invalid_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    with pytest.raises(ValueError, match="exclusive range"):
        list_episode_predecessor_outputs(
            work_item_id,
            3,
            origin_stage_index=3,
            stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )


def test_episode_predecessors_order_by_stage_index(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(
            connection, schema, row_count=3
        )

    outputs = list_episode_predecessor_outputs(
        work_item_id,
        4,
        origin_stage_index=0,
        stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )

    assert [item.stage_index for item in outputs] == [1, 2, 3]
    assert all(item.output_reference for item in outputs)


def test_unfiltered_predecessors_remain_backward_compatible(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_double_deferral_episode(connection, schema)[0]

    outputs = list_predecessor_stage_outputs(
        work_item_id,
        8,
        engine=pg_engine,
    )

    assert [(item.stage_index, item.stage_key.value) for item in outputs] == [
        (0, "optim_step"),
        (1, "eval_row"),
        (2, "eval_row"),
        (3, "eval_fanin"),
        (4, "optim_step"),
        (5, "eval_row"),
        (6, "eval_row"),
        (7, "eval_fanin"),
    ]


def test_list_stage_executions_filters_by_key_state_and_range(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = _seed_double_deferral_episode(
            connection,
            schema,
        )

    rows = list_stage_executions(
        work_item_id,
        stage_key=StageKey("eval_row"),
        min_stage_index=optim_index,
        max_stage_index=fanin_index,
        state=StageExecutionState.SUCCEEDED,
        engine=pg_engine,
    )

    assert [row.stage_index for row in rows] == [5, 6]
    assert rows[0].input_reference == "row:in:5"


def test_list_stage_executions_filters_min_only(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, _fanin_index = (
            _seed_double_deferral_episode(
                connection,
                schema,
            )
        )

    rows = list_stage_executions(
        work_item_id,
        min_stage_index=optim_index,
        engine=pg_engine,
    )

    assert [row.stage_index for row in rows] == [5, 6, 7]


def test_list_stage_executions_filters_max_only(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, _optim_index, fanin_index = (
            _seed_double_deferral_episode(
                connection,
                schema,
            )
        )

    rows = list_stage_executions(
        work_item_id,
        max_stage_index=fanin_index,
        engine=pg_engine,
    )

    assert [row.stage_index for row in rows] == [0, 1, 2, 3, 4, 5, 6]


def test_resolve_barrier_join_cluster_rejects_equal_stage_keys(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_single_deferral_episode(connection, schema)

    with pytest.raises(ValueError, match="must differ"):
        resolve_barrier_join_cluster(
            work_item_id,
            3,
            optim_step_stage_key=StageKey("eval_row"),
            eval_row_stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )


def _seed_same_key_fan_out_episode(
    connection: Connection,
    schema: LedgerSchema,
) -> int:
    work_item_id = _seed_work_item(connection, schema)
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="split",
        stage_index=0,
        input_reference="split:in",
        output_reference="split:out",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="branch",
        stage_index=1,
        input_reference="row:1",
        output_reference="row:out:1",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="branch",
        stage_index=2,
        input_reference="row:2",
        output_reference="row:out:2",
    )
    _succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key="join",
        stage_index=3,
        input_reference="join:in",
        output_reference="join:out",
        barrier=True,
    )
    return work_item_id


def test_resolve_barrier_join_cluster_rejects_same_key_fan_out_topology(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_same_key_fan_out_episode(connection, schema)

    with pytest.raises(ValueError, match="must differ"):
        resolve_barrier_join_cluster(
            work_item_id,
            3,
            optim_step_stage_key=StageKey("branch"),
            eval_row_stage_key=StageKey("branch"),
            engine=pg_engine,
        )


def test_resolve_barrier_join_cluster_returns_episode(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = _seed_double_deferral_episode(
            connection,
            schema,
        )

    cluster = resolve_barrier_join_cluster(
        work_item_id,
        fanin_index,
        optim_step_stage_key=StageKey("optim_step"),
        eval_row_stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )

    assert cluster.optim_step.stage_index == optim_index
    assert [row.stage_index for row in cluster.eval_rows] == [5, 6]
    assert cluster.fanin.stage_index == fanin_index
    assert cluster.fanin.barrier is True


def test_resolve_barrier_join_cluster_rejects_missing_optim_step(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_work_item(connection, schema)
        insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="eval_fanin",
            stage_index=1,
            input_reference="fanin:in",
            barrier=True,
            created_at=NOW,
        )

    with pytest.raises(LookupError, match="no deferring optim step"):
        resolve_barrier_join_cluster(
            work_item_id,
            1,
            optim_step_stage_key=StageKey("optim_step"),
            eval_row_stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )


def test_resolve_barrier_join_cluster_rejects_non_barrier_fanin(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_work_item(connection, schema)
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="optim_step",
            stage_index=0,
            input_reference="optim:in",
            output_reference="optim:out",
        )
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="eval_fanin",
            stage_index=1,
            input_reference="fanin:in",
            output_reference="fanin:out",
            barrier=False,
        )

    with pytest.raises(ValueError, match="not a barrier join stage"):
        resolve_barrier_join_cluster(
            work_item_id,
            1,
            optim_step_stage_key=StageKey("optim_step"),
            eval_row_stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )


def test_resolve_barrier_join_cluster_rejects_gap_in_interval(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = _seed_work_item(connection, schema)
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="optim_step",
            stage_index=0,
            input_reference="optim:in",
            output_reference="optim:out",
        )
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="eval_row",
            stage_index=1,
            input_reference="row:in:1",
            output_reference="row:out:1",
        )
        _succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="eval_fanin",
            stage_index=3,
            input_reference="fanin:in",
            output_reference="fanin:out",
            barrier=True,
        )

    with pytest.raises(ValueError, match="contiguous deferral interval"):
        resolve_barrier_join_cluster(
            work_item_id,
            3,
            optim_step_stage_key=StageKey("optim_step"),
            eval_row_stage_key=StageKey("eval_row"),
            engine=pg_engine,
        )
