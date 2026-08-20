from __future__ import annotations

import pytest
from sqlalchemy import Engine

from dr_platform._core.identities import StageKey
from dr_platform.execution.stage_completion import StageSuccessor
from dr_platform.inspection.barrier_join import resolve_barrier_join_cluster
from dr_platform.testing import (
    seed_deferral_fanout,
    validate_deferral_fanout,
)
from tests.conftest import _migrate


def _eval_row(index: int) -> StageSuccessor:
    return StageSuccessor(
        stage_key=StageKey("eval_row"),
        stage_index=index,
        input_reference=f"row:in:{index}",
    )


def _fanin(index: int) -> StageSuccessor:
    return StageSuccessor(
        stage_key=StageKey("eval_fanin"),
        stage_index=index,
        input_reference="fanin:in",
        barrier=True,
    )


def test_validate_deferral_fanout_accepts_single_row() -> None:
    validate_deferral_fanout(
        (_eval_row(1), _fanin(2)),
        origin_stage_index=0,
        eval_row_stage_key="eval_row",
        fanin_stage_key="eval_fanin",
    )


def test_validate_deferral_fanout_accepts_multi_row() -> None:
    validate_deferral_fanout(
        (_eval_row(1), _eval_row(2), _eval_row(3), _fanin(4)),
        origin_stage_index=0,
        eval_row_stage_key=StageKey("eval_row"),
        fanin_stage_key=StageKey("eval_fanin"),
    )


def test_validate_deferral_fanout_rejects_contiguity_gap() -> None:
    with pytest.raises(ValueError, match="contiguous"):
        validate_deferral_fanout(
            (_eval_row(1), _eval_row(3), _fanin(4)),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_duplicate_row_index() -> None:
    with pytest.raises(ValueError, match="contiguous"):
        validate_deferral_fanout(
            (_eval_row(1), _eval_row(1), _fanin(3)),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_missing_barrier() -> None:
    with pytest.raises(ValueError, match="barrier=True"):
        validate_deferral_fanout(
            (
                _eval_row(1),
                StageSuccessor(
                    stage_key=StageKey("eval_fanin"),
                    stage_index=2,
                    input_reference="fanin:in",
                    barrier=False,
                ),
            ),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_barrier_on_row() -> None:
    with pytest.raises(ValueError, match="must not set barrier"):
        validate_deferral_fanout(
            (
                StageSuccessor(
                    stage_key=StageKey("eval_row"),
                    stage_index=1,
                    input_reference="row:in:1",
                    barrier=True,
                ),
                _fanin(2),
            ),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_two_barriers() -> None:
    with pytest.raises(ValueError, match="exactly one fan-in"):
        validate_deferral_fanout(
            (_eval_row(1), _fanin(2), _fanin(3)),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_wrong_fanin_placement() -> None:
    with pytest.raises(ValueError, match="must be at index 3"):
        validate_deferral_fanout(
            (_eval_row(1), _eval_row(2), _fanin(4)),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def test_validate_deferral_fanout_rejects_foreign_stage_key() -> None:
    with pytest.raises(ValueError, match="unexpected successor"):
        validate_deferral_fanout(
            (
                _eval_row(1),
                StageSuccessor(
                    stage_key=StageKey("foreign"),
                    stage_index=2,
                    input_reference="foreign:in",
                ),
                _fanin(3),
            ),
            origin_stage_index=0,
            eval_row_stage_key="eval_row",
            fanin_stage_key="eval_fanin",
        )


def _successors_for_row_count(
    origin: int,
    row_count: int,
) -> tuple[StageSuccessor, ...]:
    eval_rows = tuple(
        _eval_row(origin + offset) for offset in range(1, row_count + 1)
    )
    return (*eval_rows, _fanin(origin + row_count + 1))


@pytest.mark.parametrize("row_count", [1, 2, 3])
def test_validate_deferral_fanout_round_trips_to_cluster_resolution(
    pg_engine: Engine,
    row_count: int,
) -> None:
    schema = _migrate(pg_engine)
    origin = 0
    successors = _successors_for_row_count(origin, row_count)
    validate_deferral_fanout(
        successors,
        origin_stage_index=origin,
        eval_row_stage_key="eval_row",
        fanin_stage_key="eval_fanin",
    )
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = seed_deferral_fanout(
            connection,
            origin_stage_index=origin,
            successors=successors,
            schema=schema,
            campaign_key=f"campaign-fanout-{row_count}",
            work_key=f"work-fanout-{row_count}",
            run_key=f"run-fanout-{row_count}",
        )

    cluster = resolve_barrier_join_cluster(
        work_item_id,
        fanin_index,
        optim_step_stage_key=StageKey("optim_step"),
        eval_row_stage_key=StageKey("eval_row"),
        engine=pg_engine,
        schema=schema,
    )
    eval_successors = tuple(
        successor for successor in successors if not successor.barrier
    )
    fanin_successor = next(
        successor for successor in successors if successor.barrier
    )
    assert optim_index == origin
    assert fanin_index == fanin_successor.stage_index
    assert [row.stage_index for row in cluster.eval_rows] == [
        successor.stage_index for successor in eval_successors
    ]
    assert [row.input_reference for row in cluster.eval_rows] == [
        successor.input_reference for successor in eval_successors
    ]
    assert cluster.fanin.stage_index == fanin_successor.stage_index
    assert cluster.fanin.input_reference == fanin_successor.input_reference
    assert cluster.fanin.barrier is True
