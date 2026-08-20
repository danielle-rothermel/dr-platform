"""Barrier fan-in cluster discovery for deferral episodes.

Applications emit a fan-out of branch stages plus one ``barrier=True`` join
successor. Indices are application-chosen and sparse; within one episode the
open interval ``(O, F)`` between the deferring step at ``O`` and the barrier
join at ``F`` is expected to contain only eval-row stages at contiguous
indices ``O+1 .. F-1``. The platform validates topology and references only;
payload meaning stays in the application layer.

``resolve_barrier_join_cluster`` requires distinct ``optim_step_stage_key`` and
``eval_row_stage_key`` values. The platform allows reusing a ``stage_key`` at
distinct indices elsewhere, but this helper cannot infer which same-key row is
the deferring step versus an eval row. ``BarrierJoinCluster.optim_step`` is the
canonical deferring-step record when ``origin_stage_index`` is not already
carried in the join payload; carrying that index in the payload remains the
preferred pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dr_platform._core.identities import StageKey, normalize_key
from dr_platform._core.validation import validate_nonnegative_integer
from dr_platform.inspection._validation import validate_work_item_id
from dr_platform.inspection.work_items import list_stage_executions

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform._core.ledger.executions import StageExecutionRecord
    from dr_platform._core.ledger.schema import LedgerSchema


@dataclass(frozen=True, slots=True)
class BarrierJoinCluster:
    optim_step: StageExecutionRecord
    eval_rows: tuple[StageExecutionRecord, ...]
    fanin: StageExecutionRecord


def resolve_barrier_join_cluster(  # noqa: PLR0913 -- explicit reader inputs
    work_item_id: int,
    fanin_stage_index: int,
    *,
    optim_step_stage_key: StageKey | str,
    eval_row_stage_key: StageKey | str,
    engine: Engine,
    schema: LedgerSchema | None = None,
) -> BarrierJoinCluster:
    """Resolve the deferral episode bounded by one barrier fan-in stage.

    ``cluster.optim_step`` is the canonical deferring-step record for
    discovery or verification when ``origin_stage_index`` is not payload-
    carried.
    """
    validate_work_item_id(work_item_id)
    validate_nonnegative_integer(fanin_stage_index, label="fanin stage index")
    normalized_optim_key = normalize_key(optim_step_stage_key, StageKey)
    normalized_eval_key = normalize_key(eval_row_stage_key, StageKey)
    if normalized_optim_key == normalized_eval_key:
        raise ValueError(
            "optim_step_stage_key and eval_row_stage_key must differ for "
            "barrier cluster resolution"
        )

    fanin_rows = list_stage_executions(
        work_item_id,
        engine=engine,
        schema=schema,
        min_stage_index=fanin_stage_index - 1,
        max_stage_index=fanin_stage_index + 1,
    )
    fanin_matches = [
        row for row in fanin_rows if row.stage_index == fanin_stage_index
    ]
    if not fanin_matches:
        raise LookupError(
            f"barrier fan-in stage does not exist at index {fanin_stage_index}"
        )
    fanin = fanin_matches[0]
    if not fanin.barrier:
        raise ValueError(
            f"stage at index {fanin_stage_index} is not a barrier join stage"
        )

    optim_candidates = list_stage_executions(
        work_item_id,
        engine=engine,
        schema=schema,
        stage_key=normalized_optim_key,
        max_stage_index=fanin_stage_index,
    )
    if not optim_candidates:
        raise LookupError(
            "no deferring optim step exists below the barrier fan-in stage"
        )
    optim_step = max(optim_candidates, key=lambda row: row.stage_index)
    origin_index = optim_step.stage_index

    eval_rows = list_stage_executions(
        work_item_id,
        engine=engine,
        schema=schema,
        stage_key=normalized_eval_key,
        min_stage_index=origin_index,
        max_stage_index=fanin_stage_index,
    )

    interval_rows = list_stage_executions(
        work_item_id,
        engine=engine,
        schema=schema,
        min_stage_index=origin_index,
        max_stage_index=fanin_stage_index,
    )
    foreign_rows = [
        row for row in interval_rows if row.stage_key != normalized_eval_key
    ]
    if foreign_rows:
        foreign = foreign_rows[0]
        raise ValueError(
            "unexpected stage key "
            f"{foreign.stage_key.value!r} at index {foreign.stage_index} "
            f"in deferral interval ({origin_index}, {fanin_stage_index})"
        )

    expected_indices = range(origin_index + 1, fanin_stage_index)
    actual_indices = {row.stage_index for row in eval_rows}
    if actual_indices != set(expected_indices):
        raise ValueError(
            "eval rows do not fill the contiguous deferral interval "
            f"({origin_index}, {fanin_stage_index})"
        )

    ordered_eval_rows = tuple(
        sorted(eval_rows, key=lambda row: row.stage_index)
    )
    return BarrierJoinCluster(
        optim_step=optim_step,
        eval_rows=ordered_eval_rows,
        fanin=fanin,
    )
