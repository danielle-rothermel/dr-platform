from __future__ import annotations

from dr_platform._core.identities import StageKey, normalize_key
from dr_platform._core.validation import validate_nonnegative_integer
from dr_platform.execution.stage_completion import (
    StageSuccessor,
)


def validate_deferral_fanout(  # noqa: PLR0912 -- explicit shape checks
    successors: tuple[StageSuccessor, ...],
    *,
    origin_stage_index: int,
    eval_row_stage_key: StageKey | str,
    fanin_stage_key: StageKey | str,
) -> None:
    """Raise ``ValueError`` unless successors form one deferral fan-out.

    Non-empty ``input_reference`` values are enforced by ``StageSuccessor``
    construction; this validator checks topology, keys, barrier placement, and
    index contiguity only.
    """
    validate_nonnegative_integer(
        origin_stage_index,
        label="origin stage index",
    )
    normalized_eval_key = normalize_key(eval_row_stage_key, StageKey)
    normalized_fanin_key = normalize_key(fanin_stage_key, StageKey)
    if normalized_eval_key == normalized_fanin_key:
        raise ValueError(
            "eval_row_stage_key and fanin_stage_key must differ for "
            "deferral fan-out validation"
        )

    eval_rows: list[StageSuccessor] = []
    fanin_rows: list[StageSuccessor] = []
    foreign: list[StageSuccessor] = []
    for successor in successors:
        if successor.stage_key == normalized_eval_key:
            eval_rows.append(successor)
        elif successor.stage_key == normalized_fanin_key:
            fanin_rows.append(successor)
        else:
            foreign.append(successor)

    if foreign:
        unexpected = foreign[0]
        raise ValueError(
            "unexpected successor stage key "
            f"{unexpected.stage_key.value!r} at index "
            f"{unexpected.stage_index}"
        )

    if not eval_rows:
        raise ValueError("deferral fan-out requires at least one eval row")

    if len(fanin_rows) != 1:
        raise ValueError(
            "deferral fan-out requires exactly one fan-in successor"
        )

    fanin = fanin_rows[0]
    if not fanin.barrier:
        raise ValueError(
            f"fan-in successor at index {fanin.stage_index} must set "
            "barrier=True"
        )

    for row in eval_rows:
        if row.barrier:
            raise ValueError(
                f"eval row at index {row.stage_index} must not set "
                "barrier=True"
            )

    row_count = len(eval_rows)
    expected_indices = range(
        origin_stage_index + 1,
        origin_stage_index + 1 + row_count,
    )
    actual_indices = {row.stage_index for row in eval_rows}
    if actual_indices != set(expected_indices):
        raise ValueError(
            "eval rows must occupy contiguous indices "
            f"{origin_stage_index + 1}..{origin_stage_index + row_count}"
        )

    expected_fanin_index = origin_stage_index + row_count + 1
    if fanin.stage_index != expected_fanin_index:
        raise ValueError(
            "fan-in successor must be at index "
            f"{expected_fanin_index}, not {fanin.stage_index}"
        )

    if len(successors) != row_count + 1:
        raise ValueError(
            "deferral fan-out must contain only eval rows and one fan-in"
        )
