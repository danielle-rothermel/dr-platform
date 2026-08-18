from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dr_platform._core.identities import StageKey, normalize_key
from dr_platform._core.validation import validate_non_empty_string

if TYPE_CHECKING:
    from dr_platform.pipeline.definitions import PipelineDefinition


@dataclass(frozen=True, slots=True)
class StageSuccessor:
    """One successor stage enqueued after a successful step.

    ``stage_index`` is application-chosen, may be sparse, must exceed the
    completed stage index, and must be unique within one handoff. When
    ``barrier`` is true, admission holds the successor ready until every lower
    ``stage_index`` for the same work item succeeds. ``input_reference``
    supersedes the work item submission input for that stage row.
    """

    stage_key: StageKey
    stage_index: int
    input_reference: str
    barrier: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "stage_key",
            normalize_key(self.stage_key, StageKey),
        )
        if self.stage_index < 0:
            raise ValueError("stage_index must be non-negative")
        validate_non_empty_string(
            self.input_reference,
            label="successor input reference",
        )
        if not isinstance(self.barrier, bool):
            raise TypeError("barrier must be a bool")


@dataclass(frozen=True, slots=True)
class StageCompletion:
    """Application-directed handoff after one successful stage body.

    Successor indices are application-chosen, may be sparse, must exceed the
    persisted stage index, and must be unique within one handoff.
    """

    output_reference: str
    successors: tuple[StageSuccessor, ...] = ()

    def __post_init__(self) -> None:
        validate_non_empty_string(
            self.output_reference,
            label="stage output reference",
        )
        if not isinstance(self.successors, tuple):
            raise TypeError("successors must be a tuple")


def validate_successors_for_handoff(
    *,
    pipeline: PipelineDefinition,
    current_stage_index: int,
    successors: tuple[StageSuccessor, ...],
) -> None:
    registered_keys = {stage.key.value for stage in pipeline.stages}
    seen_indexes: set[int] = set()
    for successor in successors:
        if successor.stage_index <= current_stage_index:
            raise ValueError(
                "successor stage_index must be greater than the "
                f"completed stage index ({current_stage_index})"
            )
        if successor.stage_index in seen_indexes:
            raise ValueError(
                "successor stage_index values must be unique "
                "within one handoff"
            )
        seen_indexes.add(successor.stage_index)
        if successor.stage_key.value not in registered_keys:
            raise ValueError(
                f"successor stage key is not registered: "
                f"{successor.stage_key.value!r}"
            )


def parse_stage_workflow_result(
    value: object,
    *,
    pipeline: PipelineDefinition,
    current_stage_index: int,
    linear_next_stage_key: StageKey | None = None,
) -> StageCompletion:
    if isinstance(value, StageCompletion):
        validate_successors_for_handoff(
            pipeline=pipeline,
            current_stage_index=current_stage_index,
            successors=value.successors,
        )
        return value
    if isinstance(value, str):
        if not value:
            raise ValueError(
                "stage application logic must return a non-empty "
                "output-reference string"
            )
        linear_successors: tuple[StageSuccessor, ...] = ()
        if linear_next_stage_key is not None:
            linear_successors = (
                StageSuccessor(
                    stage_key=linear_next_stage_key,
                    stage_index=current_stage_index + 1,
                    input_reference=value,
                ),
            )
        return StageCompletion(
            output_reference=value,
            successors=linear_successors,
        )
    raise TypeError(
        "stage workflow must return str or StageCompletion, "
        f"not {type(value)!r}"
    )


__all__ = [
    "StageCompletion",
    "StageSuccessor",
    "parse_stage_workflow_result",
    "validate_successors_for_handoff",
]
