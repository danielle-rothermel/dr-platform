"""Runtime declarations for linear pipelines and their stages."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dr_platform.staging.identities import (
    PipelineKey,
    StageKey,
    validate_key_value,
)

WorkflowCallable = Callable[..., object]
ArgumentsCallable = Callable[..., tuple[object, ...]]
PipelineIdentity = tuple[PipelineKey, int]


def validate_positive_integer(value: int, *, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value <= 0:
        raise ValueError(f"{label} must be positive")


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
    """One ordered stage in a linear pipeline."""

    key: StageKey
    queue_name: str
    workflow: WorkflowCallable
    args_for: ArgumentsCallable

    def __post_init__(self) -> None:
        if not isinstance(self.key, StageKey):
            raise TypeError("stage key must be a StageKey")
        validate_key_value(self.queue_name, label="queue name")
        if not callable(self.workflow):
            raise TypeError("workflow must be callable")
        if not callable(self.args_for):
            raise TypeError("args_for must be callable")


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineDefinition:
    """One immutable version of an ordered, non-empty stage chain."""

    key: PipelineKey
    version: int
    stages: tuple[StageDefinition, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.key, PipelineKey):
            raise TypeError("pipeline key must be a PipelineKey")
        validate_positive_integer(self.version, label="pipeline version")
        if not isinstance(self.stages, tuple):
            raise TypeError("pipeline stages must be a tuple")
        if not self.stages:
            raise ValueError("pipeline must declare at least one stage")
        if not all(
            isinstance(stage, StageDefinition) for stage in self.stages
        ):
            raise TypeError("pipeline stages must be StageDefinition values")

        stage_keys = tuple(stage.key for stage in self.stages)
        if len(set(stage_keys)) != len(stage_keys):
            raise ValueError("pipeline stage keys must be unique")

    @property
    def identity(self) -> PipelineIdentity:
        return self.key, self.version
