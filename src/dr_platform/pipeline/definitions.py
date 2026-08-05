from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dr_platform._core.identities import (
    PipelineKey,
    StageKey,
    validate_key_value,
)
from dr_platform._core.validation import validate_positive_integer

WorkflowCallable = Callable[..., object]
ArgumentsCallable = Callable[..., tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class PipelineIdentity:
    key: PipelineKey
    version: int

    def __post_init__(self) -> None:
        if not isinstance(self.key, PipelineKey):
            raise TypeError("pipeline key must be a PipelineKey")
        validate_positive_integer(self.version, label="pipeline version")


def validate_pipeline_identity(value: object) -> PipelineIdentity:
    if not isinstance(value, PipelineIdentity):
        raise TypeError("pipeline must be a PipelineIdentity")
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
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
        return PipelineIdentity(self.key, self.version)
