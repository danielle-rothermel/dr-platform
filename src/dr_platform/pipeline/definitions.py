from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
    validate_key_value,
)
from dr_platform._core.validation import validate_positive_integer

AsyncWorkflowCallable = Callable[..., Awaitable[str | None]]
ArgumentsCallable = Callable[..., tuple[object, ...]]


def _validate_async_workflow(value: object) -> None:
    if not callable(value) or not inspect.iscoroutinefunction(value):
        raise TypeError("workflow must be an async callable")


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
    workflow: AsyncWorkflowCallable
    args_for: ArgumentsCallable

    def __post_init__(self) -> None:
        if not isinstance(self.key, StageKey):
            raise TypeError("stage key must be a StageKey")
        validate_key_value(self.queue_name, label="queue name")
        _validate_async_workflow(self.workflow)
        if not callable(self.args_for):
            raise TypeError("args_for must be callable")


@dataclass(frozen=True, slots=True, kw_only=True)
class RunCompletionDefinition:
    key: RunCompletionKey
    queue_name: str
    workflow: AsyncWorkflowCallable
    args_for: ArgumentsCallable

    def __post_init__(self) -> None:
        if not isinstance(self.key, RunCompletionKey):
            raise TypeError("run completion key must be a RunCompletionKey")
        validate_key_value(self.queue_name, label="queue name")
        _validate_async_workflow(self.workflow)
        if not callable(self.args_for):
            raise TypeError("args_for must be callable")


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineDefinition:
    key: PipelineKey
    version: int
    stages: tuple[StageDefinition, ...]
    run_completion: RunCompletionDefinition | None = None

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
        if self.run_completion is not None:
            if not isinstance(self.run_completion, RunCompletionDefinition):
                raise TypeError(
                    "run completion must be a RunCompletionDefinition"
                )
            if self.run_completion.key.value in {
                key.value for key in stage_keys
            }:
                raise ValueError(
                    "run completion key must not collide with a stage key"
                )

    @property
    def identity(self) -> PipelineIdentity:
        return PipelineIdentity(self.key, self.version)
