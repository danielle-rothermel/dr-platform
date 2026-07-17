"""Runtime declarations for linear pipelines and their stages."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from dr_platform.staging.identities import StageKey, validate_key_value

WorkflowCallable = Callable[..., object]
ArgumentsCallable = Callable[..., tuple[object, ...]]
PipelineIdentity = tuple[str, int]


def validate_positive_integer(value: int, *, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value <= 0:
        raise ValueError(f"{label} must be positive")


@dataclass(frozen=True, slots=True, init=False)
class StageDefinition:
    """One ordered stage in a linear pipeline."""

    key: StageKey
    queue_name: str
    workflow: WorkflowCallable
    args_for: ArgumentsCallable

    def __init__(
        self,
        *,
        key: StageKey | str,
        queue_name: str,
        workflow: WorkflowCallable,
        args_for: ArgumentsCallable,
    ) -> None:
        stage_key = key if isinstance(key, StageKey) else StageKey(key)
        validate_key_value(queue_name, label="queue name")
        if not callable(workflow):
            raise TypeError("workflow must be callable")
        if not callable(args_for):
            raise TypeError("args_for must be callable")

        object.__setattr__(self, "key", stage_key)
        object.__setattr__(self, "queue_name", queue_name)
        object.__setattr__(self, "workflow", workflow)
        object.__setattr__(self, "args_for", args_for)


@dataclass(frozen=True, slots=True, init=False)
class PipelineDefinition:
    """One immutable version of an ordered, non-empty stage chain."""

    key: str
    version: int
    stages: tuple[StageDefinition, ...]

    def __init__(
        self,
        *,
        key: str,
        version: int,
        stages: tuple[StageDefinition, ...],
    ) -> None:
        validate_key_value(key, label="pipeline key")
        validate_positive_integer(version, label="pipeline version")
        if not isinstance(stages, tuple):
            raise TypeError("pipeline stages must be a tuple")
        if not stages:
            raise ValueError("pipeline must declare at least one stage")
        if not all(isinstance(stage, StageDefinition) for stage in stages):
            raise TypeError("pipeline stages must be StageDefinition values")

        stage_keys = tuple(stage.key for stage in stages)
        if len(set(stage_keys)) != len(stage_keys):
            raise ValueError("pipeline stage keys must be unique")

        object.__setattr__(self, "key", key)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "stages", stages)

    @property
    def identity(self) -> PipelineIdentity:
        return self.key, self.version
