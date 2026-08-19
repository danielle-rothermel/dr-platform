from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
    validate_key_value,
)
from dr_platform._core.validation import validate_positive_integer
from dr_platform.execution.stage_completion import StageCompletion

StageWorkflowCallable = Callable[..., Awaitable[str | StageCompletion]]
RunCompletionWorkflowCallable = Callable[..., Awaitable[str | None]]
ArgumentsCallable = Callable[..., tuple[object, ...]]


def _validate_async_workflow(value: object) -> None:
    if not callable(value) or not inspect.iscoroutinefunction(value):
        raise TypeError("workflow must be an async callable")


def _validate_args_for(value: object) -> None:
    if not callable(value):
        raise TypeError("args_for must be callable")
    if inspect.iscoroutinefunction(value) or inspect.iscoroutinefunction(
        value.__call__
    ):
        raise TypeError("args_for must be synchronous")


def selector_matches(
    selector: Mapping[str, str], labels: Mapping[str, str]
) -> bool:
    return all(labels.get(key) == value for key, value in selector.items())


def _selectors_can_both_match(
    left: Mapping[str, str],
    right: Mapping[str, str],
) -> bool:
    shared = set(left) & set(right)
    if not shared:
        return False
    return all(left[key] == right[key] for key in shared)


def _validate_label_queue_routes(
    routes: tuple[LabelQueueRoute, ...],
) -> None:
    for route in routes:
        if not route.selector:
            raise ValueError("label queue route selector must be non-empty")
    route_queue_names = [route.queue_name for route in routes]
    if len(set(route_queue_names)) != len(route_queue_names):
        raise ValueError("label queue route queue names must be distinct")
    for index, left in enumerate(routes):
        for right in routes[index + 1 :]:
            if _selectors_can_both_match(left.selector, right.selector):
                raise ValueError(
                    "label queue routes must not overlap on the same labels"
                )


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
class LabelQueueRoute:
    selector: Mapping[str, str]
    queue_name: str

    def __post_init__(self) -> None:
        if not isinstance(self.selector, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in self.selector.items()
        ):
            raise TypeError(
                "label queue route selector must map strings to strings"
            )
        object.__setattr__(
            self, "selector", immutable_mapping(dict(self.selector))
        )
        validate_key_value(self.queue_name, label="queue name")


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
    key: StageKey
    queue_name: str
    workflow: StageWorkflowCallable
    args_for: ArgumentsCallable
    label_queue_routes: tuple[LabelQueueRoute, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.key, StageKey):
            raise TypeError("stage key must be a StageKey")
        validate_key_value(self.queue_name, label="queue name")
        if not isinstance(self.label_queue_routes, tuple) or not all(
            isinstance(route, LabelQueueRoute)
            for route in self.label_queue_routes
        ):
            raise TypeError(
                "label queue routes must be a tuple of LabelQueueRoute values"
            )
        _validate_label_queue_routes(self.label_queue_routes)
        _validate_async_workflow(self.workflow)
        _validate_args_for(self.args_for)


def resolve_stage_queue_name(
    stage: StageDefinition,
    *,
    labels: Mapping[str, str],
) -> str:
    for route in stage.label_queue_routes:
        if selector_matches(route.selector, labels):
            return route.queue_name
    return stage.queue_name


@dataclass(frozen=True, slots=True, kw_only=True)
class RunCompletionDefinition:
    key: RunCompletionKey
    queue_name: str
    workflow: RunCompletionWorkflowCallable
    args_for: ArgumentsCallable

    def __post_init__(self) -> None:
        if not isinstance(self.key, RunCompletionKey):
            raise TypeError("run completion key must be a RunCompletionKey")
        validate_key_value(self.queue_name, label="queue name")
        _validate_async_workflow(self.workflow)
        _validate_args_for(self.args_for)


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
