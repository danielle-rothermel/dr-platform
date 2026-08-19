from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import pytest

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)

if TYPE_CHECKING:
    from dr_platform.execution.stage_completion import StageCompletion
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    LabelQueueRoute,
    PipelineDefinition,
    PipelineIdentity,
    RunCompletionDefinition,
    StageDefinition,
    resolve_stage_queue_name,
)
from dr_platform.pipeline.registry import PipelineRegistry


async def _workflow(*args: object) -> str:
    return repr(args)


def _args_for(*args: object) -> tuple[object, ...]:
    return args


async def _async_args_for(*args: object) -> tuple[object, ...]:
    return args


class _AsyncArgsFor:
    async def __call__(self, *args: object) -> tuple[object, ...]:
        return args


class _SynchronousArgsFor:
    def __call__(self, *args: object) -> tuple[object, ...]:
        return args


def _stage(
    key: str,
    *,
    queue_name: str | None = None,
    workflow: Callable[..., Awaitable[str | StageCompletion]] = _workflow,
) -> StageDefinition:
    return StageDefinition(
        key=StageKey(key),
        queue_name=queue_name or key,
        workflow=workflow,
        args_for=_args_for,
    )


def test_pipeline_preserves_declared_linear_stage_order() -> None:
    prepare = _stage("prepare")
    execute = _stage("execute")

    pipeline = PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(prepare, execute),
    )

    assert pipeline.identity == PipelineIdentity(PipelineKey("evaluation"), 1)
    assert pipeline.stages == (prepare, execute)
    assert pipeline.stages[0].key == StageKey("prepare")


def test_pipeline_identity_rejects_a_non_pipeline_key() -> None:
    with pytest.raises(TypeError, match="pipeline key must be a PipelineKey"):
        PipelineIdentity("evaluation", 1)  # ty: ignore[invalid-argument-type]


def test_pipeline_identity_rejects_a_non_positive_version() -> None:
    with pytest.raises(ValueError, match="pipeline version must be positive"):
        PipelineIdentity(PipelineKey("evaluation"), 0)


def test_pipeline_identity_round_trips_through_registry() -> None:
    pipeline = PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(_stage("execute"),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)

    identity = pipeline.identity
    twin = PipelineIdentity(PipelineKey("evaluation"), 1)
    assert identity == twin
    assert hash(identity) == hash(twin)
    assert registry.get(key=identity.key, version=identity.version) is pipeline


def test_pipeline_rejects_an_empty_stage_tuple() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        PipelineDefinition(key=PipelineKey("evaluation"), version=1, stages=())


def test_pipeline_rejects_duplicate_stage_keys() -> None:
    with pytest.raises(ValueError, match="stage keys must be unique"):
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(_stage("execute"), _stage("execute")),
        )


def test_stage_rejects_a_synchronous_workflow() -> None:
    def workflow() -> str:
        return "output:1"

    with pytest.raises(TypeError, match="async callable"):
        _stage("execute", workflow=workflow)  # ty: ignore[invalid-argument-type]


def test_stage_rejects_async_argument_derivation() -> None:
    with pytest.raises(TypeError, match="args_for must be synchronous"):
        StageDefinition(
            key=StageKey("execute"),
            queue_name="execution",
            workflow=_workflow,
            args_for=_async_args_for,  # ty: ignore[invalid-argument-type]
        )


def test_run_completion_rejects_async_argument_derivation() -> None:
    with pytest.raises(TypeError, match="args_for must be synchronous"):
        RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name="completion",
            workflow=_workflow,
            args_for=_async_args_for,  # ty: ignore[invalid-argument-type]
        )


def test_stage_rejects_async_callable_object_args_for() -> None:
    with pytest.raises(TypeError, match="args_for must be synchronous"):
        StageDefinition(
            key=StageKey("execute"),
            queue_name="execution",
            workflow=_workflow,
            args_for=_AsyncArgsFor(),  # ty: ignore[invalid-argument-type]
        )


def test_run_completion_rejects_async_callable_object_args_for() -> None:
    with pytest.raises(TypeError, match="args_for must be synchronous"):
        RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name="completion",
            workflow=_workflow,
            args_for=_AsyncArgsFor(),  # ty: ignore[invalid-argument-type]
        )


def test_definitions_accept_synchronous_callable_object_args_for() -> None:
    args_for = _SynchronousArgsFor()

    stage = StageDefinition(
        key=StageKey("execute"),
        queue_name="execution",
        workflow=_workflow,
        args_for=args_for,
    )
    completion = RunCompletionDefinition(
        key=RunCompletionKey("aggregate"),
        queue_name="completion",
        workflow=_workflow,
        args_for=args_for,
    )

    assert stage.args_for is args_for
    assert completion.args_for is args_for


def test_run_completion_key_cannot_collide_with_a_stage_key() -> None:
    completion = RunCompletionDefinition(
        key=RunCompletionKey("execute"),
        queue_name="completion",
        workflow=_workflow,
        args_for=_args_for,
    )
    with pytest.raises(ValueError, match="must not collide"):
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(_stage("execute"),),
            run_completion=completion,
        )


def test_wrap_pipeline_workflows_preserves_label_queue_routes() -> None:
    declared = PipelineDefinition(
        key=PipelineKey("label-routes"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="default-queue",
                label_queue_routes=(
                    LabelQueueRoute(
                        selector={"device": "cuda"},
                        queue_name="cuda-queue",
                    ),
                ),
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    wrapped = wrap_pipeline_workflows(declared, max_recovery_attempts=1)
    assert (
        wrapped.stages[0].label_queue_routes
        == declared.stages[0].label_queue_routes
    )


def test_label_queue_routes_reject_empty_selector() -> None:
    with pytest.raises(ValueError, match="selector must be non-empty"):
        StageDefinition(
            key=StageKey("execute"),
            queue_name="default-queue",
            label_queue_routes=(
                LabelQueueRoute(selector={}, queue_name="cuda-queue"),
            ),
            workflow=_workflow,
            args_for=_args_for,
        )


def test_label_queue_routes_accept_disjoint_selectors() -> None:
    stage = StageDefinition(
        key=StageKey("execute"),
        queue_name="default-queue",
        label_queue_routes=(
            LabelQueueRoute(
                selector={"accel": "cuda"},
                queue_name="cuda-queue",
            ),
            LabelQueueRoute(
                selector={"tier": "batch"},
                queue_name="batch-queue",
            ),
        ),
        workflow=_workflow,
        args_for=_args_for,
    )
    assert (
        resolve_stage_queue_name(stage, labels={"accel": "cuda"})
        == "cuda-queue"
    )
    assert (
        resolve_stage_queue_name(stage, labels={"tier": "batch"})
        == "batch-queue"
    )


def test_label_queue_routes_accept_conflicting_shared_keys() -> None:
    stage = StageDefinition(
        key=StageKey("execute"),
        queue_name="default-queue",
        label_queue_routes=(
            LabelQueueRoute(
                selector={"a": "1"},
                queue_name="queue-a",
            ),
            LabelQueueRoute(
                selector={"a": "2"},
                queue_name="queue-b",
            ),
        ),
        workflow=_workflow,
        args_for=_args_for,
    )
    assert resolve_stage_queue_name(stage, labels={"a": "1"}) == "queue-a"
    assert resolve_stage_queue_name(stage, labels={"a": "2"}) == "queue-b"


def test_label_queue_routes_reject_duplicate_queue_names() -> None:
    with pytest.raises(ValueError, match="must be distinct"):
        StageDefinition(
            key=StageKey("execute"),
            queue_name="default-queue",
            label_queue_routes=(
                LabelQueueRoute(
                    selector={"device": "cuda"},
                    queue_name="shared-queue",
                ),
                LabelQueueRoute(
                    selector={"tier": "batch"},
                    queue_name="shared-queue",
                ),
            ),
            workflow=_workflow,
            args_for=_args_for,
        )


def test_label_queue_routes_reject_overlapping_selectors() -> None:
    with pytest.raises(ValueError, match="must not overlap"):
        StageDefinition(
            key=StageKey("execute"),
            queue_name="default-queue",
            label_queue_routes=(
                LabelQueueRoute(
                    selector={"device": "cuda"},
                    queue_name="cuda-queue",
                ),
                LabelQueueRoute(
                    selector={"device": "cuda", "pool": "a"},
                    queue_name="cuda-a-queue",
                ),
            ),
            workflow=_workflow,
            args_for=_args_for,
        )


def test_resolve_stage_queue_name_uses_first_matching_route() -> None:
    stage = StageDefinition(
        key=StageKey("execute"),
        queue_name="default-queue",
        label_queue_routes=(
            LabelQueueRoute(
                selector={"device": "cuda"},
                queue_name="cuda-queue",
            ),
            LabelQueueRoute(
                selector={"device": "cpu"},
                queue_name="cpu-queue",
            ),
        ),
        workflow=_workflow,
        args_for=_args_for,
    )

    assert (
        resolve_stage_queue_name(stage, labels={"device": "cuda", "tier": "1"})
        == "cuda-queue"
    )
    assert (
        resolve_stage_queue_name(stage, labels={"tier": "1"})
        == "default-queue"
    )
