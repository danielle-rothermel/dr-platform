from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry


async def _workflow(*args: object) -> str:
    return repr(args)


def _args_for(*args: object) -> tuple[object, ...]:
    return args


def _stage(
    key: str,
    *,
    queue_name: str | None = None,
    workflow: Callable[..., Awaitable[str | None]] = _workflow,
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
