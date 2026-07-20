"""Focused tests for startup pipeline registration."""

from __future__ import annotations

import pytest

from dr_platform.staging import (
    PipelineConflictError,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageKey,
)


def _workflow(*args: object) -> object:
    return args


def _args_for(*args: object) -> tuple[object, ...]:
    return args


def _pipeline(*, queue_name: str = "execute") -> PipelineDefinition:
    return PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=queue_name,
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )


def test_registry_lookup_returns_the_registered_pipeline() -> None:
    pipeline = _pipeline()
    registry = PipelineRegistry()

    assert registry.register(pipeline) is pipeline
    assert registry.get(key=PipelineKey("evaluation"), version=1) is pipeline


def test_identical_registration_is_an_idempotent_no_op() -> None:
    first = _pipeline()
    identical = _pipeline()
    registry = PipelineRegistry()

    assert registry.register(first) is first
    assert registry.register(identical) is first
    assert registry.get(key=PipelineKey("evaluation"), version=1) is first


def test_registry_rejects_a_conflicting_definition() -> None:
    registry = PipelineRegistry()
    registry.register(_pipeline())

    with pytest.raises(PipelineConflictError) as caught:
        registry.register(_pipeline(queue_name="other-queue"))

    assert caught.value.identity == (PipelineKey("evaluation"), 1)


def test_registry_exposes_registered_pipelines_for_wiring_checks() -> None:
    pipeline = _pipeline()
    registry = PipelineRegistry()
    registry.register(pipeline)

    assert registry.pipelines() == (pipeline,)
