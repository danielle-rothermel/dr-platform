from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dr_platform.pipeline.registry import PipelineRegistry

MAX_RECOVERY_ATTEMPTS_MARKER = "_dr_platform_max_recovery_attempts"


def mark_wrapped_recovery_cap(
    workflow: object, max_recovery_attempts: int
) -> None:
    setattr(workflow, MAX_RECOVERY_ATTEMPTS_MARKER, max_recovery_attempts)


def read_wrapped_recovery_cap(workflow: object) -> int:
    value = getattr(workflow, MAX_RECOVERY_ATTEMPTS_MARKER, None)
    if not isinstance(value, int):
        raise TypeError("wrapped workflow is missing a recovery cap marker")
    return value


def validate_registry_recovery_cap(
    registry: PipelineRegistry,
    expected: int,
) -> None:
    for pipeline in registry.pipelines():
        for stage in pipeline.stages:
            cap = read_wrapped_recovery_cap(stage.workflow)
            if cap != expected:
                raise ValueError(
                    "wrapped workflow recovery cap does not match "
                    "PlatformDbosConfig.max_recovery_attempts: "
                    f"expected {expected}, found {cap} on stage "
                    f"{stage.key.value!r} in pipeline "
                    f"{pipeline.key.value!r} version {pipeline.version}"
                )
        completion = pipeline.run_completion
        if completion is not None:
            cap = read_wrapped_recovery_cap(completion.workflow)
            if cap != expected:
                raise ValueError(
                    "wrapped workflow recovery cap does not match "
                    "PlatformDbosConfig.max_recovery_attempts: "
                    f"expected {expected}, found {cap} on run completion "
                    f"{completion.key.value!r} in pipeline "
                    f"{pipeline.key.value!r} version {pipeline.version}"
                )
