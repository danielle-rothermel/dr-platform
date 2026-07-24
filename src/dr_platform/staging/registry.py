"""Startup registry for immutable pipeline versions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dr_platform.staging.definitions import PipelineIdentity

if TYPE_CHECKING:
    from dr_platform.staging.definitions import PipelineDefinition
    from dr_platform.staging.identities import PipelineKey


class PipelineConflictError(RuntimeError):
    """Raised when one pipeline identity has conflicting declarations."""

    def __init__(self, identity: PipelineIdentity) -> None:
        self.identity = identity
        super().__init__(
            "pipeline key and version are already registered with a "
            "different definition: "
            f"{identity.key.value!r} version {identity.version}"
        )


class PipelineRegistry:
    """In-memory registry populated from authoritative startup declarations."""

    def __init__(self) -> None:
        self._pipelines: dict[PipelineIdentity, PipelineDefinition] = {}

    def register(self, pipeline: PipelineDefinition) -> PipelineDefinition:
        existing = self._pipelines.get(pipeline.identity)
        if existing is None:
            self._pipelines[pipeline.identity] = pipeline
            return pipeline
        if existing != pipeline:
            raise PipelineConflictError(pipeline.identity)
        return existing

    def get(self, *, key: PipelineKey, version: int) -> PipelineDefinition:
        return self._pipelines[PipelineIdentity(key, version)]

    def pipelines(self) -> tuple[PipelineDefinition, ...]:
        """Return every registered definition for wiring-time validation."""
        return tuple(self._pipelines.values())
