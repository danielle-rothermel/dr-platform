from __future__ import annotations

from enum import UNIQUE, StrEnum, verify


@verify(UNIQUE)
class StageExecutionState(StrEnum):
    READY = "ready"
    ADMITTED = "admitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@verify(UNIQUE)
class RunCompletionExecutionState(StrEnum):
    ENQUEUED = "enqueued"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
