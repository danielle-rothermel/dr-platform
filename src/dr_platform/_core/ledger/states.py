from __future__ import annotations

from enum import UNIQUE, StrEnum, verify

from pydantic import BaseModel, ConfigDict, StrictInt, field_validator


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


# A BaseModel because it is wire-nested inside RunCompletionPayload.
class StateCount(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    state: StageExecutionState
    count: StrictInt

    @field_validator("count")
    @classmethod
    def _nonnegative_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("state count must be non-negative")
        return value
