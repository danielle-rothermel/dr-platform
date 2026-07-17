"""Minimal logical state for a stage execution."""

from __future__ import annotations

from enum import StrEnum


class StageExecutionState(StrEnum):
    READY = "ready"
    ADMITTED = "admitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
