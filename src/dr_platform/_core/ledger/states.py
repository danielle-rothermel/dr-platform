"""Logical states shared by the staged-work ledger."""

from __future__ import annotations

from enum import StrEnum


class StageExecutionState(StrEnum):
    READY = "ready"
    ADMITTED = "admitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
