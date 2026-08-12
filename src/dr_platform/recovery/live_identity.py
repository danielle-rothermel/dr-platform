from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LiveDbosIdentity:
    """Process-local DBOS identity supplied to reconciliation passes."""

    app_version: str
    executor_ids: frozenset[str]
