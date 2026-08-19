from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Collection


@dataclass(frozen=True, slots=True)
class LiveDbosIdentity:
    """Process-local DBOS identity supplied to reconciliation passes."""

    app_version: str
    executor_ids: frozenset[str] = frozenset()
    resolve_executor_ids: Callable[[], Collection[str]] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.app_version, str):
            raise TypeError("app_version must be a string")
        if not isinstance(self.executor_ids, frozenset):
            raise TypeError("executor_ids must be a frozenset")
        if self.resolve_executor_ids is not None and not callable(
            self.resolve_executor_ids
        ):
            raise TypeError(
                "resolve_executor_ids must be callable when provided"
            )

    def live_executor_ids(self) -> frozenset[str]:
        if self.resolve_executor_ids is not None:
            return frozenset(self.resolve_executor_ids())
        return self.executor_ids
