from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LiveExecutorSweepContext:
    live_executor_ids: frozenset[str]
    suppress_pending_dead_executor: bool
    resolver_unavailable: bool


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

    def resolve_for_sweep(self) -> LiveExecutorSweepContext:
        if self.resolve_executor_ids is None:
            return LiveExecutorSweepContext(
                live_executor_ids=self.executor_ids,
                suppress_pending_dead_executor=False,
                resolver_unavailable=False,
            )
        try:
            resolved = frozenset(self.resolve_executor_ids())
        except Exception as error:  # noqa: BLE001 -- resolver must not fail sweep
            logger.debug(
                "executor resolver failed during sweep",
                exc_info=error,
            )
            return LiveExecutorSweepContext(
                live_executor_ids=self.executor_ids,
                suppress_pending_dead_executor=True,
                resolver_unavailable=True,
            )
        if not resolved:
            return LiveExecutorSweepContext(
                live_executor_ids=self.executor_ids,
                suppress_pending_dead_executor=True,
                resolver_unavailable=True,
            )
        return LiveExecutorSweepContext(
            live_executor_ids=resolved | self.executor_ids,
            suppress_pending_dead_executor=False,
            resolver_unavailable=False,
        )
