from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from functools import partial
from threading import Lock
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

from dbos import DBOS

from dr_platform.execution._workflow_binding import (
    _WorkflowBinding,
    bind,
    preflight,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from sqlalchemy.engine import Connection

_P = ParamSpec("_P")
_R = TypeVar("_R")

_CHECKPOINT_EXECUTOR_ATTRIBUTE = "_dr_platform_ledger_checkpoint_executor"


class _LedgerCheckpointExecutor:
    """Runs synchronous ledger checkpoints outside the loop default pool."""

    def __init__(self, *, max_workers: int) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="dr-platform-ledger",
        )
        self._lifecycle_lock = Lock()
        self._closed = False

    @property
    def closed(self) -> bool:
        with self._lifecycle_lock:
            return self._closed

    async def run(
        self,
        function: Callable[_P, _R],
        /,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _R:
        loop = asyncio.get_running_loop()
        context = copy_context()
        call = partial(context.run, function, *args, **kwargs)
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("ledger checkpoint executor is closed")
            future = loop.run_in_executor(self._executor, call)
        return await future

    def close(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=True)


def _checkpoint_executor_is_live(existing: object) -> bool:
    """Only an executor that has not been closed still owns the workflow."""
    return not cast("_LedgerCheckpointExecutor", existing).closed


class _LedgerCheckpointBinding(_WorkflowBinding):
    """Binds one ledger checkpoint executor to wrapped stage workflows."""


def _preflight_ledger_checkpoint_executor(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    return preflight(
        workflows,
        attribute=_CHECKPOINT_EXECUTOR_ATTRIBUTE,
        is_live=_checkpoint_executor_is_live,
    )


def _bind_ledger_checkpoint_executor(
    workflows: Iterable[Callable[..., object]],
    executor: _LedgerCheckpointExecutor,
) -> _LedgerCheckpointBinding:
    return bind(
        workflows,
        executor,
        attribute=_CHECKPOINT_EXECUTOR_ATTRIBUTE,
        is_live=_checkpoint_executor_is_live,
        binding_type=_LedgerCheckpointBinding,
    )


def _require_ledger_checkpoint_executor(
    workflow: Callable[..., object],
) -> _LedgerCheckpointExecutor:
    return cast(
        "_LedgerCheckpointExecutor",
        require(workflow, attribute=_CHECKPOINT_EXECUTOR_ATTRIBUTE),
    )


def _ledger_checkpoint_connection() -> Connection:
    return DBOS.sql_session.connection()
