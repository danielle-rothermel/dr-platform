from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from functools import partial
from threading import Lock
from typing import TYPE_CHECKING, ParamSpec, TypeVar, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

_P = ParamSpec("_P")
_R = TypeVar("_R")

_CHECKPOINT_EXECUTOR_ATTRIBUTE = "_dr_platform_ledger_checkpoint_executor"
_MISSING = object()


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


class _LedgerCheckpointBinding:
    def __init__(
        self,
        *,
        workflows: tuple[Callable[..., object], ...],
        previous: tuple[object, ...],
        executor: _LedgerCheckpointExecutor,
    ) -> None:
        self._workflows = workflows
        self._previous = previous
        self._executor = executor
        self._release_lock = Lock()
        self._released = False

    def release(self) -> None:
        with self._release_lock:
            if self._released:
                return
            for workflow, previous in zip(
                self._workflows,
                self._previous,
                strict=True,
            ):
                if (
                    getattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, _MISSING)
                    is not self._executor
                ):
                    continue
                if previous is _MISSING:
                    delattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE)
                else:
                    setattr(
                        workflow,
                        _CHECKPOINT_EXECUTOR_ATTRIBUTE,
                        previous,
                    )
            self._released = True


def _distinct_workflows(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    distinct: list[Callable[..., object]] = []
    identities: set[int] = set()
    for workflow in workflows:
        identity = id(workflow)
        if identity in identities:
            continue
        identities.add(identity)
        distinct.append(workflow)
    return tuple(distinct)


def _preflight_ledger_checkpoint_executor(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    bound_workflows = _distinct_workflows(workflows)
    for workflow in bound_workflows:
        existing = getattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, None)
        existing_executor = cast("_LedgerCheckpointExecutor | None", existing)
        if existing_executor is not None and not existing_executor.closed:
            raise RuntimeError(
                "wrapped workflow already has a live runtime owner"
            )
    return bound_workflows


def _bind_ledger_checkpoint_executor(
    workflows: Iterable[Callable[..., object]],
    executor: _LedgerCheckpointExecutor,
) -> _LedgerCheckpointBinding:
    bound_workflows = _preflight_ledger_checkpoint_executor(workflows)
    previous = tuple(
        getattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, _MISSING)
        for workflow in bound_workflows
    )
    changed: list[tuple[Callable[..., object], object]] = []
    try:
        for workflow, prior in zip(
            bound_workflows,
            previous,
            strict=True,
        ):
            setattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, executor)
            changed.append((workflow, prior))
    except Exception:
        for workflow, prior in reversed(changed):
            if prior is _MISSING:
                delattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE)
            else:
                setattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, prior)
        raise
    return _LedgerCheckpointBinding(
        workflows=bound_workflows,
        previous=previous,
        executor=executor,
    )


def _require_ledger_checkpoint_executor(
    workflow: Callable[..., object],
) -> _LedgerCheckpointExecutor:
    executor = getattr(workflow, _CHECKPOINT_EXECUTOR_ATTRIBUTE, None)
    if executor is None:
        raise RuntimeError(
            "wrapped workflow requires a live dispatcher registration"
        )
    return cast("_LedgerCheckpointExecutor", executor)
