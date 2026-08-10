from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from threading import Event

import pytest

from dr_platform.execution._checkpoint import _LedgerCheckpointExecutor


def test_checkpoint_executor_propagates_submission_context() -> None:
    value = ContextVar("checkpoint_context", default="missing")

    async def exercise() -> None:
        executor = _LedgerCheckpointExecutor(max_workers=1)
        try:
            value.set("workflow-context")
            assert await executor.run(value.get) == "workflow-context"
        finally:
            executor.close()

    asyncio.run(exercise())


def test_checkpoint_executor_isolated_from_the_loop_default_executor() -> None:
    async def exercise() -> None:
        loop = asyncio.get_running_loop()
        default_executor = ThreadPoolExecutor(max_workers=1)
        loop.set_default_executor(default_executor)
        checkpoint_executor = _LedgerCheckpointExecutor(max_workers=1)
        checkpoint_started = asyncio.Event()
        release_checkpoint = Event()

        def block_checkpoint() -> None:
            loop.call_soon_threadsafe(checkpoint_started.set)
            release_checkpoint.wait()

        checkpoint = asyncio.create_task(
            checkpoint_executor.run(block_checkpoint)
        )
        try:
            await asyncio.wait_for(checkpoint_started.wait(), timeout=2)
            assert (
                await asyncio.wait_for(
                    asyncio.to_thread(lambda: "default-complete"),
                    timeout=2,
                )
                == "default-complete"
            )
        finally:
            release_checkpoint.set()
            await asyncio.wait_for(checkpoint, timeout=2)
            checkpoint_executor.close()
            default_executor.shutdown(wait=True)

    asyncio.run(exercise())


def test_cancelling_queued_checkpoint_prevents_its_execution(
    monkeypatch,
) -> None:
    async def exercise() -> None:
        executor = _LedgerCheckpointExecutor(max_workers=1)
        loop = asyncio.get_running_loop()
        first_started = asyncio.Event()
        release_first = Event()
        queued_submitted = asyncio.Event()
        queued_ran = False
        submission_count = 0

        original_submit = executor._executor.submit

        def record_submit(*args, **kwargs):
            nonlocal submission_count
            future = original_submit(*args, **kwargs)
            submission_count += 1
            if submission_count == 2:
                queued_submitted.set()
            return future

        monkeypatch.setattr(executor._executor, "submit", record_submit)

        def block_first() -> None:
            loop.call_soon_threadsafe(first_started.set)
            release_first.wait()

        def run_queued() -> None:
            nonlocal queued_ran
            queued_ran = True

        first = asyncio.create_task(executor.run(block_first))
        try:
            await asyncio.wait_for(first_started.wait(), timeout=2)
            second = asyncio.create_task(executor.run(run_queued))
            await asyncio.wait_for(queued_submitted.wait(), timeout=2)
            second.cancel()
            with pytest.raises(asyncio.CancelledError):
                await second
            release_first.set()
            await asyncio.wait_for(first, timeout=2)
            executor.close()
            assert queued_ran is False
        finally:
            release_first.set()
            executor.close()

    asyncio.run(exercise())


def test_close_drains_accepted_work_then_rejects_and_is_idempotent() -> None:
    async def exercise() -> None:
        executor = _LedgerCheckpointExecutor(max_workers=1)
        entered = tuple(asyncio.Event() for _ in range(3))
        completed: list[int] = []

        async def submit(index: int) -> None:
            entered[index].set()
            await executor.run(completed.append, index)

        tasks = tuple(asyncio.create_task(submit(index)) for index in range(3))
        await asyncio.gather(*(event.wait() for event in entered))
        executor.close()
        await asyncio.gather(*tasks)
        assert completed == [0, 1, 2]

        executor.close()
        with pytest.raises(RuntimeError, match="executor is closed"):
            await executor.run(lambda: None)

    asyncio.run(exercise())
