"""Rich heartbeat progress logging for long-running operations."""

from __future__ import annotations

import threading
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

from rich.console import Console

DEFAULT_PROGRESS_INTERVAL_SECONDS = 5.0
_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_OPERATION_WIDTH = 8


class OperationProgress(AbstractContextManager["OperationProgress"]):
    def __init__(
        self,
        operation: str,
        *,
        interval_seconds: float = DEFAULT_PROGRESS_INTERVAL_SECONDS,
        console: Console | None = None,
    ) -> None:
        self._operation = operation
        self._interval_seconds = interval_seconds
        self._console = console or Console(stderr=True, highlight=False)
        self._lock = threading.Lock()
        self._metrics: dict[str, Any] = {}
        self._status = "running"
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> OperationProgress:
        if self._interval_seconds > 0:
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"{self._operation}-progress",
                daemon=True,
            )
            self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_seconds + 1)
            self._thread = None

    def update(self, **metrics: object) -> None:
        with self._lock:
            self._metrics.update(metrics)

    def event(
        self,
        label: str,
        metrics: Mapping[str, object] | None = None,
        *,
        style: str = "green",
    ) -> None:
        merged = dict(metrics or {})
        self._emit(label=label, style=style, metrics=merged)

    def complete(self, metrics: Mapping[str, object] | None = None) -> None:
        merged = dict(metrics or {})
        with self._lock:
            self._status = "done"
            self._metrics.update(merged)
        self.event("complete", merged, style="bold green")

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            with self._lock:
                if self._status == "done":
                    return
            self._emit(label="…", style="dim", metrics=self._snapshot())

    def _snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._metrics)

    def _emit(
        self,
        *,
        label: str,
        style: str,
        metrics: Mapping[str, Any],
    ) -> None:
        timestamp = datetime.now(UTC).astimezone().strftime(_TIMESTAMP_FORMAT)
        operation = self._operation.ljust(_OPERATION_WIDTH)
        metric_text = _format_metrics(metrics)
        self._console.print(
            f"[dim]{timestamp}[/dim] "
            f"[bold cyan]{operation}[/bold cyan] "
            f"[{style}]{label}[/{style}]"
            + (f" {metric_text}" if metric_text else ""),
            highlight=False,
        )


def _format_metrics(metrics: Mapping[str, Any]) -> str:
    if not metrics:
        return ""
    parts: list[str] = []
    for key in sorted(metrics):
        value = metrics[key]
        if value is None:
            continue
        parts.append(f"[yellow]{key}[/yellow]={_format_metric_value(value)}")
    return " ".join(parts)


def _format_metric_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def operation_progress(
    operation: str,
    *,
    interval_seconds: float = DEFAULT_PROGRESS_INTERVAL_SECONDS,
    console: Console | None = None,
) -> OperationProgress:
    return OperationProgress(
        operation,
        interval_seconds=interval_seconds,
        console=console,
    )
