from __future__ import annotations

from io import StringIO

from rich.console import Console

from dr_platform import OperationProgress


def test_operation_progress_event_uses_fixed_width_timestamp() -> None:
    output = StringIO()
    console = Console(file=output, stderr=False, width=160, highlight=False)
    progress = OperationProgress(
        "rescore", interval_seconds=0, console=console
    )

    with progress:
        progress.event(
            "started",
            {
                "experiment": "encdec-budget-full-v0",
                "selected": 10,
            },
        )

    line = output.getvalue().replace("\n", " ")
    assert line.startswith("20")
    assert "rescore" in line
    assert "started" in line
    assert "experiment=encdec-budget-full-v0" in line
    assert "selected=10" in line


def test_operation_progress_update_snapshot_for_heartbeat() -> None:
    output = StringIO()
    console = Console(file=output, stderr=False, width=160, highlight=False)
    progress = OperationProgress(
        "backfill", interval_seconds=0, console=console
    )

    with progress:
        progress.update(phase="chunked", processed=100, inserted=95)
        progress._emit(label="…", style="dim", metrics=progress._snapshot())

    lines = [line for line in output.getvalue().splitlines() if line.strip()]
    assert any("processed=100" in line for line in lines)
    assert any("inserted=95" in line for line in lines)
