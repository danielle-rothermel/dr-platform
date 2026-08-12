from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryField,
    TerminalSummaryProducer,
    build_run_completion_attempt_summary,
    build_run_completion_error_summary,
)


def test_terminal_summary_field_literals_are_pinned() -> None:
    assert [member.value for member in TerminalSummaryField] == [
        "outcome",
        "producer",
        "error_type",
        "message",
        "dbos_status",
        "reason",
        "traceback",
    ]


def test_terminal_summary_producer_literals_are_pinned() -> None:
    assert [member.value for member in TerminalSummaryProducer] == [
        "application_failure",
        "abandonment",
        "cancellation",
    ]


def test_run_completion_error_summary_uses_pinned_keys() -> None:
    assert build_run_completion_error_summary(
        error_type="builtins.ValueError",
        message="broken",
    ) == {
        "error_type": "builtins.ValueError",
        "message": "broken",
    }
    assert build_run_completion_error_summary(
        error_type="dbos.abandonment",
        message="stale_app_version",
        dbos_status="PENDING",
        reason="stale_app_version",
    ) == {
        "error_type": "dbos.abandonment",
        "message": "stale_app_version",
        "dbos_status": "PENDING",
        "reason": "stale_app_version",
    }


def test_run_completion_attempt_summary_uses_pinned_keys() -> None:
    error_summary = build_run_completion_error_summary(
        error_type="builtins.ValueError",
        message="broken",
    )
    assert build_run_completion_attempt_summary(
        outcome="failed",
        error_summary=error_summary,
    ) == {
        "error_type": "builtins.ValueError",
        "message": "broken",
        "outcome": "failed",
    }
    assert build_run_completion_attempt_summary(outcome="succeeded") == {
        "outcome": "succeeded",
    }
