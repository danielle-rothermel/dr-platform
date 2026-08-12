from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryField,
    TerminalSummaryProducer,
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
