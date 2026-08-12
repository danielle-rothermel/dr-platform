from __future__ import annotations

from enum import UNIQUE, StrEnum, verify
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping


@verify(UNIQUE)
class TerminalSummaryField(StrEnum):
    """Persisted terminal-summary wire keys; spell them out at write sites."""

    OUTCOME = "outcome"
    PRODUCER = "producer"
    ERROR_TYPE = "error_type"
    MESSAGE = "message"
    DBOS_STATUS = "dbos_status"
    REASON = "reason"
    TRACEBACK = "traceback"


@verify(UNIQUE)
class TerminalSummaryProducer(StrEnum):
    """Identifies which platform path wrote a terminal summary."""

    APPLICATION_FAILURE = "application_failure"
    ABANDONMENT = "abandonment"
    CANCELLATION = "cancellation"


def build_terminal_summary(  # noqa: PLR0913 -- explicit terminal facts
    *,
    outcome: str,
    producer: TerminalSummaryProducer,
    error_type: str | None = None,
    message: str | None = None,
    dbos_status: str | None = None,
    reason: str | None = None,
    traceback_text: str | None = None,
) -> dict[str, object]:
    summary: dict[str, object] = {
        TerminalSummaryField.OUTCOME: outcome,
        TerminalSummaryField.PRODUCER: producer.value,
    }
    if error_type is not None:
        summary[TerminalSummaryField.ERROR_TYPE] = error_type
    if message is not None:
        summary[TerminalSummaryField.MESSAGE] = message
    if dbos_status is not None:
        summary[TerminalSummaryField.DBOS_STATUS] = dbos_status
    if reason is not None:
        summary[TerminalSummaryField.REASON] = reason
    if traceback_text is not None:
        summary[TerminalSummaryField.TRACEBACK] = traceback_text
    return summary


def build_terminal_outcome_summary(*, outcome: str) -> dict[str, object]:
    """Outcome-only terminal summary without a producer tag."""
    return {TerminalSummaryField.OUTCOME: outcome}


def build_run_completion_error_summary(
    *,
    error_type: str,
    message: str,
    dbos_status: str | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    summary: dict[str, object] = {
        TerminalSummaryField.ERROR_TYPE: error_type,
        TerminalSummaryField.MESSAGE: message,
    }
    if dbos_status is not None:
        summary[TerminalSummaryField.DBOS_STATUS] = dbos_status
    if reason is not None:
        summary[TerminalSummaryField.REASON] = reason
    return summary


def build_run_completion_attempt_summary(
    *,
    outcome: str,
    error_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if error_summary is None:
        return build_terminal_outcome_summary(outcome=outcome)
    return {
        **dict(error_summary),
        TerminalSummaryField.OUTCOME: outcome,
    }


def validate_evidence_reference(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("evidence reference must be a non-empty string")
    return value
