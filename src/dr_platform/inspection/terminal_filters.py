from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryField,
    TerminalSummaryProducer,
)

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement, Select

    from dr_platform._core.ledger.schema import LedgerSchema
    from dr_platform._core.ledger.states import StageExecutionState


@dataclass(frozen=True, slots=True)
class TerminalSummaryFilter:
    """Exact-match predicates on pinned terminal-summary wire keys."""

    producer: TerminalSummaryProducer | None = None
    state: StageExecutionState | None = None
    error_type: str | None = None
    has_evidence_reference: bool | None = None

    def __post_init__(self) -> None:
        if (
            self.producer is None
            and self.state is None
            and self.error_type is None
            and self.has_evidence_reference is None
        ):
            raise ValueError(
                "terminal summary filter must specify at least one predicate"
            )
        if self.error_type is not None and not self.error_type.strip():
            raise ValueError("error_type filter must be a non-empty string")


def terminal_summary_filter_clause(
    terminal_filter: TerminalSummaryFilter,
    schema: LedgerSchema,
) -> ColumnElement[bool]:
    attempts = schema.stage_attempts
    executions = schema.stage_executions
    clauses: list[ColumnElement[bool]] = []
    if terminal_filter.state is not None:
        clauses.append(executions.c.state == terminal_filter.state.value)
    if terminal_filter.producer is not None:
        clauses.append(
            attempts.c.terminal_summary[
                TerminalSummaryField.PRODUCER.value
            ].as_string()
            == terminal_filter.producer.value
        )
    if terminal_filter.error_type is not None:
        clauses.append(
            attempts.c.terminal_summary[
                TerminalSummaryField.ERROR_TYPE.value
            ].as_string()
            == terminal_filter.error_type
        )
    if terminal_filter.has_evidence_reference is True:
        clauses.append(attempts.c.evidence_reference.is_not(None))
    elif terminal_filter.has_evidence_reference is False:
        clauses.append(attempts.c.evidence_reference.is_(None))
    if not clauses:
        raise ValueError(
            "terminal summary filter must specify at least one predicate"
        )
    combined = clauses[0]
    for clause in clauses[1:]:
        combined = combined & clause
    return combined


def apply_terminal_summary_filter(
    statement: Select,
    *,
    terminal_filter: TerminalSummaryFilter,
    schema: LedgerSchema,
) -> Select:
    return statement.where(
        terminal_summary_filter_clause(terminal_filter, schema=schema)
    )
