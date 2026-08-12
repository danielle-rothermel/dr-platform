from __future__ import annotations

from dr_platform._core.ledger.terminal_summary import (
    validate_evidence_reference,
)


class StageApplicationFailure(Exception):  # noqa: N818 -- public API name
    """Application failure with optional partial evidence reference."""

    def __init__(
        self,
        message: str,
        *,
        evidence_reference: str | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence_reference = validate_evidence_reference(
            evidence_reference
        )
