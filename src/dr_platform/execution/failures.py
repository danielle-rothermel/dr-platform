from __future__ import annotations

from dr_serialize import Jsonable, validate_strict_json


class StageApplicationFailure(Exception):  # noqa: N818 -- public API name
    """Application failure with optional partial evidence payload."""

    def __init__(
        self,
        message: str,
        *,
        evidence: Jsonable | None = None,
    ) -> None:
        super().__init__(message)
        self.evidence = (
            None if evidence is None else validate_strict_json(evidence)
        )
