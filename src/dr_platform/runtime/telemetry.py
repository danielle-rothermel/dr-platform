from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, StrictBool, StrictStr

if TYPE_CHECKING:
    from collections.abc import Callable


class TelemetryInitializationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: StrictBool
    healthy: StrictBool
    error_type: StrictStr | None = None
    message: StrictStr | None = None


def initialize_telemetry_safely(
    *,
    enabled: bool,
    initializer: Callable[[], None],
) -> TelemetryInitializationResult:
    """Initialize optional telemetry without making execution depend on it."""
    if not enabled:
        return TelemetryInitializationResult(enabled=False, healthy=True)
    try:
        initializer()
    except Exception as error:  # noqa: BLE001 -- telemetry is fail-open
        return TelemetryInitializationResult(
            enabled=True,
            healthy=False,
            error_type=type(error).__name__,
            message="OTLP initialization failed",
        )
    return TelemetryInitializationResult(enabled=True, healthy=True)
