"""Fail-open OTLP bootstrap and safe diagnostic attribute validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, StrictBool, StrictStr

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_FORBIDDEN_KEY_PARTS = (
    "credential",
    "database_url",
    "error_payload",
    "output",
    "password",
    "prompt",
    "raw_metadata",
    "secret",
)
_FORBIDDEN_VALUE_MARKERS = (
    "://",
    "api_key=",
    "apikey=",
    "password=",
    "token=",
)


class TelemetryInitializationResult(BaseModel):
    """Visible diagnostics for optional telemetry bootstrap."""

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


def validated_telemetry_attributes(
    attributes: Mapping[str, str | int | float | bool],
) -> dict[str, str | int | float | bool]:
    """Return safe span attributes or reject payload/secret-shaped facts."""
    validated: dict[str, str | int | float | bool] = {}
    for key, value in attributes.items():
        normalized_key = key.casefold()
        if not key.startswith(("platform.", "whetstone.")):
            raise ValueError("telemetry attribute namespace is not allowed")
        if any(part in normalized_key for part in _FORBIDDEN_KEY_PARTS):
            raise ValueError("telemetry attribute key is forbidden")
        if isinstance(value, str) and any(
            marker in value.casefold() for marker in _FORBIDDEN_VALUE_MARKERS
        ):
            raise ValueError("telemetry attribute value is forbidden")
        validated[key] = value
    return validated
