"""Fail-open OTLP bootstrap and safe diagnostic attribute validation."""

from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, StrictBool, StrictStr

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

_CREDENTIAL_VALUE_MARKERS = (
    "://",
    "api-key",
    "api_key=",
    "apikey=",
    "authorization",
    "bearer ",
    "credential",
    "password=",
    "private_key",
    "secret",
    "token=",
)
_CREDENTIAL_VALUE_PREFIXES = ("ghp_", "github_pat_", "sk-", "sk_")
_SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+\-]{0,255}$")
_MAX_COUNTER = 2**63 - 1


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
    """Return the closed set of safe, typed span attributes."""
    validated: dict[str, str | int | float | bool] = {}
    for key, value in attributes.items():
        validator = _ATTRIBUTE_VALIDATORS.get(key)
        if validator is None:
            raise ValueError("telemetry attribute key is not approved")
        validator(value)
        validated[key] = value
    return validated


def _validate_safe_text(value: object) -> None:
    if type(value) is not str or _SAFE_TEXT.fullmatch(value) is None:
        raise ValueError("telemetry attribute value is not safe text")
    normalized = value.casefold()
    if normalized.startswith(_CREDENTIAL_VALUE_PREFIXES) or any(
        marker in normalized for marker in _CREDENTIAL_VALUE_MARKERS
    ):
        raise ValueError("telemetry attribute value resembles a credential")


def _validate_counter(value: object) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_COUNTER:
        raise ValueError("telemetry attribute value is not a valid counter")


def _validate_cost(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(  # noqa: TRY004 -- one validation failure contract
            "telemetry attribute value is not a valid cost"
        )
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError("telemetry attribute value is not a valid cost")


def _validate_throttle_delay(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(  # noqa: TRY004 -- one validation failure contract
            "telemetry attribute value is not a valid delay"
        )
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0:
        raise ValueError("telemetry attribute value is not a valid delay")


_ATTRIBUTE_VALIDATORS = {
    "platform.operation_key": _validate_safe_text,
    "platform.execution_key": _validate_safe_text,
    "platform.workflow_role": _validate_safe_text,
    "platform.attempt": _validate_counter,
    "platform.publication.destination_id": _validate_safe_text,
    "platform.publication.disposition": _validate_safe_text,
    "platform.publication.snapshot_seq": _validate_counter,
    "whetstone.provider": _validate_safe_text,
    "whetstone.model": _validate_safe_text,
    "whetstone.token_count": _validate_counter,
    "whetstone.provider_cost_usd": _validate_cost,
    "whetstone.throttle_delay_ms": _validate_throttle_delay,
}
