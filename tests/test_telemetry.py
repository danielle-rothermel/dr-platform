"""Fail-open behavior for optional telemetry initialization."""

from __future__ import annotations

import pytest

from dr_platform.dbos_config import PlatformDbosConfig, initialize_dbos_runtime
from dr_platform.telemetry import (
    initialize_telemetry_safely,
    validated_telemetry_attributes,
)


def _config(*, enable_otlp: bool = True) -> PlatformDbosConfig:
    return PlatformDbosConfig(
        database_url="postgresql+psycopg://app",
        system_database_url="postgresql+psycopg://system",
        enable_otlp=enable_otlp,
    )


def test_disabled_telemetry_skips_initialization() -> None:
    called = False

    def initialize() -> None:
        nonlocal called
        called = True

    result = initialize_telemetry_safely(
        enabled=False,
        initializer=initialize,
    )

    assert result.healthy
    assert not result.enabled
    assert not called


def test_telemetry_failure_returns_a_safe_degraded_result() -> None:
    def initialize() -> None:
        raise RuntimeError("sensitive exporter failure")

    result = initialize_telemetry_safely(
        enabled=True,
        initializer=initialize,
    )

    assert not result.healthy
    assert result.error_type == "RuntimeError"
    assert result.message == "OTLP initialization failed"
    assert "sensitive" not in result.message


def test_dbos_runtime_remains_available_when_telemetry_fails() -> None:
    runtime_initialized = False

    def initialize_runtime(_config: object) -> None:
        nonlocal runtime_initialized
        runtime_initialized = True

    def initialize_telemetry(_config: object) -> None:
        raise RuntimeError("sensitive tracer failure")

    result = initialize_dbos_runtime(
        _config(),
        app_name="app",
        runtime_initializer=initialize_runtime,
        telemetry_initializer=initialize_telemetry,
    )

    assert runtime_initialized
    assert result.enabled
    assert not result.healthy
    assert result.message == "OTLP initialization failed"


def test_dbos_runtime_initialization_failure_is_not_treated_as_telemetry() -> (
    None
):
    def initialize_runtime(_config: object) -> None:
        raise ConnectionError("database unavailable")

    with pytest.raises(ConnectionError, match="database unavailable"):
        initialize_dbos_runtime(
            _config(),
            app_name="app",
            runtime_initializer=initialize_runtime,
            telemetry_initializer=lambda _config: None,
        )


def test_unapproved_telemetry_attributes_are_rejected() -> None:
    unapproved_attributes: dict[str, str | int] = {
        "consumer.destination_id": "destination-1",
        "consumer.disposition": "completed",
        "consumer.snapshot_seq": 1,
    }

    for key, value in unapproved_attributes.items():
        with pytest.raises(
            ValueError, match="telemetry attribute key is not approved"
        ):
            validated_telemetry_attributes({key: value})
