from __future__ import annotations

from dr_platform.runtime.telemetry import initialize_telemetry_safely


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
