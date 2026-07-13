"""P6b optional telemetry and safe-attribute contracts."""

from __future__ import annotations

import pytest

from dr_platform.dbos_config import (
    PlatformDbosConfig,
    build_dbos_config,
)
from dr_platform.telemetry import (
    initialize_telemetry_safely,
    validated_telemetry_attributes,
)


def test_otlp_disabled_is_normal_and_does_not_initialize() -> None:
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


def test_otlp_failure_is_visible_and_nonfatal() -> None:
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


def test_dbos_config_uses_semconv_and_optional_trace_endpoints() -> None:
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://app",
        system_database_url="postgresql+psycopg://system",
        enable_otlp=True,
        otlp_traces_endpoints=("https://collector.example/v1/traces",),
    )

    assert build_dbos_config(config, app_name="app") == {
        "name": "app",
        "system_database_url": "postgresql+psycopg://system",
        "enable_otlp": True,
        "otel_attribute_format": "semconv",
        "otlp_traces_endpoints": ["https://collector.example/v1/traces"],
    }


def test_telemetry_attributes_allow_safe_facts_and_reject_payloads() -> None:
    safe = {
        "platform.operation_key": "operation-1",
        "platform.attempt": 2,
        "whetstone.provider_cost_usd": 0.125,
        "whetstone.token_count": 100,
        "whetstone.throttle_delay_ms": 50,
    }
    assert validated_telemetry_attributes(safe) == safe

    for attributes in (
        {"platform.prompt": "hello"},
        {"whetstone.output": "answer"},
        {"platform.database_url": "redacted"},
        {"platform.api_key": "redacted"},
        {"platform.authorization": "redacted"},
        {"platform.operation_key": "Bearer secret"},
        {"platform.operation_key": "postgresql://secret"},
        {"other.operation_key": "operation-1"},
    ):
        with pytest.raises(ValueError, match="telemetry attribute"):
            validated_telemetry_attributes(attributes)
