"""Optional telemetry behavior and safe-attribute validation."""

from __future__ import annotations

import pytest
from dbos import DBOSConfig

from dr_platform.dbos_config import (
    PlatformDbosConfig,
    build_dbos_config,
    initialize_dbos_runtime,
)
from dr_platform.telemetry import (
    TelemetryInitializationResult,
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
        "platform.execution_key": "execute:sha256",
        "platform.workflow_role": "execute",
        "platform.attempt": 2,
        "platform.publication.destination_id": "postgres-reporting",
        "platform.publication.disposition": "PROMOTED",
        "platform.publication.snapshot_seq": 7,
    }
    assert validated_telemetry_attributes(safe) == safe

    for attributes in (
        {"platform.database_url": "redacted"},
        {"platform.api_key": "redacted"},
        {"platform.authorization": "redacted"},
        {"platform.request_body": "raw application input"},
        {"platform.operation_key": "Bearer secret"},
        {"platform.operation_key": "sk-proj-supersecret"},
        {"platform.operation_key": "postgresql://secret"},
        {"other.operation_key": "operation-1"},
        {"platform.attempt": True},
        {"platform.attempt": -1},
    ):
        with pytest.raises(ValueError, match="telemetry attribute"):
            validated_telemetry_attributes(attributes)


def test_dbos_bootstrap_falls_back_after_otlp_failure() -> None:
    runtime_configs: list[DBOSConfig] = []
    telemetry_configs: list[DBOSConfig] = []

    def initialize_runtime(config: DBOSConfig) -> None:
        runtime_configs.append(config)

    def initialize_telemetry(config: DBOSConfig) -> None:
        telemetry_configs.append(config)
        if config.get("enable_otlp"):
            raise RuntimeError("sensitive exporter failure")

    result = initialize_dbos_runtime(
        PlatformDbosConfig(
            database_url="postgresql+psycopg://app",
            system_database_url="postgresql+psycopg://system",
            enable_otlp=True,
            otlp_traces_endpoints=("https://collector.example/v1/traces",),
        ),
        app_name="app",
        runtime_initializer=initialize_runtime,
        telemetry_initializer=initialize_telemetry,
    )

    assert [config.get("enable_otlp") for config in runtime_configs] == [False]
    assert [config.get("enable_otlp") for config in telemetry_configs] == [
        True,
        False,
    ]
    assert result == TelemetryInitializationResult(
        enabled=True,
        healthy=False,
        error_type="RuntimeError",
        message="OTLP initialization failed",
    )


def test_dbos_bootstrap_stays_fail_open_when_disabled_reset_fails() -> None:
    telemetry_configs: list[DBOSConfig] = []

    def initialize_telemetry(config: DBOSConfig) -> None:
        telemetry_configs.append(config)
        raise RuntimeError("sensitive tracer implementation failure")

    result = initialize_dbos_runtime(
        PlatformDbosConfig(
            database_url="postgresql+psycopg://app",
            system_database_url="postgresql+psycopg://system",
            enable_otlp=True,
        ),
        app_name="app",
        runtime_initializer=lambda _config: None,
        telemetry_initializer=initialize_telemetry,
    )

    assert [config.get("enable_otlp") for config in telemetry_configs] == [
        True,
        False,
    ]
    assert result == TelemetryInitializationResult(
        enabled=True,
        healthy=False,
        error_type="RuntimeError",
        message="OTLP initialization failed",
    )


def test_dbos_bootstrap_propagates_nontelemetry_initialization_failure() -> (
    None
):
    calls = 0

    def initialize(_config) -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("database unavailable")

    with pytest.raises(ConnectionError, match="database unavailable"):
        initialize_dbos_runtime(
            PlatformDbosConfig(
                database_url="postgresql+psycopg://app",
                system_database_url="postgresql+psycopg://system",
                enable_otlp=True,
            ),
            app_name="app",
            runtime_initializer=initialize,
            telemetry_initializer=lambda _config: None,
        )

    assert calls == 1
