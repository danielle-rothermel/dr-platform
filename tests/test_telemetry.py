"""Fail-open behavior for optional telemetry initialization."""

from __future__ import annotations

import pytest
from dbos import DBOSConfig

from dr_platform.dbos_config import (
    PlatformDbosConfig,
    build_platform_dbos_config,
    initialize_dbos_runtime,
)
from dr_platform.telemetry import initialize_telemetry_safely


def _config(
    *,
    enable_otlp: bool = True,
    otlp_traces_endpoints: tuple[str, ...] = (),
) -> PlatformDbosConfig:
    return build_platform_dbos_config(
        database_url="postgresql+psycopg://app/platform",
        enable_otlp=enable_otlp,
        otlp_traces_endpoints=otlp_traces_endpoints,
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


def test_dbos_runtime_resets_telemetry_best_effort_after_failure() -> None:
    runtime_configs: list[DBOSConfig] = []
    telemetry_configs: list[DBOSConfig] = []

    def initialize_telemetry(config: DBOSConfig) -> None:
        telemetry_configs.append(config)
        raise RuntimeError("sensitive tracer failure")

    result = initialize_dbos_runtime(
        _config(
            otlp_traces_endpoints=("https://collector.example/v1/traces",)
        ),
        app_name="app",
        runtime_initializer=runtime_configs.append,
        telemetry_initializer=initialize_telemetry,
    )

    assert len(runtime_configs) == 1
    assert runtime_configs[0]["enable_otlp"] is False
    assert [config["enable_otlp"] for config in telemetry_configs] == [
        True,
        False,
    ]
    assert all(
        config["otlp_traces_endpoints"]
        == ["https://collector.example/v1/traces"]
        for config in telemetry_configs
    )
    assert result.enabled
    assert not result.healthy
    assert result.error_type == "RuntimeError"
    assert result.message == "OTLP initialization failed"


def test_dbos_runtime_initialization_failure_is_not_treated_as_telemetry() -> (
    None
):
    def initialize_runtime(_config: DBOSConfig) -> None:
        raise ConnectionError("database unavailable")

    with pytest.raises(ConnectionError, match="database unavailable"):
        initialize_dbos_runtime(
            _config(),
            app_name="app",
            runtime_initializer=initialize_runtime,
            telemetry_initializer=lambda _config: None,
        )
