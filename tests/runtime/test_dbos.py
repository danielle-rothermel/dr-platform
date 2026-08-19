from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from dbos import DBOSConfig

from dr_platform import initialize_dbos_runtime
from dr_platform.runtime import dbos as dbos_config

if TYPE_CHECKING:
    from dr_platform.runtime.dbos import PlatformDbosConfig


def _config(
    *,
    enable_otlp: bool = True,
    otlp_traces_endpoints: tuple[str, ...] = (),
) -> PlatformDbosConfig:
    return dbos_config.build_platform_dbos_config(
        database_url="postgresql+psycopg://app/platform",
        max_recovery_attempts=1,
        enable_otlp=enable_otlp,
        otlp_traces_endpoints=otlp_traces_endpoints,
    )


def test_resolve_database_url_prefers_explicit_arg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://env/db")

    assert (
        dbos_config.resolve_database_url("postgresql://explicit/db")
        == "postgresql+psycopg://explicit/db"
    )


def test_resolve_database_url_reads_env_when_arg_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://env/db")

    assert (
        dbos_config.resolve_database_url(None) == "postgresql+psycopg://env/db"
    )


def test_resolve_database_url_raises_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(
        ValueError,
        match="--database-url or DATABASE_URL is required",
    ):
        dbos_config.resolve_database_url(None)

    with pytest.raises(
        ValueError,
        match="--database-url or DATABASE_URL is required for the worker",
    ):
        dbos_config.resolve_database_url(
            None,
            error_suffix="for the worker",
        )


def test_build_platform_dbos_config_system_url_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://app-user@app/db")
    monkeypatch.setenv(
        "DBOS_SYSTEM_DATABASE_URL",
        "postgresql://system-user@app:5432/db",
    )

    explicit = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@app/db",
        system_database_url="postgresql://explicit-user@app/db",
        max_recovery_attempts=1,
    )
    assert (
        explicit.system_database_url
        == "postgresql+psycopg://explicit-user@app/db"
    )

    from_env = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@app/db",
        max_recovery_attempts=1,
    )
    assert (
        from_env.system_database_url
        == "postgresql+psycopg://system-user@app:5432/db"
    )

    monkeypatch.delenv("DBOS_SYSTEM_DATABASE_URL", raising=False)
    from_app = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@app/db",
        max_recovery_attempts=1,
    )
    assert (
        from_app.system_database_url == "postgresql+psycopg://app-user@app/db"
    )


def test_build_platform_dbos_config_rejects_explicit_split_and_redacts_urls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DBOS_SYSTEM_DATABASE_URL", raising=False)

    with pytest.raises(
        ValueError,
        match="Platform and DBOS system databases must be colocated",
    ) as exc_info:
        dbos_config.build_platform_dbos_config(
            database_url=(
                "postgresql://app-user:app-secret@app.example/platform"
            ),
            system_database_url=(
                "postgresql://system-user:system-secret@system.example/dbos"
            ),
            max_recovery_attempts=1,
        )

    message = str(exc_info.value)
    assert "Platform and DBOS system databases must be colocated" in message
    assert "postgresql+psycopg://app-user:***@app.example/platform" in message
    assert (
        "postgresql+psycopg://system-user:***@system.example/dbos" in message
    )
    assert "app-secret" not in message
    assert "system-secret" not in message


def test_build_platform_dbos_config_redacts_query_values() -> None:
    with pytest.raises(
        ValueError,
        match="Platform and DBOS system databases must be colocated",
    ) as exc_info:
        dbos_config.build_platform_dbos_config(
            database_url=(
                "postgresql://app@app.example/platform"
                "?password=app-query-secret&sslmode=require"
            ),
            system_database_url=(
                "postgresql://system@system.example/dbos"
                "?sslpassword=system-query-secret&application_name=worker"
            ),
            max_recovery_attempts=1,
        )

    message = str(exc_info.value)
    assert "app-query-secret" not in message
    assert "system-query-secret" not in message
    assert "sslmode=require" not in message
    assert "application_name=worker" not in message


@pytest.mark.parametrize(
    "routing_query",
    [
        "host=other.example",
        "port=6543",
        "host=primary.example&host=standby.example",
        "dbname=other",
        "hostaddr=10.0.0.5",
        "service=other-service",
    ],
)
def test_build_platform_dbos_config_rejects_query_routing(
    routing_query: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "PostgreSQL database URLs must not use host, port, dbname, "
            "hostaddr, or service query parameters"
        ),
    ):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://app@db.example/platform",
            system_database_url=(
                f"postgresql://system@db.example/platform?{routing_query}"
            ),
            max_recovery_attempts=1,
        )


def test_build_platform_dbos_config_rejects_missing_database_name() -> None:
    with pytest.raises(
        ValueError,
        match="PostgreSQL database URLs must name an explicit database",
    ):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://platform_user@db.example",
            system_database_url="postgresql://dbos_user@db.example",
            max_recovery_attempts=1,
        )


def test_build_platform_dbos_config_rejects_split_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DBOS_SYSTEM_DATABASE_URL",
        "postgresql://db.example:5433/platform",
    )

    with pytest.raises(
        ValueError,
        match="Platform and DBOS system databases must be colocated",
    ):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://db.example:5432/platform",
            max_recovery_attempts=1,
        )


def test_build_platform_dbos_config_accepts_colocated_urls() -> None:
    config = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@DB.EXAMPLE/platform",
        system_database_url=(
            "postgresql+psycopg://system-user@db.example:5432/platform"
        ),
        max_recovery_attempts=1,
    )

    assert config.database_url == (
        "postgresql+psycopg://app-user@DB.EXAMPLE/platform"
    )
    assert config.system_database_url == (
        "postgresql+psycopg://system-user@db.example:5432/platform"
    )


def test_initialize_dbos_runtime_forwards_bootstrap_config() -> None:
    config = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@db.example/platform",
        system_database_url=(
            "postgresql://system-user@db.example:5432/platform"
        ),
        enable_otlp=True,
        otlp_traces_endpoints=(
            "https://collector-a.example/v1/traces",
            "https://collector-b.example/v1/traces",
        ),
        max_recovery_attempts=1,
    )
    runtime_configs: list[DBOSConfig] = []
    telemetry_configs: list[DBOSConfig] = []

    result = initialize_dbos_runtime(
        config,
        app_name="stage-worker",
        runtime_initializer=runtime_configs.append,
        telemetry_initializer=telemetry_configs.append,
    )

    assert len(runtime_configs) == 1
    runtime_config = runtime_configs[0]
    assert runtime_config["name"] == "stage-worker"
    assert runtime_config["application_database_url"] == (
        "postgresql+psycopg://app-user@db.example/platform"
    )
    assert runtime_config["system_database_url"] == (
        "postgresql+psycopg://system-user@db.example:5432/platform"
    )
    assert runtime_config["enable_otlp"] is False
    assert runtime_config["db_engine_kwargs"] == {
        "pool_size": dbos_config.DEFAULT_POOL_SIZE,
        "max_overflow": 0,
    }
    assert runtime_config["otlp_traces_endpoints"] == [
        "https://collector-a.example/v1/traces",
        "https://collector-b.example/v1/traces",
    ]

    assert len(telemetry_configs) == 1
    telemetry_config = telemetry_configs[0]
    assert telemetry_config["name"] == "stage-worker"
    assert telemetry_config["application_database_url"] == (
        "postgresql+psycopg://app-user@db.example/platform"
    )
    assert telemetry_config["system_database_url"] == (
        "postgresql+psycopg://system-user@db.example:5432/platform"
    )
    assert telemetry_config["enable_otlp"] is True
    assert telemetry_config["otlp_traces_endpoints"] == [
        "https://collector-a.example/v1/traces",
        "https://collector-b.example/v1/traces",
    ]
    assert result.enabled
    assert result.healthy


def test_initialize_dbos_runtime_skips_disabled_telemetry() -> None:
    config = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@db.example/platform",
        enable_otlp=False,
        max_recovery_attempts=1,
    )
    runtime_configs: list[DBOSConfig] = []
    telemetry_configs: list[DBOSConfig] = []

    result = dbos_config.initialize_dbos_runtime(
        config,
        app_name="stage-worker",
        runtime_initializer=runtime_configs.append,
        telemetry_initializer=telemetry_configs.append,
    )

    assert len(runtime_configs) == 1
    assert runtime_configs[0]["enable_otlp"] is False
    assert telemetry_configs == []
    assert not result.enabled
    assert result.healthy


def test_dbos_runtime_resets_telemetry_best_effort_after_failure() -> None:
    runtime_configs: list[DBOSConfig] = []
    telemetry_configs: list[DBOSConfig] = []

    def initialize_telemetry(config: DBOSConfig) -> None:
        telemetry_configs.append(config)
        raise RuntimeError("sensitive tracer failure")

    result = dbos_config.initialize_dbos_runtime(
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
        dbos_config.initialize_dbos_runtime(
            _config(),
            app_name="app",
            runtime_initializer=initialize_runtime,
            telemetry_initializer=lambda _config: None,
        )


def test_build_platform_dbos_config_rejects_non_positive_pool_size() -> None:
    with pytest.raises(ValueError, match="pool size must be positive"):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://app-user@db.example/platform",
            max_recovery_attempts=1,
            pool_size=0,
        )


def test_build_dbos_config_passes_pool_size_through_db_engine_kwargs() -> None:
    config = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@db.example/platform",
        max_recovery_attempts=1,
        pool_size=512,
    )
    dbos = dbos_config.build_dbos_config(config, app_name="stage-worker")
    assert dbos["db_engine_kwargs"] == {"pool_size": 512, "max_overflow": 0}


def test_build_dbos_config_passes_identity_fields_when_set() -> None:
    config = dbos_config.PlatformDbosConfig(
        database_url="postgresql://app-user@db.example/platform",
        system_database_url="postgresql://app-user@db.example/platform",
        max_recovery_attempts=1,
        application_version="pinned-version",
        executor_id="worker-17",
    )
    built = dbos_config.build_dbos_config(config, app_name="stage-worker")
    assert built["application_version"] == "pinned-version"
    assert built["executor_id"] == "worker-17"


def test_resolve_database_url_leaves_non_postgresql_urls_unchanged() -> None:
    assert (
        dbos_config.resolve_database_url("sqlite:///tmp.db")
        == "sqlite:///tmp.db"
    )


def test_resolve_database_url_leaves_psycopg_driver_suffix_unchanged() -> None:
    url = "postgresql+psycopg://user:pass@localhost/db"
    assert dbos_config.resolve_database_url(url) == url
