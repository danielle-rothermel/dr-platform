from __future__ import annotations

import os
from typing import TYPE_CHECKING, Literal, Self

from dbos import DBOS, DBOSConfig
from dbos._dbos_config import (
    process_config,
    translate_dbos_config_to_config_file,
)
from dbos._tracer import dbos_tracer
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)
from sqlalchemy.engine import make_url

from dr_platform._core.validation import validate_positive_integer
from dr_platform.runtime.telemetry import (
    TelemetryInitializationResult,
    initialize_telemetry_safely,
)

if TYPE_CHECKING:
    from collections.abc import Callable

DATABASE_URL_ENV = "DATABASE_URL"
DBOS_SYSTEM_DATABASE_URL_ENV = "DBOS_SYSTEM_DATABASE_URL"
POSTGRESQL_URL_PREFIX = "postgresql://"
POSTGRESQL_PSYCOPG_URL_PREFIX = "postgresql+psycopg://"


def normalize_postgresql_driver_url(database_url: str) -> str:
    if database_url.startswith(POSTGRESQL_URL_PREFIX):
        return database_url.replace(
            POSTGRESQL_URL_PREFIX,
            POSTGRESQL_PSYCOPG_URL_PREFIX,
            1,
        )
    return database_url


DEFAULT_POOL_SIZE = 10_000

# DBOS 2.27 colocated system schema; keep beside the package pin.
DBOS_WORKFLOW_STATUS_TABLE = "dbos.workflow_status"


class PlatformDbosConfig(BaseModel):
    """Queue registration and concurrency remain application-owned.

    ``pool_size`` sizes DBOS's application-database pool used by checkpoint
    transactions, not the caller's separate platform ``engine``.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
    )

    database_url: StrictStr
    system_database_url: StrictStr
    pool_size: StrictInt = DEFAULT_POOL_SIZE
    max_recovery_attempts: StrictInt
    application_version: StrictStr | None = None
    executor_id: StrictStr | None = None
    enable_otlp: StrictBool = False
    otlp_traces_endpoints: tuple[StrictStr, ...] = ()
    otel_attribute_format: Literal["semconv"] = "semconv"

    @field_validator("pool_size")
    @classmethod
    def _pool_size(cls, value: int) -> int:
        validate_positive_integer(value, label="pool size")
        return value

    @field_validator("max_recovery_attempts")
    @classmethod
    def _max_recovery_attempts(cls, value: int) -> int:
        validate_positive_integer(value, label="max recovery attempts")
        return value

    @model_validator(mode="after")
    def validate_database_colocation(self) -> Self:
        validate_database_colocation(
            database_url=self.database_url,
            system_database_url=self.system_database_url,
        )
        return self


def validate_database_colocation(
    *,
    database_url: str,
    system_database_url: str,
) -> None:
    if _database_identity(database_url) == _database_identity(
        system_database_url
    ):
        return

    redacted_database_url = _redact_database_url(database_url)
    redacted_system_database_url = _redact_database_url(system_database_url)
    raise ValueError(
        "Platform and DBOS system databases must be colocated; "
        f"platform database URL={redacted_database_url}, "
        f"DBOS system database URL={redacted_system_database_url}"
    )


def _database_identity(
    database_url: str,
) -> tuple[str | None, int | None, str | None]:
    url = make_url(normalize_postgresql_driver_url(database_url))
    is_postgres = url.get_backend_name() in {"postgres", "postgresql"}
    if is_postgres and {
        "host",
        "port",
        "dbname",
        # libpq hostaddr/service can override the URL's connection identity.
        "hostaddr",
        "service",
    }.intersection(url.query):
        raise ValueError(
            "PostgreSQL database URLs must not use host, port, dbname, "
            "hostaddr, or service query parameters"
        )
    if is_postgres and url.database is None:
        raise ValueError(
            "PostgreSQL database URLs must name an explicit database"
        )
    host = url.host.casefold() if url.host is not None else None
    port = url.port
    if is_postgres and port is None:
        port = 5432
    return host, port, url.database


def _redact_database_url(database_url: str) -> str:
    url = make_url(database_url)
    return url.difference_update_query(url.query).render_as_string(
        hide_password=True
    )


def resolve_database_url(
    database_url: str | None,
    *,
    database_url_env: str = DATABASE_URL_ENV,
    error_suffix: str = "",
) -> str:
    resolved = database_url or os.environ.get(database_url_env)
    if not resolved:
        suffix = f" {error_suffix}" if error_suffix else ""
        raise ValueError(
            f"--database-url or {database_url_env} is required{suffix}"
        )
    return normalize_postgresql_driver_url(resolved)


def build_platform_dbos_config(  # noqa: PLR0913 -- explicit bootstrap inputs
    *,
    database_url: str | None,
    system_database_url: str | None = None,
    database_url_env: str = DATABASE_URL_ENV,
    system_database_url_env: str = DBOS_SYSTEM_DATABASE_URL_ENV,
    database_url_error_suffix: str = "",
    max_recovery_attempts: int,
    application_version: str | None = None,
    executor_id: str | None = None,
    enable_otlp: bool = False,
    otlp_traces_endpoints: tuple[str, ...] = (),
    pool_size: int = DEFAULT_POOL_SIZE,
) -> PlatformDbosConfig:
    resolved_database_url = resolve_database_url(
        database_url,
        database_url_env=database_url_env,
        error_suffix=database_url_error_suffix,
    )
    resolved_system_database_url = (
        system_database_url
        or os.environ.get(system_database_url_env)
        or resolved_database_url
    )
    validate_positive_integer(
        max_recovery_attempts, label="max recovery attempts"
    )
    return PlatformDbosConfig(
        database_url=resolved_database_url,
        system_database_url=normalize_postgresql_driver_url(
            resolved_system_database_url
        ),
        max_recovery_attempts=max_recovery_attempts,
        application_version=application_version,
        executor_id=executor_id,
        enable_otlp=enable_otlp,
        otlp_traces_endpoints=otlp_traces_endpoints,
        pool_size=pool_size,
    )


def build_dbos_config(
    config: PlatformDbosConfig,
    *,
    app_name: str,
) -> DBOSConfig:
    result: DBOSConfig = {
        "name": app_name,
        "application_database_url": config.database_url,
        "system_database_url": config.system_database_url,
        "enable_otlp": config.enable_otlp,
        "otel_attribute_format": config.otel_attribute_format,
    }
    if config.otlp_traces_endpoints:
        result["otlp_traces_endpoints"] = list(config.otlp_traces_endpoints)
    if config.application_version is not None:
        result["application_version"] = config.application_version
    if config.executor_id is not None:
        result["executor_id"] = config.executor_id
    result["db_engine_kwargs"] = {
        "pool_size": config.pool_size,
        "max_overflow": 0,
    }
    return result


def initialize_dbos_runtime(
    config: PlatformDbosConfig,
    *,
    app_name: str,
    runtime_initializer: Callable[[DBOSConfig], None] | None = None,
    telemetry_initializer: Callable[[DBOSConfig], None] | None = None,
) -> TelemetryInitializationResult:
    """Only enabled OTLP initialization may fail open.

    The private DBOS config and tracer hooks are tied to the exact DBOS pin.
    DBOS startup remains fatal; launch is application-owned so workflows and
    queue listeners can be registered first.
    """
    initialize = runtime_initializer or _initialize_dbos_runtime
    initialize_telemetry = telemetry_initializer or _initialize_dbos_telemetry
    disabled = config.model_copy(update={"enable_otlp": False})
    disabled_dbos_config = build_dbos_config(disabled, app_name=app_name)

    initialize(disabled_dbos_config)
    if not config.enable_otlp:
        return TelemetryInitializationResult(enabled=False, healthy=True)

    enabled_dbos_config = build_dbos_config(config, app_name=app_name)
    result = initialize_telemetry_safely(
        enabled=True,
        initializer=lambda: initialize_telemetry(enabled_dbos_config),
    )
    if not result.healthy:
        # Preserve the degraded result if best-effort reset also fails.
        initialize_telemetry_safely(
            enabled=True,
            initializer=lambda: initialize_telemetry(disabled_dbos_config),
        )
    return result


def _initialize_dbos_runtime(config: DBOSConfig) -> None:
    DBOS(config=config)


def _initialize_dbos_telemetry(config: DBOSConfig) -> None:
    processed = process_config(
        data=translate_dbos_config_to_config_file(config), silent=True
    )
    dbos_tracer.config(processed)
