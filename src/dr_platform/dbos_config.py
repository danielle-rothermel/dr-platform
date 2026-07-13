"""DBOS config/bootstrap helpers and the single DBOS-private shim.

DBOS does not expose public exception classes for workflow start
races; the private import is deliberately isolated here so every
caller shares one compatibility point if DBOS renames them.
"""

from __future__ import annotations

import os
from enum import StrEnum
from typing import Literal

from dbos import DBOS, DBOSConfig
from dbos._error import (
    DBOSConflictingWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSWorkflowConflictIDError,
)
from pydantic import BaseModel, ConfigDict, StrictBool, StrictStr

DATABASE_URL_ENV = "DATABASE_URL"
DBOS_SYSTEM_DATABASE_URL_ENV = "DBOS_SYSTEM_DATABASE_URL"
POSTGRESQL_URL_PREFIX = "postgresql://"
POSTGRESQL_PSYCOPG_URL_PREFIX = "postgresql+psycopg://"

WORKFLOW_START_RACE_ERRORS: tuple[type[BaseException], ...] = (
    DBOSWorkflowConflictIDError,
    DBOSQueueDeduplicatedError,
    DBOSConflictingWorkflowError,
)


class DbosWorkflowStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    MAX_RECOVERY_ATTEMPTS_EXCEEDED = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    CANCELLED = "CANCELLED"
    ENQUEUED = "ENQUEUED"
    DELAYED = "DELAYED"


DBOS_ACTIVE_WORKFLOW_STATUSES = (
    DbosWorkflowStatus.ENQUEUED.value,
    DbosWorkflowStatus.PENDING.value,
    DbosWorkflowStatus.DELAYED.value,
)
DBOS_FAILED_WORKFLOW_STATUSES = (
    DbosWorkflowStatus.ERROR.value,
    DbosWorkflowStatus.CANCELLED.value,
    DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
)
MISSING_DBOS_WORKFLOW_STATUS = "MISSING"


def workflow_start_raced(*, workflow_id: str, error: BaseException) -> bool:
    """Return True when a concurrent start/enqueue won (idempotent caller)."""
    if isinstance(error, WORKFLOW_START_RACE_ERRORS):
        return True
    return isinstance(error, Exception) and (
        DBOS.get_workflow_status(workflow_id) is not None
    )


def normalize_postgresql_driver_url(database_url: str) -> str:
    if database_url.startswith(POSTGRESQL_URL_PREFIX):
        return database_url.replace(
            POSTGRESQL_URL_PREFIX,
            POSTGRESQL_PSYCOPG_URL_PREFIX,
            1,
        )
    return database_url


class PlatformDbosConfig(BaseModel):
    """Resolved URLs for the app DB and the DBOS system DB.

    Queue names and concurrency stay app-side; the library never
    registers queues on its own.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    database_url: StrictStr
    system_database_url: StrictStr
    enable_otlp: StrictBool = False
    otlp_traces_endpoints: tuple[StrictStr, ...] = ()
    otel_attribute_format: Literal["semconv"] = "semconv"


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
    enable_otlp: bool = False,
    otlp_traces_endpoints: tuple[str, ...] = (),
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
    return PlatformDbosConfig(
        database_url=resolved_database_url,
        system_database_url=normalize_postgresql_driver_url(
            resolved_system_database_url
        ),
        enable_otlp=enable_otlp,
        otlp_traces_endpoints=otlp_traces_endpoints,
    )


def build_dbos_config(
    config: PlatformDbosConfig,
    *,
    app_name: str,
) -> DBOSConfig:
    result: DBOSConfig = {
        "name": app_name,
        "system_database_url": config.system_database_url,
        "enable_otlp": config.enable_otlp,
        "otel_attribute_format": config.otel_attribute_format,
    }
    if config.otlp_traces_endpoints:
        result["otlp_traces_endpoints"] = list(
            config.otlp_traces_endpoints
        )
    return result


def destroy_dbos_runtime() -> None:
    DBOS.destroy()
