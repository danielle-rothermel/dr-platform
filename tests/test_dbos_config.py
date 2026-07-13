from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from dbos._error import (
    DBOSConflictingWorkflowError,
    DBOSQueueDeduplicatedError,
    DBOSWorkflowConflictIDError,
)

from dr_platform import dbos_config
from dr_platform.dbos_config import (
    WORKFLOW_START_RACE_ERRORS,
    workflow_start_raced,
)

WORKFLOW_ID = "generate:item-1"


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
    monkeypatch.setenv("DATABASE_URL", "postgresql://app/db")
    monkeypatch.setenv(
        "DBOS_SYSTEM_DATABASE_URL",
        "postgresql://system-env/db",
    )

    explicit = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app/db",
        system_database_url="postgresql://system-explicit/db",
    )
    assert (
        explicit.system_database_url
        == "postgresql+psycopg://system-explicit/db"
    )

    from_env = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app/db",
    )
    assert from_env.system_database_url == "postgresql+psycopg://system-env/db"

    monkeypatch.delenv("DBOS_SYSTEM_DATABASE_URL", raising=False)
    from_app = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app/db",
    )
    assert from_app.system_database_url == "postgresql+psycopg://app/db"


def test_resolve_database_url_leaves_non_postgresql_urls_unchanged() -> None:
    assert (
        dbos_config.resolve_database_url("sqlite:///tmp.db")
        == "sqlite:///tmp.db"
    )


def test_resolve_database_url_leaves_psycopg_driver_suffix_unchanged() -> None:
    url = "postgresql+psycopg://user:pass@localhost/db"
    assert dbos_config.resolve_database_url(url) == url


def test_build_dbos_config_shape() -> None:
    config = dbos_config.PlatformDbosConfig(
        database_url="postgresql+psycopg://app/db",
        system_database_url="postgresql+psycopg://system/db",
    )
    assert dbos_config.build_dbos_config(config, app_name="my-app") == {
        "name": "my-app",
        "system_database_url": "postgresql+psycopg://system/db",
        "enable_otlp": False,
        "otel_attribute_format": "semconv",
    }


def test_destroy_dbos_runtime_calls_dbos_destroy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[Any] = []

    monkeypatch.setattr(
        dbos_config.DBOS,
        "destroy",
        lambda: calls.append("destroy"),
    )

    dbos_config.destroy_dbos_runtime()

    assert calls == ["destroy"]


@pytest.mark.parametrize(
    "error",
    [
        DBOSWorkflowConflictIDError(WORKFLOW_ID),
        DBOSQueueDeduplicatedError(WORKFLOW_ID, "generation", "dedup-1"),
        DBOSConflictingWorkflowError(WORKFLOW_ID),
    ],
)
def test_workflow_start_raced_returns_true_for_typed_dbos_errors(
    error: BaseException,
) -> None:
    assert isinstance(error, WORKFLOW_START_RACE_ERRORS)
    patch_target = "dr_platform.dbos_config.DBOS.get_workflow_status"
    with patch(patch_target) as status:
        raced = workflow_start_raced(workflow_id=WORKFLOW_ID, error=error)
        assert raced is True
        status.assert_not_called()


def test_workflow_start_raced_returns_false_without_status() -> None:
    with patch(
        "dr_platform.dbos_config.DBOS.get_workflow_status",
        return_value=None,
    ):
        assert (
            workflow_start_raced(
                workflow_id=WORKFLOW_ID,
                error=ValueError("connection refused"),
            )
            is False
        )


def test_workflow_start_raced_returns_true_when_status_exists() -> None:
    with patch(
        "dr_platform.dbos_config.DBOS.get_workflow_status",
        return_value={"status": "ENQUEUED"},
    ):
        assert (
            workflow_start_raced(
                workflow_id=WORKFLOW_ID,
                error=ValueError("race lost"),
            )
            is True
        )
