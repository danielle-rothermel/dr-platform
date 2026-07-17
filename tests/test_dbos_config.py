from __future__ import annotations

import pytest

from dr_platform import dbos_config


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
    )
    assert (
        explicit.system_database_url
        == "postgresql+psycopg://explicit-user@app/db"
    )

    from_env = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@app/db",
    )
    assert (
        from_env.system_database_url
        == "postgresql+psycopg://system-user@app:5432/db"
    )

    monkeypatch.delenv("DBOS_SYSTEM_DATABASE_URL", raising=False)
    from_app = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@app/db",
    )
    assert (
        from_app.system_database_url
        == "postgresql+psycopg://app-user@app/db"
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
    ],
)
def test_build_platform_dbos_config_rejects_query_routing(
    routing_query: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "PostgreSQL database URLs must not use host, port, or dbname "
            "query parameters"
        ),
    ):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://app@db.example/platform",
            system_database_url=(
                f"postgresql://system@db.example/platform?{routing_query}"
            ),
        )


def test_build_platform_dbos_config_rejects_missing_database_name() -> None:
    with pytest.raises(
        ValueError,
        match="PostgreSQL database URLs must name an explicit database",
    ):
        dbos_config.build_platform_dbos_config(
            database_url="postgresql://platform_user@db.example",
            system_database_url="postgresql://dbos_user@db.example",
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
        )


def test_build_platform_dbos_config_accepts_colocated_urls() -> None:
    config = dbos_config.build_platform_dbos_config(
        database_url="postgresql://app-user@DB.EXAMPLE/platform",
        system_database_url=(
            "postgresql+psycopg://system-user@db.example:5432/platform"
        ),
    )

    assert config.database_url == (
        "postgresql+psycopg://app-user@DB.EXAMPLE/platform"
    )
    assert config.system_database_url == (
        "postgresql+psycopg://system-user@db.example:5432/platform"
    )


def test_resolve_database_url_leaves_non_postgresql_urls_unchanged() -> None:
    assert (
        dbos_config.resolve_database_url("sqlite:///tmp.db")
        == "sqlite:///tmp.db"
    )


def test_resolve_database_url_leaves_psycopg_driver_suffix_unchanged() -> None:
    url = "postgresql+psycopg://user:pass@localhost/db"
    assert dbos_config.resolve_database_url(url) == url
