from __future__ import annotations

import pytest
from sqlalchemy import Engine, text

from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform.testing import migrated_engine, validate_test_database_url
from dr_platform.testing.engine import DEFAULT_TEST_DATABASE_URL
from tests.conftest import TEST_DATABASE_URL, _reset_test_database


def test_validate_test_database_url_rejects_non_postgres() -> None:
    with pytest.raises(ValueError, match="must use PostgreSQL"):
        validate_test_database_url("sqlite:///dr_platform_test")


def test_validate_test_database_url_rejects_identity_override() -> None:
    with pytest.raises(ValueError, match="identity"):
        validate_test_database_url(
            "postgresql+psycopg:///dr_platform_test?dbname=production"
        )


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql+psycopg:///dr_platform_test?host=prod.example",
        "postgresql+psycopg:///dr_platform_test?port=5433",
        "postgresql+psycopg:///dr_platform_test?hostaddr=127.0.0.1",
    ],
)
def test_validate_test_database_url_rejects_connection_overrides(
    database_url: str,
) -> None:
    with pytest.raises(ValueError, match="identity"):
        validate_test_database_url(database_url)


def test_validate_test_database_url_rejects_non_test_database_name() -> None:
    with pytest.raises(ValueError, match="_test"):
        validate_test_database_url("postgresql+psycopg:///dr_platform_prod")


def test_validate_test_database_url_accepts_default_env_dsn() -> None:
    validate_test_database_url(TEST_DATABASE_URL)
    validate_test_database_url(DEFAULT_TEST_DATABASE_URL)


def test_migrated_engine_yields_usable_schema(clean_pg: str) -> None:
    _reset_test_database(clean_pg)
    with migrated_engine(clean_pg) as engine, engine.connect() as connection:
        schema = LedgerSchema()
        tables = connection.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        ).scalars()
        assert schema.work_items.name in set(tables)


def _track_engine_disposal(
    monkeypatch: pytest.MonkeyPatch,
) -> list[Engine]:
    disposed: list[Engine] = []
    original_dispose = Engine.dispose

    def tracking_dispose(self: Engine, close: bool = True) -> None:  # noqa: FBT001, FBT002
        disposed.append(self)
        original_dispose(self, close=close)

    monkeypatch.setattr(Engine, "dispose", tracking_dispose)
    return disposed


def test_migrated_engine_disposes_on_normal_exit(
    clean_pg: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_test_database(clean_pg)
    disposed = _track_engine_disposal(monkeypatch)

    with migrated_engine(clean_pg) as engine:
        captured = engine

    assert disposed.count(captured) == 1


def test_migrated_engine_disposes_on_exception(
    clean_pg: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_test_database(clean_pg)
    disposed = _track_engine_disposal(monkeypatch)
    captured: list[Engine] = []

    def probe() -> None:
        with migrated_engine(clean_pg) as engine:
            captured.append(engine)
            raise RuntimeError("probe")

    with pytest.raises(RuntimeError, match="probe"):
        probe()

    assert len(captured) == 1
    assert disposed.count(captured[0]) == 1
