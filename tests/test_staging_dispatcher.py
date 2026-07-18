"""Thin scheduled-dispatcher registration behavior."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy import create_engine

from dr_platform.dbos_config import PlatformDbosConfig
from dr_platform.staging import PipelineRegistry, dispatcher
from dr_platform.staging.admission import AdmissionSummary


class _FakeClient:
    def __init__(self, *, system_database_url: str) -> None:
        self.system_database_url = system_database_url
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


def test_registration_owns_colocated_client_and_wrapper_is_thin(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )

    def client_factory(*, system_database_url: str) -> _FakeClient:
        calls.append(("client", system_database_url))
        return client

    def decorator(kind: str, value: str) -> Callable:
        def apply(function: Callable) -> Callable:
            calls.append((kind, value))
            return function

        return apply

    monkeypatch.setattr(dispatcher, "DBOSClient", client_factory)
    monkeypatch.setattr(
        dispatcher.DBOS,
        "scheduled",
        lambda cron: decorator("cron", cron),
    )
    monkeypatch.setattr(
        dispatcher.DBOS,
        "workflow",
        lambda *, name: decorator("workflow", name),
    )
    def fake_pass(engine: object, **kwargs: object) -> AdmissionSummary:
        calls.append(("pass", (engine, kwargs)))
        return AdmissionSummary(
            admitted_counts=(),
            skipped_for_capacity=0,
            skipped_for_pause=0,
            unconfigured_stages=(),
            failed_stages=(),
        )

    monkeypatch.setattr(dispatcher, "run_admission_pass", fake_pass)
    config = PlatformDbosConfig(
        database_url=(
            "postgresql+psycopg://user:secret@db/platform"
        ),
        system_database_url=(
            "postgresql+psycopg://user:secret@db/platform"
        ),
    )
    engine = create_engine(config.database_url)

    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=PipelineRegistry(),
    )
    registration.workflow(
        datetime(2026, 7, 17, tzinfo=UTC),
        datetime(2026, 7, 17, tzinfo=UTC),
    )
    registration.close()
    engine.dispose()

    assert calls[0] == ("client", config.system_database_url)
    assert ("cron", dispatcher.DEFAULT_DISPATCHER_CRON) in calls
    assert ("workflow", dispatcher.DISPATCHER_WORKFLOW_NAME) in calls
    assert calls[-1][0] == "pass"
    assert client.destroyed
