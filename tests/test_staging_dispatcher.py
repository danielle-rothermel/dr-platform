"""Thin scheduled-dispatcher registration behavior."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine

from dr_platform.dbos_config import PlatformDbosConfig
from dr_platform.staging import (
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageKey,
    dispatcher,
)
from dr_platform.staging.admission import AdmissionSummary, StageMismatch
from dr_platform.staging.dispatcher import UnwrappedPipelineError
from dr_platform.staging.handoff import wrap_pipeline_workflows


def _declared_pipeline() -> PipelineDefinition:
    def workflow(*args: object) -> str:
        return "ref"

    def args_for(*args: object) -> tuple[object, ...]:
        return args

    return PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="execute",
                workflow=workflow,
                args_for=args_for,
            ),
        ),
    )


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
            mismatched_stages=(),
        )

    monkeypatch.setattr(dispatcher, "run_admission_pass", fake_pass)
    config = PlatformDbosConfig(
        database_url=("postgresql+psycopg://user:secret@db/platform"),
        system_database_url=("postgresql+psycopg://user:secret@db/platform"),
    )
    engine = create_engine(config.database_url)

    registry = PipelineRegistry()
    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
        batch_size=17,
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
    # The wrapper must forward the registration's own engine, client, and
    # registry with the configured batch size -- not defaults or fresh objects.
    assert calls[-1] == (
        "pass",
        (
            engine,
            {"client": client, "registry": registry, "batch_size": 17},
        ),
    )
    assert client.destroyed


def test_mismatched_stages_are_logged_as_registry_drift_at_error(
    monkeypatch,
    caplog,
) -> None:
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    monkeypatch.setattr(
        dispatcher, "DBOSClient", lambda *, system_database_url: client
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "scheduled", lambda cron: lambda function: function
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "workflow", lambda *, name: lambda function: function
    )

    def fake_pass(engine: object, **kwargs: object) -> AdmissionSummary:
        return AdmissionSummary(
            admitted_counts=(),
            skipped_for_capacity=0,
            skipped_for_pause=0,
            unconfigured_stages=(),
            failed_stages=(),
            mismatched_stages=(
                StageMismatch(
                    pipeline_key="evaluation",
                    pipeline_version=1,
                    stage_key=StageKey("execute"),
                    message="persisted stage index is outside the pipeline",
                ),
            ),
        )

    monkeypatch.setattr(dispatcher, "run_admission_pass", fake_pass)
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=PipelineRegistry(),
    )

    with caplog.at_level("ERROR", logger=dispatcher.logger.name):
        registration.workflow(
            datetime(2026, 7, 17, tzinfo=UTC),
            datetime(2026, 7, 17, tzinfo=UTC),
        )
    registration.close()
    engine.dispose()

    drift = [
        record
        for record in caplog.records
        if record.levelname == "ERROR" and "registry/data drift" in record.msg
    ]
    assert len(drift) == 1
    assert "evaluation" in drift[0].getMessage()


def _patch_dbos_wiring(monkeypatch, client: _FakeClient) -> None:
    monkeypatch.setattr(
        dispatcher, "DBOSClient", lambda *, system_database_url: client
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "scheduled", lambda cron: lambda function: function
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "workflow", lambda *, name: lambda function: function
    )


def test_registration_rejects_a_registry_with_an_unwrapped_pipeline(
    monkeypatch,
) -> None:
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    _patch_dbos_wiring(monkeypatch, client)
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    registry = PipelineRegistry()
    registry.register(_declared_pipeline())

    with pytest.raises(UnwrappedPipelineError) as caught:
        dispatcher.register_scheduled_dispatcher(
            config=config,
            engine=engine,
            registry=registry,
        )
    engine.dispose()

    assert caught.value.pipeline_key == "evaluation"
    assert caught.value.pipeline_version == 1
    # The client must not be constructed when validation rejects the registry.
    assert not client.destroyed


def test_registration_accepts_a_registry_of_wrapped_pipelines(
    monkeypatch,
) -> None:
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    _patch_dbos_wiring(monkeypatch, client)
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    registry = PipelineRegistry()
    registry.register(wrap_pipeline_workflows(_declared_pipeline()))

    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
    )
    assert registration.sweep_workflow is None
    registration.close()
    engine.dispose()

    assert client.destroyed


def test_sweep_cron_registers_a_second_scheduled_workflow(
    monkeypatch,
) -> None:
    calls: list[tuple[str, object]] = []
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    monkeypatch.setattr(
        dispatcher, "DBOSClient", lambda *, system_database_url: client
    )

    def decorator(kind: str, value: str) -> Callable:
        def apply(function: Callable) -> Callable:
            calls.append((kind, value))
            return function

        return apply

    monkeypatch.setattr(
        dispatcher.DBOS, "scheduled", lambda cron: decorator("cron", cron)
    )
    monkeypatch.setattr(
        dispatcher.DBOS,
        "workflow",
        lambda *, name: decorator("workflow", name),
    )

    def fake_sweep(engine: object, **kwargs: object) -> None:
        calls.append(("sweep", (engine, kwargs)))

    monkeypatch.setattr(dispatcher, "sweep_abandoned_stages", fake_sweep)
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    registry = PipelineRegistry()
    registry.register(wrap_pipeline_workflows(_declared_pipeline()))

    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
        sweep_cron="0 * * * * *",
        sweep_batch_size=25,
    )
    assert registration.sweep_workflow is not None
    registration.sweep_workflow(
        datetime(2026, 7, 17, tzinfo=UTC),
        datetime(2026, 7, 17, tzinfo=UTC),
    )
    registration.close()
    engine.dispose()

    assert ("cron", "0 * * * * *") in calls
    assert ("workflow", dispatcher.SWEEP_WORKFLOW_NAME) in calls
    # The sweep workflow must forward the registration's own client and the
    # configured batch size.
    assert calls[-1] == (
        "sweep",
        (engine, {"client": client, "batch_size": 25}),
    )
    assert client.destroyed
