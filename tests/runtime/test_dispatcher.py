from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from dbos import DBOS
from dbos._dbos import _get_or_create_dbos_registry
from sqlalchemy import create_engine

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)
from dr_platform.admission.runner import AdmissionSummary, StageMismatch
from dr_platform.completion.barrier import RunBarrierSummary
from dr_platform.execution._checkpoint import (
    _bind_ledger_checkpoint_executor,
    _LedgerCheckpointExecutor,
)
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.runtime import dispatcher
from dr_platform.runtime.dbos import PlatformDbosConfig
from dr_platform.runtime.dispatcher import UnwrappedPipelineError


def _declared_pipeline(key: str = "evaluation") -> PipelineDefinition:
    async def workflow(*args: object) -> str:
        return "ref"

    def args_for(*args: object) -> tuple[object, ...]:
        return args

    return PipelineDefinition(
        key=PipelineKey(key),
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
        self.destroy_calls = 0

    def destroy(self) -> None:
        self.destroy_calls += 1
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

    def fake_barrier(engine: object, **kwargs: object) -> RunBarrierSummary:
        calls.append(("barrier", (engine, kwargs)))
        return RunBarrierSummary(
            releases=(),
            failures=(),
            cursor_acquired=True,
            candidates_examined=0,
        )

    monkeypatch.setattr(dispatcher, "run_barrier_pass", fake_barrier)
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
        cron="*/2 * * * * *",
        batch_size=17,
        barrier_cron="*/7 * * * * *",
        barrier_batch_size=23,
        barrier_candidate_budget=29,
    )
    registration.workflow(
        datetime(2026, 7, 17, tzinfo=UTC),
        datetime(2026, 7, 17, tzinfo=UTC),
    )
    registration.barrier_workflow(
        datetime(2026, 7, 17, tzinfo=UTC),
        datetime(2026, 7, 17, tzinfo=UTC),
    )
    registration.close()
    engine.dispose()

    assert calls[0] == ("client", config.system_database_url)
    assert ("cron", "*/2 * * * * *") in calls
    assert ("cron", "*/7 * * * * *") in calls
    assert ("workflow", dispatcher.DISPATCHER_WORKFLOW_NAME) in calls
    assert ("workflow", dispatcher.RUN_BARRIER_WORKFLOW_NAME) in calls
    assert (
        "pass",
        (
            engine,
            {"client": client, "registry": registry, "batch_size": 17},
        ),
    ) in calls
    assert calls[-1] == (
        "barrier",
        (
            engine,
            {
                "client": client,
                "registry": registry,
                "batch_size": 23,
                "candidate_budget": 29,
            },
        ),
    )
    assert client.destroyed


@pytest.mark.parametrize("candidate_budget", [0, 4])
def test_registration_validates_run_barrier_candidate_budget(
    candidate_budget: int,
) -> None:
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    try:
        with pytest.raises(ValueError, match="run barrier candidate budget"):
            dispatcher.register_scheduled_dispatcher(
                config=config,
                engine=engine,
                registry=PipelineRegistry(),
                barrier_batch_size=5,
                barrier_candidate_budget=candidate_budget,
            )
    finally:
        engine.dispose()


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
    constructions: list[str] = []

    def _recording_factory(*, system_database_url: str) -> _FakeClient:
        constructions.append(system_database_url)
        return client

    monkeypatch.setattr(dispatcher, "DBOSClient", _recording_factory)
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
    assert constructions == []


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


def test_registration_binds_one_sized_executor_to_every_wrapper(
    monkeypatch,
) -> None:
    workers: list[int] = []
    executor_type = dispatcher._LedgerCheckpointExecutor

    def executor_factory(*, max_workers: int):
        workers.append(max_workers)
        return executor_type(max_workers=max_workers)

    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    _patch_dbos_wiring(monkeypatch, client)
    monkeypatch.setattr(
        dispatcher,
        "_LedgerCheckpointExecutor",
        executor_factory,
    )

    async def workflow(*args: object) -> str:
        return "ref"

    def args_for(*args: object) -> tuple[object, ...]:
        return args

    declared = PipelineDefinition(
        key=PipelineKey("shared-executor"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("first"),
                queue_name="first",
                workflow=workflow,
                args_for=args_for,
            ),
            StageDefinition(
                key=StageKey("second"),
                queue_name="second",
                workflow=workflow,
                args_for=args_for,
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("complete"),
            queue_name="complete",
            workflow=workflow,
            args_for=args_for,
        ),
    )
    pipeline = wrap_pipeline_workflows(declared)
    registry = PipelineRegistry()
    registry.register(pipeline)
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)

    registration = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
        batch_size=17,
        barrier_batch_size=23,
    )
    assert registration._resources is not None
    bound = registration._resources.checkpoint_executor
    workflows = [stage.workflow for stage in pipeline.stages]
    assert pipeline.run_completion is not None
    workflows.append(pipeline.run_completion.workflow)

    assert workers == [23]
    assert all(
        vars(workflow)["_dr_platform_ledger_checkpoint_executor"] is bound
        for workflow in workflows
    )

    registration.close()
    registration.close()
    engine.dispose()

    assert bound.closed
    assert client.destroy_calls == 1


@pytest.mark.parametrize("second_registry_kind", ["overlapping", "disjoint"])
def test_second_live_registration_is_rejected_before_dbos_mutation(
    monkeypatch,
    second_registry_kind: str,
) -> None:
    clients: list[_FakeClient] = []

    def client_factory(*, system_database_url: str) -> _FakeClient:
        client = _FakeClient(system_database_url=system_database_url)
        clients.append(client)
        return client

    monkeypatch.setattr(dispatcher, "DBOSClient", client_factory)
    first_registry = PipelineRegistry()
    first_registry.register(
        wrap_pipeline_workflows(_declared_pipeline("live-owner-first"))
    )
    if second_registry_kind == "overlapping":
        second_registry = first_registry
    else:
        second_registry = PipelineRegistry()
        second_registry.register(
            wrap_pipeline_workflows(_declared_pipeline("live-owner-second"))
        )
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    registration = None
    try:
        registration = dispatcher.register_scheduled_dispatcher(
            config=config,
            engine=engine,
            registry=first_registry,
        )
        dbos_registry = _get_or_create_dbos_registry()
        workflow_info = dict(dbos_registry.workflow_info_map)
        function_types = dict(dbos_registry.function_type_map)
        pollers = tuple(dbos_registry.pollers)

        with pytest.raises(RuntimeError, match="already owns this process"):
            dispatcher.register_scheduled_dispatcher(
                config=config,
                engine=engine,
                registry=second_registry,
            )

        assert len(clients) == 1
        assert dbos_registry.workflow_info_map == workflow_info
        assert dbos_registry.function_type_map == function_types
        assert tuple(dbos_registry.pollers) == pollers
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)
        engine.dispose()


def test_registry_binding_preflight_is_atomic_across_pipelines(
    monkeypatch,
) -> None:
    first = wrap_pipeline_workflows(_declared_pipeline("atomic-first"))
    second = wrap_pipeline_workflows(_declared_pipeline("atomic-second"))
    registry = PipelineRegistry()
    registry.register(first)
    registry.register(second)
    external_executor = _LedgerCheckpointExecutor(max_workers=1)
    external_binding = _bind_ledger_checkpoint_executor(
        (second.stages[0].workflow,),
        external_executor,
    )
    clients: list[_FakeClient] = []

    def client_factory(*, system_database_url: str) -> _FakeClient:
        client = _FakeClient(system_database_url=system_database_url)
        clients.append(client)
        return client

    monkeypatch.setattr(
        dispatcher,
        "DBOSClient",
        client_factory,
    )
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)
    try:
        with pytest.raises(RuntimeError, match="live runtime owner"):
            dispatcher.register_scheduled_dispatcher(
                config=config,
                engine=engine,
                registry=registry,
            )
        assert clients == []
        assert not hasattr(
            first.stages[0].workflow,
            "_dr_platform_ledger_checkpoint_executor",
        )
        assert (
            vars(second.stages[0].workflow)[
                "_dr_platform_ledger_checkpoint_executor"
            ]
            is external_executor
        )
    finally:
        external_binding.release()
        external_executor.close()
        DBOS.destroy(destroy_registry=True)
        engine.dispose()


def test_close_releases_process_ownership_for_reregistration(
    monkeypatch,
) -> None:
    clients = [
        _FakeClient(
            system_database_url="postgresql+psycopg://user:secret@db/platform"
        ),
        _FakeClient(
            system_database_url="postgresql+psycopg://user:secret@db/platform"
        ),
    ]
    monkeypatch.setattr(
        dispatcher,
        "DBOSClient",
        lambda *, system_database_url: clients.pop(0),
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "scheduled", lambda cron: lambda function: function
    )
    monkeypatch.setattr(
        dispatcher.DBOS, "workflow", lambda *, name: lambda function: function
    )
    registry = PipelineRegistry()
    registry.register(wrap_pipeline_workflows(_declared_pipeline()))
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)

    first = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
    )
    first_client = first.client
    assert isinstance(first_client, _FakeClient)
    first.close()
    second = dispatcher.register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
    )
    second_client = second.client
    assert isinstance(second_client, _FakeClient)
    second.close()
    engine.dispose()

    assert first_client.destroyed
    assert second_client.destroyed
    assert clients == []


def test_registration_failure_closes_client_and_checkpoint_executor(
    monkeypatch,
) -> None:
    client = _FakeClient(
        system_database_url="postgresql+psycopg://user:secret@db/platform"
    )
    executor = dispatcher._LedgerCheckpointExecutor(max_workers=1)
    monkeypatch.setattr(
        dispatcher, "DBOSClient", lambda *, system_database_url: client
    )
    monkeypatch.setattr(
        dispatcher,
        "_LedgerCheckpointExecutor",
        lambda *, max_workers: executor,
    )
    registry = PipelineRegistry()
    pipeline = wrap_pipeline_workflows(_declared_pipeline())
    registry.register(pipeline)

    def fail_registration(*, name: str):
        def apply(function: Callable) -> Callable:
            raise RuntimeError("workflow registration failed")

        return apply

    monkeypatch.setattr(dispatcher.DBOS, "workflow", fail_registration)
    monkeypatch.setattr(
        dispatcher.DBOS, "scheduled", lambda cron: lambda function: function
    )
    config = PlatformDbosConfig(
        database_url="postgresql+psycopg://user:secret@db/platform",
        system_database_url="postgresql+psycopg://user:secret@db/platform",
    )
    engine = create_engine(config.database_url)

    with pytest.raises(RuntimeError, match="workflow registration failed"):
        dispatcher.register_scheduled_dispatcher(
            config=config,
            engine=engine,
            registry=registry,
        )
    engine.dispose()

    assert executor.closed
    assert client.destroy_calls == 1
    assert not dispatcher._DISPATCHER_OWNERSHIP.live
    assert not hasattr(
        pipeline.stages[0].workflow,
        "_dr_platform_ledger_checkpoint_executor",
    )


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
    assert calls[-1] == (
        "sweep",
        (engine, {"client": client, "batch_size": 25}),
    )
    assert client.destroyed
