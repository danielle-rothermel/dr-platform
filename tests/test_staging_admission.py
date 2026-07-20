"""PostgreSQL guarantees for transactional staged admission."""

from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast

import pytest
from dbos import DBOS, DBOSClient, DBOSConfig, EnqueueOptions
from sqlalchemy import Engine, func, select
from sqlalchemy.exc import IntegrityError

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    RunKey,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkKey,
    stable_random_rank,
    stage_workflow_id,
)
from dr_platform.staging.admission import (
    AdmissionPayload,
    AdmissionSummary,
    StageAdmissionCount,
    StageAdmissionFailure,
    StageIdentityRecord,
    _Admit,
    _Candidate,
    _Control,
    _evaluate_candidate,
    _PassTally,
    _SkipFull,
    _SkipPaused,
    run_admission_pass,
)
from dr_platform.staging.controls import (
    list_stage_controls,
    upsert_stage_control,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import append_stage_attempt
from dr_platform.staging.stage_executions import transition_stage_execution
from dr_platform.staging.submission import WorkInput, submit
from tests.conftest import engine_dsn

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Connection

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _workflow(input_ref: str) -> str:
    return input_ref


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_ref,)


def _registry() -> PipelineRegistry:
    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name="execute-queue",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    return registry


def _migrate(engine: Engine) -> StagingSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return StagingSchema()


def _submit(
    engine: Engine,
    registry: PipelineRegistry,
    labels: tuple[Mapping[str, str], ...],
) -> None:
    submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=(PipelineKey("evaluation"), 1),
        config_ref="config:1",
        items=(
            WorkInput(
                work_key=f"work-{index}",
                input_ref=f"input:{index}",
                labels=item_labels,
            )
            for index, item_labels in enumerate(labels)
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


def _control(
    engine: Engine,
    *,
    selector: Mapping[str, str] | None,
    capacity: int,
    paused: bool = False,
) -> None:
    with engine.begin() as connection:
        upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector=selector,
            capacity=capacity,
            paused=paused,
            updated_at=NOW,
        )


def _starvation_registry() -> PipelineRegistry:
    registry = PipelineRegistry()
    for suffix in ("a", "b"):
        registry.register(
            PipelineDefinition(
                key=PipelineKey(f"pipeline-{suffix}"),
                version=1,
                stages=(
                    StageDefinition(
                        key=StageKey(f"stage-{suffix}"),
                        queue_name=f"queue-{suffix}",
                        workflow=_workflow,
                        args_for=_args_for,
                    ),
                ),
            )
        )
    return registry


def _submit_starvation_backlog(
    engine: Engine,
    registry: PipelineRegistry,
) -> None:
    campaign_key = "campaign-starvation"
    ranked_keys = sorted(
        (f"work-{index}" for index in range(64)),
        key=lambda work_key: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                CampaignKey(campaign_key), WorkKey(work_key)
            )
        ),
    )
    submit(
        campaign_key=campaign_key,
        run_key="run-a",
        pipeline=(PipelineKey("pipeline-a"), 1),
        config_ref="config:a",
        items=(
            WorkInput(work_key=work_key, input_ref=work_key, labels={})
            for work_key in ranked_keys[:5]
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )
    submit(
        campaign_key=campaign_key,
        run_key="run-b",
        pipeline=(PipelineKey("pipeline-b"), 1),
        config_ref="config:b",
        items=(
            WorkInput(
                work_key=ranked_keys[-1],
                input_ref=ranked_keys[-1],
                labels={},
            ),
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


def _pipeline_control(
    engine: Engine,
    *,
    suffix: str,
    capacity: int,
    paused: bool,
) -> None:
    with engine.begin() as connection:
        upsert_stage_control(
            connection,
            pipeline_key=f"pipeline-{suffix}",
            pipeline_version=1,
            stage_key=f"stage-{suffix}",
            selector={},
            capacity=capacity,
            paused=paused,
            updated_at=NOW,
        )


def _pipeline_states(
    engine: Engine,
    schema: StagingSchema,
) -> list[tuple[str, str]]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                select(
                    schema.pipeline_runs.c.pipeline_key,
                    schema.stage_executions.c.state,
                )
                .select_from(
                    schema.stage_executions.join(
                        schema.work_items,
                        schema.stage_executions.c.work_item_id
                        == schema.work_items.c.work_item_id,
                    ).join(
                        schema.pipeline_runs,
                        schema.work_items.c.origin_run_key
                        == schema.pipeline_runs.c.run_key,
                    )
                )
                .order_by(
                    schema.pipeline_runs.c.pipeline_key,
                    schema.stage_executions.c.rank,
                )
            ).tuples()
        )


class _RecordingClient:
    def __init__(self) -> None:
        self.enqueued: list[tuple[EnqueueOptions, tuple[object, ...]]] = []

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        *args: object,
        **_kwargs: object,
    ) -> object:
        self.enqueued.append((cast("EnqueueOptions", dict(options)), args))
        return object()


class _EnqueueThenFail:
    def __init__(self, client: DBOSClient) -> None:
        self._client = client

    def enqueue_in_transaction(
        self,
        connection: Connection,
        options: EnqueueOptions,
        *args: object,
        **kwargs: object,
    ) -> object:
        self._client.enqueue_in_transaction(
            connection,
            options,
            *args,
            **kwargs,
        )
        raise RuntimeError("failure after enqueue")


class _EnqueueOnceThenFail:
    """Enqueue the first candidate, then fail after enqueuing the second."""

    def __init__(self, client: DBOSClient) -> None:
        self._client = client
        self._calls = 0

    def enqueue_in_transaction(
        self,
        connection: Connection,
        options: EnqueueOptions,
        *args: object,
        **kwargs: object,
    ) -> object:
        self._calls += 1
        result = self._client.enqueue_in_transaction(
            connection,
            options,
            *args,
            **kwargs,
        )
        if self._calls >= 2:  # second candidate
            raise RuntimeError("failure after enqueue")
        return result


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


def _execution_states(
    engine: Engine, schema: StagingSchema
) -> list[tuple[int, str, int]]:
    with engine.connect() as connection:
        return list(
            connection.execute(
                select(
                    schema.stage_executions.c.stage_execution_id,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.current_attempt,
                ).order_by(schema.stage_executions.c.rank)
            ).tuples()
        )


def _launch_dbos_schema(database_url: str) -> None:
    config: DBOSConfig = {
        "name": "drp-admission-test",
        "system_database_url": database_url,
        "application_version": "staging-admission-v1",
        "run_admin_server": False,
        "use_listen_notify": False,
    }
    try:
        DBOS(config=config)
        DBOS.launch()
    finally:
        DBOS.destroy(destroy_registry=True)


def test_stage_capacity_uses_stable_rank_and_terminal_releases_slot(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({}, {}, {}, {}))
    _control(pg_engine, selector=None, capacity=2)
    client = _RecordingClient()

    first = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )
    states = _execution_states(pg_engine, schema)

    assert first.admitted_total == 2
    assert first.skipped_for_capacity == 2
    assert [state for _, state, _ in states] == [
        StageExecutionState.ADMITTED.value,
        StageExecutionState.ADMITTED.value,
        StageExecutionState.READY.value,
        StageExecutionState.READY.value,
    ]

    with pg_engine.begin() as connection:
        transition_stage_execution(
            connection,
            stage_execution_id=states[0][0],
            new_state=StageExecutionState.SUCCEEDED,
            updated_at=NOW + timedelta(seconds=1),
            output_reference="output:released-slot",
        )
    second = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert second.admitted_total == 1
    assert second.skipped_for_capacity == 1
    assert _execution_states(pg_engine, schema)[2][1] == (
        StageExecutionState.ADMITTED.value
    )


def test_selector_capacity_only_constrains_matching_labels(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        (
            {"cohort": "blue"},
            {"cohort": "blue"},
            {"cohort": "red"},
            {"cohort": "red"},
        ),
    )
    _control(pg_engine, selector=None, capacity=4)
    _control(pg_engine, selector={"cohort": "blue"}, capacity=1)

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW,
    )

    with pg_engine.connect() as connection:
        counts = Counter(
            connection.execute(
                select(schema.work_items.c.labels["cohort"].as_string())
                .select_from(
                    schema.work_items.join(
                        schema.stage_executions,
                        schema.work_items.c.work_item_id
                        == schema.stage_executions.c.work_item_id,
                    )
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.ADMITTED.value
                )
            ).scalars()
        )

    assert summary.admitted_total == 3
    assert summary.skipped_for_capacity == 1
    assert counts == {"blue": 1, "red": 2}


def test_paused_stage_cannot_starve_higher_rank_unpaused_stage(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _starvation_registry()
    _submit_starvation_backlog(pg_engine, registry)
    _pipeline_control(
        pg_engine,
        suffix="a",
        capacity=5,
        paused=True,
    )
    _pipeline_control(
        pg_engine,
        suffix="b",
        capacity=1,
        paused=False,
    )
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        batch_size=1,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert client.enqueued[0][0]["queue_name"] == "queue-b"
    assert _pipeline_states(pg_engine, schema) == (
        [("pipeline-a", StageExecutionState.READY.value)] * 5
        + [("pipeline-b", StageExecutionState.ADMITTED.value)]
    )


def test_capacity_full_stage_cannot_starve_higher_rank_stage_with_room(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _starvation_registry()
    _submit_starvation_backlog(pg_engine, registry)
    _pipeline_control(
        pg_engine,
        suffix="a",
        capacity=0,
        paused=False,
    )
    _pipeline_control(
        pg_engine,
        suffix="b",
        capacity=1,
        paused=False,
    )
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        batch_size=1,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert summary.skipped_for_capacity == 1
    assert client.enqueued[0][0]["queue_name"] == "queue-b"
    assert _pipeline_states(pg_engine, schema) == (
        [("pipeline-a", StageExecutionState.READY.value)] * 5
        + [("pipeline-b", StageExecutionState.ADMITTED.value)]
    )


def test_args_for_receives_only_the_frozen_admission_payload(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    observed: list[AdmissionPayload] = []

    def args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        observed.append(payload)
        return ("domain-argument",)

    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name="execute-queue",
                    workflow=_workflow,
                    args_for=args_for,
                ),
            ),
        )
    )
    _submit(pg_engine, registry, ({"cohort": "blue"},))
    _control(pg_engine, selector=None, capacity=1)
    client = _RecordingClient()

    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    assert len(observed) == 1
    assert observed[0] == AdmissionPayload(
        campaign_key=CampaignKey("campaign-1"),
        work_key=WorkKey("work-0"),
        run_key=RunKey("run-1"),
        input_ref="input:0",
        labels={"cohort": "blue"},
        pipeline_key="evaluation",
        pipeline_version=1,
        stage_key=StageKey("execute"),
        attempt_number=1,
    )
    assert client.enqueued[0][1] == ("domain-argument",)


def test_pause_keeps_matching_ready_until_resume(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        ({"cohort": "blue"}, {"cohort": "red"}),
    )
    _control(pg_engine, selector=None, capacity=2)
    _control(
        pg_engine,
        selector={"cohort": "blue"},
        capacity=2,
        paused=True,
    )
    client = _RecordingClient()

    paused = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )
    with pg_engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.work_items.c.labels["cohort"].as_string(),
                schema.stage_executions.c.state,
            ).select_from(
                schema.work_items.join(
                    schema.stage_executions,
                    schema.work_items.c.work_item_id
                    == schema.stage_executions.c.work_item_id,
                )
            )
        ).tuples()
        states_by_label = dict(rows.all())

    assert paused.admitted_total == 1
    assert paused.skipped_for_pause == 0
    assert states_by_label == {
        "blue": StageExecutionState.READY.value,
        "red": StageExecutionState.ADMITTED.value,
    }

    _control(
        pg_engine,
        selector={"cohort": "blue"},
        capacity=2,
        paused=False,
    )
    resumed = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )

    assert resumed.admitted_total == 1
    assert all(
        state == StageExecutionState.ADMITTED.value
        for _, state, _ in _execution_states(pg_engine, schema)
    )


def test_enqueue_failure_rolls_back_platform_and_dbos_rows(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({}, {}))
    _control(pg_engine, selector=None, capacity=2)
    _launch_dbos_schema(clean_pg)
    client = DBOSClient(system_database_url=clean_pg)
    workflow_ids = [
        stage_workflow_id(
            work_identity=CampaignWorkIdentity(
                CampaignKey("campaign-1"), WorkKey(work_key)
            ),
            pipeline_key=PipelineKey("evaluation"),
            pipeline_version=1,
            stage_key=StageKey("execute"),
            attempt_number=1,
        )
        for work_key in ("work-0", "work-1")
    ]
    for workflow_id in workflow_ids:
        client.delete_workflow(workflow_id)

    try:
        summary = run_admission_pass(
            pg_engine,
            client=_as_dbos_client(_EnqueueOnceThenFail(client)),
            registry=registry,
            clock=lambda: NOW,
        )

        states = _execution_states(pg_engine, schema)
        admitted = [
            state
            for _, state, _ in states
            if state == StageExecutionState.ADMITTED.value
        ]
        ready = [
            state
            for _, state, _ in states
            if state == StageExecutionState.READY.value
        ]
        # The pass no longer aborts: the healthy candidate commits while
        # the failing candidate's platform and DBOS rows roll back.
        assert summary.admitted_total == 1
        assert len(admitted) == 1
        assert len(ready) == 1
        with pg_engine.connect() as connection:
            assert (
                connection.execute(
                    select(func.count()).select_from(schema.stage_attempts)
                ).scalar_one()
                == 1
            )
        enqueued = client.list_workflows(
            workflow_ids=workflow_ids,
            load_input=False,
            load_output=False,
        )
        assert len(enqueued) == 1
        assert len(summary.failed_stages) == 1
        failure = summary.failed_stages[0]
        assert failure.pipeline_key == "evaluation"
        assert failure.stage_key == StageKey("execute")
        assert failure.error_type == "RuntimeError"
        assert failure.message == "failure after enqueue"
    finally:
        client.destroy()


def test_two_passes_cannot_admit_one_ready_row_twice(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({},))
    _control(pg_engine, selector=None, capacity=2)
    start = Barrier(2)

    def admit() -> int:
        start.wait()
        return run_admission_pass(
            pg_engine,
            client=_as_dbos_client(_RecordingClient()),
            registry=registry,
            clock=lambda: NOW,
        ).admitted_total

    with ThreadPoolExecutor(max_workers=2) as executor:
        totals = tuple(executor.map(lambda _index: admit(), range(2)))

    with pg_engine.connect() as connection:
        attempt_count = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()

    assert sum(totals) == 1
    assert attempt_count == 1
    assert _execution_states(pg_engine, schema)[0][1:] == (
        StageExecutionState.ADMITTED.value,
        1,
    )


def test_real_dbos_client_enqueues_exactly_one_deterministic_workflow(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({},))
    _control(pg_engine, selector=None, capacity=1)
    _launch_dbos_schema(clean_pg)
    client = DBOSClient(system_database_url=clean_pg)
    workflow_id = stage_workflow_id(
        work_identity=CampaignWorkIdentity(
            CampaignKey("campaign-1"), WorkKey("work-0")
        ),
        pipeline_key=PipelineKey("evaluation"),
        pipeline_version=1,
        stage_key=StageKey("execute"),
        attempt_number=1,
    )
    client.delete_workflow(workflow_id)

    try:
        summary = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=lambda: NOW,
        )
        with pg_engine.connect() as connection:
            stored_workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
            ).scalar_one()
        matches = client.list_workflows(
            workflow_ids=[workflow_id],
            load_input=False,
            load_output=False,
        )

        assert summary.admitted_total == 1
        assert stored_workflow_id == workflow_id
        assert len(matches) == 1
        assert matches[0].workflow_id == workflow_id
        assert matches[0].status == "ENQUEUED"
        assert matches[0].queue_name == "execute-queue"
    finally:
        client.destroy()


def test_attempt_workflow_id_is_unique(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({}, {}))
    monkeypatch.setattr(
        "dr_platform.staging.stage_attempts.stage_workflow_id",
        lambda **_kwargs: "duplicate-workflow-id",
    )

    def append_both() -> None:
        with pg_engine.begin() as connection:
            execution_ids = connection.execute(
                select(schema.stage_executions.c.stage_execution_id).order_by(
                    schema.stage_executions.c.stage_execution_id
                )
            ).scalars()
            for execution_id in execution_ids:
                append_stage_attempt(
                    connection,
                    stage_execution_id=execution_id,
                    created_at=NOW,
                )

    with pytest.raises(IntegrityError):
        append_both()


def test_empty_selector_is_default_and_upserts_as_one_control(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        original = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector=None,
            capacity=1,
            paused=False,
            updated_at=NOW,
        )
        replaced = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector={},
            capacity=3,
            paused=True,
            updated_at=NOW + timedelta(seconds=1),
        )
        selected = upsert_stage_control(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            selector={"cohort": "blue"},
            capacity=1,
            paused=False,
            updated_at=NOW,
        )
        red_controls = list_stage_controls(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            labels={"cohort": "red"},
        )
        blue_controls = list_stage_controls(
            connection,
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key="execute",
            labels={"cohort": "blue"},
        )

    assert replaced.stage_control_id == original.stage_control_id
    assert (
        replaced.selector,
        replaced.capacity,
        replaced.paused,
    ) == ({}, 3, True)
    assert red_controls == (replaced,)
    assert blue_controls == (replaced, selected)


def _candidate(
    *,
    labels: Mapping[str, str],
    stage_execution_id: int = 1,
    rank: int = 1,
    stage_identity: tuple[str, int, str] = ("evaluation", 1, "execute"),
) -> _Candidate:
    return _Candidate(
        stage_execution_id=stage_execution_id,
        rank=rank,
        stage_index=0,
        campaign_key="campaign-1",
        work_key="work-0",
        run_key="run-1",
        input_ref="input:0",
        labels=labels,
        pipeline_key=stage_identity[0],
        pipeline_version=stage_identity[1],
        stage_key=stage_identity[2],
    )


def _make_control(
    *,
    control_id: int,
    selector: Mapping[str, str],
    capacity: int,
    paused: bool = False,
    stage_identity: tuple[str, int, str] = ("evaluation", 1, "execute"),
) -> _Control:
    return _Control(
        control_id=control_id,
        stage_identity=stage_identity,
        selector=selector,
        capacity=capacity,
        paused=paused,
    )


def test_evaluate_candidate_paused_beats_full() -> None:
    candidate = _candidate(labels={"cohort": "blue"})
    default = _make_control(control_id=1, selector={}, capacity=1)
    paused = _make_control(
        control_id=2,
        selector={"cohort": "blue"},
        capacity=5,
        paused=True,
    )
    occupancy = {1: 5, 2: 0}

    result = _evaluate_candidate(
        candidate,
        controls=(default, paused),
        occupancy=occupancy,
    )

    assert isinstance(result, _SkipPaused)


def test_evaluate_candidate_occupancy_at_capacity_is_full() -> None:
    candidate = _candidate(labels={})
    control = _make_control(control_id=1, selector={}, capacity=2)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 2},
    )

    assert result == _SkipFull(full_control_ids=frozenset({1}))


def test_evaluate_candidate_occupancy_below_capacity_admits() -> None:
    candidate = _candidate(labels={})
    control = _make_control(control_id=1, selector={}, capacity=2)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 1},
    )

    assert result == _Admit(matching=(control,))


def test_evaluate_candidate_empty_selector_matches_any_labels() -> None:
    candidate = _candidate(labels={"cohort": "red", "tier": "gold"})
    control = _make_control(control_id=1, selector={}, capacity=3)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 0},
    )

    assert result == _Admit(matching=(control,))


def test_evaluate_candidate_reports_only_the_full_matching_control() -> None:
    candidate = _candidate(labels={"cohort": "blue"})
    default = _make_control(control_id=1, selector={}, capacity=5)
    selective = _make_control(
        control_id=2,
        selector={"cohort": "blue"},
        capacity=1,
    )

    result = _evaluate_candidate(
        candidate,
        controls=(default, selective),
        occupancy={1: 0, 2: 1},
    )

    assert result == _SkipFull(full_control_ids=frozenset({2}))


def test_evaluate_candidate_full_but_non_matching_control_does_not_block() -> (
    None
):
    candidate = _candidate(labels={"cohort": "red"})
    default = _make_control(control_id=1, selector={}, capacity=5)
    other = _make_control(
        control_id=2,
        selector={"cohort": "blue"},
        capacity=1,
    )

    result = _evaluate_candidate(
        candidate,
        controls=(default, other),
        occupancy={1: 0, 2: 1},
    )

    assert result == _Admit(matching=(default,))


def test_pass_tally_admitted_total_derives_from_per_stage_counts() -> None:
    tally = _PassTally()
    tally.record_admitted(("evaluation", 1, "execute"))
    tally.record_admitted(("evaluation", 1, "execute"))
    tally.record_admitted(("other", 2, "stage"))

    assert tally.admitted_total == 3
    assert tally.admitted == {
        ("evaluation", 1, "execute"): 2,
        ("other", 2, "stage"): 1,
    }


def test_pass_tally_to_summary_sorts_and_converts_stage_keys() -> None:
    tally = _PassTally()
    tally.record_admitted(("evaluation", 1, "zeta"))
    tally.record_admitted(("evaluation", 1, "alpha"))
    tally.record_capacity_skip()
    tally.record_pause_skip()

    summary = tally.to_summary()

    assert summary.skipped_for_capacity == 1
    assert summary.skipped_for_pause == 1
    assert summary.admitted_counts == (
        StageAdmissionCount(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("alpha"),
            count=1,
        ),
        StageAdmissionCount(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("zeta"),
            count=1,
        ),
    )
    assert all(
        isinstance(item.stage_key, StageKey)
        for item in summary.admitted_counts
    )


def _submit_two_stage_backlog(
    engine: Engine,
    registry: PipelineRegistry,
) -> None:
    for suffix in ("a", "b"):
        submit(
            campaign_key="campaign-two-stage",
            run_key=f"run-{suffix}",
            pipeline=(PipelineKey(f"pipeline-{suffix}"), 1),
            config_ref=f"config:{suffix}",
            items=(
                WorkInput(
                    work_key=f"work-{suffix}",
                    input_ref=f"input-{suffix}",
                    labels={},
                ),
            ),
            registry=registry,
            engine=engine,
            clock=lambda: NOW,
        )


def _states_by_pipeline(
    engine: Engine,
    schema: StagingSchema,
) -> dict[str, str]:
    return dict(_pipeline_states(engine, schema))


def test_unconfigured_stage_is_skipped_and_reported_then_admitted(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _starvation_registry()
    _submit_two_stage_backlog(pg_engine, registry)
    _pipeline_control(pg_engine, suffix="a", capacity=5, paused=False)
    client = _RecordingClient()

    first = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    assert first.admitted_total == 1
    assert client.enqueued[0][0]["queue_name"] == "queue-a"
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.READY.value,
    }
    assert first.unconfigured_stages == (
        StageIdentityRecord(
            pipeline_key="pipeline-b",
            pipeline_version=1,
            stage_key=StageKey("stage-b"),
        ),
    )
    assert first.failed_stages == ()

    _pipeline_control(pg_engine, suffix="b", capacity=5, paused=False)
    second = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )

    assert second.admitted_total == 1
    assert second.unconfigured_stages == ()
    assert second.failed_stages == ()
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.ADMITTED.value,
    }


def test_args_for_failure_isolates_to_its_stage(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)

    def failing_args_for(_payload: AdmissionPayload) -> tuple[object, ...]:
        raise ValueError("args_for exploded")

    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-a"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("stage-a"),
                    queue_name="queue-a",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-b"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("stage-b"),
                    queue_name="queue-b",
                    workflow=_workflow,
                    args_for=failing_args_for,
                ),
            ),
        )
    )
    _submit_two_stage_backlog(pg_engine, registry)
    _pipeline_control(pg_engine, suffix="a", capacity=5, paused=False)
    _pipeline_control(pg_engine, suffix="b", capacity=5, paused=False)
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert [options["queue_name"] for options, _ in client.enqueued] == [
        "queue-a"
    ]
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.READY.value,
    }
    with pg_engine.connect() as connection:
        attempt_count = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()
    assert attempt_count == 1
    assert summary.unconfigured_stages == ()
    assert summary.failed_stages == (
        StageAdmissionFailure(
            pipeline_key="pipeline-b",
            pipeline_version=1,
            stage_key=StageKey("stage-b"),
            error_type="ValueError",
            message="args_for exploded",
        ),
    )


def test_poison_candidate_does_not_starve_same_stage_higher_rank_row(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    campaign_key = "campaign-1"
    # The lower-ranked (locked-first) row is the poison one; its same-stage
    # sibling ranked behind it must still admit in this pass.
    ranked = sorted(
        ("work-0", "work-1"),
        key=lambda work_key: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                CampaignKey(campaign_key), WorkKey(work_key)
            )
        ),
    )
    poison_input = f"input:{ranked[0].removeprefix('work-')}"

    def args_for(payload: AdmissionPayload) -> tuple[object, ...]:
        if payload.input_ref == poison_input:
            raise ValueError("deterministic poison")
        return (payload.input_ref,)

    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name="execute-queue",
                    workflow=_workflow,
                    args_for=args_for,
                ),
            ),
        )
    )
    _submit(pg_engine, registry, ({}, {}))
    _control(pg_engine, selector=None, capacity=5)
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    # The poison row stays READY (retried next pass); its higher-rank sibling
    # in the same stage still admits this pass.
    assert summary.admitted_total == 1
    admitted = [
        state
        for _, state, _ in _execution_states(pg_engine, schema)
        if state == StageExecutionState.ADMITTED.value
    ]
    ready = [
        state
        for _, state, _ in _execution_states(pg_engine, schema)
        if state == StageExecutionState.READY.value
    ]
    assert len(admitted) == 1
    assert len(ready) == 1
    assert summary.failed_stages == (
        StageAdmissionFailure(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("execute"),
            error_type="ValueError",
            message="deterministic poison",
        ),
    )
    assert summary.mismatched_stages == ()


def test_pipeline_stage_mismatch_is_reported_and_other_stages_admit(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    # Submit under a well-formed registry, then run admission under one whose
    # pipeline-b stage key drifted from the persisted position: registry/data
    # drift, not an application boundary failure.
    submit_registry = _starvation_registry()
    _submit_two_stage_backlog(pg_engine, submit_registry)
    _pipeline_control(pg_engine, suffix="a", capacity=5, paused=False)
    _pipeline_control(pg_engine, suffix="b", capacity=5, paused=False)

    drifted_registry = PipelineRegistry()
    drifted_registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-a"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("stage-a"),
                    queue_name="queue-a",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    drifted_registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-b"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("drifted-stage-b"),
                    queue_name="queue-b",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=drifted_registry,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert [options["queue_name"] for options, _ in client.enqueued] == [
        "queue-a"
    ]
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.READY.value,
    }
    assert summary.failed_stages == ()
    assert len(summary.mismatched_stages) == 1
    mismatch = summary.mismatched_stages[0]
    assert mismatch.pipeline_key == "pipeline-b"
    assert mismatch.stage_key == StageKey("stage-b")
    assert "disagrees" in mismatch.message


def test_unconfigured_backlog_cannot_exhaust_considered_budget(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A realistic starving backlog needs batch_size + 10_000 rows; shrinking
    # the skip allowance exercises the same budget exhaustion with five.
    monkeypatch.setattr(
        "dr_platform.staging.admission.MAX_CAPACITY_SKIPS_PER_PASS", 2
    )
    schema = _migrate(pg_engine)
    registry = _starvation_registry()
    _submit_starvation_backlog(pg_engine, registry)
    _pipeline_control(pg_engine, suffix="b", capacity=1, paused=False)
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        batch_size=1,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert client.enqueued[0][0]["queue_name"] == "queue-b"
    assert summary.unconfigured_stages == (
        StageIdentityRecord(
            pipeline_key="pipeline-a",
            pipeline_version=1,
            stage_key=StageKey("stage-a"),
        ),
    )
    assert _pipeline_states(pg_engine, schema) == (
        [("pipeline-a", StageExecutionState.READY.value)] * 5
        + [("pipeline-b", StageExecutionState.ADMITTED.value)]
    )


def test_unprintable_failure_message_does_not_abort_the_pass(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)

    class _UnprintableError(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("no message for you")

    def failing_args_for(_payload: AdmissionPayload) -> tuple[object, ...]:
        raise _UnprintableError("boom")

    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-a"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("stage-a"),
                    queue_name="queue-a",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    registry.register(
        PipelineDefinition(
            key=PipelineKey("pipeline-b"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("stage-b"),
                    queue_name="queue-b",
                    workflow=_workflow,
                    args_for=failing_args_for,
                ),
            ),
        )
    )
    _submit_two_stage_backlog(pg_engine, registry)
    _pipeline_control(pg_engine, suffix="a", capacity=5, paused=False)
    _pipeline_control(pg_engine, suffix="b", capacity=5, paused=False)
    client = _RecordingClient()

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.READY.value,
    }
    assert summary.failed_stages == (
        StageAdmissionFailure(
            pipeline_key="pipeline-b",
            pipeline_version=1,
            stage_key=StageKey("stage-b"),
            error_type="_UnprintableError",
            message=repr(_UnprintableError("boom")),
        ),
    )


def test_paused_selector_only_stage_is_invisible_until_resume(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({"cohort": "blue"},))
    _control(
        pg_engine,
        selector={"cohort": "blue"},
        capacity=1,
        paused=True,
    )
    client = _RecordingClient()

    paused = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    # While paused, the SQL pause exclusion filters the stage's rows before
    # unconfigured-stage detection runs, so a stage with only a paused
    # selector control and no empty-selector default appears nowhere.
    assert paused.admitted_total == 0
    assert paused.skipped_for_pause == 0
    assert paused.unconfigured_stages == ()
    assert paused.failed_stages == ()

    _control(
        pg_engine,
        selector={"cohort": "blue"},
        capacity=1,
        paused=False,
    )
    resumed = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )

    assert resumed.admitted_total == 0
    assert resumed.unconfigured_stages == (
        StageIdentityRecord(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("execute"),
        ),
    )
    assert _execution_states(pg_engine, schema)[0][1] == (
        StageExecutionState.READY.value
    )


def test_two_passes_cannot_exceed_control_capacity(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({}, {}))
    _control(pg_engine, selector=None, capacity=1)
    start = Barrier(2)

    def admit() -> AdmissionSummary:
        start.wait()
        return run_admission_pass(
            pg_engine,
            client=_as_dbos_client(_RecordingClient()),
            registry=registry,
            clock=lambda: NOW,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        summaries = tuple(executor.map(lambda _index: admit(), range(2)))

    with pg_engine.connect() as connection:
        attempt_count = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()

    # Whichever pass acquires the control lock second must observe the
    # winner's committed occupancy and skip as full.
    assert sum(item.admitted_total for item in summaries) == 1
    assert sum(item.skipped_for_capacity for item in summaries) == 1
    assert attempt_count == 1
    assert sorted(
        state for _, state, _ in _execution_states(pg_engine, schema)
    ) == [
        StageExecutionState.ADMITTED.value,
        StageExecutionState.READY.value,
    ]
