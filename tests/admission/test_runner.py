from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast

import pytest
from dbos import DBOSClient, EnqueueOptions
from sqlalchemy import Engine, func, select
from sqlalchemy.exc import IntegrityError

from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    stage_workflow_id,
)
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import (
    list_stage_controls,
    upsert_stage_control,
)
from dr_platform.admission.runner import (
    AdmissionPayload,
    AdmissionSummary,
    StageAdmissionCount,
    StageIdentityRecord,
    _Admit,
    _Candidate,
    _Control,
    _evaluate_candidate,
    _lock_controls,
    _PassTally,
    _SkipFull,
    _StageIdentity,
    run_admission_pass,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.submission.stream import WorkInput
from dr_platform.submission.work_items import stable_random_rank
from tests.conftest import (
    NOW,
    _args_for,
    _as_dbos_client,
    _migrate,
    dbos_config,
    initialize_dbos_schema,
    submit_items,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import StagingSchema

_DEFAULT_STAGE_IDENTITY = _StageIdentity("evaluation", 1, "execute")


async def _workflow(input_reference: str) -> str:
    return input_reference


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


def _submit(
    engine: Engine,
    registry: PipelineRegistry,
    labels: tuple[Mapping[str, str], ...],
) -> None:
    submit_items(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key=f"work-{index}",
                input_reference=f"input:{index}",
                labels=item_labels,
            )
            for index, item_labels in enumerate(labels)
        ),
        expected_member_count=len(labels),
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
    submit_items(
        campaign_key=campaign_key,
        run_key="run-a",
        pipeline=PipelineIdentity(PipelineKey("pipeline-a"), 1),
        execution_config_reference="config:a",
        items=(
            WorkInput(work_key=work_key, input_reference=work_key, labels={})
            for work_key in ranked_keys[:5]
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=campaign_key,
        run_key="run-b",
        pipeline=PipelineIdentity(PipelineKey("pipeline-b"), 1),
        execution_config_reference="config:b",
        items=(
            WorkInput(
                work_key=ranked_keys[-1],
                input_reference=ranked_keys[-1],
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
        if self._calls >= 2:
            raise RuntimeError("failure after enqueue")
        return result


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
    initialize_dbos_schema(
        dbos_config(
            name="drp-admission-test",
            system_database_url=database_url,
            application_version="staging-admission-v1",
        )
    )


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


def test_admission_enqueues_only_the_serialized_platform_payload(
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

    assert observed == []
    assert len(client.enqueued) == 1
    payload = AdmissionPayload.model_validate(client.enqueued[0][1][0])
    assert payload == AdmissionPayload(
        campaign_key=CampaignKey("campaign-1"),
        work_key=WorkKey("work-0"),
        origin_run_key=RunKey("run-1"),
        input_reference="input:0",
        labels={"cohort": "blue"},
        pipeline_key="evaluation",
        pipeline_version=1,
        stage_key=StageKey("execute"),
        attempt_number=1,
    )
    wire = payload.model_dump(mode="json")
    assert set(wire) == {
        "campaign_key",
        "work_key",
        "origin_run_key",
        "input_reference",
        "labels",
        "pipeline_key",
        "pipeline_version",
        "stage_key",
        "attempt_number",
    }
    assert "run_key" not in wire
    assert client.enqueued[0][0]["queue_name"] == "execute-queue"


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


def test_selector_paused_after_candidate_selection_skips_admission(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(pg_engine, registry, ({"cohort": "blue"},))
    _control(pg_engine, selector=None, capacity=0)
    _control(
        pg_engine,
        selector={"cohort": "blue"},
        capacity=1,
    )
    client = _RecordingClient()
    lock_calls = 0

    def pause_before_control_lock(
        connection: Connection,
        *,
        schema: StagingSchema,
        identities: set[_StageIdentity],
    ) -> tuple[_Control, ...]:
        nonlocal lock_calls
        lock_calls += 1
        _control(
            pg_engine,
            selector={"cohort": "blue"},
            capacity=1,
            paused=True,
        )
        return _lock_controls(
            connection,
            schema=schema,
            identities=identities,
        )

    # No public seam can pause between selection and the control lock.
    monkeypatch.setattr(
        "dr_platform.admission.runner._lock_controls",
        pause_before_control_lock,
    )

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )

    assert lock_calls == 1
    assert summary.admitted_total == 0
    assert summary.skipped_for_pause == 1
    assert summary.skipped_for_capacity == 0
    assert client.enqueued == []
    assert [
        (state, current_attempt)
        for _, state, current_attempt in _execution_states(pg_engine, schema)
    ] == [(StageExecutionState.READY.value, 0)]
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(schema.stage_attempts)
            ).scalar_one()
            == 0
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
        start.wait(timeout=10)
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
        "dr_platform._core.ledger.attempts.stage_workflow_id",
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
    stage_identity: _StageIdentity = _DEFAULT_STAGE_IDENTITY,
) -> _Candidate:
    return _Candidate(
        stage_execution_id=stage_execution_id,
        rank=rank,
        stage_index=0,
        campaign_key="campaign-1",
        work_key="work-0",
        origin_run_key="run-1",
        input_reference="input:0",
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
    stage_identity: _StageIdentity = _DEFAULT_STAGE_IDENTITY,
) -> _Control:
    return _Control(
        control_id=control_id,
        stage_identity=stage_identity,
        selector=selector,
        capacity=capacity,
        paused=paused,
    )


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


def test_pass_tally_to_summary_sorts_and_converts_stage_keys() -> None:
    tally = _PassTally()
    tally.record_admitted(_StageIdentity("evaluation", 1, "zeta"))
    tally.record_admitted(_StageIdentity("evaluation", 1, "alpha"))
    tally.skipped_for_capacity += 1
    tally.skipped_for_pause += 1

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
        submit_items(
            campaign_key="campaign-two-stage",
            run_key=f"run-{suffix}",
            pipeline=PipelineIdentity(PipelineKey(f"pipeline-{suffix}"), 1),
            execution_config_reference=f"config:{suffix}",
            items=(
                WorkInput(
                    work_key=f"work-{suffix}",
                    input_reference=f"input-{suffix}",
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


def test_args_for_failure_cannot_poison_admission_transaction(
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

    assert summary.admitted_total == 2
    assert {options["queue_name"] for options, _ in client.enqueued} == {
        "queue-a",
        "queue-b",
    }
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.ADMITTED.value,
    }
    with pg_engine.connect() as connection:
        attempt_count = connection.execute(
            select(func.count()).select_from(schema.stage_attempts)
        ).scalar_one()
    assert attempt_count == 2
    assert summary.unconfigured_stages == ()
    assert summary.failed_stages == ()


def test_non_tuple_args_for_is_deferred_outside_admission(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    campaign_key = "campaign-1"
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
        if payload.input_reference == poison_input:
            return cast("tuple[object, ...]", ["not-a-tuple"])
        return (payload.input_reference,)

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

    assert summary.admitted_total == 2
    states = _execution_states(pg_engine, schema)
    assert [state for _, state, _ in states] == [
        StageExecutionState.ADMITTED.value,
        StageExecutionState.ADMITTED.value,
    ]
    assert [attempt for _, _, attempt in states] == [1, 1]
    assert len(client.enqueued) == 2
    assert all(
        isinstance(enqueued_args[0], dict)
        for _, enqueued_args in client.enqueued
    )
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(schema.stage_attempts)
            ).scalar_one()
            == 2
        )
    assert summary.failed_stages == ()
    assert summary.mismatched_stages == ()


def test_pipeline_stage_mismatch_is_reported_and_other_stages_admit(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
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
    # Shrink MAX_CAPACITY_SKIPS_PER_PASS (1,000,000 in production) to
    # exercise starvation cheaply.
    monkeypatch.setattr(
        "dr_platform.admission.runner.MAX_CAPACITY_SKIPS_PER_PASS", 2
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


def test_unprintable_args_for_failure_cannot_run_in_admission(
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

    assert summary.admitted_total == 2
    assert _states_by_pipeline(pg_engine, schema) == {
        "pipeline-a": StageExecutionState.ADMITTED.value,
        "pipeline-b": StageExecutionState.ADMITTED.value,
    }
    assert summary.failed_stages == ()


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
        start.wait(timeout=10)
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

    assert sum(item.admitted_total for item in summaries) == 1
    assert sum(item.skipped_for_capacity for item in summaries) == 1
    assert attempt_count == 1
    assert sorted(
        state for _, state, _ in _execution_states(pg_engine, schema)
    ) == [
        StageExecutionState.ADMITTED.value,
        StageExecutionState.READY.value,
    ]
