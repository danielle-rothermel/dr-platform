from __future__ import annotations

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast
from uuid import uuid4

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
from dr_platform._core.ledger.executions import (
    get_stage_execution,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import (
    set_stage_capacity,
    upsert_stage_control,
)
from dr_platform.admission.runner import (
    AdmissionPayload,
    AdmissionSummary,
    StageIdentityRecord,
    _Control,
    _lock_controls,
    _StageIdentity,
    run_admission_pass,
)
from dr_platform.execution.handoff import (
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.execution.stage_completion import StageSuccessor
from dr_platform.pipeline.definitions import (
    LabelQueueRoute,
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
    _RecordingClient,
    dbos_config,
    initialize_dbos_schema,
    submit_items,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import LedgerSchema

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
    schema: LedgerSchema,
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
    engine: Engine, schema: LedgerSchema
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
    schema: LedgerSchema,
) -> dict[str, str]:
    return dict(_pipeline_states(engine, schema))


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
    assert client.enqueued[0]["queue_name"] == "queue-b"
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
    assert client.enqueued[0]["queue_name"] == "queue-b"
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
    payload = AdmissionPayload.model_validate(client.enqueued_args[0][0])
    schema = _migrate(pg_engine)
    with pg_engine.connect() as connection:
        work_item_id = connection.execute(
            select(schema.work_items.c.work_item_id).where(
                schema.work_items.c.work_key == "work-0"
            )
        ).scalar_one()
    assert payload == AdmissionPayload(
        campaign_key=CampaignKey("campaign-1"),
        work_key=WorkKey("work-0"),
        work_item_id=work_item_id,
        origin_run_key=RunKey("run-1"),
        input_reference="input:0",
        labels={"cohort": "blue"},
        pipeline_key="evaluation",
        pipeline_version=1,
        stage_key=StageKey("execute"),
        stage_index=0,
        attempt_number=1,
    )
    wire = payload.model_dump(mode="json")
    assert set(wire) == {
        "campaign_key",
        "work_key",
        "work_item_id",
        "origin_run_key",
        "input_reference",
        "labels",
        "pipeline_key",
        "pipeline_version",
        "stage_key",
        "stage_index",
        "attempt_number",
    }
    assert "run_key" not in wire
    assert client.enqueued[0]["queue_name"] == "execute-queue"


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
        schema: LedgerSchema,
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
            stage_index=0,
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
        stage_index=0,
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
    assert client.enqueued[0]["queue_name"] == "queue-a"
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


class _UnprintableArgsError(RuntimeError):
    def __str__(self) -> str:
        raise RuntimeError("no message for you")


def _two_pipeline_registry(
    failing_args_for: Callable[[AdmissionPayload], tuple[object, ...]],
) -> PipelineRegistry:
    """Register a working pipeline-a beside a raising pipeline-b."""
    registry = PipelineRegistry()
    for suffix, args_for in (("a", _args_for), ("b", failing_args_for)):
        registry.register(
            PipelineDefinition(
                key=PipelineKey(f"pipeline-{suffix}"),
                version=1,
                stages=(
                    StageDefinition(
                        key=StageKey(f"stage-{suffix}"),
                        queue_name=f"queue-{suffix}",
                        workflow=_workflow,
                        args_for=args_for,
                    ),
                ),
            )
        )
    return registry


@pytest.mark.parametrize(
    "error",
    [ValueError("args_for exploded"), _UnprintableArgsError("boom")],
    ids=["printable", "unprintable"],
)
def test_args_for_failure_cannot_poison_admission_transaction(
    pg_engine: Engine,
    error: Exception,
) -> None:
    schema = _migrate(pg_engine)

    def failing_args_for(_payload: AdmissionPayload) -> tuple[object, ...]:
        raise error

    registry = _two_pipeline_registry(failing_args_for)
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
    assert {options["queue_name"] for options in client.enqueued} == {
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
        for enqueued_args in client.enqueued_args
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
    assert [options["queue_name"] for options in client.enqueued] == [
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
    assert "not registered" in mismatch.message


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
    assert client.enqueued[0]["queue_name"] == "queue-b"
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


def test_barrier_ready_join_is_skipped_until_lower_stages_succeed(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _control(pg_engine, selector=None, capacity=4)
    with pg_engine.begin() as connection:
        run_key = "run-barrier-admission"
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key=run_key,
                campaign_key="campaign-1",
                pipeline_key="evaluation",
                pipeline_version=1,
                execution_config_reference="config:1",
                expected_member_count=1,
                created_at=NOW,
            )
        )
        work_item_id = connection.execute(
            schema.work_items.insert()
            .values(
                campaign_key="campaign-1",
                work_key="work-barrier-admission",
                origin_run_key=run_key,
                input_reference="seed",
                labels={},
                rank=1,
            )
            .returning(schema.work_items.c.work_item_id)
        ).scalar_one()
        split = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=0,
            input_reference="seed",
            created_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference="split:out",
            updated_at=NOW,
        )
        insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=1,
            input_reference="row:a",
            created_at=NOW,
        )
        insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=3,
            input_reference="join:pending",
            barrier=True,
            created_at=NOW,
        )

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 1
    assert summary.skipped_for_barrier == 1


def _barrier_handoff_race_fixture(
    pg_engine: Engine,
) -> tuple[LedgerSchema, PipelineRegistry, str, int]:
    schema = _migrate(pg_engine)
    registry = _registry()
    _control(pg_engine, selector=None, capacity=4)
    with pg_engine.begin() as connection:
        run_key = "run-barrier-handoff-race"
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key=run_key,
                campaign_key="campaign-1",
                pipeline_key="evaluation",
                pipeline_version=1,
                execution_config_reference="config:1",
                expected_member_count=1,
                created_at=NOW,
            )
        )
        work_item_id = connection.execute(
            schema.work_items.insert()
            .values(
                campaign_key="campaign-1",
                work_key="work-barrier-handoff-race",
                origin_run_key=run_key,
                input_reference="seed",
                labels={},
                rank=1,
            )
            .returning(schema.work_items.c.work_item_id)
        ).scalar_one()
        split = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=0,
            input_reference="seed",
            created_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference="split:out",
            updated_at=NOW,
        )
        branch = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=1,
            input_reference="row:a",
            created_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=branch.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
        attempt = append_stage_attempt(
            connection,
            stage_execution_id=branch.stage_execution_id,
            created_at=NOW,
            admitted_at=NOW,
        )
        join = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=3,
            input_reference="join:pending",
            barrier=True,
            created_at=NOW,
        )
    return schema, registry, attempt.workflow_id, join.stage_execution_id


def test_barrier_admission_does_not_race_concurrent_handoff(
    pg_engine: Engine,
) -> None:
    schema, registry, branch_workflow_id, join_execution_id = (
        _barrier_handoff_race_fixture(pg_engine)
    )

    handoff_ready = Barrier(2)
    admission_done = Barrier(2)
    summaries: list[AdmissionSummary] = []

    def complete_branch_handoff() -> None:
        def pause_before_successor() -> None:
            handoff_ready.wait(timeout=10)
            admission_done.wait(timeout=10)

        with pg_engine.begin() as connection:
            _complete_stage_in_transaction(
                connection,
                workflow_id=branch_workflow_id,
                pipeline_key="evaluation",
                pipeline_version=1,
                stage_key="execute",
                stage_index=1,
                succeeded=True,
                output_reference="branch:out",
                terminal_summary={"outcome": "succeeded"},
                terminal_reference="branch:out",
                evidence=None,
                successors=(
                    StageSuccessor(
                        stage_key=StageKey("execute"),
                        stage_index=2,
                        input_reference="row:gap",
                    ),
                ),
                completed_at=NOW + timedelta(seconds=1),
                before_next_stage=pause_before_successor,
            )

    def admit_while_handoff_is_open() -> None:
        handoff_ready.wait(timeout=10)
        summaries.append(
            run_admission_pass(
                pg_engine,
                client=_as_dbos_client(_RecordingClient()),
                registry=registry,
                clock=lambda: NOW + timedelta(seconds=2),
            )
        )
        admission_done.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=2) as executor:
        handoff_future = executor.submit(complete_branch_handoff)
        admission_future = executor.submit(admit_while_handoff_is_open)
        handoff_future.result(timeout=10)
        admission_future.result(timeout=10)

    concurrent_summary = summaries[0]
    assert concurrent_summary.skipped_for_barrier >= 1
    assert concurrent_summary.admitted_total == 0
    states = {
        stage_execution_id: state
        for stage_execution_id, state, _attempt in _execution_states(
            pg_engine, schema
        )
    }
    assert states[join_execution_id] == StageExecutionState.READY.value

    after_handoff = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    )
    assert after_handoff.skipped_for_barrier >= 1
    assert states[join_execution_id] == StageExecutionState.READY.value

    with pg_engine.connect() as connection:
        gap_execution_id = connection.execute(
            select(schema.stage_executions.c.stage_execution_id).where(
                schema.stage_executions.c.stage_index == 2
            )
        ).scalar_one()
    with pg_engine.begin() as connection:
        gap = get_stage_execution(
            connection,
            stage_execution_id=gap_execution_id,
        )
        assert gap is not None
        if gap.state is StageExecutionState.READY:
            transition_stage_execution(
                connection,
                stage_execution_id=gap_execution_id,
                new_state=StageExecutionState.ADMITTED,
                updated_at=NOW + timedelta(seconds=4),
            )
        transition_stage_execution(
            connection,
            stage_execution_id=gap_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference="gap:out",
            updated_at=NOW + timedelta(seconds=4),
        )

    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=5),
    )
    assert admitted.admitted_total == 1
    assert admitted.skipped_for_barrier == 0
    final_states = {
        stage_execution_id: state
        for stage_execution_id, state, _attempt in _execution_states(
            pg_engine, schema
        )
    }
    assert (
        final_states[join_execution_id] == StageExecutionState.ADMITTED.value
    )


def test_barrier_cancelled_sibling_blocks_join(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _control(pg_engine, selector=None, capacity=4)
    with pg_engine.begin() as connection:
        run_key = "run-barrier-cancelled"
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key=run_key,
                campaign_key="campaign-1",
                pipeline_key="evaluation",
                pipeline_version=1,
                execution_config_reference="config:1",
                expected_member_count=1,
                created_at=NOW,
            )
        )
        work_item_id = connection.execute(
            schema.work_items.insert()
            .values(
                campaign_key="campaign-1",
                work_key="work-barrier-cancelled",
                origin_run_key=run_key,
                input_reference="seed",
                labels={},
                rank=1,
            )
            .returning(schema.work_items.c.work_item_id)
        ).scalar_one()
        split = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=0,
            input_reference="seed",
            created_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=split.stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference="split:out",
            updated_at=NOW,
        )
        branch = insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=1,
            input_reference="row:a",
            created_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=branch.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=branch.stage_execution_id,
            new_state=StageExecutionState.CANCELLED,
            updated_at=NOW,
        )
        insert_stage_execution(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=3,
            input_reference="join:pending",
            barrier=True,
            created_at=NOW,
        )

    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW,
    )

    assert summary.admitted_total == 0
    assert summary.skipped_for_barrier == 1


def test_label_queue_routes_select_enqueue_queue_name(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    declared = PipelineDefinition(
        key=PipelineKey(f"label-queue-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name="default-queue",
                label_queue_routes=(
                    LabelQueueRoute(
                        selector={"device": "cuda"},
                        queue_name="cuda-queue",
                    ),
                ),
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, max_recovery_attempts=1)
    registry = PipelineRegistry()
    registry.register(pipeline)
    with pg_engine.begin() as connection:
        upsert_stage_control(
            connection,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="execute",
            selector={},
            capacity=2,
            paused=False,
            updated_at=NOW,
        )
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:labels",
        items=(
            WorkInput(
                work_key="cpu-work",
                input_reference="input:cpu",
                labels={"device": "cpu"},
            ),
            WorkInput(
                work_key="cuda-work",
                input_reference="input:cuda",
                labels={"device": "cuda"},
            ),
        ),
        registry=registry,
        engine=pg_engine,
        expected_member_count=2,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    summary = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
    )
    assert summary.admitted_total == 2
    assert {options["queue_name"] for options in client.enqueued} == {
        "default-queue",
        "cuda-queue",
    }
    del schema


def test_admission_pagination_respects_priority_boundary(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    pipeline = PipelineDefinition(
        key=PipelineKey(f"priority-page-{suffix}"),
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
    registry = PipelineRegistry()
    registry.register(pipeline)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=2,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    submit_items(
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        pipeline=pipeline.identity,
        execution_config_reference="config:priority",
        items=(
            WorkInput(
                work_key="work-mid",
                input_reference="input:mid",
                labels={},
                priority=1,
            ),
            WorkInput(
                work_key="work-high",
                input_reference="input:high",
                labels={},
                priority=5,
            ),
            WorkInput(
                work_key="work-top",
                input_reference="input:top",
                labels={},
                priority=0,
            ),
        ),
        registry=registry,
        engine=pg_engine,
        expected_member_count=3,
        clock=lambda: NOW,
    )
    client = _RecordingClient()
    first = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW,
        batch_size=1,
    )
    assert first.admitted_total == 1
    second = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
        batch_size=1,
    )
    assert second.admitted_total == 1
    with pg_engine.connect() as connection:
        admitted_work_keys = (
            connection.execute(
                select(schema.work_items.c.work_key)
                .select_from(schema.stage_executions.join(schema.work_items))
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.ADMITTED.value
                )
                .order_by(schema.stage_executions.c.priority)
            )
            .scalars()
            .all()
        )
        remaining_ready = (
            connection.execute(
                select(schema.work_items.c.work_key)
                .select_from(schema.stage_executions.join(schema.work_items))
                .where(schema.stage_executions.c.state == "ready")
            )
            .scalars()
            .all()
        )
    assert admitted_work_keys == ["work-top", "work-mid"]
    assert remaining_ready == ["work-high"]
    del schema
