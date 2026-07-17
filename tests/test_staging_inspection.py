"""PostgreSQL behavior proofs for staging inspection and desired state."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from dbos import DBOSClient, EnqueueOptions
from sqlalchemy import Engine, event, func, select

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging import (
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkKey,
)
from dr_platform.staging.admission import AdmissionPayload, run_admission_pass
from dr_platform.staging.inspection import (
    bulk_work_statuses,
    campaign_state_counts,
    get_work_item_stages,
    inspect_campaign,
    list_campaigns,
    list_runs,
    list_work_items,
    read_controls,
    run_state_counts,
)
from dr_platform.staging.operations import retry_stage, set_stage_capacity
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import (
    append_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform.staging.stage_executions import transition_stage_execution
from dr_platform.staging.submission import WorkInput, submit
from tests.conftest import engine_dsn

if TYPE_CHECKING:
    from sqlalchemy import Connection

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _workflow(input_ref: str) -> str:
    return f"output:{input_ref}"


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


def _submit(  # noqa: PLR0913 -- explicit desired-state test facts
    engine: Engine,
    registry: PipelineRegistry,
    *,
    campaign_key: str,
    run_key: str,
    work_keys: tuple[str, ...],
    clock: datetime = NOW,
) -> None:
    submit(
        campaign_key=campaign_key,
        run_key=run_key,
        pipeline=(PipelineKey("evaluation"), 1),
        config_ref="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_ref=f"input:{work_key}",
                labels={"kind": "sample"},
            )
            for work_key in work_keys
        ),
        registry=registry,
        engine=engine,
        clock=lambda: clock,
    )


class _RecordingClient:
    def __init__(self) -> None:
        self.enqueued: list[EnqueueOptions] = []

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        *_args: object,
        **_kwargs: object,
    ) -> object:
        self.enqueued.append(cast("EnqueueOptions", dict(options)))
        return object()


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


def _execution_by_work_key(
    engine: Engine,
    schema: StagingSchema,
) -> dict[str, tuple[int, int]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.work_items.c.work_key,
                schema.work_items.c.work_item_id,
                schema.stage_executions.c.stage_execution_id,
            ).select_from(
                schema.work_items.join(
                    schema.stage_executions,
                    schema.work_items.c.work_item_id
                    == schema.stage_executions.c.work_item_id,
                )
            )
        ).tuples()
        return {row[0]: (row[1], row[2]) for row in rows}


def _terminalize(
    connection: Connection,
    *,
    stage_execution_id: int,
    state: StageExecutionState,
    at: datetime,
) -> None:
    attempt = append_stage_attempt(
        connection,
        stage_execution_id=stage_execution_id,
        created_at=at,
        admitted_at=at,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        new_state=StageExecutionState.ADMITTED,
        updated_at=at,
    )
    if state is StageExecutionState.SUCCEEDED:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=state,
            output_reference="output:done",
            updated_at=at + timedelta(seconds=1),
        )
    else:
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=state,
            updated_at=at + timedelta(seconds=1),
        )
    record_stage_attempt_terminal(
        connection,
        stage_execution_id=stage_execution_id,
        attempt_number=attempt.attempt_number,
        terminal_at=at + timedelta(seconds=1),
        terminal_summary={"outcome": state.value},
    )


def test_inspection_readers_are_bounded_cursor_stable_and_frozen(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-a",
        run_key="run-a1",
        work_keys=("work-a", "work-b"),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-a",
        run_key="run-a2",
        work_keys=("work-c",),
        clock=NOW + timedelta(seconds=1),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-b",
        run_key="run-b1",
        work_keys=("work-d",),
        clock=NOW + timedelta(seconds=2),
    )
    set_stage_capacity(
        pipeline=(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    first_campaign = list_campaigns(engine=pg_engine, limit=1)
    second_campaign = list_campaigns(
        engine=pg_engine,
        cursor=first_campaign[-1].campaign_key,
        limit=1,
    )
    runs = list_runs("campaign-a", engine=pg_engine, limit=1)
    later_runs = list_runs(
        "campaign-a",
        engine=pg_engine,
        cursor=runs[-1].run_key,
        limit=1,
    )
    first_item = list_work_items(
        "campaign-a",
        engine=pg_engine,
        state=StageExecutionState.READY,
        limit=1,
    )
    later_items = list_work_items(
        "campaign-a",
        engine=pg_engine,
        state=StageExecutionState.READY,
        cursor=first_item[-1].work_item_id,
        limit=2,
    )
    stages = get_work_item_stages(
        first_item[0].work_item_id,
        engine=pg_engine,
    )
    controls = read_controls(
        pipeline=(PipelineKey("evaluation"), 1),
        stage_key="execute",
        engine=pg_engine,
    )

    assert [str(item.campaign_key) for item in first_campaign] == [
        "campaign-a"
    ]
    assert [str(item.campaign_key) for item in second_campaign] == [
        "campaign-b"
    ]
    assert [str(item.run_key) for item in runs + later_runs] == [
        "run-a1",
        "run-a2",
    ]
    assert len(first_item + later_items) == 3
    assert len(stages) == 1
    assert stages[0].attempts == ()
    assert controls[0].capacity == 4
    assert campaign_state_counts(
        "campaign-a", engine=pg_engine
    )[0].count == 3
    assert run_state_counts("run-a1", engine=pg_engine)[0].count == 2
    with pytest.raises(FrozenInstanceError):
        first_item[0].state = StageExecutionState.FAILED  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        cast("dict[str, str]", first_item[0].labels)["new"] = "value"


def test_list_campaigns_counts_runs_and_items_independently(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-counts",
        run_key="run-1",
        work_keys=("work-1", "work-2", "work-3"),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-counts",
        run_key="run-2",
        work_keys=("work-4", "work-5"),
        clock=NOW + timedelta(seconds=1),
    )

    (campaign,) = list_campaigns(engine=pg_engine)

    assert str(campaign.campaign_key) == "campaign-counts"
    assert campaign.created_at == NOW
    assert campaign.run_count == 2
    assert campaign.work_item_count == 5


def test_inspect_campaign_rejects_an_unknown_campaign(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(ValueError, match="campaign is unknown: absent"):
        inspect_campaign("absent", engine=pg_engine)


def test_six_seed_top_up_uses_bulk_statuses_for_one_campaign(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    existing_keys = tuple(f"model-a/seed-{seed}" for seed in range(3))
    desired_keys = tuple(f"model-a/seed-{seed}" for seed in range(6))
    _submit(
        pg_engine,
        registry,
        campaign_key="estimate-v1",
        run_key="run-initial",
        work_keys=existing_keys,
    )
    executions = _execution_by_work_key(pg_engine, schema)
    with pg_engine.begin() as connection:
        _terminalize(
            connection,
            stage_execution_id=executions[existing_keys[0]][1],
            state=StageExecutionState.SUCCEEDED,
            at=NOW,
        )
        _terminalize(
            connection,
            stage_execution_id=executions[existing_keys[1]][1],
            state=StageExecutionState.FAILED,
            at=NOW,
        )
        append_stage_attempt(
            connection,
            stage_execution_id=executions[existing_keys[2]][1],
            created_at=NOW,
            admitted_at=NOW,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=executions[existing_keys[2]][1],
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW,
        )

    statuses = bulk_work_statuses(
        "estimate-v1",
        desired_keys,
        engine=pg_engine,
        chunk_size=2,
    ).statuses
    absent: list[str] = []
    reserved: list[str] = []
    for raw_key in desired_keys:
        key = WorkKey(raw_key)
        status = statuses[key]
        if not status.present:
            absent.append(raw_key)
        elif status.state is StageExecutionState.SUCCEEDED:
            continue
        elif status.state in {
            StageExecutionState.READY,
            StageExecutionState.ADMITTED,
        }:
            reserved.append(raw_key)
        elif status.state is StageExecutionState.FAILED:
            assert status.work_item_id is not None
            stage = get_work_item_stages(
                status.work_item_id,
                engine=pg_engine,
            )[-1]
            retry_stage(
                stage.execution.stage_execution_id,
                engine=pg_engine,
                clock=lambda: NOW + timedelta(seconds=2),
            )
        else:
            absent.append(raw_key)

    receipt = submit(
        campaign_key="estimate-v1",
        run_key="run-top-up",
        pipeline=(PipelineKey("evaluation"), 1),
        config_ref="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_ref=f"input:{work_key}",
                labels={"kind": "sample"},
            )
            for work_key in absent
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert absent == list(desired_keys[3:])
    assert reserved == [existing_keys[2]]
    assert receipt.inserted_count == 3
    assert receipt.already_existing_count == 0
    refreshed = bulk_work_statuses(
        "estimate-v1", desired_keys, engine=pg_engine
    ).statuses
    assert sum(status.present for status in refreshed.values()) == 6
    assert refreshed[WorkKey(existing_keys[1])].state is (
        StageExecutionState.READY
    )
    assert get_work_item_stages(
        executions[existing_keys[1]][0], engine=pg_engine
    )[0].execution.current_attempt == 2
    with pg_engine.connect() as connection:
        assert connection.execute(
            select(func.count())
            .select_from(schema.work_items)
            .where(schema.work_items.c.origin_run_key == "run-top-up")
        ).scalar_one() == 3


def test_attempts_never_count_as_additional_samples(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-attempts",
        run_key="run-attempts",
        work_keys=("sample-1",),
    )
    execution = _execution_by_work_key(pg_engine, schema)["sample-1"]
    with pg_engine.begin() as connection:
        _terminalize(
            connection,
            stage_execution_id=execution[1],
            state=StageExecutionState.FAILED,
            at=NOW,
        )
    retry_stage(
        execution[1],
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    set_stage_capacity(
        pipeline=(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=3),
    )

    campaigns = list_campaigns(engine=pg_engine)
    items = list_work_items("campaign-attempts", engine=pg_engine)
    status = bulk_work_statuses(
        "campaign-attempts", ("sample-1",), engine=pg_engine
    ).statuses[WorkKey("sample-1")]
    stages = get_work_item_stages(execution[0], engine=pg_engine)

    assert campaigns[0].work_item_count == 1
    assert len(items) == 1
    assert status.present is True
    assert status.state is StageExecutionState.ADMITTED
    assert len(stages[0].attempts) == 2
    assert campaign_state_counts(
        "campaign-attempts", engine=pg_engine
    )[0].count == 1


def test_bulk_reader_chunks_and_reports_absent_keys(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-bulk",
        run_key="run-bulk",
        work_keys=tuple(f"work-{index}" for index in range(5)),
    )
    requested = tuple(f"work-{index}" for index in range(7))
    select_count = 0

    def count_selects(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: object,
    ) -> None:
        nonlocal select_count
        if statement.lstrip().upper().startswith("SELECT"):
            select_count += 1

    event.listen(pg_engine, "before_cursor_execute", count_selects)
    try:
        result = bulk_work_statuses(
            "campaign-bulk",
            requested,
            engine=pg_engine,
            chunk_size=2,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", count_selects)

    assert select_count == 4
    assert len(result.statuses) == 7
    for key in requested[:5]:
        assert result.statuses[WorkKey(key)].present is True
    for key in requested[5:]:
        absent = result.statuses[WorkKey(key)]
        assert absent.present is False
        assert absent.state is None
        assert absent.current_stage_key is None
        assert absent.work_item_id is None
    with pytest.raises(TypeError):
        cast("dict[WorkKey, object]", result.statuses)[
            WorkKey(requested[0])
        ] = object()
