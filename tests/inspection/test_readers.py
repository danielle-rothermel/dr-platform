from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from dbos import EnqueueOptions
from sqlalchemy import Engine, event, func, select

from dr_platform._core.identities import PipelineKey, StageKey, WorkKey
from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import (
    read_controls,
    set_selector_capacity,
    set_stage_capacity,
)
from dr_platform.admission.runner import run_admission_pass
from dr_platform.inspection.campaigns import (
    inspect_campaign,
    list_campaigns,
    list_runs,
)
from dr_platform.inspection.statuses import (
    StateCount,
    bulk_work_statuses,
    campaign_state_counts,
    run_state_counts,
)
from dr_platform.inspection.work_items import (
    get_work_item_stages,
    list_work_items,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.retry import retry_stage
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    NOW,
    _args_for,
    _as_dbos_client,
    _migrate,
    submit_items,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import StagingSchema


async def _workflow(input_reference: str) -> str:
    return f"output:{input_reference}"


def _registry(*, two_stages: bool = False) -> PipelineRegistry:
    stages = [
        StageDefinition(
            key=StageKey("execute"),
            queue_name="execute-queue",
            workflow=_workflow,
            args_for=_args_for,
        )
    ]
    if two_stages:
        stages.append(
            StageDefinition(
                key=StageKey("score"),
                queue_name="score-queue",
                workflow=_workflow,
                args_for=_args_for,
            )
        )
    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=tuple(stages),
        )
    )
    return registry


def _submit(  # noqa: PLR0913 -- explicit desired-state test facts
    engine: Engine,
    registry: PipelineRegistry,
    *,
    campaign_key: str,
    run_key: str,
    work_keys: tuple[str, ...],
    clock: datetime = NOW,
) -> None:
    submit_items(
        campaign_key=campaign_key,
        run_key=run_key,
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_reference=f"input:{work_key}",
                labels={"kind": "sample"},
            )
            for work_key in work_keys
        ),
        registry=registry,
        engine=engine,
        clock=lambda: clock,
    )


def _seed_reader_data(engine: Engine) -> None:
    registry = _registry()
    _submit(
        engine,
        registry,
        campaign_key="campaign-a",
        run_key="run-a1",
        work_keys=("work-a", "work-b"),
    )
    _submit(
        engine,
        registry,
        campaign_key="campaign-a",
        run_key="run-a2",
        work_keys=("work-c",),
        clock=NOW + timedelta(seconds=1),
    )
    _submit(
        engine,
        registry,
        campaign_key="campaign-b",
        run_key="run-b1",
        work_keys=("work-d",),
        clock=NOW + timedelta(seconds=2),
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


def test_list_campaigns_paginates_by_stable_campaign_key(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)

    first_campaign = list_campaigns(engine=pg_engine, limit=1)
    second_campaign = list_campaigns(
        engine=pg_engine,
        cursor=first_campaign[-1].campaign_key,
        limit=1,
    )

    assert [str(item.campaign_key) for item in first_campaign] == [
        "campaign-a"
    ]
    assert [str(item.campaign_key) for item in second_campaign] == [
        "campaign-b"
    ]


def test_list_runs_paginates_by_stable_run_cursor(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)

    runs = list_runs("campaign-a", engine=pg_engine, limit=1)
    later_runs = list_runs(
        "campaign-a",
        engine=pg_engine,
        cursor=runs[-1].run_key,
        limit=1,
    )

    assert [str(item.run_key) for item in runs + later_runs] == [
        "run-a1",
        "run-a2",
    ]


def test_list_work_items_paginates_current_state_by_stable_item_cursor(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)

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

    assert [str(item.work_key) for item in first_item + later_items] == [
        "work-a",
        "work-b",
        "work-c",
    ]


def test_get_work_item_stages_returns_stage_history(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)
    (item, *_) = list_work_items("campaign-a", engine=pg_engine)

    stages = get_work_item_stages(
        item.work_item_id,
        engine=pg_engine,
    )

    assert len(stages) == 1
    assert stages[0].attempts == ()


def test_read_controls_returns_stage_capacity(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    set_stage_capacity(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    controls = read_controls(
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        stage_key="execute",
        engine=pg_engine,
    )

    assert controls[0].capacity == 4


def test_read_controls_filters_selectors_by_work_item_labels(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    pipeline = PipelineIdentity(PipelineKey("evaluation"), 1)
    set_stage_capacity(
        pipeline=pipeline,
        stage_key="execute",
        capacity=8,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample"},
        capacity=4,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample", "cohort": "blue"},
        capacity=2,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    set_selector_capacity(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "other"},
        capacity=1,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    controls = read_controls(
        pipeline=pipeline,
        stage_key="execute",
        labels={"kind": "sample", "cohort": "blue", "split": "validation"},
        engine=pg_engine,
    )

    assert [
        (dict(control.selector), control.capacity) for control in controls
    ] == [
        ({}, 8),
        ({"kind": "sample"}, 4),
        ({"kind": "sample", "cohort": "blue"}, 2),
    ]


def test_state_counts_report_current_items_for_campaign_and_run(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)

    assert campaign_state_counts("campaign-a", engine=pg_engine)[0].count == 3
    assert run_state_counts("run-a1", engine=pg_engine)[0].count == 2


def test_work_item_summaries_are_deeply_immutable(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)
    item = list_work_items("campaign-a", engine=pg_engine, limit=1)[0]

    with pytest.raises(FrozenInstanceError):
        item.state = StageExecutionState.FAILED  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        cast("dict[str, str]", item.labels)["new"] = "value"


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


def test_list_campaigns_rejects_an_unknown_cursor(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(
        ValueError, match="campaign cursor is unknown among campaigns"
    ):
        list_campaigns(engine=pg_engine, cursor="absent")


def test_list_runs_rejects_an_unknown_campaign(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="campaign is unknown: absent"):
        list_runs("absent", engine=pg_engine)


@pytest.mark.parametrize("cursor", ["absent", "run-b1"])
def test_list_runs_rejects_a_cursor_outside_the_campaign(
    pg_engine: Engine,
    cursor: str,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)

    with pytest.raises(
        ValueError,
        match="run cursor is unknown in this campaign",
    ):
        list_runs("campaign-a", engine=pg_engine, cursor=cursor)


def test_list_work_items_rejects_an_unknown_campaign(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="campaign is unknown: absent"):
        list_work_items("absent", engine=pg_engine)


def test_list_work_items_rejects_an_unknown_cursor(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_reader_data(pg_engine)
    with pg_engine.connect() as connection:
        unknown_cursor = (
            connection.execute(
                select(func.max(schema.work_items.c.work_item_id))
            ).scalar_one()
            + 1
        )

    with pytest.raises(
        ValueError,
        match="work item cursor is unknown in this campaign",
    ):
        list_work_items(
            "campaign-a",
            engine=pg_engine,
            cursor=unknown_cursor,
        )


def test_list_work_items_rejects_a_cross_campaign_cursor(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    _seed_reader_data(pg_engine)
    (campaign_b_item,) = list_work_items(
        "campaign-b",
        engine=pg_engine,
    )

    with pytest.raises(
        ValueError,
        match="work item cursor is unknown in this campaign",
    ):
        list_work_items(
            "campaign-a",
            engine=pg_engine,
            cursor=campaign_b_item.work_item_id,
        )


def _call_list_reader(reader: str, pg_engine: Engine, *, limit: int) -> None:
    if reader == "campaigns":
        list_campaigns(engine=pg_engine, limit=limit)
    elif reader == "runs":
        list_runs("campaign-a", engine=pg_engine, limit=limit)
    else:
        list_work_items("campaign-a", engine=pg_engine, limit=limit)


@pytest.mark.parametrize("reader", ["campaigns", "runs", "work-items"])
def test_inspection_list_readers_reject_a_non_integer_limit(
    pg_engine: Engine,
    reader: str,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(TypeError, match="inspection limit must be an integer"):
        _call_list_reader(reader, pg_engine, limit=True)


@pytest.mark.parametrize("reader", ["campaigns", "runs", "work-items"])
@pytest.mark.parametrize("limit", [0, -1])
def test_inspection_list_readers_reject_a_non_positive_limit(
    pg_engine: Engine,
    reader: str,
    limit: int,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(ValueError, match="inspection limit must be positive"):
        _call_list_reader(reader, pg_engine, limit=limit)


@pytest.mark.parametrize("reader", ["campaigns", "runs", "work-items"])
def test_inspection_list_readers_reject_a_limit_above_the_maximum(
    pg_engine: Engine,
    reader: str,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(ValueError, match="inspection limit must not exceed"):
        _call_list_reader(reader, pg_engine, limit=1_001)


def test_list_work_items_rejects_a_malformed_state(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(TypeError, match="state must be a StageExecutionState"):
        list_work_items(
            "campaign-a",
            engine=pg_engine,
            state=cast("StageExecutionState", "READY"),
        )


def test_get_work_item_stages_rejects_an_unknown_work_item(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="work item does not exist: 42"):
        get_work_item_stages(42, engine=pg_engine)


def test_inspect_campaign_rejects_an_unknown_campaign(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="campaign is unknown: absent"):
        inspect_campaign("absent", engine=pg_engine)


def test_campaign_state_counts_rejects_an_unknown_campaign(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="campaign is unknown: absent"):
        campaign_state_counts("absent", engine=pg_engine)


def test_run_state_counts_rejects_an_unknown_run(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(LookupError, match="run is unknown: absent"):
        run_state_counts("absent", engine=pg_engine)


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

    receipt = submit_items(
        campaign_key="estimate-v1",
        run_key="run-top-up",
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_reference=f"input:{work_key}",
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
    assert receipt.created_work_count == 3
    assert receipt.reused_work_count == 0
    refreshed = bulk_work_statuses(
        "estimate-v1", desired_keys, engine=pg_engine
    ).statuses
    assert sum(status.present for status in refreshed.values()) == 6
    assert refreshed[WorkKey(existing_keys[1])].state is (
        StageExecutionState.READY
    )
    assert (
        get_work_item_stages(
            executions[existing_keys[1]][0], engine=pg_engine
        )[0].execution.current_attempt
        == 2
    )
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                select(func.count())
                .select_from(schema.work_items)
                .where(schema.work_items.c.origin_run_key == "run-top-up")
            ).scalar_one()
            == 3
        )


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
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
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
    assert (
        campaign_state_counts("campaign-attempts", engine=pg_engine)[0].count
        == 1
    )


def test_bulk_statuses_accepts_empty_input(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    result = bulk_work_statuses("campaign-absent", (), engine=pg_engine)

    assert str(result.campaign_key) == "campaign-absent"
    assert tuple(result.statuses.items()) == ()
    with pytest.raises(TypeError):
        cast("dict[WorkKey, object]", result.statuses)[WorkKey("work")] = (
            object()
        )


def test_bulk_statuses_rejects_a_non_integer_chunk_size(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(
        TypeError,
        match="bulk status chunk size must be an integer",
    ):
        bulk_work_statuses(
            "campaign",
            ("work",),
            engine=pg_engine,
            chunk_size=True,
        )


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_bulk_statuses_rejects_a_non_positive_chunk_size(
    pg_engine: Engine,
    chunk_size: int,
) -> None:
    _migrate(pg_engine)

    with pytest.raises(
        ValueError,
        match="bulk status chunk size must be positive",
    ):
        bulk_work_statuses(
            "campaign",
            ("work",),
            engine=pg_engine,
            chunk_size=chunk_size,
        )


def test_bulk_statuses_deduplicates_keys_in_stable_order(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-bulk",
        run_key="run-bulk",
        work_keys=("work-a", "work-b"),
    )

    result = bulk_work_statuses(
        "campaign-bulk",
        ("work-b", "work-a", "work-b"),
        engine=pg_engine,
    )

    assert tuple(result.statuses) == (WorkKey("work-b"), WorkKey("work-a"))
    assert len(result.statuses) == 2
    assert all(status.present for status in result.statuses.values())
    with pytest.raises(TypeError):
        cast("dict[WorkKey, object]", result.statuses)[WorkKey("work-c")] = (
            object()
        )
    with pytest.raises(FrozenInstanceError):
        result.statuses[WorkKey("work-b")].state = (  # ty: ignore[invalid-assignment]
            StageExecutionState.FAILED
        )


def test_bulk_statuses_reports_unknown_campaign_keys_as_absent(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    result = bulk_work_statuses(
        "campaign-absent",
        ("work-a", "work-b"),
        engine=pg_engine,
    )

    assert str(result.campaign_key) == "campaign-absent"
    assert tuple(result.statuses) == (WorkKey("work-a"), WorkKey("work-b"))
    assert all(
        not status.present
        and status.work_item_id is None
        and status.current_stage_key is None
        and status.current_stage_index is None
        and status.state is None
        for status in result.statuses.values()
    )


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


def test_current_state_readers_ignore_earlier_terminal_stages(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry(two_stages=True)
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-multistage",
        run_key="run-ready",
        work_keys=("work-ready",),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-multistage",
        run_key="run-failed",
        work_keys=("work-failed",),
        clock=NOW + timedelta(seconds=1),
    )
    first_stages = _execution_by_work_key(pg_engine, schema)
    with pg_engine.begin() as connection:
        for work_key in ("work-ready", "work-failed"):
            work_item_id, stage_execution_id = first_stages[work_key]
            _terminalize(
                connection,
                stage_execution_id=stage_execution_id,
                state=StageExecutionState.SUCCEEDED,
                at=NOW + timedelta(seconds=10),
            )
            second_stage = insert_stage_execution(
                connection,
                work_item_id=work_item_id,
                stage_key="score",
                stage_index=1,
                created_at=NOW + timedelta(seconds=12),
            )
            if work_key == "work-failed":
                _terminalize(
                    connection,
                    stage_execution_id=second_stage.stage_execution_id,
                    state=StageExecutionState.FAILED,
                    at=NOW + timedelta(seconds=13),
                )

    ready_items = list_work_items(
        "campaign-multistage",
        engine=pg_engine,
        state=StageExecutionState.READY,
    )
    failed_items = list_work_items(
        "campaign-multistage",
        engine=pg_engine,
        state=StageExecutionState.FAILED,
    )
    succeeded_items = list_work_items(
        "campaign-multistage",
        engine=pg_engine,
        state=StageExecutionState.SUCCEEDED,
    )
    statuses = bulk_work_statuses(
        "campaign-multistage",
        ("work-ready", "work-failed"),
        engine=pg_engine,
    ).statuses

    assert [str(item.work_key) for item in ready_items] == ["work-ready"]
    assert [str(item.work_key) for item in failed_items] == ["work-failed"]
    assert succeeded_items == ()
    assert {
        key: (
            status.current_stage_key,
            status.current_stage_index,
            status.state,
        )
        for key, status in statuses.items()
    } == {
        WorkKey("work-ready"): (
            StageKey("score"),
            1,
            StageExecutionState.READY,
        ),
        WorkKey("work-failed"): (
            StageKey("score"),
            1,
            StageExecutionState.FAILED,
        ),
    }
    assert {
        count.state: count.count
        for count in campaign_state_counts(
            "campaign-multistage",
            engine=pg_engine,
        )
    } == {
        StageExecutionState.READY: 1,
        StageExecutionState.FAILED: 1,
    }
    assert run_state_counts("run-ready", engine=pg_engine) == (
        StateCount(state=StageExecutionState.READY, count=1),
    )
    assert run_state_counts("run-failed", engine=pg_engine) == (
        StateCount(state=StageExecutionState.FAILED, count=1),
    )


def test_bulk_reader_scopes_current_stage_by_campaign_with_interleaved_ids(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry = _registry()
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-x",
        run_key="run-x1",
        work_keys=("work-x1",),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-y",
        run_key="run-y1",
        work_keys=("work-y1",),
        clock=NOW + timedelta(seconds=1),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-x",
        run_key="run-x2",
        work_keys=("work-x2",),
        clock=NOW + timedelta(seconds=2),
    )
    _submit(
        pg_engine,
        registry,
        campaign_key="campaign-y",
        run_key="run-y2",
        work_keys=("work-y2",),
        clock=NOW + timedelta(seconds=3),
    )
    executions = _execution_by_work_key(pg_engine, schema)
    terminalized_at = NOW + timedelta(seconds=10)
    with pg_engine.begin() as connection:
        _terminalize(
            connection,
            stage_execution_id=executions["work-y1"][1],
            state=StageExecutionState.SUCCEEDED,
            at=terminalized_at,
        )
        _terminalize(
            connection,
            stage_execution_id=executions["work-y2"][1],
            state=StageExecutionState.FAILED,
            at=terminalized_at,
        )

    x_statuses = bulk_work_statuses(
        "campaign-x", ("work-x1", "work-x2"), engine=pg_engine
    ).statuses
    y_statuses = bulk_work_statuses(
        "campaign-y", ("work-y1", "work-y2"), engine=pg_engine
    ).statuses

    assert x_statuses[WorkKey("work-x1")].state is StageExecutionState.READY
    assert x_statuses[WorkKey("work-x2")].state is StageExecutionState.READY
    assert y_statuses[WorkKey("work-y1")].state is (
        StageExecutionState.SUCCEEDED
    )
    assert y_statuses[WorkKey("work-y2")].state is StageExecutionState.FAILED
    assert campaign_state_counts("campaign-x", engine=pg_engine) == (
        StateCount(state=StageExecutionState.READY, count=2),
    )
    y_counts = {
        entry.state: entry.count
        for entry in campaign_state_counts("campaign-y", engine=pg_engine)
    }
    assert y_counts == {
        StageExecutionState.SUCCEEDED: 1,
        StageExecutionState.FAILED: 1,
    }
