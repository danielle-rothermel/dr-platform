"""PostgreSQL guarantees for streaming staged submission."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, func, select

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging import (
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageExecutionState,
    StageKey,
)
from dr_platform.staging.runs import (
    PipelineRunConflictError,
    get_pipeline_run,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.submission import WorkInput, submit
from tests.conftest import engine_dsn

NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _workflow(*args: object) -> object:
    return args


def _args_for(*args: object) -> tuple[object, ...]:
    return args


def _registry() -> tuple[PipelineRegistry, PipelineDefinition]:
    pipeline = PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("prepare"),
                queue_name="prepare",
                workflow=_workflow,
                args_for=_args_for,
            ),
            StageDefinition(
                key=StageKey("execute"),
                queue_name="execute",
                workflow=_workflow,
                args_for=_args_for,
            ),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    return registry, pipeline


def _item(index: int) -> WorkInput:
    return WorkInput(
        work_key=f"work-{index}",
        input_ref=f"input:{index}",
        labels={"cohort": "blue"},
    )


def _migrate(engine: Engine) -> StagingSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return StagingSchema()


def test_source_commits_before_it_is_exhausted(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    observed_counts: list[int] = []

    def items() -> Iterator[WorkInput]:
        yield _item(0)
        yield _item(1)
        with pg_engine.connect() as connection:
            observed_counts.append(
                connection.execute(
                    select(func.count()).select_from(schema.work_items)
                ).scalar_one()
            )
        yield _item(2)

    receipt = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
        items=items(),
        registry=registry,
        engine=pg_engine,
        chunk_size=2,
        clock=lambda: NOW,
    )

    assert observed_counts == [2]
    assert receipt.inserted_count == 3
    assert receipt.already_existing_count == 0


def test_interrupted_replay_fills_only_absent_keys(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()

    def interrupted_items() -> Iterator[WorkInput]:
        yield _item(0)
        yield _item(1)
        yield _item(2)
        raise RuntimeError("producer interrupted")

    with pytest.raises(RuntimeError, match="producer interrupted"):
        submit(
            campaign_key="campaign-1",
            run_key="run-1",
            pipeline=pipeline.identity,
            config_ref="config:1",
            items=interrupted_items(),
            registry=registry,
            engine=pg_engine,
            chunk_size=2,
            clock=lambda: NOW,
        )

    with pg_engine.connect() as connection:
        interrupted_run = get_pipeline_run(connection, run_key="run-1")
        committed_count = connection.execute(
            select(func.count()).select_from(schema.work_items)
        ).scalar_one()

    assert interrupted_run is not None
    assert interrupted_run.submission_completed_at is None
    assert committed_count == 2

    receipt = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
        items=(_item(index) for index in range(4)),
        registry=registry,
        engine=pg_engine,
        chunk_size=2,
        clock=lambda: NOW,
    )

    with pg_engine.connect() as connection:
        completed_run = get_pipeline_run(connection, run_key="run-1")
        final_count = connection.execute(
            select(func.count()).select_from(schema.work_items)
        ).scalar_one()

    assert receipt.inserted_count == 2
    assert receipt.already_existing_count == 2
    assert completed_run is not None
    assert completed_run.submission_completed_at == NOW
    assert final_count == 4


def test_config_mismatched_resume_is_rejected(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
        items=(),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    with pytest.raises(PipelineRunConflictError):
        submit(
            campaign_key="campaign-1",
            run_key="run-1",
            pipeline=pipeline.identity,
            config_ref="config:other",
            items=(),
            registry=registry,
            engine=pg_engine,
            clock=lambda: NOW,
        )


def test_new_items_have_only_the_first_stage_ready(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    observed_while_streaming: list[tuple[str, int, str]] = []

    def items() -> Iterator[WorkInput]:
        yield _item(0)
        yield _item(1)
        with pg_engine.connect() as connection:
            observed_while_streaming.extend(
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.state,
                    ).order_by(
                        schema.stage_executions.c.stage_execution_id
                    )
                ).tuples()
            )
        yield _item(2)

    submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
        items=items(),
        registry=registry,
        engine=pg_engine,
        chunk_size=2,
        clock=lambda: NOW,
    )

    with pg_engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.stage_executions.c.stage_key,
                schema.stage_executions.c.stage_index,
                schema.stage_executions.c.state,
            ).order_by(schema.stage_executions.c.stage_execution_id)
        ).all()

    expected_committed_rows = [
        ("prepare", 0, StageExecutionState.READY.value),
        ("prepare", 0, StageExecutionState.READY.value),
    ]
    assert observed_while_streaming == expected_committed_rows
    assert rows == [
        *expected_committed_rows,
        ("prepare", 0, StageExecutionState.READY.value),
    ]


def test_cross_run_campaign_duplicates_converge(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    first = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
        items=(_item(0),),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )
    second = submit(
        campaign_key="campaign-1",
        run_key="run-2",
        pipeline=pipeline.identity,
        config_ref="config:2",
        items=(_item(0),),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    with pg_engine.connect() as connection:
        work_count = connection.execute(
            select(func.count()).select_from(schema.work_items)
        ).scalar_one()
        stage_count = connection.execute(
            select(func.count()).select_from(schema.stage_executions)
        ).scalar_one()
        origin_run_key = connection.execute(
            select(schema.work_items.c.origin_run_key)
        ).scalar_one()

    assert (first.inserted_count, first.already_existing_count) == (1, 0)
    assert (second.inserted_count, second.already_existing_count) == (0, 1)
    assert (work_count, stage_count, origin_run_key) == (1, 1, "run-1")
