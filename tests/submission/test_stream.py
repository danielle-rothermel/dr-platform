from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine, event, func, select
from sqlalchemy.exc import IntegrityError

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    StageKey,
)
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.submission.runs import (
    PipelineRunConflictError,
    get_pipeline_run,
)
from dr_platform.submission.stream import (
    RegistrationClosureError,
    RunMemberInput,
    RunMembershipConflictError,
    RunRegistrationDeclaration,
    WorkInput,
    _membership_digest_for_inputs,
    submit,
)
from tests.conftest import NOW, _migrate


async def _workflow(input_reference: str) -> str:
    return f"output:{input_reference}"


def _args_for(payload: object) -> tuple[object, ...]:
    return (payload,)


def _registry(
    *, completion: bool = False
) -> tuple[PipelineRegistry, PipelineDefinition]:
    run_completion = (
        RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name="aggregate",
            workflow=_workflow,
            args_for=_args_for,
        )
        if completion
        else None
    )
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
        run_completion=run_completion,
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    return registry, pipeline


def _member(index: int, *, ordinal: int | None = None) -> RunMemberInput:
    return RunMemberInput(
        ordinal=index if ordinal is None else ordinal,
        work=WorkInput(
            work_key=f"work-{index}",
            input_reference=f"input:{index}",
            labels={"cohort": "blue"},
        ),
    )


def _submit(  # noqa: PLR0913 -- concise test setup
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    run_key: str,
    members: tuple[RunMemberInput, ...],
    execution_config_reference: str = "config:1",
    declaration: RunRegistrationDeclaration | None = None,
    chunk_size: int = 500,
):
    return submit(
        campaign_key="campaign-1",
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference=execution_config_reference,
        declaration=(
            declaration
            if declaration is not None
            else RunRegistrationDeclaration(len(members))
        ),
        members=members,
        registry=registry,
        engine=engine,
        chunk_size=chunk_size,
        clock=lambda: NOW,
    )


def test_source_commits_chunks_before_registration_exhausts(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    observed_counts: list[int] = []

    def members() -> Iterator[RunMemberInput]:
        yield _member(0)
        yield _member(1)
        with pg_engine.connect() as connection:
            observed_counts.append(
                connection.execute(
                    select(func.count()).select_from(schema.run_memberships)
                ).scalar_one()
            )
        yield _member(2)

    receipt = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(3),
        members=members(),
        registry=registry,
        engine=pg_engine,
        chunk_size=2,
        clock=lambda: NOW,
    )

    assert observed_counts == [2]
    assert receipt.registered_member_count == 3
    assert receipt.created_work_count == 3
    assert receipt.reused_work_count == 0


def test_interrupted_registration_replays_its_exact_prefix(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()

    def interrupted() -> Iterator[RunMemberInput]:
        yield _member(0)
        yield _member(1)
        raise RuntimeError("producer interrupted")

    with pytest.raises(RuntimeError, match="producer interrupted"):
        submit(
            campaign_key="campaign-1",
            run_key="run-1",
            pipeline=pipeline.identity,
            execution_config_reference="config:1",
            declaration=RunRegistrationDeclaration(3),
            members=interrupted(),
            registry=registry,
            engine=pg_engine,
            chunk_size=1,
            clock=lambda: NOW,
        )

    with pg_engine.connect() as connection:
        open_run = get_pipeline_run(connection, run_key="run-1")
        assert open_run is not None
        assert open_run.registration_closed_at is None
        assert (
            connection.execute(
                select(func.count()).select_from(schema.run_memberships)
            ).scalar_one()
            == 2
        )

    resumed = _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=tuple(_member(index) for index in range(3)),
        declaration=RunRegistrationDeclaration(3),
        chunk_size=1,
    )
    assert resumed.registered_member_count == 3
    assert resumed.created_work_count == 3


def test_matching_closed_replay_is_constant_time_and_does_not_touch_input(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    first = _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0),),
    )

    def must_not_iterate() -> Iterator[RunMemberInput]:
        raise AssertionError("closed replay consumed its member stream")
        yield _member(0)

    replay = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(1),
        members=must_not_iterate(),
        registry=registry,
        engine=pg_engine,
        clock=lambda: datetime(2026, 7, 18, 12, tzinfo=UTC),
    )
    assert replay == first


def test_changed_declaration_is_rejected_before_touching_input(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0),),
    )

    def must_not_iterate() -> Iterator[RunMemberInput]:
        raise AssertionError("conflicting replay consumed input")
        yield _member(0)

    with pytest.raises(PipelineRunConflictError):
        submit(
            campaign_key="campaign-1",
            run_key="run-1",
            pipeline=pipeline.identity,
            execution_config_reference="config:1",
            declaration=RunRegistrationDeclaration(2),
            members=must_not_iterate(),
            registry=registry,
            engine=pg_engine,
            clock=lambda: NOW,
        )


def test_overlapping_runs_record_independent_membership_and_reuse(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    first = _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0), _member(1)),
    )
    second = _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-2",
        members=(_member(1, ordinal=0), _member(2, ordinal=1)),
    )

    with pg_engine.connect() as connection:
        memberships = (
            connection.execute(
                select(
                    schema.run_memberships.c.run_key,
                    func.count().label("count"),
                )
                .group_by(schema.run_memberships.c.run_key)
                .order_by(schema.run_memberships.c.run_key)
            )
            .tuples()
            .all()
        )
        work_count = connection.execute(
            select(func.count()).select_from(schema.work_items)
        ).scalar_one()

    assert first.created_work_count == 2
    assert second.created_work_count == 1
    assert second.reused_work_count == 1
    assert memberships == [("run-1", 2), ("run-2", 2)]
    assert work_count == 3


def test_reuse_rejects_different_execution_provenance(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0),),
    )
    with pytest.raises(
        RunMembershipConflictError, match="incompatible execution provenance"
    ):
        _submit(
            pg_engine,
            registry,
            pipeline,
            run_key="run-2",
            members=(_member(0),),
            execution_config_reference="config:other",
        )


def test_membership_digest_has_a_pinned_canonical_representation() -> None:
    digest = _membership_digest_for_inputs(
        (_member(0), _member(1)), expected_member_count=2
    )
    assert digest == (
        "f4e204efade37fe9e8ceea948429d25d3dc206ff769c191f72c4befdbcfae71e"
    )


def test_incorrect_digest_leaves_registration_open(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    with pytest.raises(RegistrationClosureError, match="digest"):
        _submit(
            pg_engine,
            registry,
            pipeline,
            run_key="run-1",
            members=(_member(0),),
            declaration=RunRegistrationDeclaration(1, "manifest:1", "0" * 64),
        )
    with pg_engine.connect() as connection:
        run = get_pipeline_run(connection, run_key="run-1")
    assert run is not None
    assert run.registration_closed_at is None


@pytest.mark.parametrize(
    "members",
    [
        (_member(0), _member(1, ordinal=0)),
        (_member(0), _member(0, ordinal=1)),
        (_member(0, ordinal=1),),
    ],
)
def test_registration_rejects_invalid_membership(
    pg_engine: Engine, members: tuple[RunMemberInput, ...]
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()
    with pytest.raises((RunMembershipConflictError, RegistrationClosureError)):
        _submit(
            pg_engine,
            registry,
            pipeline,
            run_key="run-invalid",
            members=members,
        )


def test_completion_manifest_is_required_before_registration_write(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry(completion=True)

    def must_not_iterate() -> Iterator[RunMemberInput]:
        raise AssertionError("invalid completion registration consumed input")
        yield _member(0)

    with pytest.raises(ValueError, match="manifest reference"):
        submit(
            campaign_key="campaign-1",
            run_key="run-1",
            pipeline=pipeline.identity,
            execution_config_reference="config:1",
            declaration=RunRegistrationDeclaration(1),
            members=must_not_iterate(),
            registry=registry,
            engine=pg_engine,
            clock=lambda: NOW,
        )
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                select(func.count()).select_from(schema.pipeline_runs)
            ).scalar_one()
            == 0
        )


def test_registration_chunk_statement_count_is_cardinality_independent(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry()

    def count_statements(run_key: str, count: int) -> int:
        statements = 0

        def before_cursor_execute(*_args: object) -> None:
            nonlocal statements
            statements += 1

        event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
        try:
            _submit(
                pg_engine,
                registry,
                pipeline,
                run_key=run_key,
                members=tuple(
                    _member(index + (0 if count == 1 else 1), ordinal=index)
                    for index in range(count)
                ),
                chunk_size=count,
            )
        finally:
            event.remove(
                pg_engine, "before_cursor_execute", before_cursor_execute
            )
        return statements

    assert count_statements("run-one", 1) == count_statements("run-500", 500)


def test_closed_membership_rejects_database_mutation(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0),),
    )
    with (
        pytest.raises(IntegrityError, match="closed run membership"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            schema.run_memberships.delete().where(
                schema.run_memberships.c.run_key == "run-1"
            )
        )


def test_new_work_starts_only_the_first_stage(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry()
    _submit(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=(_member(0), _member(1)),
    )
    with pg_engine.connect() as connection:
        rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                ).order_by(schema.stage_executions.c.stage_execution_id)
            )
            .tuples()
            .all()
        )
    assert rows == [
        ("prepare", 0, StageExecutionState.READY.value),
        ("prepare", 0, StageExecutionState.READY.value),
    ]
