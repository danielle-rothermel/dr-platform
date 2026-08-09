from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Lock
from typing import TYPE_CHECKING, cast

from sqlalchemy import Engine, create_engine, select, text, update

from dr_platform._core.identities import (
    PipelineKey,
    RunCompletionKey,
    RunKey,
    StageKey,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)
from dr_platform.completion.barrier import (
    _eligible_runs_statement,
    run_barrier_pass,
)
from dr_platform.completion.execution import inspect_run_completion
from dr_platform.execution.handoff import wrap_pipeline_workflows
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.submission.stream import (
    RunMemberInput,
    RunRegistrationDeclaration,
    WorkInput,
    compute_run_membership_digest,
    submit,
)
from tests.conftest import NOW, _migrate, engine_dsn

if TYPE_CHECKING:
    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy import Connection


async def _stage(input_reference: str) -> str:
    return f"output:{input_reference}"


async def _completion(manifest_reference: str) -> str:
    return f"aggregate:{manifest_reference}"


def _stage_args(payload: object) -> tuple[object, ...]:
    return (payload,)


def _completion_args(payload: object) -> tuple[object, ...]:
    return (payload,)


def _registry(key: str) -> tuple[PipelineRegistry, PipelineDefinition]:
    declared = PipelineDefinition(
        key=PipelineKey(key),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"{key}-execute",
                workflow=_stage,
                args_for=_stage_args,
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name=f"{key}-aggregate",
            workflow=_completion,
            args_for=_completion_args,
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=lambda: NOW)
    registry = PipelineRegistry()
    registry.register(pipeline)
    return registry, pipeline


def _members(*work_indexes: int) -> tuple[RunMemberInput, ...]:
    return tuple(
        RunMemberInput(
            ordinal=ordinal,
            work=WorkInput(
                work_key=f"work-{work_index}",
                input_reference=f"input:{work_index}",
                labels={},
            ),
        )
        for ordinal, work_index in enumerate(work_indexes)
    )


def _submit_run(
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    run_key: str,
    members: tuple[RunMemberInput, ...],
) -> None:
    digest = compute_run_membership_digest(
        members, expected_member_count=len(members)
    )
    submit(
        campaign_key="campaign-1",
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        declaration=RunRegistrationDeclaration(
            len(members), f"manifest:{run_key}", digest
        ),
        members=members,
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )


def _set_states(
    engine: Engine,
    states: dict[str, StageExecutionState],
) -> None:
    schema = StagingSchema()
    with engine.begin() as connection:
        rows = connection.execute(
            select(
                schema.work_items.c.work_key,
                schema.stage_executions.c.stage_execution_id,
            ).select_from(
                schema.work_items.join(
                    schema.stage_executions,
                    schema.work_items.c.work_item_id
                    == schema.stage_executions.c.work_item_id,
                )
            )
        ).tuples()
        by_key = dict(rows.all())
        for work_key, state in states.items():
            values: dict[str, object] = {"state": state.value}
            if state is StageExecutionState.SUCCEEDED:
                values["output_reference"] = f"output:{work_key}"
            connection.execute(
                update(schema.stage_executions)
                .where(
                    schema.stage_executions.c.stage_execution_id
                    == by_key[work_key]
                )
                .values(**values)
            )


class _RecordingClient:
    def __init__(self, *, fail_runs: frozenset[str] = frozenset()) -> None:
        self.fail_runs = fail_runs
        self.enqueued: list[tuple[EnqueueOptions, dict[str, object]]] = []
        self._lock = Lock()

    def enqueue_in_transaction(
        self,
        _connection: Connection,
        options: EnqueueOptions,
        payload: dict[str, object],
    ) -> object:
        if payload["run_key"] in self.fail_runs:
            raise RuntimeError(f"cannot enqueue {payload['run_key']}")
        with self._lock:
            self.enqueued.append(
                (cast("EnqueueOptions", dict(options)), dict(payload))
            )
        return object()


def test_barrier_releases_once_with_compact_immutable_facts(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry("barrier-once")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=_members(0, 1, 2),
    )
    _set_states(
        pg_engine,
        {
            "work-0": StageExecutionState.SUCCEEDED,
            "work-1": StageExecutionState.FAILED,
            "work-2": StageExecutionState.CANCELLED,
        },
    )
    client = _RecordingClient()
    released_at = NOW + timedelta(seconds=1)

    first = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: released_at,
    )
    second = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: released_at + timedelta(seconds=1),
    )

    assert len(first.releases) == 1
    assert first.failures == ()
    assert second.releases == ()
    assert len(client.enqueued) == 1
    options, payload = client.enqueued[0]
    assert options["workflow_id"].startswith("drp-run-")
    assert payload["run_key"] == "run-1"
    assert payload["member_count"] == 3
    assert payload["release_terminal_state_counts"] == [
        {"state": "succeeded", "count": 1},
        {"state": "failed", "count": 1},
        {"state": "cancelled", "count": 1},
    ]
    with pg_engine.connect() as connection:
        run = connection.execute(
            select(
                schema.pipeline_runs.c.released_at,
                schema.pipeline_runs.c.release_terminal_state_counts,
            ).where(schema.pipeline_runs.c.run_key == "run-1")
        ).one()
    assert run.released_at == released_at
    assert inspect_run_completion("run-1", engine=pg_engine).state is (
        RunCompletionExecutionState.ENQUEUED
    )


def test_barrier_waits_for_terminality_then_releases(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-waits")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=_members(0),
    )
    client = _RecordingClient()
    waiting = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    _set_states(pg_engine, {"work-0": StageExecutionState.SUCCEEDED})
    released = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    assert waiting.releases == ()
    assert len(released.releases) == 1


def test_failure_isolated_keyset_continuation_fills_enqueue_batch(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-keyset")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-a",
        members=_members(0),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-b",
        members=_members(1),
    )
    _set_states(
        pg_engine,
        {
            "work-0": StageExecutionState.SUCCEEDED,
            "work-1": StageExecutionState.SUCCEEDED,
        },
    )
    client = _RecordingClient(fail_runs=frozenset({"run-a"}))
    summary = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        batch_size=1,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert [failure.run_key for failure in summary.failures] == [
        RunKey("run-a")
    ]
    assert [release.run_key for release in summary.releases] == [
        RunKey("run-b")
    ]
    assert [payload["run_key"] for _, payload in client.enqueued] == ["run-b"]


def test_overlapping_runs_each_release(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-overlap")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-a",
        members=_members(0, 1),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-b",
        members=_members(1, 2),
    )
    _set_states(
        pg_engine,
        {
            "work-0": StageExecutionState.SUCCEEDED,
            "work-1": StageExecutionState.SUCCEEDED,
            "work-2": StageExecutionState.SUCCEEDED,
        },
    )
    client = _RecordingClient()
    summary = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert {release.run_key for release in summary.releases} == {
        RunKey("run-a"),
        RunKey("run-b"),
    }


def test_concurrent_reconcilers_create_one_execution(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-concurrent")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=_members(0),
    )
    _set_states(pg_engine, {"work-0": StageExecutionState.SUCCEEDED})
    start = Barrier(2)
    client = _RecordingClient()
    engines = (
        create_engine(engine_dsn(pg_engine)),
        create_engine(engine_dsn(pg_engine)),
    )

    def reconcile(engine: Engine):
        start.wait(timeout=10)
        return run_barrier_pass(
            engine,
            client=cast("DBOSClient", client),
            registry=registry,
            clock=lambda: NOW + timedelta(seconds=1),
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = tuple(
                executor.submit(reconcile, engine) for engine in engines
            )
            summaries = tuple(future.result() for future in futures)
    finally:
        for engine in engines:
            engine.dispose()

    assert sum(len(summary.releases) for summary in summaries) == 1
    assert len(client.enqueued) == 1


def test_post_release_member_change_does_not_repeat_completion(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry("barrier-edge-triggered")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-1",
        members=_members(0),
    )
    _set_states(pg_engine, {"work-0": StageExecutionState.FAILED})
    client = _RecordingClient()
    released_at = NOW + timedelta(seconds=1)
    run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: released_at,
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.stage_executions).values(
                state=StageExecutionState.READY.value
            )
        )
    again = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: released_at + timedelta(seconds=1),
    )
    with pg_engine.connect() as connection:
        release = connection.execute(
            select(
                schema.pipeline_runs.c.released_at,
                schema.pipeline_runs.c.release_terminal_state_counts,
            )
        ).one()
    assert again.releases == ()
    assert len(client.enqueued) == 1
    assert release.released_at == released_at
    assert release.release_terminal_state_counts[1] == {
        "state": "failed",
        "count": 1,
    }


def test_empty_completion_run_is_immediately_eligible(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-empty")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-empty",
        members=(),
    )
    client = _RecordingClient()
    summary = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    assert len(summary.releases) == 1
    assert client.enqueued[0][1]["release_terminal_state_counts"] == [
        {"state": "succeeded", "count": 0},
        {"state": "failed", "count": 0},
        {"state": "cancelled", "count": 0},
    ]


def test_barrier_candidate_and_anti_join_indexes_with_sparse_history(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry("barrier-plan")
    completion = pipeline.run_completion
    assert completion is not None
    history_size = 2_000
    terminal_history_size = 500
    closed_at = NOW + timedelta(seconds=1)
    released_at = NOW + timedelta(seconds=2)
    released_counts = [
        {"state": "succeeded", "count": 0},
        {"state": "failed", "count": 0},
        {"state": "cancelled", "count": 0},
    ]
    history_run = {
        "campaign_key": "campaign-history",
        "pipeline_key": pipeline.key.value,
        "pipeline_version": pipeline.version,
        "execution_config_reference": "config:history",
        "created_at": NOW,
    }
    released_runs = [
        history_run
        | {
            "run_key": f"released-{index:04d}",
            "expected_member_count": 0,
            "manifest_reference": f"manifest:released-{index}",
            "membership_digest": f"digest:released-{index}",
            "run_completion_key": completion.key.value,
            "registration_closed_at": closed_at,
            "registered_member_count": 0,
            "created_work_count": 0,
            "reused_work_count": 0,
            "released_at": released_at,
            "release_terminal_state_counts": released_counts,
        }
        for index in range(history_size)
    ]
    item_only_runs = [
        history_run
        | {
            "run_key": f"item-only-{index:04d}",
            "expected_member_count": 0,
            "registration_closed_at": closed_at,
            "registered_member_count": 0,
            "created_work_count": 0,
            "reused_work_count": 0,
        }
        for index in range(history_size)
    ]
    open_runs = [
        history_run
        | {
            "run_key": f"open-{index:04d}",
            "expected_member_count": int(index < terminal_history_size),
            "manifest_reference": f"manifest:open-{index}",
            "membership_digest": f"digest:open-{index}",
            "run_completion_key": completion.key.value,
        }
        for index in range(history_size)
    ]
    with pg_engine.begin() as connection:
        connection.execute(schema.pipeline_runs.insert(), released_runs)
        connection.execute(schema.pipeline_runs.insert(), item_only_runs)
        connection.execute(schema.pipeline_runs.insert(), open_runs)
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                INSERT INTO platform_work_items (
                    campaign_key, work_key, origin_run_key,
                    input_reference, labels, rank
                )
                SELECT
                    'campaign-history',
                    'open-work-' || lpad(history_index::text, 4, '0'),
                    'open-' || lpad(history_index::text, 4, '0'),
                    'input:open-' || history_index::text,
                    '{}'::jsonb,
                    history_index + 1
                FROM generate_series(0, :last_history_index) AS history_index
                RETURNING work_item_id, origin_run_key, rank
                ), inserted_memberships AS (
                INSERT INTO platform_run_memberships (
                    run_key, member_ordinal, work_item_id
                )
                SELECT origin_run_key, 0, work_item_id
                FROM inserted_work
                )
                INSERT INTO platform_stage_executions (
                    work_item_id, stage_key, stage_index, state,
                    current_attempt, rank, created_at, updated_at
                )
                SELECT
                    work_item_id, 'execute', 0, 'failed',
                    0, rank, :created_at, :created_at
                FROM inserted_work
                """
            ),
            {
                "created_at": NOW,
                "last_history_index": terminal_history_size - 1,
            },
        )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-waiting",
        members=_members(*range(2_000)),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-eligible",
        members=_members(3_001),
    )
    _set_states(pg_engine, {"work-3001": StageExecutionState.SUCCEEDED})

    statement = _eligible_runs_statement(
        schema=schema,
        limit=100,
        after=None,
    )
    sql = str(
        statement.compile(
            dialect=pg_engine.dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    with pg_engine.begin() as connection:
        connection.execute(text("ANALYZE"))
        plan = connection.execute(
            text(f"EXPLAIN (FORMAT JSON, COSTS OFF) {sql}")
        ).scalar_one()

    index_names: set[str] = set()

    def collect(value: object) -> None:
        if isinstance(value, dict):
            index_name = value.get("Index Name")
            if isinstance(index_name, str):
                index_names.add(index_name)
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(plan)
    assert any(
        name.endswith("ix_pipeline_runs_completion_candidates")
        for name in index_names
    )
    assert any(
        name.endswith("ix_stage_executions_nonterminal_work")
        for name in index_names
    )
