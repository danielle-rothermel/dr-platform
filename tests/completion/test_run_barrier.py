from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, Lock
from typing import TYPE_CHECKING, Any, cast

import pytest
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
    from collections.abc import Iterator

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
    pipeline = wrap_pipeline_workflows(
        declared, clock=lambda: NOW, max_recovery_attempts=1
    )
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


def _barrier_cursor(engine: Engine) -> str | None:
    schema = StagingSchema()
    with engine.connect() as connection:
        return connection.execute(
            select(schema.run_barrier_cursor.c.last_run_key)
        ).scalar_one()


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


def _plan_nodes(value: object) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        if isinstance(value.get("Node Type"), str):
            yield cast("dict[str, Any]", value)
        for child in value.values():
            yield from _plan_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _plan_nodes(child)


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


def test_failed_release_consumes_budget_rotates_and_remains_retryable(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-failure-rotation")
    for index, run_key in enumerate(("run-a", "run-b", "run-c")):
        _submit_run(
            pg_engine,
            registry,
            pipeline,
            run_key=run_key,
            members=_members(index),
        )
    _set_states(
        pg_engine,
        {
            "work-0": StageExecutionState.SUCCEEDED,
            "work-1": StageExecutionState.SUCCEEDED,
            "work-2": StageExecutionState.SUCCEEDED,
        },
    )
    failing = _RecordingClient(fail_runs=frozenset({"run-a"}))

    first = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", failing),
        registry=registry,
        batch_size=1,
        candidate_budget=1,
    )
    second = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", failing),
        registry=registry,
        batch_size=1,
        candidate_budget=1,
    )
    third = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", failing),
        registry=registry,
        batch_size=1,
        candidate_budget=1,
    )
    recovered = _RecordingClient()
    fourth = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", recovered),
        registry=registry,
        batch_size=1,
        candidate_budget=1,
    )

    assert first.candidates_examined == 1
    assert [failure.run_key for failure in first.failures] == [RunKey("run-a")]
    assert [release.run_key for release in second.releases] == [
        RunKey("run-b")
    ]
    assert [release.run_key for release in third.releases] == [RunKey("run-c")]
    assert [release.run_key for release in fourth.releases] == [
        RunKey("run-a")
    ]


def test_all_blocked_first_page_advances_to_later_eligible_run(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-blocked-page")
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-a-blocked",
        members=_members(0),
    )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-b-eligible",
        members=_members(1),
    )
    _set_states(pg_engine, {"work-1": StageExecutionState.SUCCEEDED})
    client = _RecordingClient()

    summary = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        batch_size=1,
        clock=lambda: NOW + timedelta(seconds=1),
    )

    assert summary.failures == ()
    assert [release.run_key for release in summary.releases] == [
        RunKey("run-b-eligible")
    ]


def test_candidate_budget_bounds_blocked_population_and_next_pass_resumes(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-budget")
    for index in range(7):
        _submit_run(
            pg_engine,
            registry,
            pipeline,
            run_key=f"run-{index:02d}-blocked",
            members=_members(index),
        )
    _submit_run(
        pg_engine,
        registry,
        pipeline,
        run_key="run-07-eligible",
        members=_members(7),
    )
    _set_states(pg_engine, {"work-7": StageExecutionState.SUCCEEDED})
    client = _RecordingClient()

    first = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        batch_size=1,
        candidate_budget=4,
        clock=lambda: NOW + timedelta(seconds=1),
    )
    second = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", client),
        registry=registry,
        batch_size=1,
        candidate_budget=4,
        clock=lambda: NOW + timedelta(seconds=2),
    )

    assert first.cursor_acquired
    assert first.candidates_examined == 4
    assert first.releases == ()
    assert second.candidates_examined == 4
    assert [release.run_key for release in second.releases] == [
        RunKey("run-07-eligible")
    ]
    assert _barrier_cursor(pg_engine) == "run-07-eligible"


def test_wraparound_stops_at_original_cursor_without_duplicate_evaluation(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry("barrier-wrap")
    for index in range(3):
        _submit_run(
            pg_engine,
            registry,
            pipeline,
            run_key=f"run-{index}",
            members=_members(index),
        )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.run_barrier_cursor).values(last_run_key="run-1")
        )

    summary = run_barrier_pass(
        pg_engine,
        client=cast("DBOSClient", _RecordingClient()),
        registry=registry,
        batch_size=1,
        candidate_budget=3,
    )

    assert summary.releases == ()
    assert summary.candidates_examined == 3
    assert _barrier_cursor(pg_engine) == "run-1"


def test_missing_required_cursor_fails_loudly(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    registry, _pipeline = _registry("barrier-missing-cursor")
    with pg_engine.begin() as connection:
        connection.execute(schema.run_barrier_cursor.delete())

    with pytest.raises(
        RuntimeError, match="required run barrier cursor row is missing"
    ):
        run_barrier_pass(
            pg_engine,
            client=cast("DBOSClient", _RecordingClient()),
            registry=registry,
        )


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


def test_cursor_lock_loser_does_not_scan_or_duplicate_completion(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, pipeline = _registry("barrier-concurrent")
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

    class GatedClient(_RecordingClient):
        def __init__(self) -> None:
            super().__init__()
            self.first_transaction_entered = Event()
            self.release_first_transaction = Event()

        def enqueue_in_transaction(
            self,
            _connection: Connection,
            options: EnqueueOptions,
            payload: dict[str, object],
        ) -> object:
            if payload["run_key"] == "run-a":
                self.first_transaction_entered.set()
                if not self.release_first_transaction.wait(timeout=10):
                    raise TimeoutError("first reconciler was not released")
            return super().enqueue_in_transaction(
                _connection, options, payload
            )

    client = GatedClient()
    engines = (
        create_engine(engine_dsn(pg_engine)),
        create_engine(engine_dsn(pg_engine)),
    )

    def reconcile_first():
        return run_barrier_pass(
            engines[0],
            client=cast("DBOSClient", client),
            registry=registry,
            batch_size=1,
            clock=lambda: NOW + timedelta(seconds=1),
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            first = executor.submit(reconcile_first)
            assert client.first_transaction_entered.wait(timeout=10)
            second = run_barrier_pass(
                engines[1],
                client=cast("DBOSClient", client),
                registry=registry,
                batch_size=1,
                clock=lambda: NOW + timedelta(seconds=1),
            )
            client.release_first_transaction.set()
            first_summary = first.result()
            third = run_barrier_pass(
                engines[1],
                client=cast("DBOSClient", client),
                registry=registry,
                batch_size=1,
                clock=lambda: NOW + timedelta(seconds=2),
            )
    finally:
        client.release_first_transaction.set()
        for engine in engines:
            engine.dispose()

    assert first_summary.failures == ()
    assert second.failures == ()
    assert second.cursor_acquired is False
    assert second.candidates_examined == 0
    assert second.releases == ()
    assert third.failures == ()
    assert {
        release.run_key
        for summary in (first_summary, third)
        for release in summary.releases
    } == {RunKey("run-a"), RunKey("run-b")}
    assert sorted(payload["run_key"] for _, payload in client.enqueued) == [
        "run-a",
        "run-b",
    ]


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


def test_barrier_candidate_page_and_probes_are_row_bounded(  # noqa: PLR0915 -- PostgreSQL plan fixture
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, pipeline = _registry("barrier-plan")
    completion = pipeline.run_completion
    assert completion is not None
    history_size = 500
    closed_at = NOW + timedelta(seconds=1)
    released_at = NOW + timedelta(seconds=2)
    released_counts = [
        {"state": "succeeded", "count": 0},
        {"state": "failed", "count": 1},
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
            "expected_member_count": 1,
            "manifest_reference": f"manifest:released-{index}",
            "membership_digest": f"digest:released-{index}",
            "run_completion_key": completion.key.value,
        }
        for index in range(history_size)
    ]
    item_only_runs = [
        history_run
        | {
            "run_key": f"item-only-{index:04d}",
            "expected_member_count": 1,
        }
        for index in range(history_size)
    ]
    open_runs = [
        history_run
        | {
            "run_key": f"open-{index:04d}",
            "expected_member_count": 1,
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
                WITH history_kind(kind) AS (
                VALUES ('released'), ('item-only'), ('open')
                ), inserted_work AS (
                INSERT INTO platform_work_items (
                    campaign_key, work_key, origin_run_key,
                    input_reference, labels, rank
                )
                SELECT
                    'campaign-history',
                    kind || '-work-' || lpad(history_index::text, 4, '0'),
                    kind || '-' || lpad(history_index::text, 4, '0'),
                    'input:' || kind || '-' || history_index::text,
                    '{}'::jsonb,
                    row_number() OVER (ORDER BY kind, history_index)
                FROM history_kind
                CROSS JOIN generate_series(
                    0, :last_history_index
                ) AS history_index
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
                    work_item_id, 'execute', 0, 'ready',
                    0, rank, :created_at, :created_at
                FROM inserted_work
                """
            ),
            {
                "created_at": NOW,
                "last_history_index": history_size - 1,
            },
        )
        connection.execute(
            update(schema.pipeline_runs)
            .where(schema.pipeline_runs.c.run_key.like("item-only-%"))
            .values(
                registration_closed_at=closed_at,
                registered_member_count=1,
                created_work_count=1,
                reused_work_count=0,
            )
        )
        connection.execute(
            update(schema.pipeline_runs)
            .where(schema.pipeline_runs.c.run_key.like("released-%"))
            .values(
                registration_closed_at=closed_at,
                registered_member_count=1,
                created_work_count=1,
                reused_work_count=0,
                released_at=released_at,
                release_terminal_state_counts=released_counts,
            )
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

    page_size = 100
    statement = _eligible_runs_statement(
        schema=schema,
        limit=page_size,
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
        evaluated = tuple(connection.execute(statement).mappings())
        plan = connection.execute(
            text(
                "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, "
                f"TIMING OFF, SUMMARY OFF) {sql}"
            )
        ).scalar_one()

    nodes = tuple(_plan_nodes(plan))
    candidate_nodes = tuple(
        node
        for node in nodes
        if node.get("Subplan Name") == "CTE candidate_runs"
    )
    evaluated_nodes = tuple(
        node
        for node in nodes
        if node.get("Subplan Name") == "CTE evaluated_runs"
    )
    locked_nodes = tuple(
        node for node in nodes if node.get("Subplan Name") == "CTE locked_runs"
    )
    membership_nodes = tuple(
        node
        for node in nodes
        if node.get("Relation Name") == "platform_run_memberships"
    )
    execution_nodes = tuple(
        node
        for node in nodes
        if node.get("Relation Name") == "platform_stage_executions"
    )
    assert len(candidate_nodes) == 1
    assert len(evaluated_nodes) == 1
    assert len(locked_nodes) == 1
    assert len(membership_nodes) == 1
    assert len(execution_nodes) == 1
    candidate = candidate_nodes[0]
    evaluated_cte = evaluated_nodes[0]
    locked_cte = locked_nodes[0]
    evaluated_plans = evaluated_cte.get("Plans")
    assert isinstance(evaluated_plans, list)
    lateral_limits = tuple(
        node
        for node in evaluated_plans
        if isinstance(node, dict)
        and node.get("Node Type") == "Limit"
        and node.get("Parent Relationship") == "Inner"
    )
    assert len(lateral_limits) == 1
    lateral_limit = lateral_limits[0]
    lateral_node_ids = {id(node) for node in _plan_nodes(lateral_limit)}
    candidate_scans = tuple(
        node
        for node in _plan_nodes(evaluated_cte)
        if node.get("Node Type") == "CTE Scan"
        and node.get("CTE Name") == "candidate_runs"
    )
    candidate_relation_nodes = tuple(
        node
        for node in _plan_nodes(candidate)
        if node.get("Relation Name") == "platform_pipeline_runs"
    )
    membership = membership_nodes[0]
    execution = execution_nodes[0]
    assert [row["run_key"] for row in evaluated] == [
        "run-eligible",
        "run-waiting",
    ]
    assert [row["eligible"] for row in evaluated] == [True, False]
    assert [row["locked"] for row in evaluated] == [True, False]
    assert candidate.get("Node Type") == "Limit"
    assert candidate.get("Actual Loops") == 1
    assert candidate.get("Actual Rows") == len(evaluated) <= page_size
    assert len(candidate_relation_nodes) == 1
    assert (
        candidate_relation_nodes[0]
        .get("Index Name", "")
        .endswith("ix_pipeline_runs_completion_candidates")
    )
    assert evaluated_cte.get("Node Type") == "Nested Loop"
    assert evaluated_cte.get("Join Type") == "Left"
    assert evaluated_cte.get("Actual Loops") == 1
    assert evaluated_cte.get("Actual Rows") == len(evaluated)
    assert len(candidate_scans) == 1
    assert candidate_scans[0].get("Actual Loops") == 1
    assert candidate_scans[0].get("Actual Rows") == len(evaluated)
    assert lateral_limit.get("Actual Loops") == len(evaluated)
    assert id(membership) in lateral_node_ids
    assert membership.get("Node Type") == "Index Only Scan"
    assert membership.get("Index Name", "").endswith("_run_work")
    assert "run_key = candidate_runs.run_key" in membership.get(
        "Index Cond", ""
    )
    assert membership.get("Actual Loops") == len(evaluated)
    assert id(execution) in lateral_node_ids
    assert execution.get("Node Type") == "Index Only Scan"
    assert execution.get("Index Name", "").endswith(
        "ix_stage_executions_nonterminal_work"
    )
    assert "platform_run_memberships.work_item_id" in execution.get(
        "Index Cond", ""
    )
    assert execution.get("Actual Loops") == len(evaluated)
    assert locked_cte.get("Node Type") == "LockRows"
    assert locked_cte.get("Actual Loops") == 1
    assert locked_cte.get("Actual Rows") == 1
