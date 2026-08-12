from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from sqlalchemy import Engine, func, select, true, update
from sqlalchemy.dialects.postgresql import insert

from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
)
from dr_platform._core.ledger.completion_attempts import (
    create_initial_run_completion_attempt,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)
from dr_platform._core.validation import validate_positive_integer
from dr_platform.completion.execution import (
    RunCompletionPayload,
    is_run_completion_wrapped,
    run_completion_workflow_id,
)
from dr_platform.inspection.statuses import StateCount

if TYPE_CHECKING:
    from collections.abc import Callable

    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform.pipeline.definitions import RunCompletionDefinition
    from dr_platform.pipeline.registry import PipelineRegistry

DEFAULT_RUN_BARRIER_BATCH_SIZE = 10_000
DEFAULT_RUN_BARRIER_CANDIDATE_BUDGET = 100_000
_TERMINAL_STATES = (
    StageExecutionState.SUCCEEDED,
    StageExecutionState.FAILED,
    StageExecutionState.CANCELLED,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunBarrierRelease:
    run_key: RunKey
    workflow_id: str


@dataclass(frozen=True, slots=True)
class RunBarrierFailure:
    run_key: RunKey
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class RunBarrierSummary:
    releases: tuple[RunBarrierRelease, ...]
    failures: tuple[RunBarrierFailure, ...]
    cursor_acquired: bool
    candidates_examined: int


@dataclass(frozen=True, slots=True)
class _Candidate:
    run_key: str
    campaign_key: str
    pipeline_key: str
    pipeline_version: int
    execution_config_reference: str
    manifest_reference: str
    membership_digest: str
    run_completion_key: str
    expected_member_count: int


@dataclass(frozen=True, slots=True)
class _CandidatePage:
    last_run_key: str | None
    evaluated_count: int
    locked_eligible: tuple[_Candidate, ...]


def run_barrier_pass(  # noqa: PLR0913 -- explicit reconciliation boundary
    database: Engine | Connection,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int = DEFAULT_RUN_BARRIER_BATCH_SIZE,
    candidate_budget: int = DEFAULT_RUN_BARRIER_CANDIDATE_BUDGET,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> RunBarrierSummary:
    validate_positive_integer(batch_size, label="run barrier batch size")
    validate_positive_integer(
        candidate_budget, label="run barrier candidate budget"
    )
    if candidate_budget < batch_size:
        raise ValueError(
            "run barrier candidate budget must be at least the batch size"
        )
    selected_schema = schema or StagingSchema()
    if isinstance(database, Engine):
        with database.begin() as connection:
            return _run_in_transaction(
                connection,
                client=client,
                registry=registry,
                batch_size=batch_size,
                candidate_budget=candidate_budget,
                clock=clock,
                schema=selected_schema,
            )
    with database.begin():
        return _run_in_transaction(
            database,
            client=client,
            registry=registry,
            batch_size=batch_size,
            candidate_budget=candidate_budget,
            clock=clock,
            schema=selected_schema,
        )


def _run_in_transaction(  # noqa: PLR0913
    connection: Connection,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int,
    candidate_budget: int,
    clock: Callable[[], datetime],
    schema: StagingSchema,
) -> RunBarrierSummary:
    cursor = _acquire_cursor(connection, schema=schema)
    if cursor is _CURSOR_NOT_ACQUIRED:
        return RunBarrierSummary(
            releases=(),
            failures=(),
            cursor_acquired=False,
            candidates_examined=0,
        )
    original_cursor = cast("str | None", cursor)
    releases: list[RunBarrierRelease] = []
    failures: list[RunBarrierFailure] = []
    examined = 0
    after = original_cursor
    upper_bound: str | None = None
    wrapped = False
    last_examined: str | None = None
    while len(releases) < batch_size and examined < candidate_budget:
        page = _lock_eligible_page(
            connection,
            schema=schema,
            limit=min(batch_size - len(releases), candidate_budget - examined),
            after=after,
            upper_bound=upper_bound,
        )
        if page.last_run_key is None:
            if wrapped or original_cursor is None:
                break
            after = None
            upper_bound = original_cursor
            wrapped = True
            continue
        after = page.last_run_key
        last_examined = page.last_run_key
        examined += page.evaluated_count
        candidates = page.locked_eligible
        if not candidates:
            continue
        counts = _terminal_counts(
            connection,
            schema=schema,
            run_keys=tuple(candidate.run_key for candidate in candidates),
        )
        for candidate in candidates:
            try:
                with connection.begin_nested():
                    release = _release_candidate(
                        connection,
                        candidate=candidate,
                        state_counts=counts[candidate.run_key],
                        client=client,
                        registry=registry,
                        released_at=clock(),
                        schema=schema,
                    )
            except Exception as error:  # noqa: BLE001 -- candidate boundary
                failures.append(
                    RunBarrierFailure(
                        run_key=RunKey(candidate.run_key),
                        error_type=type(error).__name__,
                        message=_failure_message(error),
                    )
                )
            else:
                releases.append(release)
                if len(releases) >= batch_size:
                    break
    if last_examined is not None:
        connection.execute(
            update(schema.run_barrier_cursor)
            .where(schema.run_barrier_cursor.c.singleton.is_(True))
            .values(last_run_key=last_examined)
        )
    return RunBarrierSummary(
        releases=tuple(releases),
        failures=tuple(failures),
        cursor_acquired=True,
        candidates_examined=examined,
    )


_CURSOR_NOT_ACQUIRED = object()


def _acquire_cursor(
    connection: Connection, *, schema: StagingSchema
) -> str | None | object:
    row = connection.execute(
        select(schema.run_barrier_cursor.c.last_run_key)
        .where(schema.run_barrier_cursor.c.singleton.is_(True))
        .with_for_update(skip_locked=True)
    ).one_or_none()
    if row is not None:
        return row.last_run_key
    cursor_exists = connection.execute(
        select(true())
        .select_from(schema.run_barrier_cursor)
        .where(schema.run_barrier_cursor.c.singleton.is_(True))
    ).scalar_one_or_none()
    if cursor_exists:
        return _CURSOR_NOT_ACQUIRED
    raise RuntimeError("required run barrier cursor row is missing")


def _lock_eligible_page(
    connection: Connection,
    *,
    schema: StagingSchema,
    limit: int,
    after: str | None,
    upper_bound: str | None,
) -> _CandidatePage:
    statement = _eligible_runs_statement(
        schema=schema,
        limit=limit,
        after=after,
        upper_bound=upper_bound,
    )
    rows = tuple(connection.execute(statement).mappings())
    return _CandidatePage(
        last_run_key=rows[-1]["run_key"] if rows else None,
        evaluated_count=len(rows),
        locked_eligible=tuple(
            _decode_candidate(row)
            for row in rows
            if row["eligible"] and row["locked"]
        ),
    )


def _eligible_runs_statement(
    *,
    schema: StagingSchema,
    limit: int,
    after: str | None,
    upper_bound: str | None = None,
):
    runs = schema.pipeline_runs
    memberships = schema.run_memberships
    executions = schema.stage_executions
    candidate_predicates = (
        runs.c.registration_closed_at.is_not(None),
        runs.c.run_completion_key.is_not(None),
        runs.c.released_at.is_(None),
    )
    candidate_runs_statement = (
        select(
            runs.c.run_key,
            runs.c.campaign_key,
            runs.c.pipeline_key,
            runs.c.pipeline_version,
            runs.c.execution_config_reference,
            runs.c.manifest_reference,
            runs.c.membership_digest,
            runs.c.run_completion_key,
            runs.c.expected_member_count,
        )
        .where(*candidate_predicates)
        .order_by(runs.c.run_key)
        .limit(limit)
    )
    if after is not None:
        candidate_runs_statement = candidate_runs_statement.where(
            runs.c.run_key > after
        )
    if upper_bound is not None:
        candidate_runs_statement = candidate_runs_statement.where(
            runs.c.run_key <= upper_bound
        )
    candidate_runs = candidate_runs_statement.cte(
        "candidate_runs"
    ).prefix_with("MATERIALIZED")
    nonterminal = (
        select(true().label("present"))
        .select_from(
            memberships.join(
                executions,
                memberships.c.work_item_id == executions.c.work_item_id,
            )
        )
        .where(
            memberships.c.run_key == candidate_runs.c.run_key,
            executions.c.state.in_(
                (
                    StageExecutionState.READY.value,
                    StageExecutionState.ADMITTED.value,
                )
            ),
        )
        .limit(1)
        .lateral("nonterminal")
    )
    evaluated_runs = (
        select(
            *candidate_runs.c,
            nonterminal.c.present.is_(None).label("eligible"),
        )
        .select_from(candidate_runs.outerjoin(nonterminal, true()))
        .cte("evaluated_runs")
        .prefix_with("MATERIALIZED")
    )
    locked_runs = (
        select(runs.c.run_key)
        .select_from(
            runs.join(
                evaluated_runs,
                runs.c.run_key == evaluated_runs.c.run_key,
            )
        )
        .where(evaluated_runs.c.eligible.is_(True), *candidate_predicates)
        .order_by(runs.c.run_key)
        .with_for_update(of=runs, skip_locked=True)
        .cte("locked_runs")
        .prefix_with("MATERIALIZED")
    )
    return (
        select(
            *evaluated_runs.c,
            locked_runs.c.run_key.is_not(None).label("locked"),
        )
        .select_from(
            evaluated_runs.outerjoin(
                locked_runs,
                evaluated_runs.c.run_key == locked_runs.c.run_key,
            )
        )
        .order_by(evaluated_runs.c.run_key)
    )


def _terminal_counts(
    connection: Connection,
    *,
    schema: StagingSchema,
    run_keys: tuple[str, ...],
) -> dict[str, tuple[StateCount, ...]]:
    empty = {
        run_key: tuple(
            StateCount(state=state, count=0) for state in _TERMINAL_STATES
        )
        for run_key in run_keys
    }
    if not run_keys:
        return empty
    memberships = schema.run_memberships
    executions = schema.stage_executions
    scoped = (
        select(memberships.c.run_key, memberships.c.work_item_id)
        .where(memberships.c.run_key.in_(run_keys))
        .subquery()
    )
    current = (
        select(
            scoped.c.run_key,
            scoped.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .select_from(
            scoped.join(
                executions,
                scoped.c.work_item_id == executions.c.work_item_id,
            )
        )
        .group_by(scoped.c.run_key, scoped.c.work_item_id)
        .subquery()
    )
    rows = connection.execute(
        select(
            current.c.run_key,
            executions.c.state,
            func.count().label("count"),
        )
        .select_from(
            current.join(
                executions,
                (current.c.work_item_id == executions.c.work_item_id)
                & (current.c.stage_index == executions.c.stage_index),
            )
        )
        .group_by(current.c.run_key, executions.c.state)
    ).mappings()
    grouped: dict[str, dict[StageExecutionState, int]] = {
        run_key: {} for run_key in run_keys
    }
    for row in rows:
        grouped[row["run_key"]][StageExecutionState(row["state"])] = row[
            "count"
        ]
    return {
        run_key: tuple(
            StateCount(state=state, count=grouped[run_key].get(state, 0))
            for state in _TERMINAL_STATES
        )
        for run_key in run_keys
    }


def _release_candidate(  # noqa: PLR0913 -- explicit release facts
    connection: Connection,
    *,
    candidate: _Candidate,
    state_counts: tuple[StateCount, ...],
    client: DBOSClient,
    registry: PipelineRegistry,
    released_at: datetime,
    schema: StagingSchema,
) -> RunBarrierRelease:
    if (
        sum(item.count for item in state_counts)
        != candidate.expected_member_count
    ):
        raise RuntimeError(
            "run barrier terminal counts do not cover membership"
        )
    pipeline = registry.get(
        key=PipelineKey(candidate.pipeline_key),
        version=candidate.pipeline_version,
    )
    completion = pipeline.run_completion
    if (
        completion is None
        or completion.key.value != candidate.run_completion_key
        or not is_run_completion_wrapped(completion)
    ):
        raise RuntimeError("persisted run completion disagrees with registry")
    workflow_id = run_completion_workflow_id(
        run_key=RunKey(candidate.run_key),
        pipeline_key=pipeline.key,
        pipeline_version=pipeline.version,
        completion_key=completion.key,
        attempt_number=1,
    )
    serialized_counts = [item.model_dump(mode="json") for item in state_counts]
    run_row = connection.execute(
        update(schema.pipeline_runs)
        .where(
            schema.pipeline_runs.c.run_key == candidate.run_key,
            schema.pipeline_runs.c.released_at.is_(None),
        )
        .values(
            released_at=released_at,
            release_terminal_state_counts=serialized_counts,
        )
        .returning(schema.pipeline_runs.c.run_key)
    ).scalar_one_or_none()
    if run_row is None:
        raise RuntimeError("run barrier was already released")
    executions = schema.run_completion_executions
    execution_row = connection.execute(
        insert(executions)
        .values(
            run_key=candidate.run_key,
            current_attempt=1,
            state=RunCompletionExecutionState.ENQUEUED.value,
            enqueued_at=released_at,
        )
        .returning(executions.c.run_completion_execution_id)
    ).scalar_one()
    create_initial_run_completion_attempt(
        connection,
        run_completion_execution_id=execution_row,
        run_key=RunKey(candidate.run_key),
        pipeline_key=pipeline.key,
        pipeline_version=pipeline.version,
        completion_key=completion.key,
        created_at=released_at,
        enqueued_at=released_at,
        schema=schema,
    )
    payload = RunCompletionPayload(
        campaign_key=CampaignKey(candidate.campaign_key),
        run_key=RunKey(candidate.run_key),
        pipeline_key=PipelineKey(candidate.pipeline_key),
        pipeline_version=candidate.pipeline_version,
        execution_config_reference=candidate.execution_config_reference,
        manifest_reference=candidate.manifest_reference,
        membership_digest=candidate.membership_digest,
        member_count=candidate.expected_member_count,
        released_at=released_at,
        release_terminal_state_counts=state_counts,
    )
    options: EnqueueOptions = {
        "workflow_name": _workflow_name(completion),
        "queue_name": completion.queue_name,
        "workflow_id": workflow_id,
    }
    client.enqueue_in_transaction(
        connection, options, payload.model_dump(mode="json")
    )
    return RunBarrierRelease(
        run_key=RunKey(candidate.run_key), workflow_id=workflow_id
    )


def _workflow_name(completion: RunCompletionDefinition) -> str:
    name = getattr(
        completion.workflow,
        "dbos_function_name",
        getattr(completion.workflow, "__name__", None),
    )
    if not isinstance(name, str) or not name:
        raise TypeError("run completion workflow must expose a DBOS name")
    return name


def _decode_candidate(row: RowMapping) -> _Candidate:
    return _Candidate(
        run_key=row["run_key"],
        campaign_key=row["campaign_key"],
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        execution_config_reference=row["execution_config_reference"],
        manifest_reference=cast("str", row["manifest_reference"]),
        membership_digest=cast("str", row["membership_digest"]),
        run_completion_key=cast("str", row["run_completion_key"]),
        expected_member_count=row["expected_member_count"],
    )


def _failure_message(error: Exception) -> str:
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- broken exception rendering
        return f"unprintable {type(error).__name__}"
