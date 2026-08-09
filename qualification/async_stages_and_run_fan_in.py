from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from importlib.metadata import version
from time import monotonic, perf_counter
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from dbos import DBOS, DBOSClient, Queue
from dbos._dbos import _get_dbos_instance
from sqlalchemy import Engine, create_engine, func, make_url, select, text
from sqlalchemy.pool import QueuePool

from dr_platform import (
    AdmissionPayload,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    PlatformDbosConfig,
    RunCompletionDefinition,
    RunCompletionKey,
    RunCompletionPayload,
    RunMemberInput,
    RunRegistrationDeclaration,
    StageDefinition,
    StageExecutionState,
    StageKey,
    StagingSchema,
    WorkInput,
    cancel_work,
    compute_run_membership_digest,
    initialize_dbos_runtime,
    register_scheduled_dispatcher,
    set_stage_capacity,
    submit,
    upgrade_platform_schema,
    wrap_pipeline_workflows,
)
from dr_platform.admission.runner import run_admission_pass
from dr_platform.completion.barrier import (
    _eligible_runs_statement,
    run_barrier_pass,
)
from dr_platform.inspection.campaigns import _run_summary_statement, list_runs

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
    from concurrent.futures import Future

    from sqlalchemy.engine import URL

    from dr_platform.execution._checkpoint import _LedgerCheckpointExecutor
    from dr_platform.submission.stream import SubmissionReceipt

ADMISSION_BATCH_SIZE = 200
BARRIER_BATCH_SIZE = 20
BARRIER_CANDIDATE_BUDGET = 20
SCHEDULE_INTERVAL_SECONDS = 5.0
DECLARED_SCHEDULE_HEADROOM_PERCENT = 44.0
DECLARED_ADMISSIONS_PER_HOUR = 100_000
DECLARED_COMPLETIONS_PER_HOUR = 10_000
HANDOFF_SERVICE_RATE_BOUND_SECONDS = (
    ADMISSION_BATCH_SIZE * 3_600 / DECLARED_ADMISSIONS_PER_HOUR
)
CANCELLATION_COUNT = 20
LOOP_PROBE_INTERVAL_SECONDS = 0.01
QUALIFICATION_BOUND_SECONDS = 5.0
WATCHDOG_SECONDS = 90.0
BARRIER_WAITING_RUNS = 10_000
BARRIER_RELEASED_RUNS = 10_000
BARRIER_LARGE_RUN_MEMBERS = 2_000
LIST_HISTORY_RUNS = 10_000
LIST_HISTORY_MEMBERS_PER_RUN = 25
LIST_PAGE_SIZE = 20
LIST_PAGE_MEMBERS_PER_RUN = 2


@dataclass(frozen=True, slots=True)
class Distribution:
    count: int
    minimum_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    maximum_ms: float


@dataclass(frozen=True, slots=True)
class SchedulerResult:
    batch_size: int
    interval_seconds: float
    declared_workload_per_hour: int
    configured_service_rate_per_hour: float
    configured_headroom_percent: float
    measured_pass_seconds: float
    measured_burst_rate_per_hour: float
    qualified: bool


@dataclass(frozen=True, slots=True)
class PlannerResult:
    backlog: Mapping[str, int]
    result_keys: tuple[str, ...]
    execution_ms: float
    index_names: tuple[str, ...]
    rounded_plan_row_estimates: Mapping[str, int]
    qualified: bool


@dataclass(frozen=True, slots=True)
class InstrumentationResult:
    checkpoint_queue_delay: Distribution
    checkpoint_active_workers_peak: int
    checkpoint_worker_limit: int
    stage_checkpoint_submissions: int
    completion_checkpoint_submissions: int
    unknown_checkpoint_submissions: int
    stage_ledger_pool_wait: Distribution
    completion_ledger_pool_wait: Distribution
    unknown_ledger_pool_wait_count: int
    event_loop_lag: Distribution
    app_pool_size: int
    app_pool_max_overflow: int


@dataclass(frozen=True, slots=True)
class BurstResult:
    expected_burst: int
    successful_handoffs: int
    cancellations: int
    one_loop_reused: bool
    cleanup_complete: bool
    cancellation_exact_delegations_verified: int
    cancelled_coroutines_active_before_release: int
    cancelled_coroutines_cleaned_before_release: int
    cancellation_request_to_cleanup: Distribution
    release_to_cleanup: Distribution
    cancelled_late_handoff_attempts: int
    late_return_fenced: bool
    runtime_shutdown_cleanup_complete: bool
    runtime_shutdown_cleanup_seconds: float
    handoff_seconds: float
    handoffs_per_second: float
    instrumentation: InstrumentationResult
    qualified: bool


@dataclass(frozen=True, slots=True)
class QualificationResult:
    schema_version: int
    qualified: bool
    run_at: str
    provenance: Mapping[str, object]
    declared_workload: Mapping[str, object]
    acceptance_bounds: Mapping[str, object]
    admission: SchedulerResult
    burst: BurstResult
    barrier: SchedulerResult
    barrier_planner: PlannerResult
    list_runs_planner: PlannerResult


class _BurstProbe:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.loop: asyncio.AbstractEventLoop | None = None
        self.release = asyncio.Event()
        self.stop_probe = asyncio.Event()
        self.condition = threading.Condition()
        self.entered: set[int] = set()
        self.cleaned_at: dict[int, float] = {}
        self.loop_lags: list[float] = []
        self.probe_stopped = threading.Event()
        self._probe_task: asyncio.Task[None] | None = None

    async def run(self, index: int) -> str:
        loop = asyncio.get_running_loop()
        with self.condition:
            if self.loop is None:
                self.loop = loop
                self._probe_task = loop.create_task(self._measure_loop_lag())
            elif self.loop is not loop:
                raise RuntimeError("burst workflows used multiple event loops")
            self.entered.add(index)
            self.condition.notify_all()
        try:
            await self.release.wait()
            return f"output:{index}"
        finally:
            with self.condition:
                self.cleaned_at[index] = perf_counter()
                self.condition.notify_all()

    async def _measure_loop_lag(self) -> None:
        loop = asyncio.get_running_loop()
        expected = loop.time() + LOOP_PROBE_INTERVAL_SECONDS
        try:
            while not self.stop_probe.is_set():
                await asyncio.sleep(LOOP_PROBE_INTERVAL_SECONDS)
                observed = loop.time()
                self.loop_lags.append(max(0.0, observed - expected))
                expected = observed + LOOP_PROBE_INTERVAL_SECONDS
        finally:
            self.probe_stopped.set()

    def wait_for_entered(self) -> None:
        self._wait_for(lambda: len(self.entered) == self.expected)

    def wait_for_cleaned(self, expected: set[int]) -> None:
        self._wait_for(lambda: expected <= self.cleaned_at.keys())

    def release_workflows(self) -> None:
        if self.loop is None:
            raise RuntimeError("workflow loop was not captured")
        self.loop.call_soon_threadsafe(self.release.set)

    def finish_probe(self) -> None:
        if self.loop is None:
            raise RuntimeError("workflow loop was not captured")
        self.loop.call_soon_threadsafe(self.stop_probe.set)
        if not self.probe_stopped.wait(WATCHDOG_SECONDS):
            raise TimeoutError("event-loop probe did not stop")

    def _wait_for(self, predicate: Callable[[], bool]) -> None:
        deadline = monotonic() + WATCHDOG_SECONDS
        with self.condition:
            while not predicate():
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("burst state did not reach its gate")
                self.condition.wait(timeout=remaining)


class _CheckpointInstrumentation:
    def __init__(self) -> None:
        self.checkpoint_queue_delays: list[float] = []
        self.pool_waits = {
            "stage": [],
            "completion": [],
            "unknown": [],
        }
        self._lock = threading.Lock()
        self._active = threading.local()
        self._app_pool: QueuePool | None = None
        self._checkpoint_pool: ThreadPoolExecutor | None = None
        self._worker_limit = 0
        self._active_workers = 0
        self._active_workers_peak = 0
        self._submitted = {"stage": 0, "completion": 0, "unknown": 0}
        self._completed = {"stage": 0, "completion": 0, "unknown": 0}
        self._workflow_submitted_at: dict[str, dict[str, float]] = {
            "stage": {},
            "completion": {},
            "unknown": {},
        }
        self._condition = threading.Condition(self._lock)
        self._original_submit = ThreadPoolExecutor.submit
        self._original_do_get = QueuePool._do_get

    @property
    def active_workers_peak(self) -> int:
        with self._lock:
            return self._active_workers_peak

    @property
    def worker_limit(self) -> int:
        return self._worker_limit

    def submissions(self, kind: str) -> int:
        with self._lock:
            return self._submitted[kind]

    def workflow_submissions(self, kind: str) -> Mapping[str, float]:
        with self._lock:
            return dict(self._workflow_submitted_at[kind])

    def wait_for_completed(self, *, kind: str, expected: int) -> None:
        deadline = monotonic() + WATCHDOG_SECONDS
        with self._condition:
            while self._completed[kind] < expected:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"only {self._completed[kind]}/{expected} "
                        f"{kind} checkpoints completed"
                    )
                self._condition.wait(timeout=remaining)

    def select_runtime(
        self,
        *,
        app_pool: QueuePool,
        checkpoint_executor: _LedgerCheckpointExecutor,
    ) -> None:
        self._app_pool = app_pool
        self._checkpoint_pool = checkpoint_executor._executor
        self._worker_limit = checkpoint_executor._executor._max_workers

    def install(self) -> None:
        instrumentation = self

        def measured_submit(
            pool: ThreadPoolExecutor,
            function: Callable[..., object],
            /,
            *args: object,
            **kwargs: object,
        ) -> Future[object]:
            if pool is not instrumentation._checkpoint_pool:
                return instrumentation._original_submit(
                    pool, function, *args, **kwargs
                )
            submitted_at = perf_counter()
            kind = _checkpoint_kind(function)
            workflow_id = _checkpoint_workflow_id(function)
            with instrumentation._condition:
                instrumentation._submitted[kind] += 1
                if workflow_id is not None:
                    instrumentation._workflow_submitted_at[kind][
                        workflow_id
                    ] = submitted_at

            def invoke() -> object:
                started_at = perf_counter()
                instrumentation._active.checkpoint_kind = kind
                instrumentation._active.pool_wait_seconds = 0.0
                with instrumentation._condition:
                    instrumentation.checkpoint_queue_delays.append(
                        started_at - submitted_at
                    )
                    instrumentation._active_workers += 1
                    instrumentation._active_workers_peak = max(
                        instrumentation._active_workers_peak,
                        instrumentation._active_workers,
                    )
                try:
                    return function(*args, **kwargs)
                finally:
                    with instrumentation._condition:
                        instrumentation.pool_waits[kind].append(
                            instrumentation._active.pool_wait_seconds
                        )
                        instrumentation._active_workers -= 1
                        instrumentation._completed[kind] += 1
                        instrumentation._condition.notify_all()
                    instrumentation._active.checkpoint_kind = None
                    instrumentation._active.pool_wait_seconds = 0.0

            return instrumentation._original_submit(pool, invoke)

        def measured_do_get(pool: QueuePool) -> object:
            started_at = perf_counter()
            connection = instrumentation._original_do_get(pool)
            elapsed = perf_counter() - started_at
            kind = getattr(instrumentation._active, "checkpoint_kind", None)
            if pool is instrumentation._app_pool and kind in (
                "stage",
                "completion",
                "unknown",
            ):
                instrumentation._active.pool_wait_seconds += elapsed
            return connection

        ThreadPoolExecutor.submit = (  # ty: ignore[invalid-assignment]
            measured_submit
        )
        QueuePool._do_get = measured_do_get  # ty: ignore[invalid-assignment]

    def restore(self) -> None:
        ThreadPoolExecutor.submit = self._original_submit
        QueuePool._do_get = self._original_do_get


def _checkpoint_kind(function: Callable[..., object]) -> str:
    candidates = (function,)
    if isinstance(function, partial) and function.args:
        candidates += (function.args[0],)
    names = {getattr(candidate, "__name__", "") for candidate in candidates}
    if "_complete_stage_transaction" in names:
        return "stage"
    if "_record_transaction" in names:
        return "completion"
    return "unknown"


def _checkpoint_workflow_id(
    function: Callable[..., object],
) -> str | None:
    if not isinstance(function, partial) or function.keywords is None:
        return None
    workflow_id = function.keywords.get("workflow_id")
    return workflow_id if isinstance(workflow_id, str) else None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Qualify async stage handoff and run fan-in behavior."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DR_PLATFORM_TEST_DATABASE_URL",
            "postgresql+psycopg:///dr_platform_test",
        ),
    )
    parser.add_argument(
        "--reset-test-database",
        action="store_true",
        help="Required acknowledgement that the named *_test database resets.",
    )
    return parser.parse_args()


def _validate_database_url(value: str) -> URL:
    url = make_url(value)
    if url.get_backend_name() != "postgresql":
        raise ValueError("qualification requires PostgreSQL")
    forbidden = {"dbname", "service", "servicefile"}.intersection(url.query)
    if forbidden:
        raise ValueError("database identity query overrides are not allowed")
    if url.database is None or not url.database.endswith("_test"):
        raise ValueError("qualification database name must end in '_test'")
    return url


def _reset_database(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS dbos CASCADE"))
        connection.execute(text("DROP EXTENSION IF EXISTS pgcrypto"))
        connection.execute(text("DROP SCHEMA public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))
        connection.execute(text("CREATE EXTENSION pgcrypto"))


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    index = max(0, math.ceil(percentile * len(sorted_values)) - 1)
    return sorted_values[index]


def _distribution(values: Iterable[float]) -> Distribution:
    ordered = sorted(values)
    if not ordered:
        raise RuntimeError("measurement distribution is empty")
    return Distribution(
        count=len(ordered),
        minimum_ms=round(ordered[0] * 1_000, 3),
        p50_ms=round(_percentile(ordered, 0.50) * 1_000, 3),
        p95_ms=round(_percentile(ordered, 0.95) * 1_000, 3),
        p99_ms=round(_percentile(ordered, 0.99) * 1_000, 3),
        maximum_ms=round(ordered[-1] * 1_000, 3),
    )


def _scheduler_result(
    *,
    batch_size: int,
    declared_workload_per_hour: int,
    elapsed: float,
) -> SchedulerResult:
    configured_rate = batch_size * 3_600 / SCHEDULE_INTERVAL_SECONDS
    headroom = (
        (configured_rate - declared_workload_per_hour)
        / declared_workload_per_hour
        * 100
    )
    measured_rate = batch_size * 3_600 / elapsed
    return SchedulerResult(
        batch_size=batch_size,
        interval_seconds=SCHEDULE_INTERVAL_SECONDS,
        declared_workload_per_hour=declared_workload_per_hour,
        configured_service_rate_per_hour=round(configured_rate, 3),
        configured_headroom_percent=round(headroom, 3),
        measured_pass_seconds=round(elapsed, 6),
        measured_burst_rate_per_hour=round(measured_rate, 3),
        qualified=(
            headroom >= DECLARED_SCHEDULE_HEADROOM_PERCENT
            and elapsed <= SCHEDULE_INTERVAL_SECONDS
        ),
    )


def _git_output(*args: str) -> str:
    return subprocess.check_output(  # noqa: S603 -- fixed executable and args
        ("/usr/bin/git", *args), text=True, stderr=subprocess.DEVNULL
    ).strip()


def _database_provenance(engine: Engine) -> Mapping[str, object]:
    with engine.connect() as connection:
        postgres_version = connection.execute(
            text("SHOW server_version")
        ).scalar_one()
        max_connections = connection.execute(
            text("SHOW max_connections")
        ).scalar_one()
    return {
        "database_url": engine.url.render_as_string(hide_password=True),
        "postgresql": postgres_version,
        "max_connections": int(max_connections),
    }


def _members(count: int) -> tuple[RunMemberInput, ...]:
    return tuple(
        RunMemberInput(
            ordinal=index,
            work=WorkInput(
                work_key=f"burst-work-{index:04d}",
                input_reference=str(index),
                labels={},
            ),
        )
        for index in range(count)
    )


def _build_pipeline(
    *, probe: _BurstProbe, suffix: str
) -> tuple[PipelineRegistry, PipelineDefinition]:
    async def stage(index_text: str) -> str:
        return await probe.run(int(index_text))

    async def completion(manifest_reference: str) -> str:
        return f"aggregate:{manifest_reference}"

    def stage_args(payload: AdmissionPayload) -> tuple[object, ...]:
        return (payload.input_reference,)

    def completion_args(payload: RunCompletionPayload) -> tuple[object, ...]:
        return (payload.manifest_reference,)

    declared = PipelineDefinition(
        key=PipelineKey(f"qualification-{suffix}"),
        version=1,
        stages=(
            StageDefinition(
                key=StageKey("execute"),
                queue_name=f"qualification-stage-{suffix}",
                workflow=stage,
                args_for=stage_args,
            ),
        ),
        run_completion=RunCompletionDefinition(
            key=RunCompletionKey("aggregate"),
            queue_name=f"qualification-completion-{suffix}",
            workflow=completion,
            args_for=completion_args,
        ),
    )
    pipeline = wrap_pipeline_workflows(declared)
    registry = PipelineRegistry()
    registry.register(pipeline)
    return registry, pipeline


def _submit_completion_run(
    *,
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    run_key: str,
    members: tuple[RunMemberInput, ...],
) -> SubmissionReceipt:
    digest = compute_run_membership_digest(
        members, expected_member_count=len(members)
    )
    return submit(
        campaign_key="qualification-campaign",
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference="qualification-config:v1",
        declaration=RunRegistrationDeclaration(
            len(members), f"manifest:{run_key}", digest
        ),
        members=members,
        registry=registry,
        engine=engine,
    )


def _terminal_counts(engine: Engine) -> dict[str, int]:
    schema = StagingSchema()
    with engine.connect() as connection:
        rows = connection.execute(
            select(schema.stage_executions.c.state, func.count())
            .select_from(
                schema.stage_executions.join(
                    schema.work_items,
                    schema.stage_executions.c.work_item_id
                    == schema.work_items.c.work_item_id,
                )
            )
            .where(
                schema.work_items.c.campaign_key == "qualification-campaign"
            )
            .group_by(schema.stage_executions.c.state)
        ).tuples()
        return dict(rows.all())


def _workflow_ids_for_indexes(engine: Engine, indexes: set[int]) -> set[str]:
    work_keys = tuple(f"burst-work-{index:04d}" for index in sorted(indexes))
    return _workflow_ids_for_work_keys(engine, work_keys)


def _workflow_ids_for_work_key(engine: Engine, work_key: str) -> set[str]:
    return _workflow_ids_for_work_keys(engine, (work_key,))


def _workflow_ids_for_work_keys(
    engine: Engine, work_keys: tuple[str, ...]
) -> set[str]:
    schema = StagingSchema()
    with engine.connect() as connection:
        return set(
            connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .select_from(
                    schema.stage_attempts.join(
                        schema.stage_executions,
                        schema.stage_attempts.c.stage_execution_id
                        == schema.stage_executions.c.stage_execution_id,
                    ).join(
                        schema.work_items,
                        schema.stage_executions.c.work_item_id
                        == schema.work_items.c.work_item_id,
                    )
                )
                .where(schema.work_items.c.work_key.in_(work_keys))
            ).scalars()
        )


def _wait_for_terminal(engine: Engine, expected: int) -> dict[str, int]:
    deadline = monotonic() + WATCHDOG_SECONDS
    while True:
        counts = _terminal_counts(engine)
        terminal = sum(
            counts.get(state.value, 0)
            for state in (
                StageExecutionState.SUCCEEDED,
                StageExecutionState.FAILED,
                StageExecutionState.CANCELLED,
            )
        )
        if terminal == expected:
            return counts
        if monotonic() >= deadline:
            raise TimeoutError(f"only {terminal}/{expected} stages terminal")
        threading.Event().wait(0.01)


def _wait_for_run_completions(engine: Engine, expected: int) -> None:
    schema = StagingSchema()
    deadline = monotonic() + WATCHDOG_SECONDS
    while True:
        with engine.connect() as connection:
            succeeded = connection.execute(
                select(func.count())
                .select_from(schema.run_completion_executions)
                .where(schema.run_completion_executions.c.state == "succeeded")
            ).scalar_one()
        if succeeded == expected:
            return
        if monotonic() >= deadline:
            raise TimeoutError(
                f"only {succeeded}/{expected} run completions succeeded"
            )
        threading.Event().wait(0.01)


@contextmanager
def _dbos_runtime(
    *,
    database_url: str,
    engine: Engine,
    pipeline: PipelineDefinition,
    registry: PipelineRegistry,
) -> Iterator[
    tuple[DBOSClient, QueuePool, _LedgerCheckpointExecutor, int, int]
]:
    stage_queue = Queue(
        pipeline.stages[0].queue_name,
        concurrency=ADMISSION_BATCH_SIZE,
        polling_interval_sec=0.05,
    )
    completion = pipeline.run_completion
    assert completion is not None
    completion_queue = Queue(
        completion.queue_name,
        concurrency=BARRIER_BATCH_SIZE,
        polling_interval_sec=0.05,
    )
    del stage_queue, completion_queue
    config = PlatformDbosConfig(
        database_url=database_url,
        system_database_url=database_url,
    )
    initialize_dbos_runtime(config, app_name=f"drpqual-{uuid4().hex[:10]}")
    registration = register_scheduled_dispatcher(
        config=config,
        engine=engine,
        registry=registry,
        cron="0 0 0 1 1 *",
        batch_size=ADMISSION_BATCH_SIZE,
        barrier_cron="0 0 0 1 1 *",
        barrier_batch_size=BARRIER_BATCH_SIZE,
        barrier_candidate_budget=BARRIER_CANDIDATE_BUDGET,
    )
    try:
        DBOS.launch()
        DBOS.set_latest_application_version(DBOS.application_version)
        instance = _get_dbos_instance()
        app_database = instance._app_db
        if app_database is None or not isinstance(
            app_database.engine.pool, QueuePool
        ):
            raise RuntimeError("DBOS application database lacks a QueuePool")
        pool = app_database.engine.pool
        resources = registration._resources
        if resources is None:
            raise RuntimeError("dispatcher lacks owned runtime resources")
        yield (
            registration.client,
            pool,
            resources.checkpoint_executor,
            pool.size(),
            pool._max_overflow,
        )
    finally:
        registration.close()
        DBOS.destroy(destroy_registry=True)


def _run_live_qualification(  # noqa: PLR0912,PLR0915 -- explicit live scenario
    *, engine: Engine, database_url: str
) -> tuple[SchedulerResult, BurstResult, SchedulerResult]:
    probe = _BurstProbe(ADMISSION_BATCH_SIZE)
    suffix = uuid4().hex[:8]
    registry, pipeline = _build_pipeline(probe=probe, suffix=suffix)
    members = _members(ADMISSION_BATCH_SIZE)
    _submit_completion_run(
        engine=engine,
        registry=registry,
        pipeline=pipeline,
        run_key="qualification-burst",
        members=members,
    )
    for index in range(BARRIER_BATCH_SIZE - 1):
        _submit_completion_run(
            engine=engine,
            registry=registry,
            pipeline=pipeline,
            run_key=f"qualification-empty-{index:02d}",
            members=(),
        )
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=ADMISSION_BATCH_SIZE,
        engine=engine,
    )

    instrumentation = _CheckpointInstrumentation()
    try:
        with _dbos_runtime(
            database_url=database_url,
            engine=engine,
            pipeline=pipeline,
            registry=registry,
        ) as (
            client,
            app_pool,
            checkpoint_executor,
            pool_size,
            max_overflow,
        ):
            instrumentation.select_runtime(
                app_pool=app_pool,
                checkpoint_executor=checkpoint_executor,
            )
            instrumentation.install()
            admission_started = perf_counter()
            admission_summary = run_admission_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=ADMISSION_BATCH_SIZE,
            )
            admission_elapsed = perf_counter() - admission_started
            if admission_summary.admitted_total != ADMISSION_BATCH_SIZE:
                raise RuntimeError(
                    "admission did not enqueue the declared burst: "
                    f"{admission_summary!r}"
                )
            if admission_summary.failed_stages:
                raise RuntimeError(
                    f"admission failures: {admission_summary.failed_stages!r}"
                )
            probe.wait_for_entered()

            cancelled_indexes = set(
                range(
                    ADMISSION_BATCH_SIZE - CANCELLATION_COUNT,
                    ADMISSION_BATCH_SIZE,
                )
            )
            cancellation_requested_at: dict[int, float] = {}
            exact_delegations_verified = 0
            cancelled_workflow_ids: set[str] = set()
            for index in sorted(cancelled_indexes):
                expected_workflow_ids = _workflow_ids_for_work_key(
                    engine, f"burst-work-{index:04d}"
                )
                if len(expected_workflow_ids) != 1:
                    raise RuntimeError(
                        f"work {index} did not have one admitted workflow"
                    )
                expected_workflow_id = next(iter(expected_workflow_ids))
                cancellation_requested_at[index] = perf_counter()
                outcome = cancel_work(
                    engine=engine,
                    client=client,
                    campaign_key="qualification-campaign",
                    work_key=f"burst-work-{index:04d}",
                )
                if outcome.stage_execution.state is not (
                    StageExecutionState.CANCELLED
                ):
                    raise RuntimeError(
                        f"work {index} did not become logically cancelled"
                    )
                if outcome.delegated_workflow_id != expected_workflow_id:
                    raise RuntimeError(
                        f"work {index} delegated an unexpected workflow"
                    )
                statuses = client.list_workflows(
                    workflow_ids=[expected_workflow_id],
                    load_input=False,
                    load_output=False,
                )
                if len(statuses) != 1 or statuses[0].status != "CANCELLED":
                    raise RuntimeError(
                        f"work {index} DBOS cancellation was not persisted"
                    )
                exact_delegations_verified += 1
                cancelled_workflow_ids.add(expected_workflow_id)
            persisted_cancelled_workflow_ids = _workflow_ids_for_indexes(
                engine, cancelled_indexes
            )
            if persisted_cancelled_workflow_ids != cancelled_workflow_ids:
                raise RuntimeError(
                    "cancelled works did not retain their exact workflows"
                )
            active_after_cancellation = cancelled_workflow_ids.intersection(
                _get_dbos_instance()._active_workflows_set.activeList()
            )
            cleaned_before_release = cancelled_indexes.intersection(
                probe.cleaned_at
            )
            if len(active_after_cancellation) != CANCELLATION_COUNT:
                raise RuntimeError(
                    "cancelled workflows did not all remain process-local "
                    "before release"
                )
            if cleaned_before_release:
                raise RuntimeError(
                    "cancelled workflow coroutines cleaned before release"
                )

            released_at = perf_counter()
            probe.release_workflows()
            probe.wait_for_cleaned(set(range(ADMISSION_BATCH_SIZE)))
            instrumentation.wait_for_completed(
                kind="stage", expected=ADMISSION_BATCH_SIZE
            )
            terminal_counts = _wait_for_terminal(engine, ADMISSION_BATCH_SIZE)
            handoff_elapsed = perf_counter() - released_at

            successful = terminal_counts.get(
                StageExecutionState.SUCCEEDED.value, 0
            )
            cancelled = terminal_counts.get(
                StageExecutionState.CANCELLED.value, 0
            )
            failed = terminal_counts.get(StageExecutionState.FAILED.value, 0)
            if (
                successful != ADMISSION_BATCH_SIZE - CANCELLATION_COUNT
                or cancelled != CANCELLATION_COUNT
                or failed
            ):
                raise RuntimeError(
                    f"unexpected burst terminal counts: {terminal_counts!r}"
                )

            for index in range(BARRIER_CANDIDATE_BUDGET):
                _submit_completion_run(
                    engine=engine,
                    registry=registry,
                    pipeline=pipeline,
                    run_key=f"qualification-blocked-{index:02d}",
                    members=(
                        RunMemberInput(
                            ordinal=0,
                            work=WorkInput(
                                work_key=f"blocked-work-{index:02d}",
                                input_reference=f"blocked:{index}",
                                labels={},
                            ),
                        ),
                    ),
                )

            blocked_summary = run_barrier_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=BARRIER_BATCH_SIZE,
                candidate_budget=BARRIER_CANDIDATE_BUDGET,
            )
            if (
                blocked_summary.releases
                or blocked_summary.failures
                or blocked_summary.candidates_examined
                != BARRIER_CANDIDATE_BUDGET
            ):
                raise RuntimeError(
                    "barrier did not stop at the blocked-prefix budget: "
                    f"{blocked_summary!r}"
                )
            barrier_started = perf_counter()
            barrier_summary = run_barrier_pass(
                engine,
                client=client,
                registry=registry,
                batch_size=BARRIER_BATCH_SIZE,
                candidate_budget=BARRIER_CANDIDATE_BUDGET,
            )
            barrier_elapsed = perf_counter() - barrier_started
            if len(barrier_summary.releases) != BARRIER_BATCH_SIZE:
                raise RuntimeError(
                    "barrier did not release the declared batch: "
                    f"{barrier_summary!r}"
                )
            if barrier_summary.failures:
                raise RuntimeError(
                    f"barrier failures: {barrier_summary.failures!r}"
                )
            if barrier_summary.candidates_examined != BARRIER_BATCH_SIZE:
                raise RuntimeError(
                    "barrier did not resume immediately after the blocked "
                    f"prefix: {barrier_summary!r}"
                )
            _wait_for_run_completions(engine, BARRIER_BATCH_SIZE)
            instrumentation.wait_for_completed(
                kind="completion", expected=BARRIER_BATCH_SIZE
            )
            probe.finish_probe()

            checkpoint_delays = _distribution(
                instrumentation.checkpoint_queue_delays
            )
            stage_pool_waits = _distribution(
                instrumentation.pool_waits["stage"]
            )
            completion_pool_waits = _distribution(
                instrumentation.pool_waits["completion"]
            )
            loop_lags = _distribution(probe.loop_lags)
            cancellation_cleanup_latencies = _distribution(
                probe.cleaned_at[index] - cancellation_requested_at[index]
                for index in sorted(cancelled_indexes)
            )
            release_cleanup_latencies = _distribution(
                probe.cleaned_at[index] - released_at
                for index in sorted(cancelled_indexes)
            )
            stage_workflow_submissions = instrumentation.workflow_submissions(
                "stage"
            )
            late_cancelled_handoffs = {
                workflow_id
                for workflow_id in cancelled_workflow_ids
                if stage_workflow_submissions.get(workflow_id, 0.0)
                >= released_at
            }
            late_return_fenced = all(
                (
                    len(late_cancelled_handoffs) == CANCELLATION_COUNT,
                    successful == ADMISSION_BATCH_SIZE - CANCELLATION_COUNT,
                    cancelled == CANCELLATION_COUNT,
                    failed == 0,
                )
            )
            instrumentation_result = InstrumentationResult(
                checkpoint_queue_delay=checkpoint_delays,
                checkpoint_active_workers_peak=(
                    instrumentation.active_workers_peak
                ),
                checkpoint_worker_limit=instrumentation.worker_limit,
                stage_checkpoint_submissions=instrumentation.submissions(
                    "stage"
                ),
                completion_checkpoint_submissions=(
                    instrumentation.submissions("completion")
                ),
                unknown_checkpoint_submissions=instrumentation.submissions(
                    "unknown"
                ),
                stage_ledger_pool_wait=stage_pool_waits,
                completion_ledger_pool_wait=completion_pool_waits,
                unknown_ledger_pool_wait_count=len(
                    instrumentation.pool_waits["unknown"]
                ),
                event_loop_lag=loop_lags,
                app_pool_size=pool_size,
                app_pool_max_overflow=max_overflow,
            )
            bound_ms = QUALIFICATION_BOUND_SECONDS * 1_000
            burst_qualified = all(
                (
                    probe.loop is not None,
                    len(probe.cleaned_at) == ADMISSION_BATCH_SIZE,
                    exact_delegations_verified == CANCELLATION_COUNT,
                    len(active_after_cancellation) == CANCELLATION_COUNT,
                    not cleaned_before_release,
                    len(stage_workflow_submissions) == ADMISSION_BATCH_SIZE,
                    checkpoint_delays.count
                    == ADMISSION_BATCH_SIZE + BARRIER_BATCH_SIZE,
                    instrumentation.submissions("stage")
                    == ADMISSION_BATCH_SIZE,
                    instrumentation.submissions("completion")
                    == BARRIER_BATCH_SIZE,
                    instrumentation.submissions("unknown") == 0,
                    0
                    < instrumentation.active_workers_peak
                    <= instrumentation.worker_limit,
                    stage_pool_waits.count == ADMISSION_BATCH_SIZE,
                    completion_pool_waits.count == BARRIER_BATCH_SIZE,
                    not instrumentation.pool_waits["unknown"],
                    release_cleanup_latencies.count == CANCELLATION_COUNT,
                    release_cleanup_latencies.minimum_ms >= 0,
                    late_return_fenced,
                    checkpoint_delays.maximum_ms <= bound_ms,
                    stage_pool_waits.maximum_ms <= bound_ms,
                    completion_pool_waits.maximum_ms <= bound_ms,
                    loop_lags.maximum_ms <= bound_ms,
                    cancellation_cleanup_latencies.maximum_ms <= bound_ms,
                    release_cleanup_latencies.maximum_ms <= bound_ms,
                    handoff_elapsed <= HANDOFF_SERVICE_RATE_BOUND_SECONDS,
                )
            )
            burst = BurstResult(
                expected_burst=ADMISSION_BATCH_SIZE,
                successful_handoffs=successful,
                cancellations=cancelled,
                one_loop_reused=probe.loop is not None,
                cleanup_complete=(
                    len(probe.cleaned_at) == ADMISSION_BATCH_SIZE
                ),
                cancellation_exact_delegations_verified=(
                    exact_delegations_verified
                ),
                cancelled_coroutines_active_before_release=len(
                    active_after_cancellation
                ),
                cancelled_coroutines_cleaned_before_release=len(
                    cleaned_before_release
                ),
                cancellation_request_to_cleanup=(
                    cancellation_cleanup_latencies
                ),
                release_to_cleanup=release_cleanup_latencies,
                cancelled_late_handoff_attempts=len(late_cancelled_handoffs),
                late_return_fenced=late_return_fenced,
                runtime_shutdown_cleanup_complete=False,
                runtime_shutdown_cleanup_seconds=0.0,
                handoff_seconds=round(handoff_elapsed, 6),
                handoffs_per_second=round(successful / handoff_elapsed, 3),
                instrumentation=instrumentation_result,
                qualified=burst_qualified,
            )
            return (
                _scheduler_result(
                    batch_size=ADMISSION_BATCH_SIZE,
                    declared_workload_per_hour=(DECLARED_ADMISSIONS_PER_HOUR),
                    elapsed=admission_elapsed,
                ),
                burst,
                _scheduler_result(
                    batch_size=BARRIER_BATCH_SIZE,
                    declared_workload_per_hour=(DECLARED_COMPLETIONS_PER_HOUR),
                    elapsed=barrier_elapsed,
                ),
            )
    finally:
        instrumentation.restore()


def _qualify_runtime_shutdown_cleanup(
    *, engine: Engine, database_url: str
) -> tuple[bool, float]:
    probe = _BurstProbe(1)
    suffix = uuid4().hex[:8]
    registry, pipeline = _build_pipeline(probe=probe, suffix=suffix)
    _submit_completion_run(
        engine=engine,
        registry=registry,
        pipeline=pipeline,
        run_key=f"qualification-shutdown-{suffix}",
        members=(
            RunMemberInput(
                ordinal=0,
                work=WorkInput(
                    work_key=f"shutdown-work-{suffix}",
                    input_reference="0",
                    labels={},
                ),
            ),
        ),
    )
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=StageKey("execute"),
        capacity=1,
        engine=engine,
    )
    shutdown_started = 0.0
    with _dbos_runtime(
        database_url=database_url,
        engine=engine,
        pipeline=pipeline,
        registry=registry,
    ) as (
        client,
        _app_pool,
        _checkpoint_executor,
        _pool_size,
        _max_overflow,
    ):
        summary = run_admission_pass(
            engine,
            client=client,
            registry=registry,
            batch_size=1,
        )
        if summary.admitted_total != 1:
            raise RuntimeError("shutdown probe was not admitted")
        probe.wait_for_entered()
        workflow_ids = _workflow_ids_for_work_key(
            engine, f"shutdown-work-{suffix}"
        )
        if len(workflow_ids) != 1:
            raise RuntimeError("shutdown probe lacks one admitted workflow")
        workflow_id = next(iter(workflow_ids))
        outcome = cancel_work(
            engine=engine,
            client=client,
            campaign_key="qualification-campaign",
            work_key=f"shutdown-work-{suffix}",
        )
        if outcome.stage_execution.state is not StageExecutionState.CANCELLED:
            raise RuntimeError("shutdown probe was not logically cancelled")
        if outcome.delegated_workflow_id != workflow_id:
            raise RuntimeError(
                "shutdown probe delegated an unexpected workflow"
            )
        if not workflow_ids.intersection(
            _get_dbos_instance()._active_workflows_set.activeList()
        ):
            raise RuntimeError(
                "shutdown probe was not active after cancellation"
            )
        shutdown_started = perf_counter()
    shutdown_elapsed = perf_counter() - shutdown_started
    return 0 in probe.cleaned_at, shutdown_elapsed


def _collect_plan(
    value: object,
) -> tuple[set[str], dict[str, int], float]:
    index_names: set[str] = set()
    rounded_row_estimates: dict[str, int] = {}

    def visit(node: object) -> None:
        if isinstance(node, dict):
            index_name = node.get("Index Name")
            if isinstance(index_name, str):
                index_names.add(index_name)
            relation_name = node.get("Relation Name")
            actual_rows = node.get("Actual Rows")
            actual_loops = node.get("Actual Loops")
            if (
                isinstance(relation_name, str)
                and isinstance(actual_rows, int | float)
                and isinstance(actual_loops, int | float)
            ):
                # PostgreSQL rounds Actual Rows to a per-loop average.
                rounded_row_estimates[relation_name] = (
                    rounded_row_estimates.get(relation_name, 0)
                    + int(actual_rows * actual_loops)
                )
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(value)
    root = cast("list[dict[str, Any]]", value)[0]
    execution_ms = float(root.get("Execution Time", 0.0))
    return index_names, rounded_row_estimates, execution_ms


def _plan_nodes(value: object) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        if isinstance(value.get("Node Type"), str):
            yield cast("dict[str, Any]", value)
        for child in value.values():
            yield from _plan_nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from _plan_nodes(child)


def _barrier_plan_topology_is_bounded(
    plan: object,
    *,
    evaluated_count: int,
    page_limit: int,
) -> bool:
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
    if not (
        len(candidate_nodes)
        == len(evaluated_nodes)
        == len(membership_nodes)
        == len(execution_nodes)
        == 1
    ):
        return False
    candidate = candidate_nodes[0]
    evaluated = evaluated_nodes[0]
    evaluated_plans = evaluated.get("Plans")
    if not isinstance(evaluated_plans, list):
        return False
    lateral_limits = tuple(
        node
        for node in evaluated_plans
        if isinstance(node, dict)
        and node.get("Node Type") == "Limit"
        and node.get("Parent Relationship") == "Inner"
    )
    if len(lateral_limits) != 1:
        return False
    lateral_limit = lateral_limits[0]
    lateral_node_ids = {id(node) for node in _plan_nodes(lateral_limit)}
    membership = membership_nodes[0]
    execution = execution_nodes[0]
    candidate_scans = tuple(
        node
        for node in _plan_nodes(evaluated)
        if node.get("Node Type") == "CTE Scan"
        and node.get("CTE Name") == "candidate_runs"
    )
    membership_index = membership.get("Index Name")
    membership_condition = membership.get("Index Cond")
    execution_index = execution.get("Index Name")
    execution_condition = execution.get("Index Cond")
    execution_loops = execution.get("Actual Loops")
    return bool(
        candidate.get("Node Type") == "Limit"
        and candidate.get("Actual Loops") == 1
        and candidate.get("Actual Rows") == evaluated_count
        and evaluated_count <= page_limit
        and evaluated.get("Node Type") == "Nested Loop"
        and evaluated.get("Join Type") == "Left"
        and evaluated.get("Actual Loops") == 1
        and evaluated.get("Actual Rows") == evaluated_count
        and len(candidate_scans) == 1
        and candidate_scans[0].get("Actual Loops") == 1
        and candidate_scans[0].get("Actual Rows") == evaluated_count
        and lateral_limit.get("Actual Loops") == evaluated_count
        and id(membership) in lateral_node_ids
        and id(execution) in lateral_node_ids
        and isinstance(membership_index, str)
        and membership_index.endswith("_run_work")
        and isinstance(membership_condition, str)
        and "run_key = candidate_runs.run_key" in membership_condition
        and membership.get("Actual Loops") == evaluated_count
        and isinstance(execution_index, str)
        and execution_index.endswith("ix_stage_executions_nonterminal_work")
        and isinstance(execution_condition, str)
        and "platform_run_memberships.work_item_id" in execution_condition
        and isinstance(execution_loops, int | float)
        and 0 < execution_loops <= evaluated_count
    )


def _list_runs_plan_topology_is_bounded(
    plan: object,
    *,
    selected_count: int,
    page_limit: int,
) -> bool:
    nodes = tuple(_plan_nodes(plan))
    run_nodes = tuple(
        node
        for node in nodes
        if node.get("Relation Name") == "platform_pipeline_runs"
    )
    membership_nodes = tuple(
        node
        for node in nodes
        if node.get("Relation Name") == "platform_run_memberships"
    )
    if len(run_nodes) != 1 or len(membership_nodes) != 1:
        return False
    run_node = run_nodes[0]
    membership_node = membership_nodes[0]
    run_index = run_node.get("Index Name")
    membership_index = membership_node.get("Index Name")
    membership_condition = membership_node.get("Index Cond")
    return bool(
        run_node.get("Actual Loops") == 1
        and run_node.get("Actual Rows") == selected_count
        and selected_count <= page_limit
        and isinstance(run_index, str)
        and run_index.endswith("ix_pipeline_runs_campaign_cursor")
        and membership_node.get("Actual Loops") == selected_count
        and isinstance(membership_index, str)
        and membership_index.endswith("_run_work")
        and isinstance(membership_condition, str)
        and "run_key = platform_pipeline_runs.run_key" in membership_condition
    )


def _insert_barrier_backlog(engine: Engine) -> None:
    schema = StagingSchema()
    created_at = datetime(2026, 8, 9, 12, tzinfo=UTC)
    closed_at = created_at + timedelta(seconds=1)
    released_at = created_at + timedelta(seconds=2)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    manifest_reference, membership_digest,
                    run_completion_key, created_at
                )
                SELECT
                    'planner-released-' || lpad(i::text, 5, '0'),
                    'planner-barrier', 'planner-pipeline', 1,
                    'config:planner', 1,
                    'manifest:released:' || i::text,
                    'digest:released:' || i::text,
                    'aggregate', :created_at
                FROM generate_series(0, :last_index) AS i
                """
            ),
            {
                "created_at": created_at,
                "closed_at": closed_at,
                "released_at": released_at,
                "last_index": BARRIER_RELEASED_RUNS - 1,
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'planner-barrier',
                        'planner-released-work-' || lpad(i::text, 5, '0'),
                        'planner-released-' || lpad(i::text, 5, '0'),
                        'input:planner-released:' || i::text,
                        '{}'::jsonb, i + 3000000
                    FROM generate_series(0, :last_index) AS i
                    RETURNING work_item_id, origin_run_key, rank
                ), inserted_membership AS (
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
                    work_item_id, 'execute', 0, 'ready', 0, rank,
                    :created_at, :created_at
                FROM inserted_work
                """
            ),
            {
                "created_at": created_at,
                "last_index": BARRIER_RELEASED_RUNS - 1,
            },
        )
        connection.execute(
            text(
                """
                UPDATE platform_pipeline_runs
                SET
                    registration_closed_at = :closed_at,
                    registered_member_count = 1,
                    created_work_count = 1,
                    reused_work_count = 0,
                    released_at = :released_at,
                    release_terminal_state_counts = jsonb_build_array(
                        jsonb_build_object(
                            'state', 'succeeded', 'count', 0
                        ),
                        jsonb_build_object('state', 'failed', 'count', 1),
                        jsonb_build_object(
                            'state', 'cancelled', 'count', 0
                        )
                    )
                WHERE run_key LIKE 'planner-released-%'
                """
            ),
            {"closed_at": closed_at, "released_at": released_at},
        )
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    manifest_reference, membership_digest,
                    run_completion_key, created_at
                )
                SELECT
                    'planner-waiting-' || lpad(i::text, 5, '0'),
                    'planner-barrier', 'planner-pipeline', 1,
                    'config:planner', 1,
                    'manifest:waiting:' || i::text,
                    'digest:waiting:' || i::text,
                    'aggregate', :created_at
                FROM generate_series(0, :last_index) AS i
                """
            ),
            {
                "created_at": created_at,
                "last_index": BARRIER_WAITING_RUNS - 1,
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'planner-barrier',
                        'planner-waiting-work-' || lpad(i::text, 5, '0'),
                        'planner-waiting-' || lpad(i::text, 5, '0'),
                        'input:planner:' || i::text,
                        '{}'::jsonb, i + 1000000
                    FROM generate_series(0, :last_index) AS i
                    RETURNING work_item_id, origin_run_key, rank
                ), inserted_membership AS (
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
                    work_item_id, 'execute', 0, 'ready', 0, rank,
                    :created_at, :created_at
                FROM inserted_work
                """
            ),
            {
                "created_at": created_at,
                "last_index": BARRIER_WAITING_RUNS - 1,
            },
        )
        connection.execute(
            text(
                """
                UPDATE platform_pipeline_runs
                SET
                    registration_closed_at = :closed_at,
                    registered_member_count = 1,
                    created_work_count = 1,
                    reused_work_count = 0
                WHERE run_key LIKE 'planner-waiting-%'
                """
            ),
            {"closed_at": closed_at},
        )
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key="planner-large-nonterminal",
                campaign_key="planner-barrier-large",
                pipeline_key="planner-pipeline",
                pipeline_version=1,
                execution_config_reference="config:planner",
                expected_member_count=BARRIER_LARGE_RUN_MEMBERS,
                manifest_reference="manifest:large-nonterminal",
                membership_digest="digest:large-nonterminal",
                run_completion_key="aggregate",
                created_at=created_at,
            )
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'planner-barrier-large',
                        'planner-large-work-' || lpad(i::text, 5, '0'),
                        'planner-large-nonterminal',
                        'input:planner-large:' || i::text,
                        '{}'::jsonb, i + 2000000
                    FROM generate_series(0, :last_index) AS i
                    RETURNING work_item_id, rank
                ), inserted_membership AS (
                    INSERT INTO platform_run_memberships (
                        run_key, member_ordinal, work_item_id
                    )
                    SELECT
                        'planner-large-nonterminal',
                        rank - 2000000,
                        work_item_id
                    FROM inserted_work
                )
                INSERT INTO platform_stage_executions (
                    work_item_id, stage_key, stage_index, state,
                    current_attempt, rank, created_at, updated_at
                )
                SELECT
                    work_item_id, 'execute', 0, 'ready', 0, rank,
                    :created_at, :created_at
                FROM inserted_work
                """
            ),
            {
                "created_at": created_at,
                "last_index": BARRIER_LARGE_RUN_MEMBERS - 1,
            },
        )
        connection.execute(
            schema.pipeline_runs.update()
            .where(
                schema.pipeline_runs.c.run_key == "planner-large-nonterminal"
            )
            .values(
                registration_closed_at=closed_at,
                registered_member_count=BARRIER_LARGE_RUN_MEMBERS,
                created_work_count=BARRIER_LARGE_RUN_MEMBERS,
                reused_work_count=0,
            )
        )
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key="planner-00000-eligible",
                campaign_key="planner-barrier",
                pipeline_key="planner-pipeline",
                pipeline_version=1,
                execution_config_reference="config:planner",
                expected_member_count=0,
                manifest_reference="manifest:eligible",
                membership_digest="digest:eligible",
                run_completion_key="aggregate",
                created_at=created_at,
                registration_closed_at=closed_at,
                registered_member_count=0,
                created_work_count=0,
                reused_work_count=0,
            )
        )


def _qualify_barrier_planner(engine: Engine) -> PlannerResult:
    _insert_barrier_backlog(engine)
    schema = StagingSchema()
    statement = _eligible_runs_statement(
        schema=schema, limit=BARRIER_BATCH_SIZE, after=None
    )
    sql = str(
        statement.compile(
            dialect=engine.dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    with engine.begin() as connection:
        connection.execute(text("ANALYZE"))
        large_run_members = connection.execute(
            select(func.count())
            .select_from(schema.run_memberships)
            .where(
                schema.run_memberships.c.run_key == "planner-large-nonterminal"
            )
        ).scalar_one()
        unrelated_history_members = connection.execute(
            select(func.count())
            .select_from(schema.run_memberships)
            .where(schema.run_memberships.c.run_key.like("planner-released-%"))
        ).scalar_one()
        evaluated = tuple(connection.execute(statement).mappings())
        eligible_keys = tuple(
            row["run_key"]
            for row in evaluated
            if row["eligible"] and row["locked"]
        )
        plan = connection.execute(
            text(
                f"EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF) {sql}"
            )
        ).scalar_one()
    indexes, rounded_row_estimates, execution_ms = _collect_plan(plan)
    candidate_index = any(
        name.endswith("ix_pipeline_runs_completion_candidates")
        for name in indexes
    )
    nonterminal_index = any(
        name.endswith("ix_stage_executions_nonterminal_work")
        for name in indexes
    )
    membership_index = any(
        "run_memberships" in name and name.endswith(("_pkey", "_run_work"))
        for name in indexes
    )
    plan_topology_bounded = _barrier_plan_topology_is_bounded(
        plan,
        evaluated_count=len(evaluated),
        page_limit=BARRIER_BATCH_SIZE,
    )
    return PlannerResult(
        backlog={
            "candidate_nonterminal_runs": BARRIER_WAITING_RUNS,
            "candidate_large_nonterminal_members": large_run_members,
            "evaluated_candidate_page_rows": len(evaluated),
            "unrelated_released_history_runs": BARRIER_RELEASED_RUNS,
            "unrelated_released_history_memberships": (
                unrelated_history_members
            ),
            "locked_eligible_runs": len(eligible_keys),
        },
        result_keys=eligible_keys,
        execution_ms=round(execution_ms, 3),
        index_names=tuple(sorted(indexes)),
        rounded_plan_row_estimates=rounded_row_estimates,
        qualified=(
            large_run_members == BARRIER_LARGE_RUN_MEMBERS
            and candidate_index
            and nonterminal_index
            and membership_index
            and plan_topology_bounded
            and unrelated_history_members == BARRIER_RELEASED_RUNS
            and eligible_keys == ("planner-00000-eligible",)
        ),
    )


def _insert_list_runs_backlog(engine: Engine) -> None:
    page_created_at = datetime(2026, 8, 9, 12, tzinfo=UTC)
    history_created_at = page_created_at + timedelta(days=1)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    created_at
                )
                SELECT
                    'list-page-' || lpad(i::text, 5, '0'),
                    'planner-list-runs', 'planner-list-pipeline', 1,
                    'config:list', :members_per_run, :created_at
                FROM generate_series(0, :last_index) AS i
                """
            ),
            {
                "members_per_run": LIST_PAGE_MEMBERS_PER_RUN,
                "created_at": page_created_at,
                "last_index": LIST_PAGE_SIZE - 1,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (
                    run_key, campaign_key, pipeline_key, pipeline_version,
                    execution_config_reference, expected_member_count,
                    created_at
                )
                SELECT
                    'list-history-' || lpad(i::text, 5, '0'),
                    'planner-list-runs', 'planner-list-pipeline', 1,
                    'config:list', :members_per_run, :created_at
                FROM generate_series(0, :last_index) AS i
                """
            ),
            {
                "members_per_run": LIST_HISTORY_MEMBERS_PER_RUN,
                "created_at": history_created_at,
                "last_index": LIST_HISTORY_RUNS - 1,
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'planner-list-runs',
                        'list-page-work-' || i::text,
                        'list-page-' || lpad(
                            (i / :members_per_run)::text, 5, '0'
                        ),
                        'input:list-page:' || i::text,
                        '{}'::jsonb, i + 2000000
                    FROM generate_series(0, :last_member_index) AS i
                    RETURNING work_item_id, origin_run_key, rank
                )
                INSERT INTO platform_run_memberships (
                    run_key, member_ordinal, work_item_id
                )
                SELECT
                    origin_run_key,
                    (rank - 2000000) % :members_per_run,
                    work_item_id
                FROM inserted_work
                """
            ),
            {
                "members_per_run": LIST_PAGE_MEMBERS_PER_RUN,
                "last_member_index": (
                    LIST_PAGE_SIZE * LIST_PAGE_MEMBERS_PER_RUN - 1
                ),
            },
        )
        connection.execute(
            text(
                """
                WITH inserted_work AS (
                    INSERT INTO platform_work_items (
                        campaign_key, work_key, origin_run_key,
                        input_reference, labels, rank
                    )
                    SELECT
                        'planner-list-runs',
                        'list-history-work-' || i::text,
                        'list-history-' || lpad(
                            (i / :members_per_run)::text, 5, '0'
                        ),
                        'input:list-history:' || i::text,
                        '{}'::jsonb, i + 3000000
                    FROM generate_series(0, :last_member_index) AS i
                    RETURNING work_item_id, origin_run_key, rank
                )
                INSERT INTO platform_run_memberships (
                    run_key, member_ordinal, work_item_id
                )
                SELECT
                    origin_run_key,
                    (rank - 3000000) % :members_per_run,
                    work_item_id
                FROM inserted_work
                """
            ),
            {
                "members_per_run": LIST_HISTORY_MEMBERS_PER_RUN,
                "last_member_index": (
                    LIST_HISTORY_RUNS * LIST_HISTORY_MEMBERS_PER_RUN - 1
                ),
            },
        )


def _qualify_list_runs_planner(engine: Engine) -> PlannerResult:
    _insert_list_runs_backlog(engine)
    page = list_runs("planner-list-runs", engine=engine, limit=LIST_PAGE_SIZE)
    if len(page) != LIST_PAGE_SIZE or any(
        run.registered_member_count != LIST_PAGE_MEMBERS_PER_RUN
        for run in page
    ):
        raise RuntimeError("list_runs did not return the declared first page")
    schema = StagingSchema()
    statement = _run_summary_statement(
        schema,
        campaign_key="planner-list-runs",
        limit=LIST_PAGE_SIZE,
        after=None,
    )
    sql = str(
        statement.compile(
            dialect=engine.dialect,
            compile_kwargs={"literal_binds": True},
        )
    )
    with engine.begin() as connection:
        connection.execute(
            text("ANALYZE platform_pipeline_runs, platform_run_memberships")
        )
        plan = connection.execute(
            text(
                f"EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF) {sql}"
            )
        ).scalar_one()
    indexes, rounded_row_estimates, execution_ms = _collect_plan(plan)
    plan_topology_bounded = _list_runs_plan_topology_is_bounded(
        plan,
        selected_count=len(page),
        page_limit=LIST_PAGE_SIZE,
    )
    return PlannerResult(
        backlog={
            "historical_runs": LIST_HISTORY_RUNS,
            "historical_memberships": (
                LIST_HISTORY_RUNS * LIST_HISTORY_MEMBERS_PER_RUN
            ),
            "selected_page_runs": LIST_PAGE_SIZE,
            "selected_page_memberships": (
                LIST_PAGE_SIZE * LIST_PAGE_MEMBERS_PER_RUN
            ),
        },
        result_keys=tuple(str(run.run_key) for run in page),
        execution_ms=round(execution_ms, 3),
        index_names=tuple(sorted(indexes)),
        rounded_plan_row_estimates=rounded_row_estimates,
        qualified=plan_topology_bounded,
    )


def _run(database_url: str) -> QualificationResult:
    url = _validate_database_url(database_url)
    git_status_at_start = _git_output(
        "status", "--short", "--untracked-files=all"
    )
    engine = create_engine(url)
    try:
        database = _database_provenance(engine)
        _reset_database(engine)
        upgrade_platform_schema(url.render_as_string(hide_password=False))
        admission, burst, barrier = _run_live_qualification(
            engine=engine,
            database_url=url.render_as_string(hide_password=False),
        )
        barrier_planner = _qualify_barrier_planner(engine)
        list_runs_planner = _qualify_list_runs_planner(engine)
        shutdown_cleanup, shutdown_seconds = _qualify_runtime_shutdown_cleanup(
            engine=engine,
            database_url=url.render_as_string(hide_password=False),
        )
        burst = replace(
            burst,
            runtime_shutdown_cleanup_complete=shutdown_cleanup,
            runtime_shutdown_cleanup_seconds=round(shutdown_seconds, 6),
            qualified=burst.qualified and shutdown_cleanup,
        )
        git_status_before_result = _git_output(
            "status", "--short", "--untracked-files=all"
        )
        source_tree_clean = not (
            git_status_at_start or git_status_before_result
        )
        qualified = all(
            (
                admission.qualified,
                burst.qualified,
                barrier.qualified,
                barrier_planner.qualified,
                list_runs_planner.qualified,
                source_tree_clean,
            )
        )
        provenance: dict[str, object] = {
            "git_commit": _git_output("rev-parse", "HEAD"),
            "git_branch": _git_output("branch", "--show-current"),
            "git_status_at_start": tuple(git_status_at_start.splitlines()),
            "git_status_before_result": tuple(
                git_status_before_result.splitlines()
            ),
            "source_tree_clean": source_tree_clean,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "python": sys.version.split()[0],
            "dbos": version("dbos"),
            "sqlalchemy": version("sqlalchemy"),
            "psycopg": version("psycopg"),
            "database": database,
        }
        return QualificationResult(
            schema_version=4,
            qualified=qualified,
            run_at=datetime.now(UTC).isoformat(),
            provenance=provenance,
            declared_workload={
                "admissions_per_hour": DECLARED_ADMISSIONS_PER_HOUR,
                "completion_enabled_runs_per_hour": (
                    DECLARED_COMPLETIONS_PER_HOUR
                ),
                "expected_burst_workflows": ADMISSION_BATCH_SIZE,
                "burst_cancellations": CANCELLATION_COUNT,
            },
            acceptance_bounds={
                "scope": "qualification-only; not a standing SLO",
                "schedule_pass_seconds": SCHEDULE_INTERVAL_SECONDS,
                "required_schedule_headroom_percent": (
                    DECLARED_SCHEDULE_HEADROOM_PERCENT
                ),
                "burst_component_max_seconds": (QUALIFICATION_BOUND_SECONDS),
                "handoff_service_rate_bound_seconds": (
                    HANDOFF_SERVICE_RATE_BOUND_SECONDS
                ),
                "correctness": (
                    "state/event gated; time is measurement or watchdog only"
                ),
            },
            admission=admission,
            burst=burst,
            barrier=barrier,
            barrier_planner=barrier_planner,
            list_runs_planner=list_runs_planner,
        )
    finally:
        engine.dispose()


def main() -> int:
    args = _parse_args()
    if not args.reset_test_database:
        raise SystemExit("--reset-test-database is required")
    result = _run(args.database_url)
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.qualified else 1


if __name__ == "__main__":
    raise SystemExit(main())
