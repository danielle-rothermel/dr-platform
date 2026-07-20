"""End-to-end and transactional proofs for linear stage handoff."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dbos import DBOS, DBOSClient, DBOSConfig, EnqueueOptions, Queue
from sqlalchemy import Engine, func, select

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkKey,
    stable_random_rank,
)
from dr_platform.staging.admission import AdmissionPayload, run_admission_pass
from dr_platform.staging.controls import upsert_stage_control
from dr_platform.staging.handoff import (
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.submission import WorkInput, submit
from dr_platform.staging.sweep import sweep_abandoned_stages
from tests.conftest import engine_dsn

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_ref,)


def _pipeline(
    *,
    key: str,
    stage_logic: tuple[tuple[str, Callable[[str], str]], ...],
) -> PipelineDefinition:
    return PipelineDefinition(
        key=PipelineKey(key),
        version=1,
        stages=tuple(
            StageDefinition(
                key=StageKey(stage_key),
                queue_name=f"{key}-{stage_key}-queue",
                workflow=logic,
                args_for=_args_for,
            )
            for stage_key, logic in stage_logic
        ),
    )


def _migrate(engine: Engine) -> StagingSchema:
    upgrade_platform_schema(engine_dsn(engine))
    return StagingSchema()


def _configure_controls(
    engine: Engine,
    pipeline: PipelineDefinition,
    *,
    capacity: int,
) -> None:
    with engine.begin() as connection:
        for stage in pipeline.stages:
            upsert_stage_control(
                connection,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key,
                selector={},
                capacity=capacity,
                paused=False,
                updated_at=_utc_now(),
            )


def _submit_items(  # noqa: PLR0913 -- explicit submission facts
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    campaign_key: str,
    run_key: str,
    items: tuple[WorkInput, ...],
    clock: Callable[[], datetime] = _utc_now,
) -> None:
    submit(
        campaign_key=campaign_key,
        run_key=run_key,
        pipeline=pipeline.identity,
        config_ref="config:smoke",
        items=items,
        registry=registry,
        engine=engine,
        clock=clock,
    )


def _launch_dbos(database_url: str, *, suffix: str) -> None:
    config: DBOSConfig = {
        "name": f"drp-handoff-{suffix}",
        "system_database_url": database_url,
        "application_database_url": database_url,
        "application_version": f"handoff-{suffix}",
        "run_admin_server": False,
        "use_listen_notify": False,
        "notification_listener_polling_interval_sec": 0.01,
    }
    DBOS(config=config)
    DBOS.launch()


def _wait_for(
    predicate: Callable[[], bool],
    *,
    timeout_seconds: float = 5,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for DBOS stage workflow")


def _wait_for_workflow_statuses(
    client: DBOSClient,
    workflow_ids: list[str],
    *,
    expected_status: str,
) -> None:
    _wait_for(
        lambda: (
            len(
                client.list_workflows(
                    workflow_ids=workflow_ids,
                    status=expected_status,
                    load_input=False,
                    load_output=False,
                )
            )
            == len(workflow_ids)
        )
    )


def _stage_state_count(
    engine: Engine,
    schema: StagingSchema,
    *,
    stage_index: int,
    state: StageExecutionState,
) -> int:
    with engine.connect() as connection:
        return connection.execute(
            select(func.count())
            .select_from(schema.stage_executions)
            .where(
                schema.stage_executions.c.stage_index == stage_index,
                schema.stage_executions.c.state == state.value,
            )
        ).scalar_one()


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


def _as_dbos_client(client: object) -> DBOSClient:
    return cast("DBOSClient", client)


def _recorded_workflow_id(options: EnqueueOptions) -> str:
    workflow_id = options.get("workflow_id")
    assert workflow_id is not None
    return workflow_id


def test_three_stage_pipeline_streams_end_to_end_through_wrapped_workflows(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def prepare(input_ref: str) -> str:
        return f"prepared:{input_ref}"

    def execute(input_ref: str) -> str:
        return f"executed:{input_ref}"

    def score(input_ref: str) -> str:
        return f"scored:{input_ref}"

    declared = _pipeline(
        key=f"evaluation-{suffix}",
        stage_logic=(
            ("prepare", prepare),
            ("execute", execute),
            ("score", score),
        ),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        items=(
            WorkInput(work_key="work-a", input_ref="input:a", labels={}),
            WorkInput(work_key="work-b", input_ref="input:b", labels={}),
        ),
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    client: DBOSClient | None = None
    try:
        _launch_dbos(clean_pg, suffix=suffix)
        client = DBOSClient(system_database_url=clean_pg)
        for stage_index in range(3):
            summary = run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            )
            assert summary.admitted_total == 2
            _wait_for(
                lambda stage_index=stage_index: (
                    _stage_state_count(
                        pg_engine,
                        schema,
                        stage_index=stage_index,
                        state=StageExecutionState.SUCCEEDED,
                    )
                    == 2
                )
            )

        with pg_engine.connect() as connection:
            rows = (
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.state,
                        schema.stage_executions.c.output_reference,
                    ).order_by(
                        schema.stage_executions.c.work_item_id,
                        schema.stage_executions.c.stage_index,
                    )
                )
                .tuples()
                .all()
            )
            workflow_ids = (
                connection.execute(select(schema.stage_attempts.c.workflow_id))
                .scalars()
                .all()
            )

        assert len(rows) == 6
        assert {row[0] for row in rows} == {0, 1, 2}
        assert {row[1] for row in rows} == {"prepare", "execute", "score"}
        assert all(
            row[2] == StageExecutionState.SUCCEEDED.value for row in rows
        )
        assert all(row[3] for row in rows)
        assert len(workflow_ids) == 6
        _wait_for_workflow_statuses(
            client,
            list(workflow_ids),
            expected_status="SUCCESS",
        )
    finally:
        if client is not None:
            client.destroy()
        DBOS.destroy(destroy_registry=True)


def test_completion_and_next_ready_insert_roll_back_together(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="atomic-handoff",
        stage_logic=(
            ("prepare", lambda input_ref: f"prepared:{input_ref}"),
            ("execute", lambda input_ref: f"executed:{input_ref}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-atomic",
        run_key="run-atomic",
        items=(WorkInput(work_key="work", input_ref="input", labels={}),),
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0][0])

    def fail_before_successor() -> None:
        raise RuntimeError("injected between completion and handoff")

    with (
        pytest.raises(RuntimeError, match="injected between"),
        pg_engine.begin() as connection,
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            next_stage_key="execute",
            next_stage_index=1,
            completed_at=_utc_now(),
            before_next_stage=fail_before_successor,
        )

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                )
            )
            .tuples()
            .all()
        )
        terminal_at = connection.execute(
            select(schema.stage_attempts.c.terminal_at)
        ).scalar_one()

    assert execution_rows == [("prepare", "admitted", None)]
    assert terminal_at is None


def test_application_failure_is_recorded_in_band_and_releases_capacity(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    campaign_key = f"campaign-failure-{suffix}"
    work_keys = ("work-a", "work-b")
    first_work_key = min(
        work_keys,
        key=lambda work_key: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                CampaignKey(campaign_key), WorkKey(work_key)
            )
        ),
    )

    def sometimes_fails(input_ref: str) -> str:
        if input_ref == "input:fail":
            raise RuntimeError("application stage failed")
        return f"output:{input_ref}"

    declared = _pipeline(
        key=f"failure-{suffix}",
        stage_logic=(("execute", sometimes_fails),),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=f"run-failure-{suffix}",
        items=tuple(
            WorkInput(
                work_key=work_key,
                input_ref=(
                    "input:fail" if work_key == first_work_key else "input:ok"
                ),
                labels={},
            )
            for work_key in work_keys
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    client: DBOSClient | None = None
    try:
        _launch_dbos(clean_pg, suffix=suffix)
        client = DBOSClient(system_database_url=clean_pg)
        first = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert first.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            failed_workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
        _wait_for_workflow_statuses(
            client,
            [failed_workflow_id],
            expected_status="SUCCESS",
        )

        second = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert second.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
    finally:
        if client is not None:
            client.destroy()
        DBOS.destroy(destroy_registry=True)


class _UnprintableError(RuntimeError):
    def __str__(self) -> str:
        raise ValueError("this error message cannot be rendered")


def test_application_failure_with_unprintable_error_lands_failed(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def raises_unprintable(_input_ref: str) -> str:
        raise _UnprintableError

    declared = _pipeline(
        key=f"unprintable-{suffix}",
        stage_logic=(("execute", raises_unprintable),),
    )
    pipeline = wrap_pipeline_workflows(declared, clock=_utc_now)
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-unprintable-{suffix}",
        run_key=f"run-unprintable-{suffix}",
        items=(WorkInput(work_key="work", input_ref="input", labels={}),),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    client: DBOSClient | None = None
    try:
        _launch_dbos(clean_pg, suffix=suffix)
        client = DBOSClient(system_database_url=clean_pg)
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            terminal_summary = connection.execute(
                select(schema.stage_attempts.c.terminal_summary)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
    finally:
        if client is not None:
            client.destroy()
        DBOS.destroy(destroy_registry=True)

    expected_type = (
        f"{_UnprintableError.__module__}.{_UnprintableError.__qualname__}"
    )
    assert terminal_summary["error_type"] == expected_type
    assert terminal_summary["message"] == (
        f"<unprintable {expected_type} message>"
    )


@dataclass(frozen=True, slots=True)
class _Status:
    workflow_id: str
    status: str
    error: Exception | None = None


class _StatusClient:
    def __init__(self, statuses: tuple[_Status, ...]) -> None:
        self._statuses = statuses

    def list_workflows(self, **_kwargs: object) -> list[_Status]:
        return list(self._statuses)


class _PagingStatusClient:
    """Return only the statuses whose ids are in the requested page.

    Unlike :class:`_StatusClient`, this honours ``workflow_ids`` so keyset
    pages queried by a single sweep are served independently, proving the
    sweep advances past the first page.
    """

    def __init__(self, statuses: tuple[_Status, ...]) -> None:
        self._by_id = {status.workflow_id: status for status in statuses}
        self.requested_ids: list[tuple[str, ...]] = []

    def list_workflows(
        self, *, workflow_ids: list[str], **_kwargs: object
    ) -> list[_Status]:
        self.requested_ids.append(tuple(workflow_ids))
        return [
            self._by_id[workflow_id]
            for workflow_id in workflow_ids
            if workflow_id in self._by_id
        ]


def test_sweep_projects_only_cancelled_or_abandoned_admitted_attempts(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-pipeline",
        stage_logic=(("execute", lambda input_ref: f"output:{input_ref}"),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=3)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep",
        run_key="run-sweep",
        items=tuple(
            WorkInput(
                work_key=f"work-{index}",
                input_ref=f"input:{index}",
                labels={},
            )
            for index in range(4)
        ),
    )
    admission_client = _RecordingClient()
    first = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert first.admitted_total == 3
    workflow_ids = tuple(
        _recorded_workflow_id(options)
        for options, _args in admission_client.enqueued
    )
    status_client = _StatusClient(
        (
            _Status(workflow_ids[0], "CANCELLED"),
            _Status(
                workflow_ids[1],
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
            _Status(workflow_ids[2], "PENDING"),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        clock=_utc_now,
    )

    with pg_engine.connect() as connection:
        state_counts = {
            row[0]: row[1]
            for row in connection.execute(
                select(
                    schema.stage_executions.c.state,
                    func.count(),
                ).group_by(schema.stage_executions.c.state)
            ).all()
        }
        terminal_count = connection.execute(
            select(func.count())
            .select_from(schema.stage_attempts)
            .where(schema.stage_attempts.c.terminal_at.is_not(None))
        ).scalar_one()

    assert summary.inspected_count == 3
    assert summary.projected_count == 2
    assert {item.state for item in summary.projections} == {
        StageExecutionState.CANCELLED,
        StageExecutionState.FAILED,
    }
    assert state_counts == {
        StageExecutionState.ADMITTED.value: 1,
        StageExecutionState.CANCELLED.value: 1,
        StageExecutionState.FAILED.value: 1,
        StageExecutionState.READY.value: 1,
    }
    assert terminal_count == 2

    second = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(_RecordingClient()),
        registry=registry,
        clock=_utc_now,
    )
    assert second.admitted_total == 1


def test_sweep_projects_an_abandoned_attempt_with_an_unprintable_error(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-unprintable",
        stage_logic=(("execute", lambda input_ref: f"output:{input_ref}"),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep-unprintable",
        run_key="run-sweep-unprintable",
        items=(WorkInput(work_key="work", input_ref="input", labels={}),),
    )
    admission_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0][0])
    status_client = _StatusClient(
        (
            _Status(
                workflow_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                _UnprintableError(),
            ),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        clock=_utc_now,
    )

    with pg_engine.connect() as connection:
        terminal_summary = connection.execute(
            select(schema.stage_attempts.c.terminal_summary).where(
                schema.stage_attempts.c.terminal_at.is_not(None)
            )
        ).scalar_one()

    assert summary.projected_count == 1
    assert terminal_summary["message"] == "<unprintable error message>"


def test_sweep_paginates_to_reach_abandoned_attempt_in_later_page(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-page-pipeline",
        stage_logic=(("execute", lambda input_ref: f"output:{input_ref}"),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    admitted_total = 5
    _configure_controls(pg_engine, pipeline, capacity=admitted_total)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-sweep-page",
        run_key="run-sweep-page",
        items=tuple(
            WorkInput(
                work_key=f"work-{index}",
                input_ref=f"input:{index}",
                labels={},
            )
            for index in range(admitted_total)
        ),
    )
    admission_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == admitted_total

    # Order the admitted workflow ids by stage_execution_id so the abandoned
    # one sits strictly beyond the first keyset page.
    with pg_engine.connect() as connection:
        ordered_ids = [
            row[0]
            for row in connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .order_by(schema.stage_executions.c.stage_execution_id)
            ).all()
        ]
    assert len(ordered_ids) == admitted_total
    abandoned_id = ordered_ids[-1]
    status_client = _PagingStatusClient(
        (
            _Status(
                abandoned_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
        )
    )

    batch_size = 2
    summary = sweep_abandoned_stages(
        pg_engine,
        client=_as_dbos_client(status_client),
        batch_size=batch_size,
        clock=_utc_now,
    )

    # A single sweep must inspect every admitted attempt across pages and
    # project the abandoned one that lives past the first page.
    assert summary.inspected_count == admitted_total
    assert summary.projected_count == 1
    assert summary.projections[0].workflow_id == abandoned_id
    assert summary.projections[0].state == StageExecutionState.FAILED
    # More than one page was queried, reaching the abandoned id later.
    assert len(status_client.requested_ids) > 1
    assert any(abandoned_id in page for page in status_client.requested_ids)
