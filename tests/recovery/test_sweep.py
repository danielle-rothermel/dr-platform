from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import Engine, func, select

import dr_platform.recovery.cancellation as cancellation_module
import dr_platform.recovery.sweep as sweep_module
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution.handoff import (
    StageHandoffMismatchError,
    _complete_stage_in_transaction,
)
from dr_platform.execution.stage_completion import StageSuccessor
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.cancellation import (
    CancellationDisposition,
    WorkCancellationResult,
    cancel_work,
)
from dr_platform.recovery.live_identity import LiveDbosIdentity
from dr_platform.recovery.sweep import sweep_abandoned_stages
from dr_platform.submission.stream import WorkInput
from tests.conftest import (
    _as_dbos_client,
    _migrate,
    _RecordingCanceller,
    _RecordingClient,
    _WorkflowStatus,
    default_live_dbos_identity,
)
from tests.execution.test_handoff import (
    _configure_controls,
    _pipeline,
    _recorded_workflow_id,
    _submit_and_admit_one,
    _submit_items,
    _UnprintableError,
    _utc_now,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from dr_platform.pipeline.definitions import PipelineDefinition


class _StatusClient:
    def __init__(self, statuses: tuple[_WorkflowStatus, ...]) -> None:
        self._statuses = statuses

    def list_workflows(self, **_kwargs: object) -> list[_WorkflowStatus]:
        return list(self._statuses)


class _BarrierStatusClient:
    def __init__(self, status: _WorkflowStatus, barrier: Barrier) -> None:
        self._status = status
        self._barrier = barrier

    def list_workflows(self, **_kwargs: object) -> list[_WorkflowStatus]:
        self._barrier.wait(timeout=10)
        return [self._status]


class _PagingStatusClient:
    def __init__(self, statuses: tuple[_WorkflowStatus, ...]) -> None:
        self._by_id = {status.workflow_id: status for status in statuses}
        self.requested_ids: list[tuple[str, ...]] = []

    def list_workflows(
        self, *, workflow_ids: list[str], **_kwargs: object
    ) -> list[_WorkflowStatus]:
        self.requested_ids.append(tuple(workflow_ids))
        return [
            self._by_id[workflow_id]
            for workflow_id in workflow_ids
            if workflow_id in self._by_id
        ]


def _commit_successful_handoff(
    engine: Engine,
    *,
    workflow_id: str,
    pipeline: PipelineDefinition,
    completed_at: datetime,
    before_next_stage: Callable[[], None] | None = None,
) -> None:
    with engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key=pipeline.stages[0].key.value,
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=pipeline.stages[1].key,
                    stage_index=1,
                    input_reference="output:prepare",
                ),
            ),
            completed_at=completed_at,
            before_next_stage=before_next_stage,
        )


def _release_after_projection(
    monkeypatch: pytest.MonkeyPatch,
    barrier: Barrier,
) -> None:
    """Release the contender after sweep projection fixes write order."""
    project = cast(
        "Callable[..., bool]",
        sweep_module._project_terminal_status,
    )

    def project_then_release(*args: object, **kwargs: object) -> bool:
        applied = project(*args, **kwargs)
        assert applied
        barrier.wait(timeout=10)
        return applied

    monkeypatch.setattr(
        sweep_module,
        "_project_terminal_status",
        project_then_release,
    )


@pytest.mark.parametrize("winner", ["handoff", "sweep"])
def test_sweep_race_with_successful_handoff_has_one_terminal_outcome(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-handoff-race",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-sweep-handoff-race",
        run_key="run-sweep-handoff-race",
    )
    barrier = Barrier(2)
    race_time = _utc_now() + timedelta(seconds=1)
    abandoned = _WorkflowStatus(
        workflow_id,
        "ERROR",
        RuntimeError("reported abandoned"),
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        if winner == "handoff":

            def release_sweep_after_handoff() -> None:
                barrier.wait(timeout=10)

            handoff = executor.submit(
                _commit_successful_handoff,
                pg_engine,
                workflow_id=workflow_id,
                pipeline=pipeline,
                completed_at=race_time,
                before_next_stage=release_sweep_after_handoff,
            )
            summary = sweep_abandoned_stages(
                pg_engine,
                live_identity=default_live_dbos_identity(app_version="test"),
                client=_as_dbos_client(
                    _BarrierStatusClient(abandoned, barrier)
                ),
                clock=lambda: race_time + timedelta(seconds=1),
            )
            handoff.result()
        else:
            _release_after_projection(monkeypatch, barrier)

            def handoff_after_projection() -> None:
                barrier.wait(timeout=10)
                _commit_successful_handoff(
                    pg_engine,
                    workflow_id=workflow_id,
                    pipeline=pipeline,
                    completed_at=race_time + timedelta(seconds=1),
                )

            handoff = executor.submit(handoff_after_projection)
            summary = sweep_abandoned_stages(
                pg_engine,
                live_identity=default_live_dbos_identity(app_version="test"),
                client=_as_dbos_client(_StatusClient((abandoned,))),
                clock=lambda: race_time,
            )
            with pytest.raises(StageHandoffMismatchError):
                handoff.result()

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .tuples()
            .all()
        )
        attempts = connection.execute(
            select(
                schema.stage_attempts.c.terminal_at,
                schema.stage_attempts.c.terminal_summary,
            )
        ).all()

    assert len(attempts) == 1
    assert attempts[0].terminal_at is not None
    if winner == "handoff":
        assert summary.projected_count == 0
        assert execution_rows == [
            (0, "succeeded", "output:prepare"),
            (1, "ready", None),
        ]
        assert attempts[0].terminal_summary == {"outcome": "succeeded"}
    else:
        assert summary.projected_count == 1
        assert execution_rows == [(0, "failed", None)]
        assert attempts[0].terminal_summary == {
            "outcome": "failed",
            "producer": "abandonment",
            "dbos_status": "ERROR",
            "message": "reported abandoned",
        }


@pytest.mark.parametrize("winner", ["cancellation", "sweep"])
def test_sweep_race_with_operator_cancellation_has_one_terminal_outcome(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    winner: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-cancellation-race",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-sweep-cancellation-race",
        run_key="run-sweep-cancellation-race",
    )
    barrier = Barrier(2)
    race_time = _utc_now() + timedelta(seconds=1)
    abandoned = _WorkflowStatus(
        workflow_id,
        "ERROR",
        RuntimeError("reported abandoned"),
    )
    canceller = _RecordingCanceller()

    with ThreadPoolExecutor(max_workers=1) as executor:
        if winner == "cancellation":
            cancel_one = cast(
                "Callable[..., object]",
                cancellation_module._cancel_one_execution,
            )

            def cancel_then_release(
                *args: object,
                **kwargs: object,
            ) -> object:
                result = cancel_one(*args, **kwargs)
                barrier.wait(timeout=10)
                return result

            monkeypatch.setattr(
                cancellation_module,
                "_cancel_one_execution",
                cancel_then_release,
            )
            cancellation = executor.submit(
                cancel_work,
                engine=pg_engine,
                client=canceller,
                work_item_id=work_item_id,
                clock=lambda: race_time,
            )
            summary = sweep_abandoned_stages(
                pg_engine,
                live_identity=default_live_dbos_identity(app_version="test"),
                client=_as_dbos_client(
                    _BarrierStatusClient(abandoned, barrier)
                ),
                clock=lambda: race_time + timedelta(seconds=1),
            )
            result = cancellation.result()
        else:
            _release_after_projection(monkeypatch, barrier)

            def cancel_after_projection() -> WorkCancellationResult:
                barrier.wait(timeout=10)
                return cancel_work(
                    engine=pg_engine,
                    client=canceller,
                    work_item_id=work_item_id,
                    clock=lambda: race_time + timedelta(seconds=1),
                )

            cancellation = executor.submit(cancel_after_projection)
            summary = sweep_abandoned_stages(
                pg_engine,
                live_identity=default_live_dbos_identity(app_version="test"),
                client=_as_dbos_client(_StatusClient((abandoned,))),
                clock=lambda: race_time,
            )
            result = cancellation.result()

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                ).order_by(schema.stage_executions.c.stage_index)
            )
            .tuples()
            .all()
        )
        attempts = connection.execute(
            select(
                schema.stage_attempts.c.terminal_at,
                schema.stage_attempts.c.terminal_summary,
            )
        ).all()

    assert execution_rows == [(0, "cancelled", None)]
    assert len(attempts) == 1
    assert attempts[0].terminal_at is not None
    if winner == "cancellation":
        assert summary.projected_count == 0
        assert len(result.cancellations) == 1
        assert result.cancellations[0].disposition is (
            CancellationDisposition.CANCELLED_ADMITTED
        )
        assert attempts[0].terminal_summary == {
            "outcome": "cancelled",
            "producer": "cancellation",
            "reason": "operator_requested",
        }
        assert canceller.cancelled == [(workflow_id, False)]
    else:
        assert summary.projected_count == 1
        assert len(result.cancellations) == 1
        assert result.cancellations[0].disposition is (
            CancellationDisposition.CANCELLED_FAILED
        )
        assert attempts[0].terminal_summary == {
            "outcome": "failed",
            "producer": "abandonment",
            "dbos_status": "ERROR",
            "message": "reported abandoned",
        }
        assert canceller.cancelled == []


def test_sweep_projects_only_cancelled_or_abandoned_admitted_attempts(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="sweep-pipeline",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
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
                input_reference=f"input:{index}",
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
        _recorded_workflow_id(options) for options in admission_client.enqueued
    )
    status_client = _StatusClient(
        (
            _WorkflowStatus(workflow_ids[0], "CANCELLED"),
            _WorkflowStatus(
                workflow_ids[1],
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
            _WorkflowStatus(workflow_ids[2], "PENDING"),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
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
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
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
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    admission_client = _RecordingClient()
    admitted = run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0])
    status_client = _StatusClient(
        (
            _WorkflowStatus(
                workflow_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                _UnprintableError(),
            ),
        )
    )

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
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
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
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
                input_reference=f"input:{index}",
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
            _WorkflowStatus(
                abandoned_id,
                "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
                RuntimeError("recovery exhausted"),
            ),
        )
    )

    batch_size = 2
    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
        client=_as_dbos_client(status_client),
        batch_size=batch_size,
        clock=_utc_now,
    )

    assert summary.inspected_count == admitted_total
    assert summary.projected_count == 1
    assert summary.projections[0].workflow_id == abandoned_id
    assert summary.projections[0].state == StageExecutionState.FAILED
    assert len(status_client.requested_ids) > 1
    assert any(abandoned_id in page for page in status_client.requested_ids)


def _admit_one_for_sweep(
    pg_engine: Engine,
    *,
    pipeline_key: str,
    campaign_key: str,
    run_key: str,
) -> tuple[PipelineRegistry, str]:
    pipeline = _pipeline(
        key=pipeline_key,
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=run_key,
        items=(
            WorkInput(
                work_key="work-0",
                input_reference="input:0",
                labels={},
            ),
        ),
    )
    admission_client = _RecordingClient()
    assert (
        run_admission_pass(
            pg_engine,
            client=_as_dbos_client(admission_client),
            registry=registry,
            clock=_utc_now,
        ).admitted_total
        == 1
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0])
    return registry, workflow_id


def test_sweep_projects_pending_with_dead_executor(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-dead-executor",
        campaign_key="campaign-dead-executor",
        run_key="run-dead-executor",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        executor_id="other-executor",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 1
    assert summary.projections[0].state is StageExecutionState.FAILED
    assert summary.projections[0].dbos_status == "PENDING"


def test_sweep_skips_pending_with_missing_identity_fields(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-missing-identity",
        campaign_key="campaign-missing-identity",
        run_key="run-missing-identity",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        app_version=None,
                        executor_id=None,
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 0


def test_sweep_resolver_includes_peer_executor_ids(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-resolver",
        campaign_key="campaign-resolver",
        run_key="run-resolver",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=LiveDbosIdentity(
            app_version="test",
            resolve_executor_ids=lambda: ("local", "other-executor"),
        ),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        executor_id="other-executor",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 0


def test_sweep_suppresses_dead_executor_when_resolver_raises_squeue_case(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-resolver-squeue",
        campaign_key="campaign-resolver-squeue",
        run_key="run-resolver-squeue",
    )
    del registry

    def _raise_resolver() -> tuple[str, ...]:
        raise RuntimeError("resolver unavailable")

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=LiveDbosIdentity(
            app_version="test",
            executor_ids=frozenset({"reconciler-local"}),
            resolve_executor_ids=_raise_resolver,
        ),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        executor_id="slurm-node-17",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 0
    assert summary.executor_resolver_unavailable is True
    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
    assert state == StageExecutionState.ADMITTED.value


def test_sweep_suppresses_dead_executor_when_resolver_returns_empty(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-resolver-empty",
        campaign_key="campaign-resolver-empty",
        run_key="run-resolver-empty",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=LiveDbosIdentity(
            app_version="test",
            executor_ids=frozenset({"reconciler-local"}),
            resolve_executor_ids=lambda: (),
        ),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        app_version="test",
                        executor_id="slurm-node-17",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 0
    assert summary.executor_resolver_unavailable is True
    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
    assert state == StageExecutionState.ADMITTED.value


def test_sweep_still_projects_stale_app_version_when_resolver_unavailable(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-resolver-stale",
        campaign_key="campaign-resolver-stale",
        run_key="run-resolver-stale",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=LiveDbosIdentity(
            app_version="test",
            executor_ids=frozenset({"reconciler-local"}),
            resolve_executor_ids=lambda: (),
        ),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        app_version="old-version",
                        executor_id="slurm-node-17",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 1
    assert summary.executor_resolver_unavailable is True
    assert summary.projections[0].state == StageExecutionState.FAILED


def test_sweep_skips_pending_with_live_identity(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    registry, workflow_id = _admit_one_for_sweep(
        pg_engine,
        pipeline_key="sweep-live-skip",
        campaign_key="campaign-live-skip",
        run_key="run-live-skip",
    )
    del registry

    summary = sweep_abandoned_stages(
        pg_engine,
        live_identity=default_live_dbos_identity(app_version="test"),
        client=_as_dbos_client(
            _StatusClient(
                (
                    _WorkflowStatus(
                        workflow_id,
                        "PENDING",
                        app_version="test",
                        executor_id="local",
                    ),
                )
            )
        ),
        clock=_utc_now,
    )

    assert summary.projected_count == 0
    with pg_engine.connect() as connection:
        state = connection.execute(
            select(schema.stage_executions.c.state)
        ).scalar_one()
    assert state == StageExecutionState.ADMITTED.value
