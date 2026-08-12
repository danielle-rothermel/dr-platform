from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import UNIQUE, StrEnum, verify
from typing import TYPE_CHECKING

from sqlalchemy import Engine, select

from dr_platform._core.identities import RunKey, StageKey
from dr_platform._core.ledger.attempts import record_stage_attempt_terminal
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryProducer,
    build_run_completion_error_summary,
    build_terminal_summary,
)
from dr_platform._core.validation import validate_positive_integer
from dr_platform.completion.execution import (
    RunCompletionOutcomeError,
    record_run_completion_outcome,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dbos import DBOSClient
    from sqlalchemy import Connection

    from dr_platform.recovery.live_identity import LiveDbosIdentity


DEFAULT_SWEEP_BATCH_SIZE = 10_000


@verify(UNIQUE)
class AbandonmentEvidence(StrEnum):
    STALE_APP_VERSION = "stale_app_version"
    DEAD_EXECUTOR = "dead_executor"


@verify(UNIQUE)
class DbosWorkflowStatus(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    MAX_RECOVERY_ATTEMPTS_EXCEEDED = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
    CANCELLED = "CANCELLED"
    ENQUEUED = "ENQUEUED"
    DELAYED = "DELAYED"


_FAILED_DBOS_STATUSES = frozenset(
    {
        DbosWorkflowStatus.ERROR.value,
        DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
    }
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_error_message(error: object) -> str:
    # Broken DBOS error rendering must not abort the projection page.
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- defend against a broken __str__
        return "<unprintable error message>"


def _pending_abandonment_evidence(
    *,
    app_version: str | None,
    executor_id: str | None,
    live_identity: LiveDbosIdentity,
) -> AbandonmentEvidence | None:
    if app_version is None or executor_id is None:
        return None
    if app_version != live_identity.app_version:
        return AbandonmentEvidence.STALE_APP_VERSION
    if executor_id not in live_identity.executor_ids:
        return AbandonmentEvidence.DEAD_EXECUTOR
    return None


@dataclass(frozen=True, slots=True)
class SweepProjection:
    workflow_id: str
    stage_execution_id: int
    stage_key: StageKey
    state: StageExecutionState
    dbos_status: str


@dataclass(frozen=True, slots=True)
class SweepSummary:
    projections: tuple[SweepProjection, ...]
    inspected_count: int

    @property
    def projected_count(self) -> int:
        return len(self.projections)


@dataclass(frozen=True, slots=True)
class RunCompletionSweepProjection:
    workflow_id: str
    run_completion_execution_id: int
    run_key: RunKey
    state: RunCompletionExecutionState
    dbos_status: str


@dataclass(frozen=True, slots=True)
class RunCompletionSweepSummary:
    projections: tuple[RunCompletionSweepProjection, ...]
    inspected_count: int

    @property
    def projected_count(self) -> int:
        return len(self.projections)


@dataclass(frozen=True, slots=True)
class _AdmittedAttempt:
    workflow_id: str
    stage_execution_id: int
    attempt_number: int
    stage_key: StageKey


def sweep_abandoned_stages(  # noqa: PLR0913 -- explicit projection boundary
    engine: Engine,
    *,
    client: DBOSClient,
    live_identity: LiveDbosIdentity,
    batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> SweepSummary:
    """Project terminal DBOS abandonment without resuming or retrying.

    Missing workflows are ignored. Identity-orphaned PENDING rows project to
    FAILED using structural evidence only; live-identity PENDING rows are left
    for startup recovery and the recovery cap. ``batch_size`` is a page size,
    not a cap; every ADMITTED attempt is visited each call because an external
    cursor could skip newly admitted rows behind it. Platform state wins races
    with out-of-band DBOS resume through the handoff identity guard.
    """
    validate_positive_integer(batch_size, label="sweep batch size")
    selected_schema = schema or StagingSchema()
    projections: list[SweepProjection] = []
    inspected_count = 0
    cursor: int | None = None
    while True:
        with engine.connect() as connection:
            admitted = _list_admitted_attempts(
                connection,
                schema=selected_schema,
                limit=batch_size,
                after=cursor,
            )
        if not admitted:
            break
        inspected_count += len(admitted)
        cursor = admitted[-1].stage_execution_id

        statuses = client.list_workflows(
            workflow_ids=[attempt.workflow_id for attempt in admitted],
            load_input=False,
            load_output=False,
        )
        statuses_by_id = {status.workflow_id: status for status in statuses}
        for attempt in admitted:
            status = statuses_by_id.get(attempt.workflow_id)
            if status is None:
                continue
            target_state: StageExecutionState | None = None
            abandonment_reason: str | None = None
            if status.status == DbosWorkflowStatus.CANCELLED.value:
                target_state = StageExecutionState.CANCELLED
            elif status.status in _FAILED_DBOS_STATUSES:
                target_state = StageExecutionState.FAILED
            elif status.status == DbosWorkflowStatus.PENDING.value:
                evidence = _pending_abandonment_evidence(
                    app_version=status.app_version,
                    executor_id=status.executor_id,
                    live_identity=live_identity,
                )
                if evidence is None:
                    continue
                target_state = StageExecutionState.FAILED
                abandonment_reason = evidence.value
            else:
                continue
            terminal_summary = build_terminal_summary(
                outcome=target_state.value,
                producer=TerminalSummaryProducer.ABANDONMENT,
                dbos_status=status.status,
                message=(
                    None
                    if status.error is None
                    else _safe_error_message(status.error)
                ),
                reason=abandonment_reason,
            )
            # Read per projection so committed pages cannot move time backward.
            terminal_at = clock()
            with engine.begin() as connection:
                if not _project_terminal_status(
                    connection,
                    attempt=attempt,
                    target_state=target_state,
                    terminal_summary=terminal_summary,
                    terminal_at=terminal_at,
                    schema=selected_schema,
                ):
                    continue
            projections.append(
                SweepProjection(
                    workflow_id=attempt.workflow_id,
                    stage_execution_id=attempt.stage_execution_id,
                    stage_key=attempt.stage_key,
                    state=target_state,
                    dbos_status=status.status,
                )
            )
        if len(admitted) < batch_size:
            break
    return SweepSummary(
        projections=tuple(projections),
        inspected_count=inspected_count,
    )


def _list_admitted_attempts(
    connection: Connection,
    *,
    schema: StagingSchema,
    limit: int,
    after: int | None = None,
) -> tuple[_AdmittedAttempt, ...]:
    executions = schema.stage_executions
    attempts = schema.stage_attempts
    conditions = [
        executions.c.state == StageExecutionState.ADMITTED.value,
        attempts.c.attempt_number == executions.c.current_attempt,
        attempts.c.terminal_at.is_(None),
    ]
    if after is not None:
        conditions.append(executions.c.stage_execution_id > after)
    rows = connection.execute(
        select(
            attempts.c.workflow_id,
            executions.c.stage_execution_id,
            attempts.c.attempt_number,
            executions.c.stage_key,
        )
        .select_from(
            executions.join(
                attempts,
                executions.c.stage_execution_id
                == attempts.c.stage_execution_id,
            )
        )
        .where(*conditions)
        .order_by(executions.c.stage_execution_id)
        .limit(limit)
    ).mappings()
    return tuple(
        _AdmittedAttempt(
            workflow_id=row["workflow_id"],
            stage_execution_id=row["stage_execution_id"],
            attempt_number=row["attempt_number"],
            stage_key=StageKey(row["stage_key"]),
        )
        for row in rows
    )


def _project_terminal_status(  # noqa: PLR0913 -- explicit projection facts
    connection: Connection,
    *,
    attempt: _AdmittedAttempt,
    target_state: StageExecutionState,
    terminal_summary: Mapping[str, object],
    terminal_at: datetime,
    schema: StagingSchema,
) -> bool:
    executions = schema.stage_executions
    current = (
        connection.execute(
            select(
                executions.c.state,
                executions.c.current_attempt,
            )
            .where(
                executions.c.stage_execution_id == attempt.stage_execution_id
            )
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if current is None:
        return False
    if (
        current["state"] != StageExecutionState.ADMITTED.value
        or current["current_attempt"] != attempt.attempt_number
    ):
        return False

    transition_stage_execution(
        connection,
        stage_execution_id=attempt.stage_execution_id,
        new_state=target_state,
        updated_at=terminal_at,
        schema=schema,
    )
    record_stage_attempt_terminal(
        connection,
        stage_execution_id=attempt.stage_execution_id,
        attempt_number=attempt.attempt_number,
        terminal_at=terminal_at,
        terminal_summary=terminal_summary,
        terminal_reference=attempt.workflow_id,
        schema=schema,
    )
    return True


def _dbos_failure_error_summary(status: object) -> dict[str, object]:
    dbos_status = getattr(status, "status", None)
    error = getattr(status, "error", None)
    message = str(dbos_status)
    if error is not None:
        message = _safe_error_message(error)
    return build_run_completion_error_summary(
        error_type="dbos.abandonment",
        message=message,
        dbos_status=str(dbos_status),
    )


def _run_completion_abandonment_error_summary(
    status: object,
    *,
    live_identity: LiveDbosIdentity,
) -> dict[str, object] | None:
    dbos_status = getattr(status, "status", None)
    if dbos_status in _FAILED_DBOS_STATUSES:
        return _dbos_failure_error_summary(status)
    if dbos_status == DbosWorkflowStatus.PENDING.value:
        evidence = _pending_abandonment_evidence(
            app_version=getattr(status, "app_version", None),
            executor_id=getattr(status, "executor_id", None),
            live_identity=live_identity,
        )
        if evidence is None:
            return None
        return build_run_completion_error_summary(
            error_type="dbos.abandonment",
            message=evidence.value,
            dbos_status=str(dbos_status),
            reason=evidence.value,
        )
    return None


@dataclass(frozen=True, slots=True)
class _EnqueuedCompletionAttempt:
    workflow_id: str
    run_completion_execution_id: int
    attempt_number: int
    run_key: RunKey


def sweep_abandoned_run_completions(  # noqa: PLR0913 -- explicit projection boundary
    engine: Engine,
    *,
    client: DBOSClient,
    live_identity: LiveDbosIdentity,
    batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> RunCompletionSweepSummary:
    """Project terminal DBOS abandonment for enqueued run completions.

    Errored, recovery-exhausted, and identity-orphaned pending DBOS statuses
    are projected. Live-identity pending rows are left for startup recovery
    and the configured recovery cap.
    """
    validate_positive_integer(batch_size, label="sweep batch size")
    selected_schema = schema or StagingSchema()
    projections: list[RunCompletionSweepProjection] = []
    inspected_count = 0
    cursor: int | None = None
    while True:
        with engine.connect() as connection:
            enqueued = _list_enqueued_completion_attempts(
                connection,
                schema=selected_schema,
                limit=batch_size,
                after=cursor,
            )
        if not enqueued:
            break
        inspected_count += len(enqueued)
        cursor = enqueued[-1].run_completion_execution_id

        statuses = client.list_workflows(
            workflow_ids=[attempt.workflow_id for attempt in enqueued],
            load_input=False,
            load_output=False,
        )
        statuses_by_id = {status.workflow_id: status for status in statuses}
        for attempt in enqueued:
            status = statuses_by_id.get(attempt.workflow_id)
            if status is None:
                continue
            error_summary = _run_completion_abandonment_error_summary(
                status,
                live_identity=live_identity,
            )
            if error_summary is None:
                continue
            terminal_at = clock()
            try:
                with engine.begin() as connection:
                    record_run_completion_outcome(
                        connection,
                        workflow_id=attempt.workflow_id,
                        succeeded=False,
                        output_reference=None,
                        error_summary=error_summary,
                        terminal_at=terminal_at,
                        schema=selected_schema,
                    )
            except RunCompletionOutcomeError:
                continue
            projections.append(
                RunCompletionSweepProjection(
                    workflow_id=attempt.workflow_id,
                    run_completion_execution_id=(
                        attempt.run_completion_execution_id
                    ),
                    run_key=attempt.run_key,
                    state=RunCompletionExecutionState.FAILED,
                    dbos_status=str(getattr(status, "status", "")),
                )
            )
        if len(enqueued) < batch_size:
            break
    return RunCompletionSweepSummary(
        projections=tuple(projections),
        inspected_count=inspected_count,
    )


def _list_enqueued_completion_attempts(
    connection: Connection,
    *,
    schema: StagingSchema,
    limit: int,
    after: int | None = None,
) -> tuple[_EnqueuedCompletionAttempt, ...]:
    executions = schema.run_completion_executions
    attempts = schema.run_completion_attempts
    conditions = [
        executions.c.state == RunCompletionExecutionState.ENQUEUED.value,
        attempts.c.attempt_number == executions.c.current_attempt,
        attempts.c.terminal_at.is_(None),
    ]
    if after is not None:
        conditions.append(executions.c.run_completion_execution_id > after)
    rows = connection.execute(
        select(
            attempts.c.workflow_id,
            executions.c.run_completion_execution_id,
            attempts.c.attempt_number,
            executions.c.run_key,
        )
        .select_from(
            executions.join(
                attempts,
                executions.c.run_completion_execution_id
                == attempts.c.run_completion_execution_id,
            )
        )
        .where(*conditions)
        .order_by(executions.c.run_completion_execution_id)
        .limit(limit)
    ).mappings()
    return tuple(
        _EnqueuedCompletionAttempt(
            workflow_id=row["workflow_id"],
            run_completion_execution_id=row["run_completion_execution_id"],
            attempt_number=row["attempt_number"],
            run_key=RunKey(row["run_key"]),
        )
        for row in rows
    )
