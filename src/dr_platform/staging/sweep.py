"""Lazy projection of DBOS-abandoned stage workflows into platform state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import Engine, select

from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.identities import StageKey
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import record_stage_attempt_terminal
from dr_platform.staging.stage_executions import transition_stage_execution
from dr_platform.staging.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dbos import DBOSClient
    from sqlalchemy import Connection


DEFAULT_SWEEP_BATCH_SIZE = 100

_FAILED_DBOS_STATUSES = frozenset(
    {
        DbosWorkflowStatus.ERROR.value,
        DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED.value,
    }
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


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
class _AdmittedAttempt:
    workflow_id: str
    stage_execution_id: int
    attempt_number: int
    stage_key: StageKey


def sweep_abandoned_stages(
    engine: Engine,
    *,
    client: DBOSClient,
    batch_size: int = DEFAULT_SWEEP_BATCH_SIZE,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> SweepSummary:
    """Project terminal DBOS abandonment for currently ADMITTED stages.

    Missing and active workflows are deliberately ignored.  The sweep never
    retries, resumes, replaces, or waits for a workflow.

    ``batch_size`` is a keyset page size, not a cap: a single sweep paginates
    through every ADMITTED attempt so long-running healthy attempts with low
    ids cannot starve abandoned ones out of inspection.  An out-of-band DBOS
    resume races benignly with projection -- platform state is authoritative,
    so a workflow that resumes and completes after projection fails the
    handoff identity guard (:class:`StageHandoffMismatchError`) rather than
    corrupting state.
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
            if status.status == DbosWorkflowStatus.CANCELLED.value:
                target_state = StageExecutionState.CANCELLED
            elif status.status in _FAILED_DBOS_STATUSES:
                target_state = StageExecutionState.FAILED
            else:
                continue
            terminal_summary: dict[str, object] = {
                "outcome": target_state.value,
                "dbos_status": status.status,
            }
            if status.error is not None:
                terminal_summary["message"] = str(status.error)
            # Read the clock per projection: pages commit separately, so a
            # single up-front timestamp can fall behind a row bumped after the
            # sweep started and drive updated_at backwards.
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
                executions.c.stage_execution_id
                == attempt.stage_execution_id
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
