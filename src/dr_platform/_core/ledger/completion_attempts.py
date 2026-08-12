from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dr_serialize import json_hash
from sqlalchemy import null, select, update

from dr_platform._core.frozen import immutable_json_mapping
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.validation import validate_positive_integer

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform._core.identities import (
        PipelineKey,
        RunCompletionKey,
        RunKey,
    )

RUN_COMPLETION_WORKFLOW_ID_PREFIX = "drp-run-"
RUN_COMPLETION_WORKFLOW_ID_DIGEST_LENGTH = 64


@dataclass(frozen=True, slots=True)
class RunCompletionAttemptRecord:
    run_completion_attempt_id: int
    run_completion_execution_id: int
    attempt_number: int
    workflow_id: str
    terminal_summary: Mapping[str, object] | None
    terminal_reference: str | None
    created_at: datetime
    enqueued_at: datetime | None
    terminal_at: datetime | None


class RunCompletionAttemptSequenceError(RuntimeError):
    pass


class RunCompletionAttemptTerminalError(RuntimeError):
    pass


def run_completion_workflow_id(
    *,
    run_key: RunKey,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
    attempt_number: int,
) -> str:
    validate_positive_integer(pipeline_version, label="pipeline version")
    validate_positive_integer(attempt_number, label="attempt number")
    digest = json_hash(
        {
            "run_key": run_key.value,
            "pipeline_key": pipeline_key.value,
            "pipeline_version": pipeline_version,
            "run_completion_key": completion_key.value,
        },
        length=RUN_COMPLETION_WORKFLOW_ID_DIGEST_LENGTH,
    )
    return f"{RUN_COMPLETION_WORKFLOW_ID_PREFIX}{digest}-a{attempt_number}"


def create_initial_run_completion_attempt(  # noqa: PLR0913
    connection: Connection,
    *,
    run_completion_execution_id: int,
    run_key: RunKey,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
    created_at: datetime,
    enqueued_at: datetime,
    schema: StagingSchema | None = None,
) -> RunCompletionAttemptRecord:
    return _insert_run_completion_attempt(
        connection,
        schema=schema or StagingSchema(),
        run_completion_execution_id=run_completion_execution_id,
        run_key=run_key,
        pipeline_key=pipeline_key,
        pipeline_version=pipeline_version,
        completion_key=completion_key,
        attempt_number=1,
        created_at=created_at,
        enqueued_at=enqueued_at,
        terminal_at=None,
        terminal_summary=None,
        terminal_reference=None,
    )


def append_run_completion_attempt(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    run_completion_execution_id: int,
    run_key: RunKey,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
    created_at: datetime,
    enqueued_at: datetime | None = None,
    terminal_at: datetime | None = None,
    terminal_summary: Mapping[str, object] | None = None,
    terminal_reference: str | None = None,
    schema: StagingSchema | None = None,
) -> RunCompletionAttemptRecord:
    selected_schema = schema or StagingSchema()
    executions = selected_schema.run_completion_executions
    source = (
        connection.execute(
            select(executions.c.current_attempt, executions.c.enqueued_at)
            .where(
                executions.c.run_completion_execution_id
                == run_completion_execution_id
            )
            .with_for_update(of=executions)
        )
        .mappings()
        .one_or_none()
    )
    if source is None:
        raise LookupError(
            "run completion execution does not exist: "
            f"{run_completion_execution_id}"
        )
    if created_at < source["enqueued_at"]:
        raise ValueError(
            "run completion attempt created_at cannot precede enqueued_at"
        )

    attempt_number = source["current_attempt"] + 1
    record = _insert_run_completion_attempt(
        connection,
        schema=selected_schema,
        run_completion_execution_id=run_completion_execution_id,
        run_key=run_key,
        pipeline_key=pipeline_key,
        pipeline_version=pipeline_version,
        completion_key=completion_key,
        attempt_number=attempt_number,
        created_at=created_at,
        enqueued_at=enqueued_at,
        terminal_at=terminal_at,
        terminal_summary=terminal_summary,
        terminal_reference=terminal_reference,
    )
    changed = connection.execute(
        update(executions)
        .where(
            executions.c.run_completion_execution_id
            == run_completion_execution_id,
            executions.c.current_attempt == attempt_number - 1,
        )
        .values(current_attempt=attempt_number)
    ).rowcount
    if changed != 1:
        raise RunCompletionAttemptSequenceError(
            "run completion attempt sequence changed while appending"
        )
    return record


def record_run_completion_attempt_terminal(  # noqa: PLR0913
    connection: Connection,
    *,
    run_completion_execution_id: int,
    attempt_number: int,
    terminal_at: datetime,
    terminal_summary: Mapping[str, object] | None,
    terminal_reference: str | None,
    schema: StagingSchema | None = None,
) -> RunCompletionAttemptRecord:
    selected_schema = schema or StagingSchema()
    attempts = selected_schema.run_completion_attempts
    row = (
        connection.execute(
            select(attempts)
            .where(
                attempts.c.run_completion_execution_id
                == run_completion_execution_id,
                attempts.c.attempt_number == attempt_number,
            )
            .with_for_update(of=attempts)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            "run completion attempt does not exist: "
            f"{run_completion_execution_id} attempt {attempt_number}"
        )
    if row["terminal_at"] is not None:
        raise RunCompletionAttemptTerminalError(
            "run completion attempt is already terminal"
        )
    summary = null() if terminal_summary is None else dict(terminal_summary)
    updated = (
        connection.execute(
            update(attempts)
            .where(
                attempts.c.run_completion_attempt_id
                == row["run_completion_attempt_id"]
            )
            .values(
                terminal_at=terminal_at,
                terminal_summary=summary,
                terminal_reference=terminal_reference,
            )
            .returning(*attempts.c)
        )
        .mappings()
        .one()
    )
    return _decode_run_completion_attempt(updated)


def get_run_completion_attempt(
    connection: Connection,
    *,
    run_completion_execution_id: int,
    attempt_number: int,
    schema: StagingSchema | None = None,
) -> RunCompletionAttemptRecord | None:
    selected_schema = schema or StagingSchema()
    attempts = selected_schema.run_completion_attempts
    row = (
        connection.execute(
            select(attempts).where(
                attempts.c.run_completion_execution_id
                == run_completion_execution_id,
                attempts.c.attempt_number == attempt_number,
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return _decode_run_completion_attempt(row)


def get_run_completion_attempt_by_workflow_id(
    connection: Connection,
    *,
    workflow_id: str,
    schema: StagingSchema | None = None,
) -> RunCompletionAttemptRecord | None:
    selected_schema = schema or StagingSchema()
    attempts = selected_schema.run_completion_attempts
    row = (
        connection.execute(
            select(attempts).where(attempts.c.workflow_id == workflow_id)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return _decode_run_completion_attempt(row)


def _insert_run_completion_attempt(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    schema: StagingSchema,
    run_completion_execution_id: int,
    run_key: RunKey,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
    attempt_number: int,
    created_at: datetime,
    enqueued_at: datetime | None,
    terminal_at: datetime | None,
    terminal_summary: Mapping[str, object] | None,
    terminal_reference: str | None,
) -> RunCompletionAttemptRecord:
    workflow_id = run_completion_workflow_id(
        run_key=run_key,
        pipeline_key=pipeline_key,
        pipeline_version=pipeline_version,
        completion_key=completion_key,
        attempt_number=attempt_number,
    )
    summary = null() if terminal_summary is None else dict(terminal_summary)
    attempts = schema.run_completion_attempts
    row = (
        connection.execute(
            attempts.insert()
            .values(
                run_completion_execution_id=run_completion_execution_id,
                attempt_number=attempt_number,
                workflow_id=workflow_id,
                terminal_summary=summary,
                terminal_reference=terminal_reference,
                created_at=created_at,
                enqueued_at=enqueued_at,
                terminal_at=terminal_at,
            )
            .returning(*attempts.c)
        )
        .mappings()
        .one()
    )
    return _decode_run_completion_attempt(row)


def _decode_run_completion_attempt(
    row: RowMapping,
) -> RunCompletionAttemptRecord:
    terminal_summary = row["terminal_summary"]
    return RunCompletionAttemptRecord(
        run_completion_attempt_id=row["run_completion_attempt_id"],
        run_completion_execution_id=row["run_completion_execution_id"],
        attempt_number=row["attempt_number"],
        workflow_id=row["workflow_id"],
        terminal_summary=(
            None
            if terminal_summary is None
            else immutable_json_mapping(terminal_summary)
        ),
        terminal_reference=row["terminal_reference"],
        created_at=row["created_at"],
        enqueued_at=row["enqueued_at"],
        terminal_at=row["terminal_at"],
    )
