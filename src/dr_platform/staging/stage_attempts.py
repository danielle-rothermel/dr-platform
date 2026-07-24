"""Persistence leaf operations for ordered stage-attempt sequences."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from sqlalchemy import null, select, update

from dr_platform.staging.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    StageKey,
    WorkKey,
)
from dr_platform.staging.recipes import stage_workflow_id
from dr_platform.staging.records import StageAttemptRecord
from dr_platform.staging.schema import StagingSchema

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


class StageAttemptSequenceError(RuntimeError):
    """An attempt number did not append to the current sequence."""


class StageAttemptTerminalError(RuntimeError):
    """An attempt was missing or had already reached a terminal outcome."""


def append_stage_attempt(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    stage_execution_id: int,
    created_at: datetime,
    admitted_at: datetime | None = None,
    terminal_at: datetime | None = None,
    terminal_summary: Mapping[str, object] | None = None,
    terminal_reference: str | None = None,
    schema: StagingSchema | None = None,
) -> StageAttemptRecord:
    """Append the next attempt with its deterministic DBOS workflow ID."""
    selected_schema = schema or StagingSchema()
    executions = selected_schema.stage_executions
    work_items = selected_schema.work_items
    runs = selected_schema.pipeline_runs
    statement = (
        select(
            executions.c.current_attempt,
            executions.c.stage_key,
            executions.c.updated_at,
            work_items.c.campaign_key,
            work_items.c.work_key,
            runs.c.pipeline_key,
            runs.c.pipeline_version,
        )
        .select_from(
            executions.join(
                work_items,
                executions.c.work_item_id == work_items.c.work_item_id,
            ).join(
                runs,
                work_items.c.origin_run_key == runs.c.run_key,
            )
        )
        .where(executions.c.stage_execution_id == stage_execution_id)
        .with_for_update(of=executions)
    )
    source = connection.execute(statement).mappings().one_or_none()
    if source is None:
        raise LookupError(
            f"stage execution does not exist: {stage_execution_id}"
        )
    if created_at < source["updated_at"]:
        raise ValueError(
            "stage attempt created_at cannot precede the stage execution "
            "updated_at"
        )

    attempt_number = source["current_attempt"] + 1
    workflow_id = stage_workflow_id(
        work_identity=CampaignWorkIdentity(
            CampaignKey(source["campaign_key"]),
            WorkKey(source["work_key"]),
        ),
        pipeline_key=PipelineKey(source["pipeline_key"]),
        pipeline_version=source["pipeline_version"],
        stage_key=StageKey(source["stage_key"]),
        attempt_number=attempt_number,
    )
    summary = null() if terminal_summary is None else dict(terminal_summary)
    attempts = selected_schema.stage_attempts
    row = (
        connection.execute(
            attempts.insert()
            .values(
                stage_execution_id=stage_execution_id,
                attempt_number=attempt_number,
                workflow_id=workflow_id,
                terminal_summary=summary,
                terminal_reference=terminal_reference,
                created_at=created_at,
                admitted_at=admitted_at,
                terminal_at=terminal_at,
            )
            .returning(*attempts.c)
        )
        .mappings()
        .one()
    )
    changed = connection.execute(
        update(executions)
        .where(
            executions.c.stage_execution_id == stage_execution_id,
            executions.c.current_attempt == attempt_number - 1,
        )
        .values(current_attempt=attempt_number, updated_at=created_at)
    ).rowcount
    if changed != 1:
        raise StageAttemptSequenceError(
            "stage attempt sequence changed while appending"
        )
    return _decode_stage_attempt(row)


def get_stage_attempt(
    connection: Connection,
    *,
    stage_execution_id: int,
    attempt_number: int,
    schema: StagingSchema | None = None,
) -> StageAttemptRecord | None:
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_attempts
    row = (
        connection.execute(
            table.select().where(
                table.c.stage_execution_id == stage_execution_id,
                table.c.attempt_number == attempt_number,
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_stage_attempt(row)


def mark_stage_attempt_admitted(
    connection: Connection,
    *,
    stage_execution_id: int,
    attempt_number: int,
    admitted_at: datetime,
    schema: StagingSchema | None = None,
) -> StageAttemptRecord:
    """Mark one prepared attempt admitted exactly once."""
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_attempts
    row = (
        connection.execute(
            table.select()
            .where(
                table.c.stage_execution_id == stage_execution_id,
                table.c.attempt_number == attempt_number,
            )
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            "stage attempt does not exist: "
            f"execution {stage_execution_id}, attempt {attempt_number}"
        )
    if row["admitted_at"] is not None or row["terminal_at"] is not None:
        raise StageAttemptSequenceError(
            "only a pending stage attempt can be admitted"
        )
    updated_row = (
        connection.execute(
            update(table)
            .where(table.c.stage_attempt_id == row["stage_attempt_id"])
            .values(admitted_at=admitted_at)
            .returning(*table.c)
        )
        .mappings()
        .one()
    )
    return _decode_stage_attempt(updated_row)


def list_stage_attempts(
    connection: Connection,
    *,
    stage_execution_id: int,
    schema: StagingSchema | None = None,
) -> tuple[StageAttemptRecord, ...]:
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_attempts
    statement = (
        table.select()
        .where(table.c.stage_execution_id == stage_execution_id)
        .order_by(table.c.attempt_number)
    )
    return tuple(
        _decode_stage_attempt(row)
        for row in connection.execute(statement).mappings()
    )


def record_stage_attempt_terminal(  # noqa: PLR0913 -- explicit outcome facts
    connection: Connection,
    *,
    stage_execution_id: int,
    attempt_number: int,
    terminal_at: datetime,
    terminal_summary: Mapping[str, object],
    terminal_reference: str | None = None,
    schema: StagingSchema | None = None,
) -> StageAttemptRecord:
    """Record one terminal attempt outcome exactly once."""
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_attempts
    row = (
        connection.execute(
            table.select()
            .where(
                table.c.stage_execution_id == stage_execution_id,
                table.c.attempt_number == attempt_number,
            )
            .with_for_update()
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            "stage attempt does not exist: "
            f"execution {stage_execution_id}, attempt {attempt_number}"
        )
    if row["terminal_at"] is not None:
        raise StageAttemptTerminalError(
            "stage attempt is already terminal: "
            f"execution {stage_execution_id}, attempt {attempt_number}"
        )

    updated_row = (
        connection.execute(
            update(table)
            .where(table.c.stage_attempt_id == row["stage_attempt_id"])
            .values(
                terminal_at=terminal_at,
                terminal_summary=dict(terminal_summary),
                terminal_reference=terminal_reference,
            )
            .returning(*table.c)
        )
        .mappings()
        .one()
    )
    return _decode_stage_attempt(updated_row)


def _decode_stage_attempt(row: RowMapping) -> StageAttemptRecord:
    summary = row["terminal_summary"]
    return StageAttemptRecord(
        stage_attempt_id=row["stage_attempt_id"],
        stage_execution_id=row["stage_execution_id"],
        attempt_number=row["attempt_number"],
        workflow_id=row["workflow_id"],
        terminal_summary=(
            None
            if summary is None
            else _freeze_json_mapping(cast("Mapping[str, object]", summary))
        ),
        terminal_reference=row["terminal_reference"],
        created_at=row["created_at"],
        admitted_at=row["admitted_at"],
        terminal_at=row["terminal_at"],
    )


def _freeze_json_mapping(
    value: Mapping[str, object],
) -> Mapping[str, object]:
    return MappingProxyType(
        {key: _freeze_json_value(item) for key, item in value.items()}
    )


def _freeze_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return _freeze_json_mapping(cast("Mapping[str, object]", value))
    if isinstance(value, list):
        return tuple(_freeze_json_value(item) for item in value)
    return value
