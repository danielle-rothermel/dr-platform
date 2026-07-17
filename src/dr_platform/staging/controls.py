"""Persistence leaf operations for stage capacity controls."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import cast, select
from sqlalchemy.dialects.postgresql import JSONB, insert

from dr_platform.staging._validation import validate_labels
from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.identities import StageKey, validate_key_value
from dr_platform.staging.records import StageControlRecord, immutable_mapping
from dr_platform.staging.schema import StagingSchema

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


def upsert_stage_control(  # noqa: PLR0913 -- explicit control facts
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    selector: Mapping[str, str] | None,
    capacity: int,
    paused: bool,
    updated_at: datetime,
    schema: StagingSchema | None = None,
) -> StageControlRecord:
    """Create or replace one exact-selector stage control."""
    selected_schema = schema or StagingSchema()
    validate_key_value(pipeline_key, label="pipeline key")
    validate_positive_integer(pipeline_version, label="pipeline version")
    normalized_stage_key = (
        stage_key if isinstance(stage_key, StageKey) else StageKey(stage_key)
    )
    normalized_selector = validate_labels(
        {} if selector is None else selector,
        label="stage control selector",
    )
    if (
        isinstance(capacity, bool)
        or not isinstance(capacity, int)
        or capacity < 0
    ):
        raise ValueError("stage control capacity must be non-negative")
    if not isinstance(paused, bool):
        raise TypeError("stage control paused flag must be a bool")

    table = selected_schema.stage_controls
    row = (
        connection.execute(
            insert(table)
            .values(
                pipeline_key=pipeline_key,
                pipeline_version=pipeline_version,
                stage_key=normalized_stage_key.value,
                selector=normalized_selector,
                capacity=capacity,
                paused=paused,
                updated_at=updated_at,
            )
            .on_conflict_do_update(
                index_elements=[
                    "pipeline_key",
                    "pipeline_version",
                    "stage_key",
                    "selector",
                ],
                set_={
                    "capacity": capacity,
                    "paused": paused,
                    "updated_at": updated_at,
                },
            )
            .returning(*table.c)
        )
        .mappings()
        .one()
    )
    return _decode_stage_control(row)


def get_stage_control(  # noqa: PLR0913 -- explicit control identity
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    selector: Mapping[str, str] | None = None,
    schema: StagingSchema | None = None,
) -> StageControlRecord | None:
    selected_schema = schema or StagingSchema()
    normalized_stage_key = (
        stage_key if isinstance(stage_key, StageKey) else StageKey(stage_key)
    )
    normalized_selector = validate_labels(
        {} if selector is None else selector,
        label="stage control selector",
    )
    table = selected_schema.stage_controls
    row = (
        connection.execute(
            table.select().where(
                table.c.pipeline_key == pipeline_key,
                table.c.pipeline_version == pipeline_version,
                table.c.stage_key == normalized_stage_key.value,
                table.c.selector == normalized_selector,
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_stage_control(row)


def list_stage_controls(  # noqa: PLR0913 -- explicit control identity
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    schema: StagingSchema | None = None,
) -> tuple[StageControlRecord, ...]:
    """Read defaults and selectors contained by the supplied labels."""
    selected_schema = schema or StagingSchema()
    normalized_stage_key = (
        stage_key if isinstance(stage_key, StageKey) else StageKey(stage_key)
    )
    table = selected_schema.stage_controls
    statement = select(table).where(
        table.c.pipeline_key == pipeline_key,
        table.c.pipeline_version == pipeline_version,
        table.c.stage_key == normalized_stage_key.value,
    )
    if labels is not None:
        normalized_labels = validate_labels(labels, label="work item labels")
        statement = statement.where(
            cast(normalized_labels, JSONB).contains(table.c.selector)
        )
    statement = statement.order_by(table.c.stage_control_id)
    return tuple(
        _decode_stage_control(row)
        for row in connection.execute(statement).mappings()
    )


def _decode_stage_control(row: RowMapping) -> StageControlRecord:
    return StageControlRecord(
        stage_control_id=row["stage_control_id"],
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        stage_key=StageKey(row["stage_key"]),
        selector=immutable_mapping(row["selector"]),
        capacity=row["capacity"],
        paused=row["paused"],
        updated_at=row["updated_at"],
    )
