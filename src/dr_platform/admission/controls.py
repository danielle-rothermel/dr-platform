from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import cast, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert

from dr_platform._core.clock import utc_now
from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    StageKey,
    normalize_key,
    validate_key_value,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.validation import (
    validate_labels,
    validate_nonnegative_integer,
    validate_positive_integer,
)
from dr_platform.pipeline.definitions import (
    PipelineIdentity,
    validate_pipeline_identity,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from sqlalchemy import Connection, Engine
    from sqlalchemy.engine import RowMapping


@dataclass(frozen=True, slots=True)
class StageControlRecord:
    stage_control_id: int
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    selector: Mapping[str, str]
    capacity: int
    paused: bool
    updated_at: datetime


def set_stage_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    """Set stage-wide capacity while preserving any existing pause flag."""
    return _set_capacity(
        pipeline=pipeline,
        stage_key=stage_key,
        labels={},
        capacity=capacity,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def set_selector_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str],
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    """Set selector capacity while preserving any existing pause flag."""
    return _set_capacity(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        capacity=capacity,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def pause(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    return _set_paused(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        paused=True,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def resume(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    engine: Engine,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    return _set_paused(
        pipeline=pipeline,
        stage_key=stage_key,
        labels=labels,
        paused=False,
        engine=engine,
        clock=clock,
        schema=schema,
    )


def read_controls(
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    engine: Engine,
    labels: Mapping[str, str] | None = None,
    schema: LedgerSchema | None = None,
) -> tuple[StageControlRecord, ...]:
    """With work labels, return controls whose selectors they contain."""
    identity = validate_pipeline_identity(pipeline)
    selected_schema = schema or LedgerSchema()
    with engine.connect() as connection:
        return list_stage_controls(
            connection,
            pipeline_key=identity.key.value,
            pipeline_version=identity.version,
            stage_key=stage_key,
            labels=labels,
            schema=selected_schema,
        )


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
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    selected_schema = schema or LedgerSchema()
    validate_key_value(pipeline_key, label="pipeline key")
    validate_positive_integer(pipeline_version, label="pipeline version")
    normalized_stage_key = normalize_key(stage_key, StageKey)
    normalized_selector = validate_labels(
        {} if selector is None else selector,
        label="stage control selector",
    )
    validate_nonnegative_integer(capacity, label="stage control capacity")
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


def set_stage_control_capacity(  # noqa: PLR0913 -- explicit control facts
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    selector: Mapping[str, str] | None,
    capacity: int,
    updated_at: datetime,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    selected_schema = schema or LedgerSchema()
    normalized_stage_key = normalize_key(stage_key, StageKey)
    normalized_selector = validate_labels(
        {} if selector is None else selector,
        label="stage control selector",
    )
    validate_nonnegative_integer(capacity, label="stage control capacity")
    validate_key_value(pipeline_key, label="pipeline key")
    validate_positive_integer(pipeline_version, label="pipeline version")

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
                paused=False,
                updated_at=updated_at,
            )
            .on_conflict_do_update(
                index_elements=[
                    "pipeline_key",
                    "pipeline_version",
                    "stage_key",
                    "selector",
                ],
                set_={"capacity": capacity, "updated_at": updated_at},
            )
            .returning(*table.c)
        )
        .mappings()
        .one()
    )
    return _decode_stage_control(row)


def set_stage_control_paused(  # noqa: PLR0913 -- explicit control identity
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    selector: Mapping[str, str] | None,
    paused: bool,
    updated_at: datetime,
    schema: LedgerSchema | None = None,
) -> StageControlRecord:
    selected_schema = schema or LedgerSchema()
    normalized_stage_key = normalize_key(stage_key, StageKey)
    normalized_selector = validate_labels(
        {} if selector is None else selector,
        label="stage control selector",
    )
    if not isinstance(paused, bool):
        raise TypeError("stage control paused flag must be a bool")

    table = selected_schema.stage_controls
    row = (
        connection.execute(
            update(table)
            .where(
                table.c.pipeline_key == pipeline_key,
                table.c.pipeline_version == pipeline_version,
                table.c.stage_key == normalized_stage_key.value,
                table.c.selector == normalized_selector,
            )
            .values(paused=paused, updated_at=updated_at)
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            "stage control does not exist for the exact selector"
        )
    return _decode_stage_control(row)


def list_stage_controls(  # noqa: PLR0913 -- explicit control identity
    connection: Connection,
    *,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None = None,
    schema: LedgerSchema | None = None,
) -> tuple[StageControlRecord, ...]:
    selected_schema = schema or LedgerSchema()
    normalized_stage_key = normalize_key(stage_key, StageKey)
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


def _set_capacity(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str],
    capacity: int,
    engine: Engine,
    clock: Callable[[], datetime],
    schema: LedgerSchema | None,
) -> StageControlRecord:
    identity = validate_pipeline_identity(pipeline)
    selected_schema = schema or LedgerSchema()
    with engine.begin() as connection:
        return set_stage_control_capacity(
            connection,
            pipeline_key=identity.key.value,
            pipeline_version=identity.version,
            stage_key=stage_key,
            selector=labels,
            capacity=capacity,
            updated_at=clock(),
            schema=selected_schema,
        )


def _set_paused(  # noqa: PLR0913 -- explicit operator dependencies
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    labels: Mapping[str, str] | None,
    paused: bool,
    engine: Engine,
    clock: Callable[[], datetime],
    schema: LedgerSchema | None,
) -> StageControlRecord:
    identity = validate_pipeline_identity(pipeline)
    selected_schema = schema or LedgerSchema()
    with engine.begin() as connection:
        return set_stage_control_paused(
            connection,
            pipeline_key=identity.key.value,
            pipeline_version=identity.version,
            stage_key=stage_key,
            selector=labels,
            paused=paused,
            updated_at=clock(),
            schema=selected_schema,
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
