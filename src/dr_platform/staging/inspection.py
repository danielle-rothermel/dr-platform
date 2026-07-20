"""Bounded, domain-facing readers for staged pipeline state."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_, select

from dr_platform.staging.controls import list_stage_controls
from dr_platform.staging.definitions import (
    PipelineIdentity,
    validate_positive_integer,
)
from dr_platform.staging.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform.staging.records import (
    StageAttemptRecord,
    StageControlRecord,
    StageExecutionRecord,
    immutable_mapping,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import list_stage_attempts
from dr_platform.staging.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from datetime import datetime

    from sqlalchemy import Connection, Engine
    from sqlalchemy.engine import RowMapping

DEFAULT_INSPECTION_LIMIT = 100
MAX_INSPECTION_LIMIT = 1_000
DEFAULT_BULK_STATUS_CHUNK_SIZE = 500
PIPELINE_IDENTITY_PARTS = 2


@dataclass(frozen=True, slots=True)
class CampaignSummary:
    campaign_key: CampaignKey
    created_at: datetime
    run_count: int
    work_item_count: int


@dataclass(frozen=True, slots=True)
class RunSummary:
    run_key: RunKey
    campaign_key: CampaignKey
    pipeline_key: str
    pipeline_version: int
    execution_config_reference: str
    created_at: datetime
    submission_completed_at: datetime | None
    originated_work_item_count: int


@dataclass(frozen=True, slots=True)
class WorkItemSummary:
    work_item_id: int
    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    labels: Mapping[str, str]
    current_stage_execution_id: int
    current_stage_key: StageKey
    current_stage_index: int
    state: StageExecutionState


@dataclass(frozen=True, slots=True)
class StageExecutionSummary:
    execution: StageExecutionRecord
    attempts: tuple[StageAttemptRecord, ...]


@dataclass(frozen=True, slots=True)
class StateCount:
    state: StageExecutionState
    count: int


@dataclass(frozen=True, slots=True)
class BulkWorkStatus:
    work_key: WorkKey
    present: bool
    work_item_id: int | None
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None


@dataclass(frozen=True, slots=True)
class BulkStatusResult:
    campaign_key: CampaignKey
    statuses: Mapping[WorkKey, BulkWorkStatus]


def inspect_campaign(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> CampaignSummary:
    """Return one campaign's stable identity and aggregate counts."""
    selected_schema = schema or StagingSchema()
    normalized_campaign = _campaign_key(campaign_key)
    statement = _campaign_summary_statement(selected_schema)
    with engine.connect() as connection:
        row = connection.execute(
            statement.where(
                statement.selected_columns.campaign_key
                == normalized_campaign.value
            )
        ).mappings().one_or_none()
    if row is None:
        raise LookupError(f"campaign is unknown: {normalized_campaign.value}")
    return _decode_campaign_summary(row)


def list_campaigns(
    *,
    engine: Engine,
    cursor: CampaignKey | str | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    schema: StagingSchema | None = None,
) -> tuple[CampaignSummary, ...]:
    """Return a keyset page ordered by stable campaign identity."""
    _validate_limit(limit)
    selected_schema = schema or StagingSchema()
    normalized_cursor = _campaign_key(cursor) if cursor is not None else None
    statement = _campaign_summary_statement(selected_schema).limit(limit)
    with engine.connect() as connection:
        if normalized_cursor is not None:
            _require_campaign(
                connection,
                campaign_key=normalized_cursor,
                schema=selected_schema,
            )
            statement = statement.where(
                statement.selected_columns.campaign_key
                > normalized_cursor.value
            )
        return tuple(
            _decode_campaign_summary(row)
            for row in connection.execute(statement).mappings()
        )


def list_runs(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    cursor: RunKey | str | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    schema: StagingSchema | None = None,
) -> tuple[RunSummary, ...]:
    """Return a keyset page of immutable runs in one campaign."""
    _validate_limit(limit)
    selected_schema = schema or StagingSchema()
    normalized_campaign = _campaign_key(campaign_key)
    normalized_cursor = _run_key(cursor) if cursor is not None else None
    runs = selected_schema.pipeline_runs
    items = selected_schema.work_items
    statement = (
        select(
            *runs.c,
            func.count(items.c.work_item_id).label("item_count"),
        )
        .select_from(
            runs.outerjoin(
                items,
                runs.c.run_key == items.c.origin_run_key,
            )
        )
        .where(runs.c.campaign_key == normalized_campaign.value)
        .group_by(*runs.c)
        .order_by(runs.c.created_at, runs.c.run_key)
        .limit(limit)
    )
    with engine.connect() as connection:
        _require_campaign(
            connection,
            campaign_key=normalized_campaign,
            schema=selected_schema,
        )
        if normalized_cursor is not None:
            cursor_created_at = connection.execute(
                select(runs.c.created_at).where(
                    runs.c.campaign_key == normalized_campaign.value,
                    runs.c.run_key == normalized_cursor.value,
                )
            ).scalar_one_or_none()
            if cursor_created_at is None:
                raise ValueError("run cursor is unknown in this campaign")
            statement = statement.where(
                or_(
                    runs.c.created_at > cursor_created_at,
                    and_(
                        runs.c.created_at == cursor_created_at,
                        runs.c.run_key > normalized_cursor.value,
                    ),
                )
            )
        return tuple(
            _decode_run_summary(row)
            for row in connection.execute(statement).mappings()
        )


def list_work_items(  # noqa: PLR0913 -- explicit reader filters
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    state: StageExecutionState | None = None,
    cursor: int | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    schema: StagingSchema | None = None,
) -> tuple[WorkItemSummary, ...]:
    """Return logical items once each, filtered by current stage state."""
    _validate_limit(limit)
    if state is not None and not isinstance(state, StageExecutionState):
        raise TypeError("state must be a StageExecutionState")
    selected_schema = schema or StagingSchema()
    normalized_campaign = _campaign_key(campaign_key)
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    current = _current_stage_indexes(selected_schema)
    statement = (
        select(
            items.c.work_item_id,
            items.c.campaign_key,
            items.c.work_key,
            items.c.origin_run_key,
            items.c.labels,
            executions.c.stage_execution_id,
            executions.c.stage_key,
            executions.c.stage_index,
            executions.c.state,
        )
        .select_from(
            items.join(
                current,
                current.c.work_item_id == items.c.work_item_id,
            ).join(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .where(items.c.campaign_key == normalized_campaign.value)
        .order_by(items.c.work_item_id)
        .limit(limit)
    )
    if state is not None:
        statement = statement.where(executions.c.state == state.value)
    with engine.connect() as connection:
        _require_campaign(
            connection,
            campaign_key=normalized_campaign,
            schema=selected_schema,
        )
        if cursor is not None:
            _validate_work_item_cursor(
                connection,
                cursor=cursor,
                campaign_key=normalized_campaign,
                schema=selected_schema,
            )
            statement = statement.where(items.c.work_item_id > cursor)
        return tuple(
            _decode_work_item_summary(row)
            for row in connection.execute(statement).mappings()
        )


def get_work_item_stages(
    work_item_id: int,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StageExecutionSummary, ...]:
    """Return every logical stage and its append-only attempts."""
    _validate_work_item_id(work_item_id)
    selected_schema = schema or StagingSchema()
    table = selected_schema.stage_executions
    with engine.connect() as connection:
        rows = connection.execute(
            select(table)
            .where(table.c.work_item_id == work_item_id)
            .order_by(table.c.stage_index, table.c.stage_execution_id)
        ).mappings()
        summaries = tuple(
            StageExecutionSummary(
                execution=_decode_stage_execution(row),
                attempts=list_stage_attempts(
                    connection,
                    stage_execution_id=row["stage_execution_id"],
                    schema=selected_schema,
                ),
            )
            for row in rows
        )
    if not summaries:
        raise LookupError(f"work item does not exist: {work_item_id}")
    return summaries


def read_controls(
    *,
    pipeline: PipelineIdentity,
    stage_key: StageKey | str,
    engine: Engine,
    labels: Mapping[str, str] | None = None,
    schema: StagingSchema | None = None,
) -> tuple[StageControlRecord, ...]:
    """Read all controls, or only controls matching supplied work labels."""
    pipeline_key, pipeline_version = _validate_pipeline(pipeline)
    selected_schema = schema or StagingSchema()
    with engine.connect() as connection:
        return list_stage_controls(
            connection,
            pipeline_key=pipeline_key.value,
            pipeline_version=pipeline_version,
            stage_key=stage_key,
            labels=labels,
            schema=selected_schema,
        )


def campaign_state_counts(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    """Derive current logical item counts directly from platform rows.

    Raises ``LookupError`` for an unknown campaign rather than returning an
    empty tuple, so a typo'd key is distinguishable from a drained one.
    """
    return _state_counts(
        engine=engine,
        campaign_key=_campaign_key(campaign_key),
        run_key=None,
        schema=schema,
    )


def run_state_counts(
    run_key: RunKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    """Derive current counts for items whose provenance is one run.

    Raises ``LookupError`` for an unknown run rather than returning an empty
    tuple, so a typo'd key is distinguishable from a drained one.
    """
    return _state_counts(
        engine=engine,
        campaign_key=None,
        run_key=_run_key(run_key),
        schema=schema,
    )


def bulk_work_statuses(
    campaign_key: CampaignKey | str,
    work_keys: Iterable[WorkKey | str],
    *,
    engine: Engine,
    chunk_size: int = DEFAULT_BULK_STATUS_CHUNK_SIZE,
    schema: StagingSchema | None = None,
) -> BulkStatusResult:
    """Read current statuses with exactly one SELECT per input chunk.

    The default chunk size is 500.  Every requested key is returned; absent
    campaign keys have ``present=False`` and ``None`` for all state fields.
    Duplicate input keys are queried and returned once.
    """
    validate_positive_integer(chunk_size, label="bulk status chunk size")
    normalized_campaign = _campaign_key(campaign_key)
    normalized_keys = tuple(dict.fromkeys(_work_key(key) for key in work_keys))
    selected_schema = schema or StagingSchema()
    statuses: dict[WorkKey, BulkWorkStatus] = {
        key: BulkWorkStatus(
            work_key=key,
            present=False,
            work_item_id=None,
            current_stage_key=None,
            current_stage_index=None,
            state=None,
        )
        for key in normalized_keys
    }
    with engine.connect() as connection:
        for start in range(0, len(normalized_keys), chunk_size):
            chunk = normalized_keys[start : start + chunk_size]
            for row in connection.execute(
                _bulk_status_statement(
                    campaign_key=normalized_campaign,
                    work_keys=chunk,
                    schema=selected_schema,
                )
            ).mappings():
                key = WorkKey(row["work_key"])
                statuses[key] = BulkWorkStatus(
                    work_key=key,
                    present=True,
                    work_item_id=row["work_item_id"],
                    current_stage_key=StageKey(row["stage_key"]),
                    current_stage_index=row["stage_index"],
                    state=StageExecutionState(row["state"]),
                )
    return BulkStatusResult(
        campaign_key=normalized_campaign,
        statuses=MappingProxyType(statuses),
    )


def _state_counts(
    *,
    engine: Engine,
    campaign_key: CampaignKey | None,
    run_key: RunKey | None,
    schema: StagingSchema | None,
) -> tuple[StateCount, ...]:
    selected_schema = schema or StagingSchema()
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    current = _current_stage_indexes(selected_schema)
    statement = (
        select(executions.c.state, func.count().label("count"))
        .select_from(
            items.join(
                current,
                current.c.work_item_id == items.c.work_item_id,
            ).join(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .group_by(executions.c.state)
        .order_by(executions.c.state)
    )
    if campaign_key is not None:
        statement = statement.where(
            items.c.campaign_key == campaign_key.value
        )
    else:
        assert run_key is not None
        statement = statement.where(
            items.c.origin_run_key == run_key.value
        )
    with engine.connect() as connection:
        if campaign_key is not None:
            _require_campaign(
                connection,
                campaign_key=campaign_key,
                schema=selected_schema,
            )
        else:
            assert run_key is not None
            _require_run(
                connection,
                run_key=run_key,
                schema=selected_schema,
            )
        return tuple(
            StateCount(
                state=StageExecutionState(row["state"]),
                count=row["count"],
            )
            for row in connection.execute(statement).mappings()
        )


def _bulk_status_statement(
    *,
    campaign_key: CampaignKey,
    work_keys: tuple[WorkKey, ...],
    schema: StagingSchema,
):
    items = schema.work_items
    executions = schema.stage_executions
    current = _current_stage_indexes(schema)
    return (
        select(
            items.c.work_key,
            items.c.work_item_id,
            executions.c.stage_key,
            executions.c.stage_index,
            executions.c.state,
        )
        .select_from(
            items.join(
                current,
                current.c.work_item_id == items.c.work_item_id,
            ).join(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .where(
            items.c.campaign_key == campaign_key.value,
            items.c.work_key.in_([key.value for key in work_keys]),
        )
        .order_by(items.c.work_key)
    )


def _current_stage_indexes(schema: StagingSchema):
    executions = schema.stage_executions
    return (
        select(
            executions.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .group_by(executions.c.work_item_id)
        .subquery()
    )


def _campaign_summary_statement(schema: StagingSchema):
    runs = schema.pipeline_runs
    items = schema.work_items
    run_agg = (
        select(
            runs.c.campaign_key,
            func.min(runs.c.created_at).label("created_at"),
            func.count().label("run_count"),
        )
        .group_by(runs.c.campaign_key)
        .subquery()
    )
    item_agg = (
        select(
            items.c.campaign_key,
            func.count().label("work_item_count"),
        )
        .group_by(items.c.campaign_key)
        .subquery()
    )
    return (
        select(
            run_agg.c.campaign_key,
            run_agg.c.created_at,
            run_agg.c.run_count,
            func.coalesce(item_agg.c.work_item_count, 0).label(
                "work_item_count"
            ),
        )
        .select_from(
            run_agg.outerjoin(
                item_agg,
                run_agg.c.campaign_key == item_agg.c.campaign_key,
            )
        )
        .order_by(run_agg.c.campaign_key)
    )


def _decode_campaign_summary(row: RowMapping) -> CampaignSummary:
    return CampaignSummary(
        campaign_key=CampaignKey(row["campaign_key"]),
        created_at=row["created_at"],
        run_count=row["run_count"],
        work_item_count=row["work_item_count"],
    )


def _decode_run_summary(row: RowMapping) -> RunSummary:
    return RunSummary(
        run_key=RunKey(row["run_key"]),
        campaign_key=CampaignKey(row["campaign_key"]),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        execution_config_reference=row["execution_config_reference"],
        created_at=row["created_at"],
        submission_completed_at=row["submission_completed_at"],
        originated_work_item_count=row["item_count"],
    )


def _decode_work_item_summary(row: RowMapping) -> WorkItemSummary:
    return WorkItemSummary(
        work_item_id=row["work_item_id"],
        campaign_key=CampaignKey(row["campaign_key"]),
        work_key=WorkKey(row["work_key"]),
        origin_run_key=RunKey(row["origin_run_key"]),
        labels=immutable_mapping(row["labels"]),
        current_stage_execution_id=row["stage_execution_id"],
        current_stage_key=StageKey(row["stage_key"]),
        current_stage_index=row["stage_index"],
        state=StageExecutionState(row["state"]),
    )


def _decode_stage_execution(row: RowMapping) -> StageExecutionRecord:
    return StageExecutionRecord(
        stage_execution_id=row["stage_execution_id"],
        work_item_id=row["work_item_id"],
        stage_key=StageKey(row["stage_key"]),
        stage_index=row["stage_index"],
        state=StageExecutionState(row["state"]),
        current_attempt=row["current_attempt"],
        rank=row["rank"],
        output_reference=row["output_reference"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _require_campaign(
    connection: Connection,
    *,
    campaign_key: CampaignKey,
    schema: StagingSchema,
) -> None:
    exists = connection.execute(
        select(schema.pipeline_runs.c.campaign_key).where(
            schema.pipeline_runs.c.campaign_key == campaign_key.value
        )
    ).first()
    if exists is None:
        raise LookupError(f"campaign is unknown: {campaign_key.value}")


def _require_run(
    connection: Connection,
    *,
    run_key: RunKey,
    schema: StagingSchema,
) -> None:
    exists = connection.execute(
        select(schema.pipeline_runs.c.run_key).where(
            schema.pipeline_runs.c.run_key == run_key.value
        )
    ).first()
    if exists is None:
        raise LookupError(f"run is unknown: {run_key.value}")


def _validate_work_item_cursor(
    connection: Connection,
    *,
    cursor: int,
    campaign_key: CampaignKey,
    schema: StagingSchema,
) -> None:
    _validate_work_item_id(cursor)
    exists = connection.execute(
        select(schema.work_items.c.work_item_id).where(
            schema.work_items.c.work_item_id == cursor,
            schema.work_items.c.campaign_key == campaign_key.value,
        )
    ).scalar_one_or_none()
    if exists is None:
        raise ValueError("work item cursor is unknown in this campaign")


def _validate_limit(limit: int) -> None:
    validate_positive_integer(limit, label="inspection limit")
    if limit > MAX_INSPECTION_LIMIT:
        raise ValueError(
            f"inspection limit must not exceed {MAX_INSPECTION_LIMIT}"
        )


def _validate_work_item_id(work_item_id: int) -> None:
    if (
        isinstance(work_item_id, bool)
        or not isinstance(work_item_id, int)
        or work_item_id <= 0
    ):
        raise ValueError("work item id must be a positive integer")


def _validate_pipeline(pipeline: PipelineIdentity) -> PipelineIdentity:
    if (
        not isinstance(pipeline, tuple)
        or len(pipeline) != PIPELINE_IDENTITY_PARTS
        or not isinstance(pipeline[0], PipelineKey)
        or not isinstance(pipeline[1], int)
    ):
        raise TypeError("pipeline must be a (key, version) tuple")
    validate_positive_integer(pipeline[1], label="pipeline version")
    return pipeline


def _campaign_key(value: CampaignKey | str) -> CampaignKey:
    return value if isinstance(value, CampaignKey) else CampaignKey(value)


def _run_key(value: RunKey | str) -> RunKey:
    return value if isinstance(value, RunKey) else RunKey(value)


def _work_key(value: WorkKey | str) -> WorkKey:
    return value if isinstance(value, WorkKey) else WorkKey(value)
