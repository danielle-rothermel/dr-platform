"""Work-item and stage-history inspection readers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, select

from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import CampaignKey, RunKey, StageKey, WorkKey
from dr_platform._core.ledger.attempts import (
    StageAttemptRecord,
    list_stage_attempts,
)
from dr_platform._core.ledger.executions import StageExecutionRecord
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.inspection._validation import (
    DEFAULT_INSPECTION_LIMIT,
    normalize_campaign_key,
    require_campaign,
    validate_limit,
    validate_work_item_cursor,
    validate_work_item_id,
)
from dr_platform.inspection.statuses import current_stage_indexes

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Engine
    from sqlalchemy.engine import RowMapping


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
    validate_limit(limit)
    if state is not None and not isinstance(state, StageExecutionState):
        raise TypeError("state must be a StageExecutionState")
    selected_schema = schema or StagingSchema()
    normalized_campaign = normalize_campaign_key(campaign_key)
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    campaign_item_ids = select(items.c.work_item_id).where(
        items.c.campaign_key == normalized_campaign.value
    )
    current = current_stage_indexes(selected_schema, campaign_item_ids)
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
        require_campaign(
            connection,
            campaign_key=normalized_campaign,
            schema=selected_schema,
        )
        if cursor is not None:
            validate_work_item_cursor(
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
    """Return every logical stage and its ordered attempts."""
    validate_work_item_id(work_item_id)
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
