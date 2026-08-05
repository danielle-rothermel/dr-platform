"""Current-state counts and bulk status inspection."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, select

from dr_platform._core.identities import CampaignKey, RunKey, StageKey, WorkKey
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import validate_positive_integer
from dr_platform.inspection._validation import (
    normalize_campaign_key,
    normalize_run_key,
    normalize_work_key,
    require_campaign,
    require_run,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from sqlalchemy import Engine, Select

DEFAULT_BULK_STATUS_CHUNK_SIZE = 500


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


def campaign_state_counts(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    """Derive current logical item counts for one campaign."""
    return _state_counts(
        engine=engine,
        campaign_key=normalize_campaign_key(campaign_key),
        run_key=None,
        schema=schema,
    )


def run_state_counts(
    run_key: RunKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    """Derive current counts for items whose provenance is one run."""
    return _state_counts(
        engine=engine,
        campaign_key=None,
        run_key=normalize_run_key(run_key),
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
    """Read current statuses with exactly one SELECT per input chunk."""
    validate_positive_integer(chunk_size, label="bulk status chunk size")
    normalized_campaign = normalize_campaign_key(campaign_key)
    normalized_keys = tuple(
        dict.fromkeys(normalize_work_key(key) for key in work_keys)
    )
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
    if campaign_key is not None:
        scoped_item_ids = select(items.c.work_item_id).where(
            items.c.campaign_key == campaign_key.value
        )
    else:
        assert run_key is not None
        scoped_item_ids = select(items.c.work_item_id).where(
            items.c.origin_run_key == run_key.value
        )
    current = current_stage_indexes(selected_schema, scoped_item_ids)
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
        statement = statement.where(items.c.campaign_key == campaign_key.value)
    else:
        assert run_key is not None
        statement = statement.where(items.c.origin_run_key == run_key.value)
    with engine.connect() as connection:
        if campaign_key is not None:
            require_campaign(
                connection,
                campaign_key=campaign_key,
                schema=selected_schema,
            )
        else:
            assert run_key is not None
            require_run(
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
    requested_item_ids = select(items.c.work_item_id).where(
        items.c.campaign_key == campaign_key.value,
        items.c.work_key.in_([key.value for key in work_keys]),
    )
    current = current_stage_indexes(schema, requested_item_ids)
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


def current_stage_indexes(schema: StagingSchema, work_item_ids: Select):
    """Group stages only for one already-filtered work-item set."""
    executions = schema.stage_executions
    scoped_ids = work_item_ids.subquery()
    return (
        select(
            executions.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .select_from(
            executions.join(
                scoped_ids,
                scoped_ids.c.work_item_id == executions.c.work_item_id,
            )
        )
        .group_by(executions.c.work_item_id)
        .subquery()
    )
