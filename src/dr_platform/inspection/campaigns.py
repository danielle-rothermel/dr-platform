from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_, select

from dr_platform._core.identities import CampaignKey, RunKey, normalize_key
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform.inspection._validation import (
    DEFAULT_INSPECTION_LIMIT,
    require_campaign,
    validate_campaign_cursor,
    validate_limit,
)

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Engine
    from sqlalchemy.engine import RowMapping


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
    expected_member_count: int
    manifest_reference: str | None
    membership_digest: str | None
    created_at: datetime
    registration_closed_at: datetime | None
    registered_member_count: int | None
    released_at: datetime | None


def inspect_campaign(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> CampaignSummary:
    selected_schema = schema or StagingSchema()
    normalized_campaign = normalize_key(campaign_key, CampaignKey)
    statement = _campaign_summary_statement(selected_schema)
    with engine.connect() as connection:
        row = (
            connection.execute(
                statement.where(
                    statement.selected_columns.campaign_key
                    == normalized_campaign.value
                )
            )
            .mappings()
            .one_or_none()
        )
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
    validate_limit(limit)
    selected_schema = schema or StagingSchema()
    normalized_cursor = (
        normalize_key(cursor, CampaignKey) if cursor is not None else None
    )
    statement = _campaign_summary_statement(selected_schema).limit(limit)
    with engine.connect() as connection:
        if normalized_cursor is not None:
            validate_campaign_cursor(
                connection,
                cursor=normalized_cursor,
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
    validate_limit(limit)
    selected_schema = schema or StagingSchema()
    normalized_campaign = normalize_key(campaign_key, CampaignKey)
    normalized_cursor = (
        normalize_key(cursor, RunKey) if cursor is not None else None
    )
    runs = selected_schema.pipeline_runs
    with engine.connect() as connection:
        require_campaign(
            connection,
            campaign_key=normalized_campaign,
            schema=selected_schema,
        )
        after = None
        if normalized_cursor is not None:
            cursor_created_at = connection.execute(
                select(runs.c.created_at).where(
                    runs.c.campaign_key == normalized_campaign.value,
                    runs.c.run_key == normalized_cursor.value,
                )
            ).scalar_one_or_none()
            if cursor_created_at is None:
                raise ValueError("run cursor is unknown in this campaign")
            after = (cursor_created_at, normalized_cursor.value)
        statement = _run_summary_statement(
            selected_schema,
            campaign_key=normalized_campaign.value,
            limit=limit,
            after=after,
        )
        return tuple(
            _decode_run_summary(row)
            for row in connection.execute(statement).mappings()
        )


def _run_summary_statement(
    schema: StagingSchema,
    *,
    campaign_key: str,
    limit: int,
    after: tuple[datetime, str] | None,
):
    runs = schema.pipeline_runs
    memberships = schema.run_memberships
    selected_runs = (
        select(*runs.c)
        .where(runs.c.campaign_key == campaign_key)
        .order_by(runs.c.created_at, runs.c.run_key)
        .limit(limit)
    )
    if after is not None:
        cursor_created_at, cursor_run_key = after
        selected_runs = selected_runs.where(
            or_(
                runs.c.created_at > cursor_created_at,
                and_(
                    runs.c.created_at == cursor_created_at,
                    runs.c.run_key > cursor_run_key,
                ),
            )
        )
    selected_page = selected_runs.subquery("selected_runs")
    return (
        select(
            *selected_page.c,
            func.count(memberships.c.work_item_id).label("member_count"),
        )
        .select_from(
            selected_page.outerjoin(
                memberships,
                selected_page.c.run_key == memberships.c.run_key,
            )
        )
        .group_by(*selected_page.c)
        .order_by(selected_page.c.created_at, selected_page.c.run_key)
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
        expected_member_count=row["expected_member_count"],
        manifest_reference=row["manifest_reference"],
        membership_digest=row["membership_digest"],
        created_at=row["created_at"],
        registration_closed_at=row["registration_closed_at"],
        registered_member_count=(
            row["registered_member_count"]
            if row["registration_closed_at"] is not None
            else row["member_count"]
        ),
        released_at=row["released_at"],
    )
