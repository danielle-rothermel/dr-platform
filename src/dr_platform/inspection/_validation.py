from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dr_platform._core.identities import CampaignKey, RunKey, WorkKey
from dr_platform._core.validation import validate_positive_integer

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import StagingSchema

DEFAULT_INSPECTION_LIMIT = 10_000


def require_campaign(
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


def require_run(
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


def validate_campaign_cursor(
    connection: Connection,
    *,
    cursor: CampaignKey,
    schema: StagingSchema,
) -> None:
    exists = connection.execute(
        select(schema.pipeline_runs.c.campaign_key).where(
            schema.pipeline_runs.c.campaign_key == cursor.value
        )
    ).first()
    if exists is None:
        raise ValueError("campaign cursor is unknown among campaigns")


def validate_work_item_cursor(
    connection: Connection,
    *,
    cursor: int,
    campaign_key: CampaignKey,
    schema: StagingSchema,
) -> None:
    validate_work_item_id(cursor)
    exists = connection.execute(
        select(schema.work_items.c.work_item_id).where(
            schema.work_items.c.work_item_id == cursor,
            schema.work_items.c.campaign_key == campaign_key.value,
        )
    ).scalar_one_or_none()
    if exists is None:
        raise ValueError("work item cursor is unknown in this campaign")


def validate_limit(limit: int) -> None:
    validate_positive_integer(limit, label="inspection limit")


def validate_work_item_id(work_item_id: int) -> None:
    if (
        isinstance(work_item_id, bool)
        or not isinstance(work_item_id, int)
        or work_item_id <= 0
    ):
        raise ValueError("work item id must be a positive integer")


def validate_run_member_cursor(
    connection: Connection,
    *,
    cursor: int,
    run_key: RunKey,
    schema: StagingSchema,
) -> None:
    if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
        raise ValueError("run member cursor must be a non-negative integer")
    exists = connection.execute(
        select(schema.run_memberships.c.member_ordinal).where(
            schema.run_memberships.c.run_key == run_key.value,
            schema.run_memberships.c.member_ordinal == cursor,
        )
    ).scalar_one_or_none()
    if exists is None:
        raise ValueError("run member cursor is unknown in this run")


def normalize_campaign_key(value: CampaignKey | str) -> CampaignKey:
    return value if isinstance(value, CampaignKey) else CampaignKey(value)


def normalize_run_key(value: RunKey | str) -> RunKey:
    return value if isinstance(value, RunKey) else RunKey(value)


def normalize_work_key(value: WorkKey | str) -> WorkKey:
    return value if isinstance(value, WorkKey) else WorkKey(value)
