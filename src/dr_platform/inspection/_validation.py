from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dr_platform._core.validation import (
    validate_nonnegative_integer,
    validate_positive_integer,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform._core.identities import CampaignKey, RunKey
    from dr_platform._core.ledger.schema import LedgerSchema

DEFAULT_INSPECTION_LIMIT = 10_000


def require_campaign(
    connection: Connection,
    *,
    campaign_key: CampaignKey,
    schema: LedgerSchema,
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
    schema: LedgerSchema,
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
    schema: LedgerSchema,
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
    schema: LedgerSchema,
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


def validate_exclusive_stage_index_range(
    *,
    min_stage_index: int | None,
    max_stage_index: int,
) -> None:
    """Validate an exclusive index window on ``stage_index``."""
    validate_nonnegative_integer(max_stage_index, label="max stage index")
    if min_stage_index is not None:
        validate_nonnegative_integer(
            min_stage_index,
            label="min stage index",
        )
        if min_stage_index >= max_stage_index:
            raise ValueError(
                "min stage index must be less than max stage index "
                "(exclusive range)"
            )


def validate_optional_exclusive_stage_index_bounds(
    *,
    min_stage_index: int | None,
    max_stage_index: int | None,
    default_max_stage_index: int | None = None,
) -> int | None:
    """Validate optional exclusive bounds; return the effective upper bound."""
    effective_max = (
        max_stage_index
        if max_stage_index is not None
        else default_max_stage_index
    )
    if (
        min_stage_index is not None
        and max_stage_index is None
        and effective_max is None
    ):
        validate_nonnegative_integer(
            min_stage_index,
            label="min stage index",
        )
    elif effective_max is not None:
        validate_exclusive_stage_index_range(
            min_stage_index=min_stage_index,
            max_stage_index=effective_max,
        )
    return effective_max


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
    schema: LedgerSchema,
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
