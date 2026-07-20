"""Persistence leaf operations for campaign-scoped work items."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from dr_platform.staging._validation import (
    validate_labels,
    validate_non_empty_string,
)
from dr_platform.staging.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    RunKey,
    WorkKey,
)
from dr_platform.staging.recipes import stable_random_rank
from dr_platform.staging.records import WorkItemRecord, immutable_mapping
from dr_platform.staging.runs import get_pipeline_run
from dr_platform.staging.schema import StagingSchema

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


class WorkItemConflictError(RuntimeError):
    """A campaign/work identity was reused with different immutable facts."""


def insert_work_item(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    campaign_key: CampaignKey | str,
    work_key: WorkKey | str,
    origin_run_key: RunKey | str,
    input_reference: str,
    labels: Mapping[str, str],
    schema: StagingSchema | None = None,
) -> WorkItemRecord:
    """Insert one work item, or resolve an identical idempotent replay."""
    selected_schema = schema or StagingSchema()
    identity = CampaignWorkIdentity(
        campaign_key
        if isinstance(campaign_key, CampaignKey)
        else CampaignKey(campaign_key),
        work_key if isinstance(work_key, WorkKey) else WorkKey(work_key),
    )
    normalized_run_key = (
        origin_run_key
        if isinstance(origin_run_key, RunKey)
        else RunKey(origin_run_key)
    )
    reference = validate_non_empty_string(
        input_reference, label="input reference"
    )
    normalized_labels = validate_labels(labels, label="work item labels")
    run = get_pipeline_run(
        connection,
        run_key=normalized_run_key,
        schema=selected_schema,
    )
    if run is None:
        raise LookupError(f"pipeline run does not exist: {normalized_run_key}")
    if run.campaign_key != identity.campaign_key:
        raise WorkItemConflictError(
            "work item campaign does not match its origin run"
        )

    rank = stable_random_rank(work_identity=identity)
    table = selected_schema.work_items
    row = (
        connection.execute(
            insert(table)
            .values(
                campaign_key=identity.campaign_key.value,
                work_key=identity.work_key.value,
                origin_run_key=normalized_run_key.value,
                input_reference=reference,
                labels=normalized_labels,
                rank=rank,
            )
            .on_conflict_do_nothing(
                index_elements=["campaign_key", "work_key"]
            )
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        existing = get_work_item(
            connection,
            campaign_key=identity.campaign_key,
            work_key=identity.work_key,
            schema=selected_schema,
        )
        assert existing is not None
        if (
            existing.origin_run_key != normalized_run_key
            or existing.input_reference != reference
            or dict(existing.labels) != normalized_labels
            or existing.rank != rank
        ):
            raise WorkItemConflictError(
                "campaign/work identity is already bound to different "
                "immutable facts"
            )
        return existing
    return _decode_work_item(row)


def get_work_item(
    connection: Connection,
    *,
    campaign_key: CampaignKey | str,
    work_key: WorkKey | str,
    schema: StagingSchema | None = None,
) -> WorkItemRecord | None:
    selected_schema = schema or StagingSchema()
    identity = CampaignWorkIdentity(
        campaign_key
        if isinstance(campaign_key, CampaignKey)
        else CampaignKey(campaign_key),
        work_key if isinstance(work_key, WorkKey) else WorkKey(work_key),
    )
    table = selected_schema.work_items
    row = (
        connection.execute(
            table.select().where(
                table.c.campaign_key == identity.campaign_key.value,
                table.c.work_key == identity.work_key.value,
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_work_item(row)


def list_work_items(
    connection: Connection,
    *,
    campaign_key: CampaignKey | str | None = None,
    label_selector: Mapping[str, str] | None = None,
    schema: StagingSchema | None = None,
) -> tuple[WorkItemRecord, ...]:
    """Read work items, optionally using PostgreSQL JSONB containment."""
    selected_schema = schema or StagingSchema()
    table = selected_schema.work_items
    statement = select(table).order_by(table.c.work_item_id)
    if campaign_key is not None:
        normalized_campaign_key = (
            campaign_key
            if isinstance(campaign_key, CampaignKey)
            else CampaignKey(campaign_key)
        )
        statement = statement.where(
            table.c.campaign_key == normalized_campaign_key.value
        )
    if label_selector is not None:
        selector = validate_labels(label_selector, label="label selector")
        statement = statement.where(table.c.labels.contains(selector))
    return tuple(
        _decode_work_item(row)
        for row in connection.execute(statement).mappings()
    )


def _decode_work_item(row: RowMapping) -> WorkItemRecord:
    return WorkItemRecord(
        work_item_id=row["work_item_id"],
        campaign_key=CampaignKey(row["campaign_key"]),
        work_key=WorkKey(row["work_key"]),
        origin_run_key=RunKey(row["origin_run_key"]),
        input_reference=row["input_reference"],
        labels=immutable_mapping(row["labels"]),
        rank=row["rank"],
    )
