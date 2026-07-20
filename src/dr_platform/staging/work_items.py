"""Persistence leaf operations for campaign-scoped work items."""

from __future__ import annotations

from dataclasses import dataclass
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
from dr_platform.staging.records import (
    PipelineRunRecord,
    WorkItemRecord,
    immutable_mapping,
)
from dr_platform.staging.runs import get_pipeline_run
from dr_platform.staging.schema import StagingSchema

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


class WorkItemConflictError(RuntimeError):
    """A campaign/work identity was reused with different immutable facts."""


@dataclass(frozen=True, slots=True)
class WorkItemInsertResult:
    """The stored item and whether this call inserted it."""

    work_item: WorkItemRecord
    inserted: bool


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
    return insert_work_item_with_result(
        connection,
        campaign_key=campaign_key,
        work_key=work_key,
        origin_run_key=origin_run_key,
        input_reference=input_reference,
        labels=labels,
        schema=schema,
    ).work_item


def insert_work_item_with_result(  # noqa: PLR0913
    connection: Connection,
    *,
    campaign_key: CampaignKey | str,
    work_key: WorkKey | str,
    origin_run_key: RunKey | str,
    input_reference: str,
    labels: Mapping[str, str],
    schema: StagingSchema | None = None,
    pipeline_run: PipelineRunRecord | None = None,
) -> WorkItemInsertResult:
    """Insert one item and report its campaign-idempotency disposition.

    ``pipeline_run`` lets a caller that already resolved the origin run
    (e.g. once per chunk in a streaming submit loop) skip the per-item
    ``get_pipeline_run`` SELECT. Direct callers that omit it still get the
    lookup performed here, keyed by ``origin_run_key``.
    """
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
    if pipeline_run is not None:
        if pipeline_run.run_key != normalized_run_key:
            raise WorkItemConflictError(
                "supplied pipeline run does not match origin_run_key"
            )
        run = pipeline_run
    else:
        run = get_pipeline_run(
            connection,
            run_key=normalized_run_key,
            schema=selected_schema,
        )
        if run is None:
            raise LookupError(
                f"pipeline run does not exist: {normalized_run_key}"
            )
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
        if existing is None:
            # ON CONFLICT DO NOTHING fired, so a row for this campaign/work
            # identity was committed by another transaction. The read-back
            # requires READ COMMITTED to observe it; under REPEATABLE READ
            # this branch can be legitimately reached from a snapshot taken
            # before that commit, which is a caller/isolation error, not a
            # missing row.
            raise RuntimeError(
                "work item conflicted but no row was found on read-back "
                f"(campaign_key={identity.campaign_key.value!r}, "
                f"work_key={identity.work_key.value!r}); this requires "
                "READ COMMITTED isolation"
            )
        # The origin run is first-writer provenance. Later runs in the same
        # campaign converge on that work item when its application facts match.
        if (
            existing.input_reference != reference
            or dict(existing.labels) != normalized_labels
            or existing.rank != rank
        ):
            raise WorkItemConflictError(
                "campaign/work identity is already bound to different "
                "immutable facts"
            )
        return WorkItemInsertResult(work_item=existing, inserted=False)
    return WorkItemInsertResult(
        work_item=_decode_work_item(row),
        inserted=True,
    )


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
