from __future__ import annotations

from dataclasses import dataclass
from enum import UNIQUE, StrEnum, verify
from typing import TYPE_CHECKING

from dr_serialize import json_hash
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    RunKey,
    WorkKey,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.validation import (
    validate_labels,
    validate_non_empty_string,
)
from dr_platform.submission.runs import PipelineRunRecord, get_pipeline_run

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

SHUFFLE_RANK_BITS = 63
SHUFFLE_RANK_HEX_LENGTH = 16
SHUFFLE_RANK_MAX = (1 << SHUFFLE_RANK_BITS) - 1


@verify(UNIQUE)
class WorkRankDigestField(StrEnum):
    """Persisted wire keys; spell them out at hashing sites, never iterate."""

    CAMPAIGN_KEY = "campaign_key"
    WORK_KEY = "work_key"


def stable_random_rank(*, work_identity: CampaignWorkIdentity) -> int:
    """Derive a stable positive signed-63-bit rank for campaign work."""
    digest = json_hash(
        {
            WorkRankDigestField.CAMPAIGN_KEY: (
                work_identity.campaign_key.value
            ),
            WorkRankDigestField.WORK_KEY: work_identity.work_key.value,
        },
        length=SHUFFLE_RANK_HEX_LENGTH,
    )
    return (int(digest, 16) % SHUFFLE_RANK_MAX) + 1


class WorkItemConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WorkItemRecord:
    work_item_id: int
    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    input_reference: str
    labels: Mapping[str, str]
    rank: int


@dataclass(frozen=True, slots=True)
class WorkItemInsertResult:
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
            raise ValueError(
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
            # Concurrent inserts are visible on read-back only under the
            # required READ COMMITTED isolation level.
            raise RuntimeError(
                "work item conflicted but no row was found on read-back "
                f"(campaign_key={identity.campaign_key.value!r}, "
                f"work_key={identity.work_key.value!r}); this requires "
                "READ COMMITTED isolation"
            )
        # Origin run is first-writer provenance across matching submissions.
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
