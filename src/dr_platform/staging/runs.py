"""Persistence leaf operations for immutable pipeline runs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from dr_platform.staging._validation import validate_non_empty_string
from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.identities import (
    CampaignKey,
    RunKey,
    validate_key_value,
)
from dr_platform.staging.records import PipelineRunRecord
from dr_platform.staging.schema import StagingSchema

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


class PipelineRunConflictError(RuntimeError):
    """A run key was reused with conflicting immutable provenance."""


def insert_pipeline_run(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    run_key: RunKey | str,
    campaign_key: CampaignKey | str,
    pipeline_key: str,
    pipeline_version: int,
    execution_config_reference: str,
    created_at: datetime,
    submission_completed_at: datetime | None = None,
    schema: StagingSchema | None = None,
) -> PipelineRunRecord:
    """Insert a run, or resolve an identical replay to the stored run."""
    selected_schema = schema or StagingSchema()
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    normalized_campaign_key = (
        campaign_key
        if isinstance(campaign_key, CampaignKey)
        else CampaignKey(campaign_key)
    )
    validate_key_value(pipeline_key, label="pipeline key")
    validate_positive_integer(pipeline_version, label="pipeline version")
    config_reference = validate_non_empty_string(
        execution_config_reference,
        label="execution config reference",
    )
    table = selected_schema.pipeline_runs
    row = (
        connection.execute(
            insert(table)
            .values(
                run_key=normalized_run_key.value,
                campaign_key=normalized_campaign_key.value,
                pipeline_key=pipeline_key,
                pipeline_version=pipeline_version,
                execution_config_reference=config_reference,
                created_at=created_at,
                submission_completed_at=submission_completed_at,
            )
            .on_conflict_do_nothing(index_elements=["run_key"])
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        existing = get_pipeline_run(
            connection,
            run_key=normalized_run_key,
            schema=selected_schema,
        )
        assert existing is not None
        if (
            existing.campaign_key != normalized_campaign_key
            or existing.pipeline_key != pipeline_key
            or existing.pipeline_version != pipeline_version
            or existing.execution_config_reference != config_reference
        ):
            raise PipelineRunConflictError(
                "run key is already bound to different immutable "
                f"provenance: {normalized_run_key.value!r}"
            )
        return existing
    return _decode_pipeline_run(row)


def get_pipeline_run(
    connection: Connection,
    *,
    run_key: RunKey | str,
    schema: StagingSchema | None = None,
) -> PipelineRunRecord | None:
    selected_schema = schema or StagingSchema()
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    table = selected_schema.pipeline_runs
    row = (
        connection.execute(
            table.select().where(
                table.c.run_key == normalized_run_key.value
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_pipeline_run(row)


def mark_submission_completed(
    connection: Connection,
    *,
    run_key: RunKey | str,
    completed_at: datetime,
    schema: StagingSchema | None = None,
) -> PipelineRunRecord:
    """Record the first normal completion of a run's item source."""
    selected_schema = schema or StagingSchema()
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    table = selected_schema.pipeline_runs
    row = (
        connection.execute(
            update(table)
            .where(
                table.c.run_key == normalized_run_key.value,
                table.c.submission_completed_at.is_(None),
            )
            .values(submission_completed_at=completed_at)
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return _decode_pipeline_run(row)

    existing = get_pipeline_run(
        connection,
        run_key=normalized_run_key,
        schema=selected_schema,
    )
    if existing is None:
        raise LookupError(f"pipeline run does not exist: {normalized_run_key}")
    return existing


def _decode_pipeline_run(row: RowMapping) -> PipelineRunRecord:
    return PipelineRunRecord(
        run_key=RunKey(row["run_key"]),
        campaign_key=CampaignKey(row["campaign_key"]),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        execution_config_reference=row["execution_config_reference"],
        created_at=row["created_at"],
        submission_completed_at=row["submission_completed_at"],
    )
