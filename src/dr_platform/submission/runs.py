from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from dr_platform._core.identities import (
    CampaignKey,
    RunKey,
    validate_key_value,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.validation import (
    validate_non_empty_string,
    validate_nonnegative_integer,
    validate_positive_integer,
)

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping


class PipelineRunConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PipelineRunRecord:
    run_key: RunKey
    campaign_key: CampaignKey
    pipeline_key: str
    pipeline_version: int
    execution_config_reference: str
    expected_member_count: int
    manifest_reference: str | None
    membership_digest: str | None
    run_completion_key: str | None
    created_at: datetime
    registration_closed_at: datetime | None
    registered_member_count: int | None
    created_work_count: int | None
    reused_work_count: int | None
    released_at: datetime | None
    release_terminal_state_counts: tuple[dict[str, object], ...] | None


def insert_pipeline_run(  # noqa: PLR0913 -- explicit persistence facts
    connection: Connection,
    *,
    run_key: RunKey | str,
    campaign_key: CampaignKey | str,
    pipeline_key: str,
    pipeline_version: int,
    execution_config_reference: str,
    created_at: datetime,
    expected_member_count: int = 0,
    manifest_reference: str | None = None,
    membership_digest: str | None = None,
    run_completion_key: str | None = None,
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
    validate_nonnegative_integer(
        expected_member_count, label="expected member count"
    )
    if (manifest_reference is None) != (membership_digest is None):
        raise ValueError(
            "manifest reference and membership digest must be supplied "
            "together"
        )
    if manifest_reference is not None:
        manifest_reference = validate_non_empty_string(
            manifest_reference, label="manifest reference"
        )
        membership_digest = validate_non_empty_string(
            membership_digest, label="membership digest"
        )
    if run_completion_key is not None:
        validate_key_value(run_completion_key, label="run completion key")
        if manifest_reference is None:
            raise ValueError("run completion requires a manifest binding")

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
                expected_member_count=expected_member_count,
                manifest_reference=manifest_reference,
                membership_digest=membership_digest,
                run_completion_key=run_completion_key,
                created_at=created_at,
            )
            .on_conflict_do_nothing(index_elements=["run_key"])
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
        raise RuntimeError(
            "pipeline run conflicted but no row was found on read-back "
            f"(run_key={normalized_run_key.value!r}); this requires "
            "READ COMMITTED isolation"
        )
    expected = (
        normalized_campaign_key,
        pipeline_key,
        pipeline_version,
        config_reference,
        expected_member_count,
        manifest_reference,
        membership_digest,
        run_completion_key,
    )
    actual = (
        existing.campaign_key,
        existing.pipeline_key,
        existing.pipeline_version,
        existing.execution_config_reference,
        existing.expected_member_count,
        existing.manifest_reference,
        existing.membership_digest,
        existing.run_completion_key,
    )
    if actual != expected:
        raise PipelineRunConflictError(
            "run key is already bound to different immutable provenance: "
            f"{normalized_run_key.value!r}"
        )
    return existing


def get_pipeline_run(
    connection: Connection,
    *,
    run_key: RunKey | str,
    for_update: bool = False,
    schema: StagingSchema | None = None,
) -> PipelineRunRecord | None:
    selected_schema = schema or StagingSchema()
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    table = selected_schema.pipeline_runs
    statement = table.select().where(
        table.c.run_key == normalized_run_key.value
    )
    if for_update:
        statement = statement.with_for_update(of=table)
    row = connection.execute(statement).mappings().one_or_none()
    return None if row is None else _decode_pipeline_run(row)


def close_registration(  # noqa: PLR0913 -- explicit receipt facts
    connection: Connection,
    *,
    run_key: RunKey,
    membership_digest: str | None,
    member_count: int,
    created_work_count: int,
    reused_work_count: int,
    closed_at: datetime,
    schema: StagingSchema,
) -> PipelineRunRecord:
    table = schema.pipeline_runs
    row = (
        connection.execute(
            update(table)
            .where(
                table.c.run_key == run_key.value,
                table.c.registration_closed_at.is_(None),
            )
            .values(
                membership_digest=membership_digest,
                registration_closed_at=closed_at,
                registered_member_count=member_count,
                created_work_count=created_work_count,
                reused_work_count=reused_work_count,
            )
            .returning(*table.c)
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        return _decode_pipeline_run(row)
    existing = get_pipeline_run(
        connection, run_key=run_key, for_update=True, schema=schema
    )
    if existing is None:
        raise LookupError(f"pipeline run does not exist: {run_key}")
    return existing


def _decode_pipeline_run(row: RowMapping) -> PipelineRunRecord:
    counts = row["release_terminal_state_counts"]
    return PipelineRunRecord(
        run_key=RunKey(row["run_key"]),
        campaign_key=CampaignKey(row["campaign_key"]),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        execution_config_reference=row["execution_config_reference"],
        expected_member_count=row["expected_member_count"],
        manifest_reference=row["manifest_reference"],
        membership_digest=row["membership_digest"],
        run_completion_key=row["run_completion_key"],
        created_at=row["created_at"],
        registration_closed_at=row["registration_closed_at"],
        registered_member_count=row["registered_member_count"],
        created_work_count=row["created_work_count"],
        reused_work_count=row["reused_work_count"],
        released_at=row["released_at"],
        release_terminal_state_counts=(
            None if counts is None else tuple(dict(item) for item in counts)
        ),
    )
