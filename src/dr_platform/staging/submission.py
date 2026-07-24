"""Streaming submission for staged pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dr_platform.staging._validation import (
    validate_labels,
    validate_non_empty_string,
)
from dr_platform.staging.definitions import (
    PipelineIdentity,
    validate_pipeline_identity,
    validate_positive_integer,
)
from dr_platform.staging.identities import (
    CampaignKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform.staging.records import immutable_mapping
from dr_platform.staging.runs import (
    get_pipeline_run,
    insert_pipeline_run,
    mark_submission_completed,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_executions import insert_stage_execution
from dr_platform.staging.work_items import insert_work_item_with_result

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from sqlalchemy import Engine

    from dr_platform.staging.registry import PipelineRegistry

DEFAULT_CHUNK_SIZE = 500


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True, init=False)
class WorkInput:
    """One validated, immutable item at the submission boundary."""

    work_key: WorkKey
    input_reference: str
    labels: Mapping[str, str]

    def __init__(
        self,
        *,
        work_key: WorkKey | str,
        input_reference: str,
        labels: Mapping[str, str],
    ) -> None:
        normalized_work_key = (
            work_key if isinstance(work_key, WorkKey) else WorkKey(work_key)
        )
        normalized_input_reference = validate_non_empty_string(
            input_reference,
            label="input reference",
        )
        normalized_labels = validate_labels(labels, label="work input labels")
        object.__setattr__(self, "work_key", normalized_work_key)
        object.__setattr__(self, "input_reference", normalized_input_reference)
        object.__setattr__(
            self,
            "labels",
            immutable_mapping(normalized_labels),
        )


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    """Run identity and item dispositions committed by this call."""

    run_key: RunKey
    inserted_count: int
    already_existing_count: int


def submit(  # noqa: PLR0913 -- explicit submission boundary
    *,
    campaign_key: CampaignKey | str,
    run_key: RunKey | str,
    pipeline: PipelineIdentity,
    execution_config_reference: str,
    items: Iterable[WorkInput],
    registry: PipelineRegistry,
    engine: Engine,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> SubmissionReceipt:
    """Incrementally commit campaign work from an arbitrary iterable."""
    validate_positive_integer(chunk_size, label="chunk size")
    validate_pipeline_identity(pipeline)
    selected_schema = schema or StagingSchema()
    normalized_campaign_key = (
        campaign_key
        if isinstance(campaign_key, CampaignKey)
        else CampaignKey(campaign_key)
    )
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    pipeline_definition = registry.get(
        key=pipeline.key,
        version=pipeline.version,
    )
    first_stage = pipeline_definition.stages[0]

    with engine.begin() as connection:
        insert_pipeline_run(
            connection,
            run_key=normalized_run_key,
            campaign_key=normalized_campaign_key,
            pipeline_key=pipeline_definition.key.value,
            pipeline_version=pipeline_definition.version,
            execution_config_reference=execution_config_reference,
            created_at=clock(),
            schema=selected_schema,
        )

    inserted_count = 0
    already_existing_count = 0
    chunk: list[WorkInput] = []
    for item in items:
        if not isinstance(item, WorkInput):
            raise TypeError("items must yield WorkInput values")
        chunk.append(item)
        if len(chunk) < chunk_size:
            continue
        inserted, existing = _commit_chunk(
            engine=engine,
            schema=selected_schema,
            campaign_key=normalized_campaign_key,
            run_key=normalized_run_key,
            first_stage_key=first_stage.key,
            chunk=chunk,
            clock=clock,
        )
        inserted_count += inserted
        already_existing_count += existing
        chunk.clear()

    if chunk:
        inserted, existing = _commit_chunk(
            engine=engine,
            schema=selected_schema,
            campaign_key=normalized_campaign_key,
            run_key=normalized_run_key,
            first_stage_key=first_stage.key,
            chunk=chunk,
            clock=clock,
        )
        inserted_count += inserted
        already_existing_count += existing

    with engine.begin() as connection:
        mark_submission_completed(
            connection,
            run_key=normalized_run_key,
            completed_at=clock(),
            schema=selected_schema,
        )

    return SubmissionReceipt(
        run_key=normalized_run_key,
        inserted_count=inserted_count,
        already_existing_count=already_existing_count,
    )


def _commit_chunk(  # noqa: PLR0913 -- explicit chunk dependencies
    *,
    engine: Engine,
    schema: StagingSchema,
    campaign_key: CampaignKey,
    run_key: RunKey,
    first_stage_key: StageKey,
    chunk: list[WorkInput],
    clock: Callable[[], datetime],
) -> tuple[int, int]:
    inserted_count = 0
    already_existing_count = 0
    with engine.begin() as connection:
        created_at = clock()
        # Resolved once per chunk rather than once per item: at 10^4-10^5
        # items/chunk_size chunks, a per-item get_pipeline_run SELECT here
        # would triple this loop's statement count for no new information,
        # since the run is immutable for the lifetime of this chunk's
        # transaction.
        run = get_pipeline_run(connection, run_key=run_key, schema=schema)
        if run is None:
            raise LookupError(f"pipeline run does not exist: {run_key}")
        for item in chunk:
            result = insert_work_item_with_result(
                connection,
                campaign_key=campaign_key,
                work_key=item.work_key,
                origin_run_key=run_key,
                input_reference=item.input_reference,
                labels=item.labels,
                schema=schema,
                pipeline_run=run,
            )
            if not result.inserted:
                already_existing_count += 1
                continue
            insert_stage_execution(
                connection,
                work_item_id=result.work_item.work_item_id,
                stage_key=first_stage_key,
                stage_index=0,
                created_at=created_at,
                schema=schema,
            )
            inserted_count += 1
    return inserted_count, already_existing_count
