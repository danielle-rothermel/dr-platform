from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select

from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import CampaignKey, RunKey, StageKey, WorkKey
from dr_platform._core.ledger.attempts import get_stage_attempt
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.validation import validate_nonnegative_integer
from dr_platform.admission.runner import AdmissionPayload
from dr_platform.inspection._validation import validate_work_item_id

if TYPE_CHECKING:
    from sqlalchemy import Engine


def admission_payload_for_stage(
    engine: Engine,
    *,
    work_item_id: int,
    stage_index: int,
    schema: LedgerSchema | None = None,
    attempt_number: int = 1,
) -> AdmissionPayload:
    """Assemble the admission payload for one seeded stage row."""
    validate_work_item_id(work_item_id)
    validate_nonnegative_integer(stage_index, label="stage index")
    if attempt_number < 1:
        raise ValueError("attempt number must be positive")
    selected_schema = schema or LedgerSchema()
    executions = selected_schema.stage_executions
    work_items = selected_schema.work_items
    runs = selected_schema.pipeline_runs
    statement = (
        select(
            executions.c.stage_execution_id,
            executions.c.work_item_id,
            executions.c.stage_index,
            executions.c.stage_key,
            work_items.c.campaign_key,
            work_items.c.work_key,
            work_items.c.origin_run_key,
            func.coalesce(
                executions.c.input_reference,
                work_items.c.input_reference,
            ).label("input_reference"),
            work_items.c.labels,
            runs.c.pipeline_key,
            runs.c.pipeline_version,
        )
        .select_from(
            executions.join(
                work_items,
                executions.c.work_item_id == work_items.c.work_item_id,
            ).join(
                runs,
                work_items.c.origin_run_key == runs.c.run_key,
            )
        )
        .where(
            executions.c.work_item_id == work_item_id,
            executions.c.stage_index == stage_index,
        )
    )
    with engine.connect() as connection:
        row = connection.execute(statement).mappings().first()
        if row is None:
            raise LookupError(
                f"stage execution does not exist at index {stage_index} "
                f"for work item {work_item_id}"
            )
        attempt = get_stage_attempt(
            connection,
            stage_execution_id=row["stage_execution_id"],
            attempt_number=attempt_number,
            schema=selected_schema,
        )
        if attempt is None:
            raise LookupError(
                f"attempt {attempt_number} does not exist for stage "
                f"execution {row['stage_execution_id']}"
            )

    return AdmissionPayload(
        campaign_key=CampaignKey(row["campaign_key"]),
        work_key=WorkKey(row["work_key"]),
        work_item_id=row["work_item_id"],
        origin_run_key=RunKey(row["origin_run_key"]),
        input_reference=row["input_reference"],
        labels=immutable_mapping(row["labels"]),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        stage_key=StageKey(row["stage_key"]),
        stage_index=row["stage_index"],
        attempt_number=attempt_number,
    )
