from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    CampaignKey,
    RunKey,
    StageKey,
    WorkKey,
    normalize_key,
)
from dr_platform._core.ledger.attempts import (
    StageAttemptRecord,
    list_stage_attempts,
)
from dr_platform._core.ledger.executions import StageExecutionRecord
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.work_item_status import (
    list_predecessor_stage_outputs_statement,
    list_stage_executions_statement,
    work_item_status_rows,
)
from dr_platform._core.validation import validate_nonnegative_integer
from dr_platform.inspection._validation import (
    DEFAULT_INSPECTION_LIMIT,
    require_campaign,
    validate_limit,
    validate_optional_exclusive_stage_index_bounds,
    validate_work_item_cursor,
    validate_work_item_id,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Engine
    from sqlalchemy.engine import RowMapping


@dataclass(frozen=True, slots=True)
class WorkItemSummary:
    work_item_id: int
    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    labels: Mapping[str, str]
    current_stage_execution_id: int
    current_stage_key: StageKey
    current_stage_index: int
    state: StageExecutionState


@dataclass(frozen=True, slots=True)
class PredecessorStageOutput:
    stage_index: int
    stage_key: StageKey
    input_reference: str | None
    output_reference: str


@dataclass(frozen=True, slots=True)
class StageExecutionSummary:
    execution: StageExecutionRecord
    attempts: tuple[StageAttemptRecord, ...]


def list_work_items(  # noqa: PLR0913 -- explicit reader filters
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    state: StageExecutionState | None = None,
    cursor: int | None = None,
    limit: int = DEFAULT_INSPECTION_LIMIT,
    schema: LedgerSchema | None = None,
) -> tuple[WorkItemSummary, ...]:
    validate_limit(limit)
    if state is not None and not isinstance(state, StageExecutionState):
        raise TypeError("state must be a StageExecutionState")
    selected_schema = schema or LedgerSchema()
    normalized_campaign = normalize_key(campaign_key, CampaignKey)
    items = selected_schema.work_items
    campaign_item_ids = select(items.c.work_item_id).where(
        items.c.campaign_key == normalized_campaign.value
    )
    status = work_item_status_rows(selected_schema, campaign_item_ids)
    statement = (
        select(
            items.c.work_item_id,
            items.c.campaign_key,
            items.c.work_key,
            items.c.origin_run_key,
            items.c.labels,
            status.c.stage_execution_id,
            status.c.stage_key,
            status.c.stage_index,
            status.c.state,
        )
        .select_from(
            items.join(
                status,
                status.c.work_item_id == items.c.work_item_id,
            )
        )
        .where(items.c.campaign_key == normalized_campaign.value)
        .order_by(items.c.work_item_id)
        .limit(limit)
    )
    if state is not None:
        statement = statement.where(status.c.state == state.value)
    with engine.connect() as connection:
        require_campaign(
            connection,
            campaign_key=normalized_campaign,
            schema=selected_schema,
        )
        if cursor is not None:
            validate_work_item_cursor(
                connection,
                cursor=cursor,
                campaign_key=normalized_campaign,
                schema=selected_schema,
            )
            statement = statement.where(items.c.work_item_id > cursor)
        return tuple(
            _decode_work_item_summary(row)
            for row in connection.execute(statement).mappings()
        )


def get_work_item_stages(
    work_item_id: int,
    *,
    engine: Engine,
    schema: LedgerSchema | None = None,
) -> tuple[StageExecutionSummary, ...]:
    validate_work_item_id(work_item_id)
    selected_schema = schema or LedgerSchema()
    table = selected_schema.stage_executions
    with engine.connect() as connection:
        rows = connection.execute(
            select(table)
            .where(table.c.work_item_id == work_item_id)
            .order_by(table.c.stage_index, table.c.stage_execution_id)
        ).mappings()
        summaries = tuple(
            StageExecutionSummary(
                execution=_decode_stage_execution(row),
                attempts=list_stage_attempts(
                    connection,
                    stage_execution_id=row["stage_execution_id"],
                    schema=selected_schema,
                ),
            )
            for row in rows
        )
    if not summaries:
        raise LookupError(f"work item does not exist: {work_item_id}")
    return summaries


def list_predecessor_stage_outputs(  # noqa: PLR0913 -- explicit reader filters
    work_item_id: int,
    below_stage_index: int,
    *,
    engine: Engine,
    schema: LedgerSchema | None = None,
    stage_key: StageKey | str | None = None,
    min_stage_index: int | None = None,
    max_stage_index: int | None = None,
) -> tuple[PredecessorStageOutput, ...]:
    """Return succeeded lower-index sibling outputs for join stage bodies.

    Rows are ordered by ascending ``stage_index``. Only ``SUCCEEDED``
    executions with a non-null ``output_reference`` are included. Complements
    the admission barrier gate, which blocks until every lower ``stage_index``
    for the same work item is ``SUCCEEDED``.

    ``min_stage_index`` and ``max_stage_index`` are **exclusive** bounds on
    ``stage_index`` (``stage_index > min``, ``stage_index < max``). When
    ``max_stage_index`` is omitted, the exclusive upper bound is
    ``below_stage_index`` (unlike ``list_stage_executions``, which has no
    implicit cap). Filters are ANDed; unset filters apply no constraint.

    Typical fan-in for one deferral episode at index ``F`` after deferring
    ``optim_step`` at ``O``::

        list_predecessor_stage_outputs(
            work_item_id,
            below_stage_index=F,
            stage_key=STAGE_EVAL_ROW,
            min_stage_index=O,
            engine=engine,
        )
    """
    validate_work_item_id(work_item_id)
    validate_nonnegative_integer(below_stage_index, label="below stage index")
    if max_stage_index is not None and max_stage_index > below_stage_index:
        raise ValueError("max stage index must not exceed below stage index")
    validate_optional_exclusive_stage_index_bounds(
        min_stage_index=min_stage_index,
        max_stage_index=max_stage_index,
        default_max_stage_index=below_stage_index,
    )
    normalized_stage_key = (
        normalize_key(stage_key, StageKey).value
        if stage_key is not None
        else None
    )
    selected_schema = schema or LedgerSchema()
    with engine.connect() as connection:
        rows = connection.execute(
            list_predecessor_stage_outputs_statement(
                selected_schema,
                work_item_id=work_item_id,
                below_stage_index=below_stage_index,
                stage_key=normalized_stage_key,
                min_stage_index=min_stage_index,
                max_stage_index=max_stage_index,
            )
        ).mappings()
        return tuple(
            PredecessorStageOutput(
                stage_index=row["stage_index"],
                stage_key=StageKey(row["stage_key"]),
                input_reference=row["input_reference"],
                output_reference=row["output_reference"],
            )
            for row in rows
        )


def list_episode_predecessor_outputs(  # noqa: PLR0913 -- explicit reader inputs
    work_item_id: int,
    fanin_stage_index: int,
    *,
    origin_stage_index: int,
    stage_key: StageKey | str,
    engine: Engine,
    schema: LedgerSchema | None = None,
) -> tuple[PredecessorStageOutput, ...]:
    """Episode-scoped predecessor outputs.

    Delegates to ``list_predecessor_stage_outputs`` with
    ``min_stage_index=origin_stage_index`` and the implicit exclusive cap at
    ``fanin_stage_index``. Only ``SUCCEEDED`` rows with a non-null
    ``output_reference`` are returned, ordered by ascending ``stage_index``.

    When the deferring step index is not already in the join payload, discover
    or verify it with ``resolve_barrier_join_cluster(...).optim_step``;
    carrying ``origin_stage_index`` in the payload remains the preferred
    pattern.
    """
    return list_predecessor_stage_outputs(
        work_item_id,
        fanin_stage_index,
        engine=engine,
        schema=schema,
        stage_key=stage_key,
        min_stage_index=origin_stage_index,
    )


def list_stage_executions(  # noqa: PLR0913 -- explicit reader filters
    work_item_id: int,
    *,
    engine: Engine,
    schema: LedgerSchema | None = None,
    stage_key: StageKey | str | None = None,
    min_stage_index: int | None = None,
    max_stage_index: int | None = None,
    state: StageExecutionState | None = None,
) -> tuple[StageExecutionRecord, ...]:
    """Return stage executions for one work item with optional filters.

    ``min_stage_index`` and ``max_stage_index`` are **exclusive** bounds on
    ``stage_index``. Unlike ``list_predecessor_stage_outputs``, there is no
    implicit upper bound: a min-only query is open-ended above. Rows are
    ordered by ``stage_index``, then ``stage_execution_id``. An empty tuple
    is valid.
    """
    validate_work_item_id(work_item_id)
    if state is not None and not isinstance(state, StageExecutionState):
        raise TypeError("state must be a StageExecutionState")
    validate_optional_exclusive_stage_index_bounds(
        min_stage_index=min_stage_index,
        max_stage_index=max_stage_index,
        default_max_stage_index=None,
    )
    normalized_stage_key = (
        normalize_key(stage_key, StageKey).value
        if stage_key is not None
        else None
    )
    selected_schema = schema or LedgerSchema()
    with engine.connect() as connection:
        rows = connection.execute(
            list_stage_executions_statement(
                selected_schema,
                work_item_id=work_item_id,
                stage_key=normalized_stage_key,
                min_stage_index=min_stage_index,
                max_stage_index=max_stage_index,
                state=state,
            )
        ).mappings()
        return tuple(_decode_stage_execution(row) for row in rows)


def _decode_work_item_summary(row: RowMapping) -> WorkItemSummary:
    return WorkItemSummary(
        work_item_id=row["work_item_id"],
        campaign_key=CampaignKey(row["campaign_key"]),
        work_key=WorkKey(row["work_key"]),
        origin_run_key=RunKey(row["origin_run_key"]),
        labels=immutable_mapping(row["labels"]),
        current_stage_execution_id=row["stage_execution_id"],
        current_stage_key=StageKey(row["stage_key"]),
        current_stage_index=row["stage_index"],
        state=StageExecutionState(row["state"]),
    )


def _decode_stage_execution(row: RowMapping) -> StageExecutionRecord:
    return StageExecutionRecord(
        stage_execution_id=row["stage_execution_id"],
        work_item_id=row["work_item_id"],
        stage_key=StageKey(row["stage_key"]),
        stage_index=row["stage_index"],
        state=StageExecutionState(row["state"]),
        current_attempt=row["current_attempt"],
        rank=row["rank"],
        priority=row["priority"],
        input_reference=row["input_reference"],
        output_reference=row["output_reference"],
        barrier=row["barrier"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
