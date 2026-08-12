from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel, ConfigDict, StrictInt, field_validator
from sqlalchemy import Text, and_, column, func, or_, select, values

from dr_platform._core.frozen import immutable_json_mapping
from dr_platform._core.identities import CampaignKey, RunKey, StageKey, WorkKey
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import validate_positive_integer
from dr_platform.inspection._validation import (
    normalize_campaign_key,
    normalize_run_key,
    normalize_work_key,
    require_campaign,
    require_run,
)
from dr_platform.inspection.terminal_filters import (
    TerminalSummaryFilter,
    terminal_summary_filter_clause,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from sqlalchemy import Engine, Select

DEFAULT_BULK_STATUS_CHUNK_SIZE = 10_000


class StateCount(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    state: StageExecutionState
    count: StrictInt

    @field_validator("count")
    @classmethod
    def _nonnegative_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("state count must be non-negative")
        return value


@dataclass(frozen=True, slots=True)
class BulkWorkStatus:
    work_key: WorkKey
    present: bool
    work_item_id: int | None
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None


@dataclass(frozen=True, slots=True)
class BulkStatusResult:
    campaign_key: CampaignKey
    statuses: Mapping[WorkKey, BulkWorkStatus]


@dataclass(frozen=True, slots=True)
class BulkWorkTerminalStatus:
    work_key: WorkKey
    present: bool
    work_item_id: int | None
    stage_execution_id: int | None
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None
    terminal_summary: Mapping[str, object] | None
    evidence_reference: str | None


@dataclass(frozen=True, slots=True)
class BulkTerminalStatusResult:
    campaign_key: CampaignKey
    statuses: Mapping[WorkKey, BulkWorkTerminalStatus]


def campaign_state_counts(
    campaign_key: CampaignKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    return _state_counts(
        engine=engine,
        campaign_key=normalize_campaign_key(campaign_key),
        run_key=None,
        schema=schema,
    )


def run_state_counts(
    run_key: RunKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> tuple[StateCount, ...]:
    """Return state counts for one run.

    Use bulk_run_state_counts for more than one key.
    """
    return _state_counts(
        engine=engine,
        campaign_key=None,
        run_key=normalize_run_key(run_key),
        schema=schema,
    )


def bulk_run_state_counts(
    run_keys: Iterable[RunKey | str],
    *,
    engine: Engine,
    chunk_size: int = DEFAULT_BULK_STATUS_CHUNK_SIZE,
    schema: StagingSchema | None = None,
) -> Mapping[RunKey, tuple[StateCount, ...] | None]:
    """Return one aggregate result per key with one SELECT per input chunk."""
    validate_positive_integer(chunk_size, label="bulk run count chunk size")
    normalized_keys = tuple(
        dict.fromkeys(normalize_run_key(key) for key in run_keys)
    )
    results: dict[RunKey, tuple[StateCount, ...] | None] = dict.fromkeys(
        normalized_keys
    )
    selected_schema = schema or StagingSchema()
    with engine.connect() as connection:
        for start in range(0, len(normalized_keys), chunk_size):
            chunk = normalized_keys[start : start + chunk_size]
            grouped: dict[RunKey, list[StateCount]] = {
                key: [] for key in chunk
            }
            present: set[RunKey] = set()
            for row in connection.execute(
                _bulk_run_counts_statement(
                    run_keys=chunk, schema=selected_schema
                )
            ).mappings():
                key = RunKey(row["requested_run_key"])
                if row["present"]:
                    present.add(key)
                if row["state"] is not None:
                    grouped[key].append(
                        StateCount(
                            state=StageExecutionState(row["state"]),
                            count=row["count"],
                        )
                    )
            for key in present:
                results[key] = tuple(grouped[key])
    return MappingProxyType(results)


def bulk_work_statuses(
    campaign_key: CampaignKey | str,
    work_keys: Iterable[WorkKey | str],
    *,
    engine: Engine,
    chunk_size: int = DEFAULT_BULK_STATUS_CHUNK_SIZE,
    schema: StagingSchema | None = None,
) -> BulkStatusResult:
    """Execute exactly one SELECT per input chunk."""
    validate_positive_integer(chunk_size, label="bulk status chunk size")
    normalized_campaign = normalize_campaign_key(campaign_key)
    normalized_keys = tuple(
        dict.fromkeys(normalize_work_key(key) for key in work_keys)
    )
    selected_schema = schema or StagingSchema()
    statuses: dict[WorkKey, BulkWorkStatus] = {
        key: BulkWorkStatus(
            work_key=key,
            present=False,
            work_item_id=None,
            current_stage_key=None,
            current_stage_index=None,
            state=None,
        )
        for key in normalized_keys
    }
    with engine.connect() as connection:
        for start in range(0, len(normalized_keys), chunk_size):
            chunk = normalized_keys[start : start + chunk_size]
            for row in connection.execute(
                _bulk_status_statement(
                    campaign_key=normalized_campaign,
                    work_keys=chunk,
                    schema=selected_schema,
                )
            ).mappings():
                key = WorkKey(row["work_key"])
                statuses[key] = BulkWorkStatus(
                    work_key=key,
                    present=True,
                    work_item_id=row["work_item_id"],
                    current_stage_key=StageKey(row["stage_key"]),
                    current_stage_index=row["stage_index"],
                    state=StageExecutionState(row["state"]),
                )
    return BulkStatusResult(
        campaign_key=normalized_campaign,
        statuses=MappingProxyType(statuses),
    )


def bulk_work_terminal_statuses(  # noqa: PLR0913 -- explicit reader filters
    campaign_key: CampaignKey | str,
    work_keys: Iterable[WorkKey | str],
    *,
    engine: Engine,
    terminal_filter: TerminalSummaryFilter | None = None,
    chunk_size: int = DEFAULT_BULK_STATUS_CHUNK_SIZE,
    schema: StagingSchema | None = None,
) -> BulkTerminalStatusResult:
    """Execute exactly one SELECT per input chunk over current attempts."""
    validate_positive_integer(
        chunk_size, label="bulk terminal status chunk size"
    )
    normalized_campaign = normalize_campaign_key(campaign_key)
    normalized_keys = tuple(
        dict.fromkeys(normalize_work_key(key) for key in work_keys)
    )
    selected_schema = schema or StagingSchema()
    if terminal_filter is None:
        statuses: dict[WorkKey, BulkWorkTerminalStatus] = {
            key: BulkWorkTerminalStatus(
                work_key=key,
                present=False,
                work_item_id=None,
                stage_execution_id=None,
                current_stage_key=None,
                current_stage_index=None,
                state=None,
                terminal_summary=None,
                evidence_reference=None,
            )
            for key in normalized_keys
        }
    else:
        statuses = {}
    with engine.connect() as connection:
        for start in range(0, len(normalized_keys), chunk_size):
            chunk = normalized_keys[start : start + chunk_size]
            for row in connection.execute(
                _bulk_terminal_status_statement(
                    campaign_key=normalized_campaign,
                    work_keys=chunk,
                    terminal_filter=terminal_filter,
                    schema=selected_schema,
                )
            ).mappings():
                decoded = _decode_bulk_work_terminal_status(row)
                statuses[decoded.work_key] = decoded
    return BulkTerminalStatusResult(
        campaign_key=normalized_campaign,
        statuses=MappingProxyType(statuses),
    )


def _state_counts(
    *,
    engine: Engine,
    campaign_key: CampaignKey | None,
    run_key: RunKey | None,
    schema: StagingSchema | None,
) -> tuple[StateCount, ...]:
    selected_schema = schema or StagingSchema()
    items = selected_schema.work_items
    executions = selected_schema.stage_executions
    if campaign_key is not None:
        scoped_item_ids = select(items.c.work_item_id).where(
            items.c.campaign_key == campaign_key.value
        )
    else:
        assert run_key is not None
        memberships = selected_schema.run_memberships
        scoped_item_ids = select(memberships.c.work_item_id).where(
            memberships.c.run_key == run_key.value
        )
    current = current_stage_indexes(selected_schema, scoped_item_ids)
    statement = (
        select(executions.c.state, func.count().label("count"))
        .select_from(
            items.join(
                current,
                current.c.work_item_id == items.c.work_item_id,
            ).join(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .group_by(executions.c.state)
        .order_by(executions.c.state)
    )
    if campaign_key is not None:
        statement = statement.where(items.c.campaign_key == campaign_key.value)
    else:
        assert run_key is not None
        statement = statement.where(items.c.work_item_id.in_(scoped_item_ids))
    with engine.connect() as connection:
        if campaign_key is not None:
            require_campaign(
                connection,
                campaign_key=campaign_key,
                schema=selected_schema,
            )
        else:
            assert run_key is not None
            require_run(
                connection,
                run_key=run_key,
                schema=selected_schema,
            )
        return tuple(
            StateCount(
                state=StageExecutionState(row["state"]),
                count=row["count"],
            )
            for row in connection.execute(statement).mappings()
        )


def _bulk_status_statement(
    *,
    campaign_key: CampaignKey,
    work_keys: tuple[WorkKey, ...],
    schema: StagingSchema,
):
    items = schema.work_items
    executions = schema.stage_executions
    requested_item_ids = select(items.c.work_item_id).where(
        items.c.campaign_key == campaign_key.value,
        items.c.work_key.in_([key.value for key in work_keys]),
    )
    current = current_stage_indexes(schema, requested_item_ids)
    return (
        select(
            items.c.work_key,
            items.c.work_item_id,
            executions.c.stage_key,
            executions.c.stage_index,
            executions.c.state,
        )
        .select_from(
            items.join(
                current,
                current.c.work_item_id == items.c.work_item_id,
            ).join(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
        )
        .where(
            items.c.campaign_key == campaign_key.value,
            items.c.work_key.in_([key.value for key in work_keys]),
        )
        .order_by(items.c.work_key)
    )


def _bulk_terminal_status_statement(
    *,
    campaign_key: CampaignKey,
    work_keys: tuple[WorkKey, ...],
    terminal_filter: TerminalSummaryFilter | None,
    schema: StagingSchema,
):
    items = schema.work_items
    executions = schema.stage_executions
    attempts = schema.stage_attempts
    if terminal_filter is None:
        requested_item_ids = select(items.c.work_item_id).where(
            items.c.campaign_key == campaign_key.value,
            items.c.work_key.in_([key.value for key in work_keys]),
        )
        current = current_stage_indexes(schema, requested_item_ids)
        return (
            select(
                items.c.work_key,
                items.c.work_item_id,
                executions.c.stage_execution_id,
                executions.c.stage_key,
                executions.c.stage_index,
                executions.c.state,
                attempts.c.terminal_summary,
                attempts.c.evidence_reference,
            )
            .select_from(
                items.join(
                    current,
                    current.c.work_item_id == items.c.work_item_id,
                )
                .join(
                    executions,
                    and_(
                        executions.c.work_item_id == current.c.work_item_id,
                        executions.c.stage_index == current.c.stage_index,
                    ),
                )
                .outerjoin(
                    attempts,
                    and_(
                        attempts.c.stage_execution_id
                        == executions.c.stage_execution_id,
                        attempts.c.attempt_number
                        == executions.c.current_attempt,
                    ),
                )
            )
            .where(
                items.c.campaign_key == campaign_key.value,
                items.c.work_key.in_([key.value for key in work_keys]),
            )
            .order_by(items.c.work_key)
        )

    requested = (
        values(column("work_key", Text), name="requested_work_keys")
        .data([(key.value,) for key in work_keys])
        .cte("requested_work_keys")
    )
    requested_item_ids = select(items.c.work_item_id).where(
        items.c.campaign_key == campaign_key.value,
        items.c.work_key.in_([key.value for key in work_keys]),
    )
    current = current_stage_indexes(schema, requested_item_ids)
    return (
        select(
            requested.c.work_key,
            items.c.work_item_id,
            executions.c.stage_execution_id,
            executions.c.stage_key,
            executions.c.stage_index,
            executions.c.state,
            attempts.c.terminal_summary,
            attempts.c.evidence_reference,
        )
        .select_from(
            requested.outerjoin(
                items,
                and_(
                    items.c.campaign_key == campaign_key.value,
                    items.c.work_key == requested.c.work_key,
                ),
            )
            .outerjoin(
                current,
                current.c.work_item_id == items.c.work_item_id,
            )
            .outerjoin(
                executions,
                and_(
                    executions.c.work_item_id == current.c.work_item_id,
                    executions.c.stage_index == current.c.stage_index,
                ),
            )
            .outerjoin(
                attempts,
                and_(
                    attempts.c.stage_execution_id
                    == executions.c.stage_execution_id,
                    attempts.c.attempt_number == executions.c.current_attempt,
                ),
            )
        )
        .where(
            or_(
                items.c.work_item_id.is_(None),
                executions.c.stage_execution_id.is_(None),
                terminal_summary_filter_clause(
                    terminal_filter,
                    schema=schema,
                ),
            )
        )
        .order_by(requested.c.work_key)
    )


def _decode_bulk_work_terminal_status(row) -> BulkWorkTerminalStatus:
    work_key = WorkKey(row["work_key"])
    if row["work_item_id"] is None:
        return BulkWorkTerminalStatus(
            work_key=work_key,
            present=False,
            work_item_id=None,
            stage_execution_id=None,
            current_stage_key=None,
            current_stage_index=None,
            state=None,
            terminal_summary=None,
            evidence_reference=None,
        )
    summary = row["terminal_summary"]
    stage_key = row["stage_key"]
    state = row["state"]
    if row["stage_execution_id"] is None:
        return BulkWorkTerminalStatus(
            work_key=work_key,
            present=False,
            work_item_id=row["work_item_id"],
            stage_execution_id=None,
            current_stage_key=None,
            current_stage_index=None,
            state=None,
            terminal_summary=None,
            evidence_reference=None,
        )
    return BulkWorkTerminalStatus(
        work_key=work_key,
        present=True,
        work_item_id=row["work_item_id"],
        stage_execution_id=row["stage_execution_id"],
        current_stage_key=StageKey(stage_key),
        current_stage_index=row["stage_index"],
        state=StageExecutionState(state),
        terminal_summary=(
            None
            if summary is None
            else immutable_json_mapping(cast("Mapping[str, object]", summary))
        ),
        evidence_reference=row["evidence_reference"],
    )


def _bulk_run_counts_statement(
    *,
    run_keys: tuple[RunKey, ...],
    schema: StagingSchema,
):
    requested = (
        values(column("run_key", Text), name="requested_runs")
        .data([(key.value,) for key in run_keys])
        .cte("requested_runs")
    )
    runs = schema.pipeline_runs
    memberships = schema.run_memberships
    executions = schema.stage_executions
    scoped = (
        select(memberships.c.run_key, memberships.c.work_item_id)
        .where(memberships.c.run_key.in_([key.value for key in run_keys]))
        .subquery()
    )
    current = (
        select(
            scoped.c.run_key,
            scoped.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .select_from(
            scoped.join(
                executions,
                scoped.c.work_item_id == executions.c.work_item_id,
            )
        )
        .group_by(scoped.c.run_key, scoped.c.work_item_id)
        .subquery()
    )
    current_states = current.join(
        executions,
        and_(
            current.c.work_item_id == executions.c.work_item_id,
            current.c.stage_index == executions.c.stage_index,
        ),
    )
    return (
        select(
            requested.c.run_key.label("requested_run_key"),
            runs.c.run_key.is_not(None).label("present"),
            executions.c.state,
            func.count(executions.c.stage_execution_id).label("count"),
        )
        .select_from(
            requested.outerjoin(
                runs, requested.c.run_key == runs.c.run_key
            ).outerjoin(current_states, runs.c.run_key == current.c.run_key)
        )
        .group_by(requested.c.run_key, runs.c.run_key, executions.c.state)
        .order_by(requested.c.run_key, executions.c.state)
    )


def current_stage_indexes(schema: StagingSchema, work_item_ids: Select):
    executions = schema.stage_executions
    scoped_ids = work_item_ids.subquery()
    return (
        select(
            executions.c.work_item_id,
            func.max(executions.c.stage_index).label("stage_index"),
        )
        .select_from(
            executions.join(
                scoped_ids,
                scoped_ids.c.work_item_id == executions.c.work_item_id,
            )
        )
        .group_by(executions.c.work_item_id)
        .subquery()
    )
