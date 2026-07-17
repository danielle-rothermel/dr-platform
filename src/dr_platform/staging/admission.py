"""Transactional admission of READY staged work into DBOS queues.

``args_for`` receives one :class:`AdmissionPayload`.  This package-owned,
frozen record contains only routing identity and immutable submission facts;
applications remain responsible for turning those facts into workflow
arguments, and the package never interprets the resulting domain payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING

from sqlalchemy import Connection, Engine, and_, func, select, tuple_

from dr_platform.staging.definitions import (
    StageDefinition,
    validate_positive_integer,
)
from dr_platform.staging.identities import (
    CampaignKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import append_stage_attempt
from dr_platform.staging.stage_executions import transition_stage_execution
from dr_platform.staging.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy.engine import RowMapping

    from dr_platform.staging.registry import PipelineRegistry

DEFAULT_ADMISSION_BATCH_SIZE = 100

_StageIdentity = tuple[str, int, str]


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class AdmissionPayload:
    """Minimal immutable context supplied to a stage's ``args_for``."""

    campaign_key: CampaignKey
    work_key: WorkKey
    run_key: RunKey
    input_ref: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    attempt_number: int


@dataclass(frozen=True, slots=True)
class StageAdmissionCount:
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    count: int


@dataclass(frozen=True, slots=True)
class AdmissionSummary:
    admitted_counts: tuple[StageAdmissionCount, ...]
    skipped_for_capacity: int
    skipped_for_pause: int

    @property
    def admitted_total(self) -> int:
        return sum(item.count for item in self.admitted_counts)


class MissingStageControlError(RuntimeError):
    """A registered stage has no required empty-selector capacity control."""


class PipelineStageMismatchError(RuntimeError):
    """Persisted stage position disagrees with the registered pipeline."""


@dataclass(frozen=True, slots=True)
class _Candidate:
    stage_execution_id: int
    stage_index: int
    campaign_key: str
    work_key: str
    run_key: str
    input_ref: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: int
    stage_key: str

    @property
    def stage_identity(self) -> _StageIdentity:
        return self.pipeline_key, self.pipeline_version, self.stage_key


@dataclass(frozen=True, slots=True)
class _Control:
    control_id: int
    stage_identity: _StageIdentity
    selector: Mapping[str, str]
    capacity: int
    paused: bool


def run_admission_pass(  # noqa: PLR0913 -- explicit admission dependencies
    database: Engine | Connection,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
    clock: Callable[[], datetime] = _utc_now,
    schema: StagingSchema | None = None,
) -> AdmissionSummary:
    """Run one bounded admission pass and commit it as one transaction.

    A supplied connection must be idle; this function begins and owns its
    transaction just as it does when supplied an engine.
    """
    validate_positive_integer(batch_size, label="admission batch size")
    selected_schema = schema or StagingSchema()
    if isinstance(database, Engine):
        with database.begin() as connection:
            return _admit_in_transaction(
                connection,
                client=client,
                registry=registry,
                batch_size=batch_size,
                clock=clock,
                schema=selected_schema,
            )
    with database.begin():
        return _admit_in_transaction(
            database,
            client=client,
            registry=registry,
            batch_size=batch_size,
            clock=clock,
            schema=selected_schema,
        )


def _admit_in_transaction(  # noqa: PLR0913 -- transaction dependencies
    connection: Connection,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int,
    clock: Callable[[], datetime],
    schema: StagingSchema,
) -> AdmissionSummary:
    candidates = _lock_candidates(connection, schema=schema, limit=batch_size)
    if not candidates:
        return AdmissionSummary((), 0, 0)

    controls = _lock_controls(
        connection,
        schema=schema,
        identities={item.stage_identity for item in candidates},
    )
    controls_by_stage: dict[_StageIdentity, list[_Control]] = {}
    for control in controls:
        controls_by_stage.setdefault(control.stage_identity, []).append(
            control
        )
    for identity in {item.stage_identity for item in candidates}:
        if not any(
            not control.selector
            for control in controls_by_stage.get(identity, ())
        ):
            raise MissingStageControlError(
                "stage has no empty-selector capacity control: "
                f"{identity[0]!r} version {identity[1]} stage {identity[2]!r}"
            )

    occupancy = _load_occupancy(
        connection,
        schema=schema,
        control_ids=tuple(control.control_id for control in controls),
    )
    admitted: dict[_StageIdentity, int] = {}
    skipped_for_capacity = 0
    skipped_for_pause = 0
    admitted_at = clock()

    for candidate in candidates:
        matching = tuple(
            control
            for control in controls_by_stage[candidate.stage_identity]
            if _selector_matches(control.selector, candidate.labels)
        )
        if any(control.paused for control in matching):
            skipped_for_pause += 1
            continue
        if any(
            occupancy[control.control_id] >= control.capacity
            for control in matching
        ):
            skipped_for_capacity += 1
            continue

        stage = _registered_stage(candidate, registry=registry)
        attempt = append_stage_attempt(
            connection,
            stage_execution_id=candidate.stage_execution_id,
            created_at=admitted_at,
            admitted_at=admitted_at,
            schema=schema,
        )
        transition_stage_execution(
            connection,
            stage_execution_id=candidate.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=admitted_at,
            schema=schema,
        )
        payload = AdmissionPayload(
            campaign_key=CampaignKey(candidate.campaign_key),
            work_key=WorkKey(candidate.work_key),
            run_key=RunKey(candidate.run_key),
            input_ref=candidate.input_ref,
            labels=candidate.labels,
            pipeline_key=candidate.pipeline_key,
            pipeline_version=candidate.pipeline_version,
            stage_key=StageKey(candidate.stage_key),
            attempt_number=attempt.attempt_number,
        )
        workflow_args = stage.args_for(payload)
        if not isinstance(workflow_args, tuple):
            raise TypeError("stage args_for must return a tuple")
        options: EnqueueOptions = {
            "workflow_name": _workflow_name(stage),
            "queue_name": stage.queue_name,
            "workflow_id": attempt.workflow_id,
        }
        client.enqueue_in_transaction(
            connection,
            options,
            *workflow_args,
        )
        for control in matching:
            occupancy[control.control_id] += 1
        admitted[candidate.stage_identity] = (
            admitted.get(candidate.stage_identity, 0) + 1
        )

    counts = tuple(
        StageAdmissionCount(
            pipeline_key=identity[0],
            pipeline_version=identity[1],
            stage_key=StageKey(identity[2]),
            count=count,
        )
        for identity, count in sorted(admitted.items())
    )
    return AdmissionSummary(
        admitted_counts=counts,
        skipped_for_capacity=skipped_for_capacity,
        skipped_for_pause=skipped_for_pause,
    )


def _lock_candidates(
    connection: Connection,
    *,
    schema: StagingSchema,
    limit: int,
) -> tuple[_Candidate, ...]:
    executions = schema.stage_executions
    work_items = schema.work_items
    runs = schema.pipeline_runs
    statement = (
        select(
            executions.c.stage_execution_id,
            executions.c.stage_index,
            work_items.c.campaign_key,
            work_items.c.work_key,
            work_items.c.origin_run_key.label("run_key"),
            work_items.c.input_reference,
            work_items.c.labels,
            runs.c.pipeline_key,
            runs.c.pipeline_version,
            executions.c.stage_key,
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
        .where(executions.c.state == StageExecutionState.READY.value)
        .order_by(executions.c.rank, executions.c.stage_execution_id)
        .limit(limit)
        .with_for_update(of=executions, skip_locked=True)
    )
    return tuple(
        _decode_candidate(row)
        for row in connection.execute(statement).mappings()
    )


def _lock_controls(
    connection: Connection,
    *,
    schema: StagingSchema,
    identities: set[_StageIdentity],
) -> tuple[_Control, ...]:
    table = schema.stage_controls
    statement = (
        select(table)
        .where(
            tuple_(
                table.c.pipeline_key,
                table.c.pipeline_version,
                table.c.stage_key,
            ).in_(sorted(identities))
        )
        .order_by(
            table.c.pipeline_key,
            table.c.pipeline_version,
            table.c.stage_key,
            table.c.stage_control_id,
        )
        .with_for_update()
    )
    return tuple(
        _decode_control(row)
        for row in connection.execute(statement).mappings()
    )


def _load_occupancy(
    connection: Connection,
    *,
    schema: StagingSchema,
    control_ids: tuple[int, ...],
) -> dict[int, int]:
    controls = schema.stage_controls
    executions = schema.stage_executions
    work_items = schema.work_items
    runs = schema.pipeline_runs
    admitted = executions.join(
        work_items,
        executions.c.work_item_id == work_items.c.work_item_id,
    ).join(runs, work_items.c.origin_run_key == runs.c.run_key)
    statement = (
        select(
            controls.c.stage_control_id,
            func.count(executions.c.stage_execution_id),
        )
        .select_from(
            controls.outerjoin(
                admitted,
                and_(
                    controls.c.pipeline_key == runs.c.pipeline_key,
                    controls.c.pipeline_version == runs.c.pipeline_version,
                    controls.c.stage_key == executions.c.stage_key,
                    executions.c.state
                    == StageExecutionState.ADMITTED.value,
                    work_items.c.labels.contains(controls.c.selector),
                ),
            )
        )
        .where(controls.c.stage_control_id.in_(control_ids))
        .group_by(controls.c.stage_control_id)
    )
    rows = connection.execute(statement).tuples().all()
    return dict(rows)


def _selector_matches(
    selector: Mapping[str, str], labels: Mapping[str, str]
) -> bool:
    return all(labels.get(key) == value for key, value in selector.items())


def _registered_stage(
    candidate: _Candidate,
    *,
    registry: PipelineRegistry,
) -> StageDefinition:
    pipeline = registry.get(
        key=candidate.pipeline_key,
        version=candidate.pipeline_version,
    )
    try:
        stage = pipeline.stages[candidate.stage_index]
    except IndexError as error:
        raise PipelineStageMismatchError(
            "persisted stage index is outside the registered pipeline: "
            f"{candidate.stage_index}"
        ) from error
    if stage.key.value != candidate.stage_key:
        raise PipelineStageMismatchError(
            "persisted stage key disagrees with its registered position: "
            f"{candidate.stage_key!r}"
        )
    return stage


def _workflow_name(stage: StageDefinition) -> str:
    workflow_name = getattr(
        stage.workflow,
        "dbos_function_name",
        getattr(stage.workflow, "__name__", None),
    )
    if not isinstance(workflow_name, str) or not workflow_name:
        raise TypeError("stage workflow must expose a non-empty DBOS name")
    return workflow_name


def _decode_candidate(row: RowMapping) -> _Candidate:
    return _Candidate(
        stage_execution_id=row["stage_execution_id"],
        stage_index=row["stage_index"],
        campaign_key=row["campaign_key"],
        work_key=row["work_key"],
        run_key=row["run_key"],
        input_ref=row["input_reference"],
        labels=MappingProxyType(dict(row["labels"])),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        stage_key=row["stage_key"],
    )


def _decode_control(row: RowMapping) -> _Control:
    return _Control(
        control_id=row["stage_control_id"],
        stage_identity=(
            row["pipeline_key"],
            row["pipeline_version"],
            row["stage_key"],
        ),
        selector=MappingProxyType(dict(row["selector"])),
        capacity=row["capacity"],
        paused=row["paused"],
    )
