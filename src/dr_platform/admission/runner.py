from __future__ import annotations

from collections.abc import Mapping  # noqa: TC003 -- Pydantic resolves it
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, NamedTuple, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    StrictInt,
    field_serializer,
    field_validator,
    model_validator,
)
from sqlalchemy import (
    Connection,
    Engine,
    and_,
    exists,
    func,
    or_,
    select,
    tuple_,
)

from dr_platform._core.clock import utc_now
from dr_platform._core.frozen import immutable_mapping
from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
    StageKey,
    WorkKey,
    normalize_key,
    validate_key_value,
)
from dr_platform._core.ledger.attempts import (
    StageAttemptRecord,
    append_stage_attempt,
    get_stage_attempt,
    mark_stage_attempt_admitted,
)
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.validation import (
    validate_non_empty_string,
    validate_positive_integer,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy.engine import RowMapping

    from dr_platform.pipeline.definitions import StageDefinition
    from dr_platform.pipeline.registry import PipelineRegistry

DEFAULT_ADMISSION_BATCH_SIZE = 10_000
# Watchdog against an unbounded capacity-skip scan within one admission pass.
MAX_CAPACITY_SKIPS_PER_PASS = 1_000_000


class _StageIdentity(NamedTuple):
    """Row-value identity; stays a real tuple for SQLAlchemy IN predicates."""

    pipeline_key: str
    pipeline_version: int
    stage_key: str


def _failure_message(error: Exception) -> str:
    # Broken application exception rendering must not abort the shared pass.
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- boundary
        try:
            return repr(error)
        except Exception:  # noqa: BLE001 -- boundary
            return f"unprintable {type(error).__name__}"


class AdmissionPayload(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    input_reference: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: StrictInt
    stage_key: StageKey
    attempt_number: StrictInt

    @field_validator("campaign_key", mode="before")
    @classmethod
    def _campaign_key(cls, value: object) -> CampaignKey:
        return normalize_key(cast("CampaignKey | str", value), CampaignKey)

    @field_validator("work_key", mode="before")
    @classmethod
    def _work_key(cls, value: object) -> WorkKey:
        return normalize_key(cast("WorkKey | str", value), WorkKey)

    @field_validator("origin_run_key", mode="before")
    @classmethod
    def _origin_run_key(cls, value: object) -> RunKey:
        return normalize_key(cast("RunKey | str", value), RunKey)

    @field_validator("stage_key", mode="before")
    @classmethod
    def _stage_key(cls, value: object) -> StageKey:
        return normalize_key(cast("StageKey | str", value), StageKey)

    @field_validator("labels", mode="after")
    @classmethod
    def _labels(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return immutable_mapping(value)

    @field_serializer(
        "campaign_key",
        "work_key",
        "origin_run_key",
        "stage_key",
    )
    def _serialize_key(self, value: object) -> str:
        return str(value)

    @field_serializer("labels")
    def _serialize_labels(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def _validate_payload(self) -> AdmissionPayload:
        validate_non_empty_string(
            self.input_reference, label="input reference"
        )
        validate_key_value(self.pipeline_key, label="pipeline key")
        validate_positive_integer(
            self.pipeline_version, label="pipeline version"
        )
        validate_positive_integer(self.attempt_number, label="attempt number")
        return self


@dataclass(frozen=True, slots=True)
class StageAdmissionCount:
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    count: int


@dataclass(frozen=True, slots=True)
class StageIdentityRecord:
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey


@dataclass(frozen=True, slots=True)
class StageAdmissionFailure:
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class StageMismatch:
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    message: str


@dataclass(frozen=True, slots=True)
class AdmissionSummary:
    admitted_counts: tuple[StageAdmissionCount, ...]
    skipped_for_capacity: int
    skipped_for_pause: int
    unconfigured_stages: tuple[StageIdentityRecord, ...]
    failed_stages: tuple[StageAdmissionFailure, ...]
    mismatched_stages: tuple[StageMismatch, ...]

    @property
    def admitted_total(self) -> int:
        return sum(item.count for item in self.admitted_counts)


class PipelineStageMismatchError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _Candidate:
    stage_execution_id: int
    rank: int
    stage_index: int
    campaign_key: str
    work_key: str
    origin_run_key: str
    input_reference: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: int
    stage_key: str

    @property
    def stage_identity(self) -> _StageIdentity:
        return _StageIdentity(
            pipeline_key=self.pipeline_key,
            pipeline_version=self.pipeline_version,
            stage_key=self.stage_key,
        )


@dataclass(frozen=True, slots=True)
class _Control:
    control_id: int
    stage_identity: _StageIdentity
    selector: Mapping[str, str]
    capacity: int
    paused: bool


@dataclass(slots=True)
class _PassTally:
    admitted: dict[_StageIdentity, int] = field(default_factory=dict)
    skipped_for_capacity: int = 0
    skipped_for_pause: int = 0
    unconfigured: set[_StageIdentity] = field(default_factory=set)
    failed: dict[_StageIdentity, StageAdmissionFailure] = field(
        default_factory=dict
    )
    mismatched: dict[_StageIdentity, StageMismatch] = field(
        default_factory=dict
    )
    excluded_candidates: set[int] = field(default_factory=set)

    @property
    def admitted_total(self) -> int:
        return sum(self.admitted.values())

    def record_admitted(self, identity: _StageIdentity) -> None:
        self.admitted[identity] = self.admitted.get(identity, 0) + 1

    def record_failure(self, candidate: _Candidate, error: Exception) -> None:
        identity = candidate.stage_identity
        self.excluded_candidates.add(candidate.stage_execution_id)
        self.failed.setdefault(
            identity,
            StageAdmissionFailure(
                pipeline_key=identity.pipeline_key,
                pipeline_version=identity.pipeline_version,
                stage_key=StageKey(identity.stage_key),
                error_type=type(error).__name__,
                message=_failure_message(error),
            ),
        )

    def record_mismatch(
        self, candidate: _Candidate, error: PipelineStageMismatchError
    ) -> None:
        identity = candidate.stage_identity
        self.excluded_candidates.add(candidate.stage_execution_id)
        self.mismatched.setdefault(
            identity,
            StageMismatch(
                pipeline_key=identity.pipeline_key,
                pipeline_version=identity.pipeline_version,
                stage_key=StageKey(identity.stage_key),
                message=_failure_message(error),
            ),
        )

    def to_summary(self) -> AdmissionSummary:
        counts = tuple(
            StageAdmissionCount(
                pipeline_key=identity.pipeline_key,
                pipeline_version=identity.pipeline_version,
                stage_key=StageKey(identity.stage_key),
                count=count,
            )
            for identity, count in sorted(self.admitted.items())
        )
        unconfigured_stages = tuple(
            StageIdentityRecord(
                pipeline_key=identity.pipeline_key,
                pipeline_version=identity.pipeline_version,
                stage_key=StageKey(identity.stage_key),
            )
            for identity in sorted(self.unconfigured)
        )
        failed_stages = tuple(
            self.failed[identity] for identity in sorted(self.failed)
        )
        mismatched_stages = tuple(
            self.mismatched[identity] for identity in sorted(self.mismatched)
        )
        return AdmissionSummary(
            admitted_counts=counts,
            skipped_for_capacity=self.skipped_for_capacity,
            skipped_for_pause=self.skipped_for_pause,
            unconfigured_stages=unconfigured_stages,
            failed_stages=failed_stages,
            mismatched_stages=mismatched_stages,
        )


@dataclass(frozen=True, slots=True)
class _Page:
    candidates: tuple[_Candidate, ...]
    controls_by_stage: dict[_StageIdentity, tuple[_Control, ...]]
    occupancy: dict[int, int]
    unconfigured: set[_StageIdentity]


@dataclass(frozen=True, slots=True)
class _Admit:
    matching: tuple[_Control, ...]


@dataclass(frozen=True, slots=True)
class _SkipPaused:
    pass


@dataclass(frozen=True, slots=True)
class _SkipFull:
    full_control_ids: frozenset[int]


def run_admission_pass(  # noqa: PLR0913 -- explicit admission dependencies
    engine: Engine,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int = DEFAULT_ADMISSION_BATCH_SIZE,
    clock: Callable[[], datetime] = utc_now,
    schema: LedgerSchema | None = None,
) -> AdmissionSummary:
    """Own one transaction and admit at most ``batch_size`` candidates.

    At most ``batch_size + MAX_CAPACITY_SKIPS_PER_PASS`` rows are examined.
    Missing controls exclude a stage; ledger-write and enqueue failures and
    registry drift exclude only their candidate, without aborting other
    admissions.
    """
    validate_positive_integer(batch_size, label="admission batch size")
    selected_schema = schema or LedgerSchema()
    with engine.begin() as connection:
        return _admit_in_transaction(
            connection,
            client=client,
            registry=registry,
            batch_size=batch_size,
            clock=clock,
            schema=selected_schema,
        )


def _admit_in_transaction(  # noqa: PLR0912, PLR0913 -- pass evaluation loop
    connection: Connection,
    *,
    client: DBOSClient,
    registry: PipelineRegistry,
    batch_size: int,
    clock: Callable[[], datetime],
    schema: LedgerSchema,
) -> AdmissionSummary:
    tally = _PassTally()
    admitted_at = clock()
    considered = 0
    considered_limit = batch_size + MAX_CAPACITY_SKIPS_PER_PASS
    after: tuple[int, int] | None = None
    full_control_ids: set[int] = set()

    while tally.admitted_total < batch_size and considered < considered_limit:
        page = _lock_page(
            connection,
            schema=schema,
            limit=min(
                batch_size - tally.admitted_total,
                considered_limit - considered,
            ),
            after=after,
            full_control_ids=full_control_ids,
            excluded=tally.unconfigured,
            excluded_candidates=tally.excluded_candidates,
        )
        if page is None:
            break
        considered += len(page.candidates)
        last_candidate = page.candidates[-1]
        after = last_candidate.rank, last_candidate.stage_execution_id
        tally.unconfigured.update(page.unconfigured)

        for candidate in page.candidates:
            if candidate.stage_identity in tally.unconfigured:
                continue
            if candidate.stage_execution_id in tally.excluded_candidates:
                continue
            controls = page.controls_by_stage[candidate.stage_identity]
            match _evaluate_candidate(
                candidate,
                controls=controls,
                occupancy=page.occupancy,
            ):
                case _SkipPaused():
                    tally.skipped_for_pause += 1
                case _SkipFull(full_control_ids=ids):
                    tally.skipped_for_capacity += 1
                    full_control_ids.update(ids)
                case _Admit(matching=matching):
                    # Ledger-write and enqueue failures roll back only this
                    # candidate; registry drift is reported separately.
                    try:
                        with connection.begin_nested():
                            _admit_candidate(
                                connection,
                                candidate=candidate,
                                client=client,
                                registry=registry,
                                admitted_at=admitted_at,
                                schema=schema,
                            )
                    except PipelineStageMismatchError as error:
                        tally.record_mismatch(candidate, error)
                        continue
                    except Exception as error:  # noqa: BLE001 -- boundary
                        tally.record_failure(candidate, error)
                        continue
                    tally.record_admitted(candidate.stage_identity)
                    for control in matching:
                        page.occupancy[control.control_id] += 1
                        if (
                            page.occupancy[control.control_id]
                            >= control.capacity
                        ):
                            full_control_ids.add(control.control_id)
                    if tally.admitted_total >= batch_size:
                        break

    return tally.to_summary()


def _lock_page(  # noqa: PLR0913 -- explicit paging predicates
    connection: Connection,
    *,
    schema: LedgerSchema,
    limit: int,
    after: tuple[int, int] | None,
    full_control_ids: set[int],
    excluded: set[_StageIdentity],
    excluded_candidates: set[int],
) -> _Page | None:
    candidates = _lock_candidates(
        connection,
        schema=schema,
        limit=limit,
        after=after,
        full_control_ids=full_control_ids,
        excluded=excluded,
        excluded_candidates=excluded_candidates,
    )
    if not candidates:
        return None
    identities = {item.stage_identity for item in candidates}
    controls = _lock_controls(
        connection,
        schema=schema,
        identities=identities,
    )
    grouped: dict[_StageIdentity, list[_Control]] = {}
    for control in controls:
        grouped.setdefault(control.stage_identity, []).append(control)
    unconfigured = _unconfigured_identities(
        identities=identities,
        controls_by_stage=grouped,
    )
    occupancy = _load_occupancy(
        connection,
        schema=schema,
        control_ids=tuple(control.control_id for control in controls),
    )
    return _Page(
        candidates=candidates,
        controls_by_stage={
            identity: tuple(stage_controls)
            for identity, stage_controls in grouped.items()
        },
        occupancy=occupancy,
        unconfigured=unconfigured,
    )


def _evaluate_candidate(
    candidate: _Candidate,
    *,
    controls: tuple[_Control, ...],
    occupancy: Mapping[int, int],
) -> _Admit | _SkipPaused | _SkipFull:
    matching = tuple(
        control
        for control in controls
        if _selector_matches(control.selector, candidate.labels)
    )
    if any(control.paused for control in matching):
        return _SkipPaused()
    full = tuple(
        control
        for control in matching
        if occupancy[control.control_id] >= control.capacity
    )
    if full:
        return _SkipFull(
            full_control_ids=frozenset(control.control_id for control in full)
        )
    return _Admit(matching=matching)


def _admit_candidate(  # noqa: PLR0913 -- explicit admission facts
    connection: Connection,
    *,
    candidate: _Candidate,
    client: DBOSClient,
    registry: PipelineRegistry,
    admitted_at: datetime,
    schema: LedgerSchema,
) -> None:
    stage = _registered_stage(candidate, registry=registry)
    attempt = _attempt_for_admission(
        connection,
        stage_execution_id=candidate.stage_execution_id,
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
        origin_run_key=RunKey(candidate.origin_run_key),
        input_reference=candidate.input_reference,
        labels=candidate.labels,
        pipeline_key=candidate.pipeline_key,
        pipeline_version=candidate.pipeline_version,
        stage_key=StageKey(candidate.stage_key),
        attempt_number=attempt.attempt_number,
    )
    options: EnqueueOptions = {
        "workflow_name": _workflow_name(stage),
        "queue_name": stage.queue_name,
        "workflow_id": attempt.workflow_id,
    }
    client.enqueue_in_transaction(
        connection,
        options,
        payload.model_dump(mode="json"),
    )


def _attempt_for_admission(
    connection: Connection,
    *,
    stage_execution_id: int,
    admitted_at: datetime,
    schema: LedgerSchema,
) -> StageAttemptRecord:
    executions = schema.stage_executions
    current_attempt = connection.execute(
        select(executions.c.current_attempt).where(
            executions.c.stage_execution_id == stage_execution_id
        )
    ).scalar_one()
    if current_attempt:
        prepared = get_stage_attempt(
            connection,
            stage_execution_id=stage_execution_id,
            attempt_number=current_attempt,
            schema=schema,
        )
        assert prepared is not None
        if prepared.admitted_at is None and prepared.terminal_at is None:
            return mark_stage_attempt_admitted(
                connection,
                stage_execution_id=stage_execution_id,
                attempt_number=current_attempt,
                admitted_at=admitted_at,
                schema=schema,
            )
    return append_stage_attempt(
        connection,
        stage_execution_id=stage_execution_id,
        created_at=admitted_at,
        admitted_at=admitted_at,
        schema=schema,
    )


def _lock_candidates(  # noqa: PLR0913 -- explicit paging predicates
    connection: Connection,
    *,
    schema: LedgerSchema,
    limit: int,
    after: tuple[int, int] | None,
    full_control_ids: set[int],
    excluded: set[_StageIdentity],
    excluded_candidates: set[int],
) -> tuple[_Candidate, ...]:
    executions = schema.stage_executions
    work_items = schema.work_items
    runs = schema.pipeline_runs
    controls = schema.stage_controls
    paused = exists(
        select(1)
        .select_from(controls)
        .where(
            controls.c.pipeline_key == runs.c.pipeline_key,
            controls.c.pipeline_version == runs.c.pipeline_version,
            controls.c.stage_key == executions.c.stage_key,
            controls.c.paused.is_(True),
            work_items.c.labels.contains(controls.c.selector),
        )
        .correlate(executions, work_items, runs)
    )
    statement = (
        select(
            executions.c.stage_execution_id,
            executions.c.rank,
            executions.c.stage_index,
            work_items.c.campaign_key,
            work_items.c.work_key,
            work_items.c.origin_run_key.label("origin_run_key"),
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
        .where(
            executions.c.state == StageExecutionState.READY.value,
            ~paused,
        )
        .order_by(executions.c.rank, executions.c.stage_execution_id)
    )
    if after is not None:
        statement = statement.where(
            or_(
                executions.c.rank > after[0],
                and_(
                    executions.c.rank == after[0],
                    executions.c.stage_execution_id > after[1],
                ),
            )
        )
    if full_control_ids:
        at_capacity = exists(
            select(1)
            .select_from(controls)
            .where(
                controls.c.stage_control_id.in_(sorted(full_control_ids)),
                controls.c.pipeline_key == runs.c.pipeline_key,
                controls.c.pipeline_version == runs.c.pipeline_version,
                controls.c.stage_key == executions.c.stage_key,
                work_items.c.labels.contains(controls.c.selector),
            )
            .correlate(executions, work_items, runs)
        )
        statement = statement.where(~at_capacity)
    if excluded:
        # Filter unhealthy backlogs so they cannot exhaust the pass budget.
        statement = statement.where(
            tuple_(
                runs.c.pipeline_key,
                runs.c.pipeline_version,
                executions.c.stage_key,
            ).not_in(sorted(excluded))
        )
    if excluded_candidates:
        # Preserve eligibility for later rows from the same stage.
        statement = statement.where(
            executions.c.stage_execution_id.not_in(sorted(excluded_candidates))
        )
    statement = statement.limit(limit).with_for_update(
        of=executions,
        skip_locked=True,
    )
    return tuple(
        _decode_candidate(row)
        for row in connection.execute(statement).mappings()
    )


def _unconfigured_identities(
    *,
    identities: set[_StageIdentity],
    controls_by_stage: dict[_StageIdentity, list[_Control]],
) -> set[_StageIdentity]:
    return {
        identity
        for identity in identities
        if not any(
            not control.selector
            for control in controls_by_stage.get(identity, ())
        )
    }


def _lock_controls(
    connection: Connection,
    *,
    schema: LedgerSchema,
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
    schema: LedgerSchema,
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
                    executions.c.state == StageExecutionState.ADMITTED.value,
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
        key=PipelineKey(candidate.pipeline_key),
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
        rank=row["rank"],
        stage_index=row["stage_index"],
        campaign_key=row["campaign_key"],
        work_key=row["work_key"],
        origin_run_key=row["origin_run_key"],
        input_reference=row["input_reference"],
        labels=immutable_mapping(row["labels"]),
        pipeline_key=row["pipeline_key"],
        pipeline_version=row["pipeline_version"],
        stage_key=row["stage_key"],
    )


def _decode_control(row: RowMapping) -> _Control:
    return _Control(
        control_id=row["stage_control_id"],
        stage_identity=_StageIdentity(
            pipeline_key=row["pipeline_key"],
            pipeline_version=row["pipeline_version"],
            stage_key=row["stage_key"],
        ),
        selector=immutable_mapping(row["selector"]),
        capacity=row["capacity"],
        paused=row["paused"],
    )
