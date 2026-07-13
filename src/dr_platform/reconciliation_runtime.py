"""Payload-free DBOS lifecycle observation and bounded reconciliation."""

from __future__ import annotations

import os
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Protocol, runtime_checkable

from dbos import DBOSClient
from dbos._schemas.system_database import SystemSchema
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)
from sqlalchemy import select

from dr_platform.dbos_config import (
    DBOS_SYSTEM_DATABASE_URL_ENV,
    DbosWorkflowStatus,
)
from dr_platform.manifests import (  # noqa: TC001 -- Pydantic runtime field
    ExecutionTargetRef,
)
from dr_platform.records import AttemptRecord, FailureSnapshot, ItemRecord
from dr_platform.status import FailureClass

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy import Engine

    from dr_platform.cancellation import WorkflowCanceller
    from dr_platform.db import PlatformSchema
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.targets import TargetResolver

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]

DEFAULT_RECONCILIATION_PAGE_SIZE = 100
DEFAULT_CLAIM_LEASE_SECONDS = 60
DEFAULT_MISSING_GRACE_SECONDS = 60
DEFAULT_MISSING_REQUIRED_OBSERVATIONS = 3
MAX_EXACT_WORKFLOW_MATCHES = 2


def _system_database_url(engine: Engine) -> str:
    """Return the DBOS URL without leaking it through diagnostics."""
    configured = os.environ.get(DBOS_SYSTEM_DATABASE_URL_ENV)
    if configured:
        return configured
    # `str(URL)` deliberately masks credentials for display. DBOS consumes this
    # value as a connection URL, so use SQLAlchemy's explicit runtime rendering
    # only at this non-logging boundary.
    return engine.url.render_as_string(hide_password=False)


class ReconciliationObservationDisposition(StrEnum):
    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    ERROR = "error"
    CANCELLED = "cancelled"
    RECOVERY_EXHAUSTED = "recovery_exhausted"
    ABSENT = "absent"
    UNCERTAIN = "uncertain"


class ReconciliationCandidate(BaseModel):
    """One current Attempt and its durable target-resolution context."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    item: ItemRecord
    attempt: AttemptRecord
    target_ref: ExecutionTargetRef

    @model_validator(mode="after")
    def validate_candidate(self) -> ReconciliationCandidate:
        if self.item.item_id != self.attempt.item_id:
            raise ValueError("reconciliation Attempt does not belong to Item")
        if self.item.current_attempt != self.attempt.attempt:
            raise ValueError("reconciliation Attempt is not current")
        return self


class ReconciliationObservation(BaseModel):
    """Normalized payload-free lifecycle fact for persistence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    disposition: ReconciliationObservationDisposition
    dbos_status: DbosWorkflowStatus | None = None
    failure: FailureSnapshot | None = None

    @model_validator(mode="after")
    def validate_observation(self) -> ReconciliationObservation:
        if self.disposition is ReconciliationObservationDisposition.ABSENT:
            if self.dbos_status is not None or self.failure is not None:
                raise ValueError("absent observations carry no DBOS status")
            return self
        if self.disposition is ReconciliationObservationDisposition.UNCERTAIN:
            if self.failure is None:
                raise ValueError("uncertain observations require a failure")
            return self
        if self.dbos_status is None:
            raise ValueError("observed workflows require a DBOS status")
        if (
            self.disposition is ReconciliationObservationDisposition.ERROR
        ) != (self.failure is not None):
            raise ValueError("only DBOS errors require classified failure")
        return self


class DbosStepObservation(BaseModel):
    """Allowlisted DBOS step-timeline fields with no payload columns."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    function_id: NonNegativeInt
    function_name: NonEmptyStr
    child_workflow_id: NonEmptyStr | None = None
    started_at_epoch_ms: NonNegativeInt | None = None
    completed_at_epoch_ms: NonNegativeInt | None = None

    @model_validator(mode="after")
    def validate_timeline(self) -> DbosStepObservation:
        if (
            self.started_at_epoch_ms is not None
            and self.completed_at_epoch_ms is not None
            and self.completed_at_epoch_ms < self.started_at_epoch_ms
        ):
            raise ValueError("step completion cannot precede its start")
        return self


class ReconcileOptions(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    page_size: PositiveInt = DEFAULT_RECONCILIATION_PAGE_SIZE
    operation_key: NonEmptyStr | None = None
    claim_lease_seconds: PositiveInt = DEFAULT_CLAIM_LEASE_SECONDS
    missing_grace_seconds: PositiveInt = DEFAULT_MISSING_GRACE_SECONDS
    missing_required_observations: PositiveInt = (
        DEFAULT_MISSING_REQUIRED_OBSERVATIONS
    )


class ReconcileResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    recovered_call_started_count: NonNegativeInt
    observed_count: NonNegativeInt
    changed_count: NonNegativeInt
    enqueue_reset_count: NonNegativeInt
    execution_retry_count: NonNegativeInt
    missing_count: NonNegativeInt
    replacement_enqueue_count: NonNegativeInt
    pending_enqueue_count: NonNegativeInt


@runtime_checkable
class LifecycleObservationReader(Protocol):
    def observe(
        self,
        *,
        workflow_id: str,
    ) -> ReconciliationObservation: ...

    def read_step_history(
        self,
        *,
        workflow_id: str,
        limit: PositiveInt = 100,
    ) -> tuple[DbosStepObservation, ...]: ...


class WorkflowMetadataDisposition(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    AMBIGUOUS = "ambiguous"


class WorkflowMetadataObservation(BaseModel):
    """Authoritative payload-free DBOS application-version metadata."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    disposition: WorkflowMetadataDisposition
    application_version: NonEmptyStr | None = None

    @model_validator(mode="after")
    def validate_metadata(self) -> WorkflowMetadataObservation:
        if (self.disposition is WorkflowMetadataDisposition.AVAILABLE) != (
            self.application_version is not None
        ):
            raise ValueError(
                "only available workflow metadata has an application version"
            )
        return self


@runtime_checkable
class WorkflowMetadataReader(Protocol):
    def read_workflow_metadata(
        self, *, workflow_id: str
    ) -> WorkflowMetadataObservation: ...


class DbosLifecycleReader:
    """Installed DBOS 2.26 status and payload-free step-history adapter."""

    def __init__(self, client: DBOSClient) -> None:
        self._client = client

    def observe(  # noqa: PLR0911 -- explicit fail-closed boundary cases
        self,
        *,
        workflow_id: str,
    ) -> ReconciliationObservation:
        try:
            matches = self._client.list_workflows(
                workflow_ids=[workflow_id],
                limit=MAX_EXACT_WORKFLOW_MATCHES,
                load_input=False,
                load_output=False,
            )
        except Exception as error:  # noqa: BLE001 -- external read boundary
            return _uncertain_observation(
                workflow_id,
                error_type="DbosWorkflowLookupFailed",
                message=(
                    "authoritative DBOS workflow lookup failed: "
                    f"{type(error).__name__}"
                ),
            )
        if not matches:
            return ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.ABSENT,
            )
        if len(matches) != 1:
            return _uncertain_observation(
                workflow_id,
                error_type="DbosWorkflowLookupAmbiguous",
                message="authoritative DBOS lookup returned multiple rows",
            )
        status_row = matches[0]
        if getattr(status_row, "workflow_id", None) != workflow_id:
            return _uncertain_observation(
                workflow_id,
                error_type="DbosWorkflowIdentityMismatch",
                message="DBOS lookup returned a different workflow identity",
            )
        if getattr(status_row, "parent_workflow_id", None) is not None:
            return _uncertain_observation(
                workflow_id,
                error_type="DbosWorkflowTopologyDrift",
                message="DBOS workflow violates top-level-only topology",
            )
        raw_status = getattr(status_row, "status", None)
        try:
            status = DbosWorkflowStatus(raw_status)
        except (TypeError, ValueError):
            return _uncertain_observation(
                workflow_id,
                error_type="DbosWorkflowStatusUnknown",
                message="DBOS returned an unknown workflow status",
            )
        return _normalized_observation(
            workflow_id=workflow_id,
            status=status,
        )

    def read_workflow_metadata(
        self, *, workflow_id: str
    ) -> WorkflowMetadataObservation:
        try:
            matches = self._client.list_workflows(
                workflow_ids=[workflow_id],
                limit=MAX_EXACT_WORKFLOW_MATCHES,
                load_input=False,
                load_output=False,
            )
        except Exception:  # noqa: BLE001 -- external read boundary
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.UNAVAILABLE,
            )
        if len(matches) > 1:
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.AMBIGUOUS,
            )
        if not matches:
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.UNAVAILABLE,
            )
        status_row = matches[0]
        application_version = getattr(status_row, "application_version", None)
        if (
            getattr(status_row, "workflow_id", None) != workflow_id
            or not isinstance(application_version, str)
            or not application_version
        ):
            return WorkflowMetadataObservation(
                workflow_id=workflow_id,
                disposition=WorkflowMetadataDisposition.UNAVAILABLE,
            )
        return WorkflowMetadataObservation(
            workflow_id=workflow_id,
            disposition=WorkflowMetadataDisposition.AVAILABLE,
            application_version=application_version,
        )

    def read_step_history(
        self,
        *,
        workflow_id: str,
        limit: PositiveInt = 100,
    ) -> tuple[DbosStepObservation, ...]:
        steps = SystemSchema.operation_outputs
        columns = (
            steps.c.workflow_uuid,
            steps.c.function_id,
            steps.c.function_name,
            steps.c.child_workflow_id,
            steps.c.started_at_epoch_ms,
            steps.c.completed_at_epoch_ms,
        )
        statement = (
            select(*columns)
            .where(steps.c.workflow_uuid == workflow_id)
            .order_by(steps.c.function_id)
            .limit(limit)
        )
        with self._client._sys_db.engine.connect() as connection:
            rows = connection.execute(statement).mappings()
            return tuple(
                DbosStepObservation(
                    workflow_id=row["workflow_uuid"],
                    function_id=row["function_id"],
                    function_name=row["function_name"],
                    child_workflow_id=row["child_workflow_id"],
                    started_at_epoch_ms=row["started_at_epoch_ms"],
                    completed_at_epoch_ms=row["completed_at_epoch_ms"],
                )
                for row in rows
            )


def reconcile(  # noqa: PLR0913 -- explicit lifecycle facade
    engine: Engine,
    *,
    resolver: TargetResolver,
    queue_lookup: QueueLookup | None = None,
    options: ReconcileOptions | None = None,
    schema: PlatformSchema | None = None,
    reader: LifecycleObservationReader | None = None,
    recovery_observer: WorkflowObserver | None = None,
    enqueue_adapter: PhysicalEnqueueAdapter | None = None,
    compensation_canceller: WorkflowCanceller | None = None,
) -> ReconcileResult:
    """Recover call-started Claims, then reconcile one bounded Attempt page."""
    from dr_platform.claims import ClaimPageOptions  # noqa: PLC0415
    from dr_platform.enqueue_runtime import (  # noqa: PLC0415
        DbosWorkflowObserver,
        enqueue_pending_page,
        enqueue_replacement_page,
        recover_call_started_page,
    )
    from dr_platform.reconciliation import (  # noqa: PLC0415 -- cycle boundary
        apply_reconciliation_observations,
        load_missing_reobservation_page,
        load_reconciliation_page,
    )

    selected = options or ReconcileOptions()
    if reader is None or queue_lookup is None:
        owned_client: DBOSClient | None = DBOSClient(
            system_database_url=_system_database_url(engine)
        )
    else:
        owned_client = None
    if reader is None:
        assert owned_client is not None
        selected_reader: LifecycleObservationReader = DbosLifecycleReader(
            owned_client
        )
    else:
        selected_reader = reader
    selected_queue_lookup = queue_lookup or owned_client
    assert selected_queue_lookup is not None
    try:
        if compensation_canceller is not None:
            # Compensation never mutates its terminal Attempt.  The optional
            # adapter keeps this facade payload-free while allowing health
            # reconciliation to replay a bounded late-enqueue repair page.
            from dr_platform.cancellation import (  # noqa: PLC0415
                repair_late_enqueue_compensations,
            )

            repair_late_enqueue_compensations(
                engine=engine,
                canceller=compensation_canceller,
                schema=schema,
                limit=selected.page_size,
                missing_grace_seconds=selected.missing_grace_seconds,
                missing_required_observations=(
                    selected.missing_required_observations
                ),
            )
        recovery = recover_call_started_page(
            engine,
            resolver=resolver,
            queue_lookup=selected_queue_lookup,
            options=ClaimPageOptions(
                page_size=selected.page_size,
                lease_seconds=selected.claim_lease_seconds,
            ),
            schema=schema,
            adapter=enqueue_adapter,
            observer=recovery_observer or DbosWorkflowObserver(),
            operation_key=selected.operation_key,
        )
        remaining = selected.page_size - len(recovery.items)
        actionable = (
            load_reconciliation_page(
                engine,
                page_size=remaining,
                schema=schema,
                operation_key=selected.operation_key,
            )
            if remaining > 0
            else ()
        )
        remaining -= len(actionable)
        missing = (
            load_missing_reobservation_page(
                engine,
                page_size=remaining,
                schema=schema,
                operation_key=selected.operation_key,
            )
            if remaining > 0
            else ()
        )
        candidates = actionable + missing
        observations = _observe_candidates(
            candidates,
            resolver=resolver,
            reader=selected_reader,
        )
        persisted = apply_reconciliation_observations(
            engine,
            observations=observations,
            resolver=resolver,
            options=selected,
            schema=schema,
            candidates=candidates,
        )
        remaining -= len(missing)
        replacement_count = 0
        pending_count = 0
        if remaining > 0:
            common = {
                "resolver": resolver,
                "queue_lookup": selected_queue_lookup,
                "schema": schema,
                "adapter": enqueue_adapter,
                "operation_key": selected.operation_key,
            }
            replacements = enqueue_replacement_page(
                engine,
                **common,
                options=ClaimPageOptions(
                    page_size=remaining,
                    lease_seconds=selected.claim_lease_seconds,
                ),
            )
            replacement_count = len(replacements.items)
            remaining -= replacement_count
            if remaining > 0:
                pending = enqueue_pending_page(
                    engine,
                    **common,
                    options=ClaimPageOptions(
                        page_size=remaining,
                        lease_seconds=selected.claim_lease_seconds,
                    ),
                )
                pending_count = len(pending.items)
    finally:
        if owned_client is not None:
            owned_client.destroy()
    return ReconcileResult(
        recovered_call_started_count=len(recovery.items),
        observed_count=persisted.observed_count,
        changed_count=persisted.changed_count,
        enqueue_reset_count=persisted.enqueue_reset_count,
        execution_retry_count=persisted.execution_retry_count,
        missing_count=persisted.missing_count,
        replacement_enqueue_count=replacement_count,
        pending_enqueue_count=pending_count,
    )


def _observe_candidates(
    candidates: tuple[ReconciliationCandidate, ...],
    *,
    resolver: TargetResolver,
    reader: LifecycleObservationReader,
) -> Mapping[str, ReconciliationObservation]:
    observations: dict[str, ReconciliationObservation] = {}
    for candidate in candidates:
        resolver.resolve(candidate.target_ref)
        workflow_id = candidate.attempt.workflow_id
        if workflow_id in observations:
            continue
        observations[workflow_id] = reader.observe(
            workflow_id=workflow_id,
        )
    return observations


def _normalized_observation(
    *,
    workflow_id: str,
    status: DbosWorkflowStatus,
) -> ReconciliationObservation:
    disposition_by_status = {
        DbosWorkflowStatus.PENDING: (
            ReconciliationObservationDisposition.ACTIVE
        ),
        DbosWorkflowStatus.ENQUEUED: (
            ReconciliationObservationDisposition.ACTIVE
        ),
        DbosWorkflowStatus.DELAYED: (
            ReconciliationObservationDisposition.ACTIVE
        ),
        DbosWorkflowStatus.SUCCESS: (
            ReconciliationObservationDisposition.SUCCEEDED
        ),
        DbosWorkflowStatus.CANCELLED: (
            ReconciliationObservationDisposition.CANCELLED
        ),
        DbosWorkflowStatus.MAX_RECOVERY_ATTEMPTS_EXCEEDED: (
            ReconciliationObservationDisposition.RECOVERY_EXHAUSTED
        ),
        DbosWorkflowStatus.ERROR: ReconciliationObservationDisposition.ERROR,
    }
    disposition = disposition_by_status[status]
    failure = None
    if disposition is ReconciliationObservationDisposition.ERROR:
        # DBOS's payload-free status API supplies no authoritative failure
        # classification.  Never invent an exception to feed a classifier:
        # an application-aware reader may return a typed ERROR observation,
        # while this generic adapter must fail closed.
        failure = FailureSnapshot(
            failure_class=FailureClass.PERMANENT,
            error_type="DbosWorkflowErrorUnclassifiable",
            message=(
                "authoritative DBOS workflow failure classification is "
                "unavailable"
            ),
        )
    return ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=disposition,
        dbos_status=status,
        failure=failure,
    )


def _uncertain_observation(
    workflow_id: str,
    *,
    error_type: str,
    message: str,
) -> ReconciliationObservation:
    return ReconciliationObservation(
        workflow_id=workflow_id,
        disposition=ReconciliationObservationDisposition.UNCERTAIN,
        failure=FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type=error_type,
            message=message,
        ),
    )
