"""Physical DBOS enqueue boundary for one durably claimed Attempt."""

from __future__ import annotations

from collections.abc import Callable  # noqa: TC003 -- Pydantic resolves it
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Protocol,
    runtime_checkable,
)

from dbos import (
    DBOS,
    SetEnqueueOptions,
    SetWorkflowAttributes,
    SetWorkflowID,
)
from dr_serialize import (
    SerializationError,
    Serializer,
    postgres_jsonb_limits,
)
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)

import dr_platform.claims as claims_module
from dr_platform.dbos_config import WORKFLOW_START_RACE_ERRORS
from dr_platform.records import (
    AttemptRecord,
    EnqueueClaimRecord,
    FailureSnapshot,
    ItemRecord,
)
from dr_platform.status import (
    AttemptEnqueueState,
    EnqueueClaimDisposition,
    FailureClass,
    WorkflowTopology,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.claims import ClaimedAttempt, ClaimPageOptions
    from dr_platform.db import PlatformSchema
    from dr_platform.manifests import ExecutionTargetRef
    from dr_platform.targets import ExecutionTarget, TargetResolver

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]

EXECUTION_KEY_ATTRIBUTE = "platform.execution_key"
WORKFLOW_ROLE_ATTRIBUTE = "platform.workflow_role"
ATTEMPT_ATTRIBUTE = "platform.attempt"

class QueueConfigurationError(RuntimeError):
    """A target queue is absent or violates the persisted priority contract."""


class EnqueuePreparationError(RuntimeError):
    """A claimed Attempt cannot safely cross the DBOS call boundary."""


class PhysicalEnqueueDisposition(StrEnum):
    ENQUEUED = "enqueued"
    WORKFLOW_ALREADY_PRESENT = "workflow_already_present"
    ENQUEUE_ERROR = "enqueue_error"
    UNCERTAIN = "uncertain"


class EnqueueClaimExecutionDisposition(StrEnum):
    OUTCOME_RECORDED = "outcome_recorded"
    LOST_AUTHORITY = "lost_authority"
    UNCERTAIN = "uncertain"


class WorkflowObservationDisposition(StrEnum):
    EXISTING = "existing"
    ABSENT = "absent"
    UNCERTAIN = "uncertain"


class PhysicalEnqueueOutcome(BaseModel):
    """Safe physical outcome returned after the call-start fact is durable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    disposition: PhysicalEnqueueDisposition
    effective_service_priority: PositiveInt | None = None
    failure: FailureSnapshot | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> PhysicalEnqueueOutcome:
        success = self.disposition in {
            PhysicalEnqueueDisposition.ENQUEUED,
            PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
        }
        if success != (self.effective_service_priority is not None):
            raise ValueError(
                "successful physical enqueue outcomes require an effective "
                "priority"
            )
        if success == (self.failure is not None):
            raise ValueError(
                "enqueue errors and uncertainty require a failure snapshot; "
                "successful outcomes forbid one"
            )
        return self


class PreparedEnqueueCall(BaseModel):
    """Validated runtime-only DBOS call assembled before Claim call-start."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    item_id: NonEmptyStr
    workflow_id: NonEmptyStr
    execution_key: NonEmptyStr
    workflow_role: NonEmptyStr
    attempt: NonNegativeInt
    queue_name: NonEmptyStr
    managed_workflow_name: NonEmptyStr
    service_priority: PositiveInt
    workflow: Callable[..., object] = Field(exclude=True)
    classify_error: Callable[[BaseException], FailureSnapshot] = Field(
        exclude=True
    )
    args: tuple[Any, ...] = Field(exclude=True)
    attributes: dict[StrictStr, StrictStr | StrictInt]


class EnqueueClaimExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: EnqueueClaimExecutionDisposition
    outcome: PhysicalEnqueueOutcome | None = None

    @model_validator(mode="after")
    def validate_result(self) -> EnqueueClaimExecutionResult:
        if self.disposition is EnqueueClaimExecutionDisposition.LOST_AUTHORITY:
            return self
        if self.outcome is None:
            raise ValueError(
                "recorded and uncertain executions require an outcome"
            )
        if (
            self.disposition is EnqueueClaimExecutionDisposition.UNCERTAIN
        ) != (
            self.outcome.disposition is PhysicalEnqueueDisposition.UNCERTAIN
        ):
            raise ValueError(
                "execution disposition does not match physical outcome"
            )
        return self


class EnqueuePageItemResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: NonEmptyStr
    attempt: NonNegativeInt
    claim_id: NonEmptyStr
    execution: EnqueueClaimExecutionResult


class EnqueuePageResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[EnqueuePageItemResult, ...]


class WorkflowObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: NonEmptyStr
    disposition: WorkflowObservationDisposition
    outcome: PhysicalEnqueueOutcome | None = None
    failure: FailureSnapshot | None = None

    @model_validator(mode="after")
    def validate_observation(self) -> WorkflowObservation:
        if self.disposition is WorkflowObservationDisposition.EXISTING:
            if (
                self.outcome is None
                or self.outcome.disposition
                is not PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT
                or self.failure is not None
            ):
                raise ValueError(
                    "existing observation requires only an existing outcome"
                )
        elif self.disposition is WorkflowObservationDisposition.ABSENT:
            if self.outcome is not None or self.failure is not None:
                raise ValueError(
                    "absent observation carries no outcome or failure"
                )
        elif self.outcome is not None or self.failure is None:
            raise ValueError("uncertain observation requires only a failure")
        return self


@runtime_checkable
class QueueLookup(Protocol):
    def retrieve_queue(self, name: str) -> object | None: ...


@runtime_checkable
class ClaimTransitionStore(Protocol):
    def mark_enqueue_call_started(
        self,
        *,
        claim: EnqueueClaimRecord,
    ) -> bool: ...

    def record_enqueue_outcome(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> bool: ...

    def ensure_lost_outcome_compensation(
        self,
        *,
        claim: EnqueueClaimRecord,
        outcome: PhysicalEnqueueOutcome,
    ) -> None: ...


@runtime_checkable
class RecoveryClaimTransitionStore(ClaimTransitionStore, Protocol):
    def replace_call_started_after_absence(
        self,
        *,
        claimed: ClaimedAttempt,
    ) -> ClaimedAttempt | None: ...


@runtime_checkable
class PhysicalEnqueueAdapter(Protocol):
    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome: ...


@runtime_checkable
class WorkflowObserver(Protocol):
    def observe(self, call: PreparedEnqueueCall) -> WorkflowObservation: ...


def validate_priority_queue(
    *,
    queue_lookup: QueueLookup,
    target: ExecutionTarget,
) -> None:
    """Fail closed without changing the persisted operator queue config."""
    queue = queue_lookup.retrieve_queue(target.queue_name)
    if queue is None:
        raise QueueConfigurationError(
            f"DBOS queue {target.queue_name!r} is not registered"
        )
    if getattr(queue, "database_backed_queue", None) is not True:
        raise QueueConfigurationError(
            f"DBOS queue {target.queue_name!r} is not database-backed"
        )
    if getattr(queue, "priority_enabled", None) is not True:
        raise QueueConfigurationError(
            f"DBOS queue {target.queue_name!r} does not enable priority"
        )


def prepare_enqueue_call(
    *,
    item: ItemRecord,
    attempt: AttemptRecord,
    target: ExecutionTarget,
) -> PreparedEnqueueCall:
    """Revalidate one current Attempt and build its DBOS call."""
    if attempt.item_id != item.item_id:
        raise EnqueuePreparationError("Attempt does not belong to the Item")
    if attempt.enqueue_state is not AttemptEnqueueState.CLAIMING:
        raise EnqueuePreparationError("Attempt is not in CLAIMING state")
    if attempt.current_claim_id is None:
        raise EnqueuePreparationError("claiming Attempt has no current Claim")
    if target.topology is not WorkflowTopology.TOP_LEVEL_ONLY:
        raise EnqueuePreparationError(
            "only top-level workflows may be enqueued"
        )
    if target.workflow_role != attempt.workflow_role:
        raise EnqueuePreparationError(
            "Attempt workflow_role does not match its execution target"
        )

    execution = target.execution_for(item, attempt.attempt)
    if (
        execution.execution_key != attempt.execution_key
        or execution.workflow_id != attempt.workflow_id
    ):
        raise EnqueuePreparationError(
            "target execution identity does not match the persisted Attempt"
        )
    args = target.args_for(item, attempt.attempt)
    _validate_serializable_arguments(args)
    attributes: dict[str, str | int] = {
        EXECUTION_KEY_ATTRIBUTE: attempt.execution_key,
        WORKFLOW_ROLE_ATTRIBUTE: attempt.workflow_role,
        ATTEMPT_ATTRIBUTE: attempt.attempt,
    }
    return PreparedEnqueueCall(
        item_id=item.item_id,
        workflow_id=attempt.workflow_id,
        execution_key=attempt.execution_key,
        workflow_role=attempt.workflow_role,
        attempt=attempt.attempt,
        queue_name=target.queue_name,
        managed_workflow_name=target.managed_workflow_name,
        service_priority=attempt.requested_service_priority,
        workflow=target.workflow,
        classify_error=target.classify_error,
        args=args,
        attributes=attributes,
    )


def execute_enqueue_claim(
    *,
    claim: EnqueueClaimRecord,
    call: PreparedEnqueueCall,
    store: ClaimTransitionStore,
    adapter: PhysicalEnqueueAdapter | None = None,
) -> EnqueueClaimExecutionResult:
    """Cross DBOS only after the exact Claim's call-start CAS commits."""
    _validate_claim_call(claim=claim, call=call)
    if not store.mark_enqueue_call_started(claim=claim):
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.LOST_AUTHORITY,
        )

    runtime = adapter or DbosEnqueueAdapter()
    outcome = runtime.enqueue(call)
    if outcome.workflow_id != claim.workflow_id:
        raise EnqueuePreparationError(
            "physical enqueue outcome named a different workflow"
        )
    if outcome.disposition is PhysicalEnqueueDisposition.UNCERTAIN:
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.UNCERTAIN,
            outcome=outcome,
        )

    if store.record_enqueue_outcome(claim=claim, outcome=outcome):
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.OUTCOME_RECORDED,
            outcome=outcome,
        )

    if outcome.disposition in {
        PhysicalEnqueueDisposition.ENQUEUED,
        PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
    }:
        store.ensure_lost_outcome_compensation(claim=claim, outcome=outcome)
    return EnqueueClaimExecutionResult(
        disposition=EnqueueClaimExecutionDisposition.LOST_AUTHORITY,
        outcome=outcome,
    )


def enqueue_pending_page(  # noqa: PLR0913 -- explicit kernel facade
    engine: Engine,
    *,
    resolver: TargetResolver,
    queue_lookup: QueueLookup,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    adapter: PhysicalEnqueueAdapter | None = None,
    operation_key: str | None = None,
) -> EnqueuePageResult:
    """Validate every target queue, then claim and enqueue one bounded page."""
    page = claims_module.claim_pending_attempts(
        engine,
        admit_targets=_target_admission(
            resolver=resolver,
            queue_lookup=queue_lookup,
        ),
        options=options,
        schema=schema,
        operation_key=operation_key,
    )
    store = claims_module.PostgresClaimTransitionStore(engine, schema=schema)
    return _execute_claim_page(
        page.claims,
        resolver=resolver,
        store=store,
        adapter=adapter,
    )


def enqueue_replacement_page(  # noqa: PLR0913 -- explicit kernel facade
    engine: Engine,
    *,
    resolver: TargetResolver,
    queue_lookup: QueueLookup,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    adapter: PhysicalEnqueueAdapter | None = None,
    operation_key: str | None = None,
) -> EnqueuePageResult:
    """Replace never-started expired Claims only after queue admission."""
    page = claims_module.replace_expired_unstarted_claims(
        engine,
        admit_targets=_target_admission(
            resolver=resolver,
            queue_lookup=queue_lookup,
        ),
        options=options,
        schema=schema,
        operation_key=operation_key,
    )
    store = claims_module.PostgresClaimTransitionStore(engine, schema=schema)
    return _execute_claim_page(
        page.claims,
        resolver=resolver,
        store=store,
        adapter=adapter,
    )


def recover_call_started_page(  # noqa: PLR0913 -- explicit kernel facade
    engine: Engine,
    *,
    resolver: TargetResolver,
    queue_lookup: QueueLookup,
    options: ClaimPageOptions | None = None,
    schema: PlatformSchema | None = None,
    adapter: PhysicalEnqueueAdapter | None = None,
    observer: WorkflowObserver | None = None,
    operation_key: str | None = None,
) -> EnqueuePageResult:
    """Observe expired CALL_STARTED Claims before any replacement enqueue."""
    page = claims_module.load_call_started_recovery_page(
        engine,
        options=options,
        schema=schema,
        operation_key=operation_key,
    )
    admission = _target_admission(
        resolver=resolver,
        queue_lookup=queue_lookup,
    )
    store = claims_module.PostgresClaimTransitionStore(
        engine,
        schema=schema,
        options=options,
        admit_targets=admission,
    )
    workflow_observer = observer or DbosWorkflowObserver()
    items: list[EnqueuePageItemResult] = []
    for claimed in page.claims:
        target = resolver.resolve(claimed.target_ref)
        call = prepare_enqueue_call(
            item=claimed.item,
            attempt=claimed.attempt_record,
            target=target,
        )
        observation = workflow_observer.observe(call)
        execution = _resolve_call_started_observation(
            claimed=claimed,
            call=call,
            observation=observation,
            store=store,
            resolver=resolver,
            adapter=adapter,
        )
        items.append(
            EnqueuePageItemResult(
                item_id=claimed.item_id,
                attempt=claimed.attempt,
                claim_id=claimed.claim_id,
                execution=execution,
            )
        )
    return EnqueuePageResult(items=tuple(items))


def _resolve_call_started_observation(  # noqa: PLR0913
    *,
    claimed: ClaimedAttempt,
    call: PreparedEnqueueCall,
    observation: WorkflowObservation,
    store: RecoveryClaimTransitionStore,
    resolver: TargetResolver,
    adapter: PhysicalEnqueueAdapter | None,
) -> EnqueueClaimExecutionResult:
    if observation.workflow_id != claimed.workflow_id:
        raise EnqueuePreparationError(
            "workflow observation named a different workflow"
        )
    if observation.disposition is WorkflowObservationDisposition.UNCERTAIN:
        assert observation.failure is not None
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.UNCERTAIN,
            outcome=PhysicalEnqueueOutcome(
                workflow_id=claimed.workflow_id,
                disposition=PhysicalEnqueueDisposition.UNCERTAIN,
                failure=observation.failure,
            ),
        )
    if observation.disposition is WorkflowObservationDisposition.EXISTING:
        assert observation.outcome is not None
        if store.record_enqueue_outcome(
            claim=claimed.claim,
            outcome=observation.outcome,
        ):
            return EnqueueClaimExecutionResult(
                disposition=EnqueueClaimExecutionDisposition.OUTCOME_RECORDED,
                outcome=observation.outcome,
            )
        store.ensure_lost_outcome_compensation(
            claim=claimed.claim,
            outcome=observation.outcome,
        )
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.LOST_AUTHORITY,
            outcome=observation.outcome,
        )

    replacement = store.replace_call_started_after_absence(claimed=claimed)
    if replacement is None:
        return EnqueueClaimExecutionResult(
            disposition=EnqueueClaimExecutionDisposition.LOST_AUTHORITY,
        )
    replacement_call = prepare_enqueue_call(
        item=replacement.item,
        attempt=replacement.attempt_record,
        target=resolver.resolve(replacement.target_ref),
    )
    if replacement_call.workflow_id != call.workflow_id:
        raise EnqueuePreparationError(
            "replacement Claim changed the persisted workflow identity"
        )
    return execute_enqueue_claim(
        claim=replacement.claim,
        call=replacement_call,
        store=store,
        adapter=adapter,
    )


def _target_admission(
    *,
    resolver: TargetResolver,
    queue_lookup: QueueLookup,
) -> _TargetAdmission:
    return _TargetAdmission(resolver=resolver, queue_lookup=queue_lookup)


class _TargetAdmission:
    def __init__(
        self,
        *,
        resolver: TargetResolver,
        queue_lookup: QueueLookup,
    ) -> None:
        self._resolver = resolver
        self._queue_lookup = queue_lookup

    def __call__(
        self,
        target_refs: tuple[ExecutionTargetRef, ...],
    ) -> None:
        for target_ref in target_refs:
            validate_priority_queue(
                queue_lookup=self._queue_lookup,
                target=self._resolver.resolve(target_ref),
            )


def _execute_claim_page(
    claims: tuple[ClaimedAttempt, ...],
    *,
    resolver: TargetResolver,
    store: ClaimTransitionStore,
    adapter: PhysicalEnqueueAdapter | None,
) -> EnqueuePageResult:
    items: list[EnqueuePageItemResult] = []
    for claimed in claims:
        call = prepare_enqueue_call(
            item=claimed.item,
            attempt=claimed.attempt_record,
            target=resolver.resolve(claimed.target_ref),
        )
        execution = execute_enqueue_claim(
            claim=claimed.claim,
            call=call,
            store=store,
            adapter=adapter,
        )
        items.append(
            EnqueuePageItemResult(
                item_id=claimed.item_id,
                attempt=claimed.attempt,
                claim_id=claimed.claim_id,
                execution=execution,
            )
        )
    return EnqueuePageResult(items=tuple(items))


class DbosEnqueueAdapter:
    """Installed DBOS 2.26 physical enqueue integration."""

    def enqueue(self, call: PreparedEnqueueCall) -> PhysicalEnqueueOutcome:
        if DBOS.workflow_id is not None:
            return PhysicalEnqueueOutcome(
                workflow_id=call.workflow_id,
                disposition=PhysicalEnqueueDisposition.ENQUEUE_ERROR,
                failure=FailureSnapshot(
                    failure_class=FailureClass.PERMANENT,
                    error_type="DbosChildWorkflowProhibited",
                    message=(
                        "platform workflows may only be enqueued from a "
                        "top-level application context"
                    ),
                ),
            )
        try:
            with (
                SetWorkflowID(call.workflow_id),
                SetEnqueueOptions(priority=call.service_priority),
                SetWorkflowAttributes(dict(call.attributes)),
            ):
                handle = DBOS.enqueue_workflow(
                    call.queue_name,
                    call.workflow,
                    *call.args,
                )
        except Exception as error:  # noqa: BLE001 -- external call boundary
            return self._exception_outcome(call=call, error=error)

        if handle.get_workflow_id() != call.workflow_id:
            return _uncertain_outcome(
                call.workflow_id,
                "DBOS returned a handle for a different workflow",
            )
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=PhysicalEnqueueDisposition.ENQUEUED,
            effective_service_priority=call.service_priority,
        )

    def _exception_outcome(
        self,
        *,
        call: PreparedEnqueueCall,
        error: Exception,
    ) -> PhysicalEnqueueOutcome:
        try:
            existing = DBOS.get_workflow_status(call.workflow_id)
        except Exception as status_error:  # noqa: BLE001 -- uncertainty gate
            return _uncertain_outcome(
                call.workflow_id,
                "DBOS enqueue failed and authoritative status lookup also "
                f"failed: {type(error).__name__}; "
                f"{type(status_error).__name__}",
            )

        if existing is not None:
            return _existing_workflow_outcome(call=call, status=existing)
        if isinstance(error, WORKFLOW_START_RACE_ERRORS):
            return _uncertain_outcome(
                call.workflow_id,
                "DBOS reported a workflow race but no authoritative workflow "
                "row was visible",
            )
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=PhysicalEnqueueDisposition.ENQUEUE_ERROR,
            failure=call.classify_error(error),
        )


class DbosWorkflowObserver:
    """Payload-free authoritative status observer for call-start recovery."""

    def observe(self, call: PreparedEnqueueCall) -> WorkflowObservation:
        try:
            status = DBOS.get_workflow_status(call.workflow_id)
        except Exception as error:  # noqa: BLE001 -- uncertainty gate
            return WorkflowObservation(
                workflow_id=call.workflow_id,
                disposition=WorkflowObservationDisposition.UNCERTAIN,
                failure=FailureSnapshot(
                    failure_class=FailureClass.UNKNOWN,
                    error_type="DbosWorkflowObservationFailed",
                    message=(
                        "authoritative DBOS status lookup failed: "
                        f"{type(error).__name__}"
                    ),
                ),
            )
        if status is None:
            return WorkflowObservation(
                workflow_id=call.workflow_id,
                disposition=WorkflowObservationDisposition.ABSENT,
            )
        outcome = _existing_workflow_outcome(call=call, status=status)
        if outcome.disposition is PhysicalEnqueueDisposition.UNCERTAIN:
            assert outcome.failure is not None
            return WorkflowObservation(
                workflow_id=call.workflow_id,
                disposition=WorkflowObservationDisposition.UNCERTAIN,
                failure=outcome.failure,
            )
        return WorkflowObservation(
            workflow_id=call.workflow_id,
            disposition=WorkflowObservationDisposition.EXISTING,
            outcome=outcome,
        )


def _existing_workflow_outcome(
    *,
    call: PreparedEnqueueCall,
    status: object,
) -> PhysicalEnqueueOutcome:
    workflow_id = getattr(status, "workflow_id", None)
    workflow_name = getattr(status, "name", None)
    queue_name = getattr(status, "queue_name", None)
    priority = getattr(status, "priority", None)
    parent_workflow_id = getattr(status, "parent_workflow_id", None)
    if workflow_id != call.workflow_id:
        return _uncertain_outcome(
            call.workflow_id,
            "DBOS status lookup returned a different workflow identity",
        )
    if parent_workflow_id is not None:
        return _uncertain_outcome(
            call.workflow_id,
            "existing DBOS workflow violates top-level-only topology",
        )
    if workflow_name != call.managed_workflow_name:
        return _uncertain_outcome(
            call.workflow_id,
            "existing DBOS workflow name conflicts with its target",
        )
    if queue_name != call.queue_name:
        return _uncertain_outcome(
            call.workflow_id,
            "existing DBOS workflow queue conflicts with its target",
        )
    if (
        not isinstance(priority, int)
        or isinstance(priority, bool)
        or priority <= 0
    ):
        return _uncertain_outcome(
            call.workflow_id,
            "existing DBOS workflow has no valid persisted priority",
        )
    return PhysicalEnqueueOutcome(
        workflow_id=call.workflow_id,
        disposition=PhysicalEnqueueDisposition.WORKFLOW_ALREADY_PRESENT,
        effective_service_priority=priority,
    )


def _validate_claim_call(
    *,
    claim: EnqueueClaimRecord,
    call: PreparedEnqueueCall,
) -> None:
    if claim.disposition is not EnqueueClaimDisposition.CLAIMED:
        raise EnqueuePreparationError(
            "Claim is not eligible to start a DBOS call"
        )
    if claim.workflow_id != call.workflow_id:
        raise EnqueuePreparationError(
            "Claim workflow does not match prepared call"
        )
    if claim.item_id != call.item_id:
        raise EnqueuePreparationError(
            "Claim Item does not match prepared call"
        )
    if claim.attempt != call.attempt:
        raise EnqueuePreparationError(
            "Claim attempt does not match prepared call"
        )


def _validate_serializable_arguments(args: tuple[Any, ...]) -> None:
    try:
        Serializer(limits=postgres_jsonb_limits()).to_jsonable(args)
    except SerializationError as error:
        raise EnqueuePreparationError(
            f"workflow arguments are not safely serializable: {error}"
        ) from error


def _uncertain_outcome(
    workflow_id: str, message: str
) -> PhysicalEnqueueOutcome:
    return PhysicalEnqueueOutcome(
        workflow_id=workflow_id,
        disposition=PhysicalEnqueueDisposition.UNCERTAIN,
        failure=FailureSnapshot(
            failure_class=FailureClass.UNKNOWN,
            error_type="DbosEnqueueOutcomeUncertain",
            message=message,
        ),
    )
