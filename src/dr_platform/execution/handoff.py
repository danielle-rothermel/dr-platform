from __future__ import annotations

import hashlib
import re
import traceback
from typing import TYPE_CHECKING, cast

from dbos import DBOS
from dr_store.content_addressing import format_object_reference
from pydantic import BaseModel, ConfigDict, StrictBool, StrictInt
from sqlalchemy import select

from dr_platform._core.clock import utc_now
from dr_platform._core.identities import PipelineKey, StageKey
from dr_platform._core.ledger.attempts import record_stage_attempt_terminal
from dr_platform._core.ledger.evidence import STAGE_FAILURE_EVIDENCE_SCHEMA
from dr_platform._core.ledger.executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryProducer,
    build_terminal_outcome_summary,
    build_terminal_summary,
)
from dr_platform.admission.runner import AdmissionPayload
from dr_platform.completion.execution import (
    is_run_completion_wrapped,
    wrap_run_completion,
)
from dr_platform.execution._checkpoint import (
    _ledger_checkpoint_connection,
    _require_ledger_checkpoint_executor,
)
from dr_platform.execution._object_store import (
    _active_object_store,
    _object_store_context,
    _require_object_store,
)
from dr_platform.execution._recovery_cap import mark_wrapped_recovery_cap
from dr_platform.execution.failures import StageApplicationFailure
from dr_platform.execution.stage_completion import (
    StageSuccessor,
    parse_stage_workflow_result,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    RunCompletionWorkflowCallable,
    StageDefinition,
    StageWorkflowCallable,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from dr_serialize import Jsonable
    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform._core.identities import PipelineKey


_WRAPPED_STAGE_MARKER = "_dr_platform_wrapped_stage"


class StageHandoffMismatchError(RuntimeError):
    pass


def is_pipeline_wrapped(pipeline: PipelineDefinition) -> bool:
    """Only wrapped definitions run completion; raw ones remain ADMITTED."""
    stages_wrapped = all(
        getattr(stage.workflow, _WRAPPED_STAGE_MARKER, False)
        for stage in pipeline.stages
    )
    completion = pipeline.run_completion
    return stages_wrapped and (
        completion is None or is_run_completion_wrapped(completion)
    )


def _pipeline_checkpoint_workflows(
    pipeline: PipelineDefinition,
) -> tuple[StageWorkflowCallable | RunCompletionWorkflowCallable, ...]:
    completion = pipeline.run_completion
    return (
        *(stage.workflow for stage in pipeline.stages),
        *(() if completion is None else (completion.workflow,)),
    )


def _pipeline_stage_workflows(
    pipeline: PipelineDefinition,
) -> tuple[StageWorkflowCallable, ...]:
    return tuple(stage.workflow for stage in pipeline.stages)


def _safe_error_message(error: BaseException, *, error_type: str) -> str:
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- defend against a broken __str__
        return f"<unprintable {error_type} message>"


def wrap_pipeline_workflows(
    pipeline: PipelineDefinition,
    *,
    max_recovery_attempts: int,
    clock: Callable[[], datetime] = utc_now,
) -> PipelineDefinition:
    """Return a declaration whose stages use package-owned DBOS wrappers.

    Register and submit the returned declaration. Recovery is capped by
    ``max_recovery_attempts``; operator ``retry_stage`` is the visible requeue
    path after a loud FAILED outcome.
    """
    wrapped_stages = tuple(
        StageDefinition(
            key=stage.key,
            queue_name=stage.queue_name,
            workflow=_wrap_stage_workflow(
                pipeline=pipeline,
                registration_stage_index=stage_index,
                max_recovery_attempts=max_recovery_attempts,
                clock=clock,
            ),
            args_for=stage.args_for,
        )
        for stage_index, stage in enumerate(pipeline.stages)
    )
    return PipelineDefinition(
        key=pipeline.key,
        version=pipeline.version,
        stages=wrapped_stages,
        run_completion=(
            None
            if pipeline.run_completion is None
            else wrap_run_completion(
                pipeline.run_completion,
                pipeline_key=pipeline.key,
                pipeline_version=pipeline.version,
                max_recovery_attempts=max_recovery_attempts,
                clock=clock,
            )
        ),
    )


def _wrap_stage_workflow(
    *,
    pipeline: PipelineDefinition,
    registration_stage_index: int,
    max_recovery_attempts: int,
    clock: Callable[[], datetime],
) -> StageWorkflowCallable:
    stage = pipeline.stages[registration_stage_index]
    next_stage = (
        pipeline.stages[registration_stage_index + 1]
        if registration_stage_index + 1 < len(pipeline.stages)
        else None
    )
    workflow_name = _stage_workflow_name(
        pipeline_key=pipeline.key,
        pipeline_version=pipeline.version,
        stage_key=stage.key,
    )

    def _complete_stage_transaction(  # noqa: PLR0913
        *,
        workflow_id: str,
        pipeline_key: str,
        pipeline_version: int,
        stage_key: str,
        stage_index: int,
        succeeded: bool,
        output_reference: str | None,
        terminal_summary: Mapping[str, object],
        terminal_reference: str | None,
        evidence: Jsonable | None,
        successors_data: tuple[dict[str, object], ...],
    ) -> None:
        # Read nondeterministic time only in the checkpointed transaction.
        successors = _decode_successors(successors_data)
        _complete_stage_in_transaction(
            _ledger_checkpoint_connection(),
            workflow_id=workflow_id,
            pipeline_key=pipeline_key,
            pipeline_version=pipeline_version,
            stage_key=stage_key,
            stage_index=stage_index,
            succeeded=succeeded,
            output_reference=output_reference,
            terminal_summary=terminal_summary,
            terminal_reference=terminal_reference,
            evidence=evidence,
            successors=successors,
            completed_at=clock(),
        )

    complete_stage = DBOS.transaction(
        isolation_level="READ COMMITTED",
        name=f"{workflow_name}_complete",
    )(_complete_stage_transaction)

    @DBOS.workflow(
        name=workflow_name,
        max_recovery_attempts=max_recovery_attempts,
    )
    async def run_stage(payload_data: dict[str, object]) -> str | None:
        checkpoint_executor = _require_ledger_checkpoint_executor(run_stage)
        workflow_id = _current_workflow_id()
        payload = AdmissionPayload.model_validate(payload_data)
        persisted_stage_index = payload.stage_index
        try:
            workflow_args = _validate_workflow_args(stage.args_for(payload))
            raw_result = await stage.workflow(*workflow_args)
            if isinstance(raw_result, str) and (
                persisted_stage_index != registration_stage_index
            ):
                raise TypeError(  # noqa: TRY301
                    "stage running at non-registration index "
                    f"{persisted_stage_index} must return "
                    "StageCompletion, not str"
                )
            completion = parse_stage_workflow_result(
                raw_result,
                pipeline=pipeline,
                current_stage_index=persisted_stage_index,
                linear_next_stage_key=(
                    None if next_stage is None else next_stage.key
                ),
            )
        except Exception as error:  # noqa: BLE001 -- application boundary
            error_type = f"{type(error).__module__}.{type(error).__qualname__}"
            evidence = (
                error.evidence
                if isinstance(error, StageApplicationFailure)
                else None
            )
            object_store = _require_object_store(run_stage)
            with _object_store_context(object_store):
                await checkpoint_executor.run(
                    complete_stage,
                    workflow_id=workflow_id,
                    pipeline_key=pipeline.key.value,
                    pipeline_version=pipeline.version,
                    stage_key=stage.key.value,
                    stage_index=persisted_stage_index,
                    succeeded=False,
                    output_reference=None,
                    terminal_summary=build_terminal_summary(
                        outcome=StageExecutionState.FAILED.value,
                        producer=TerminalSummaryProducer.APPLICATION_FAILURE,
                        error_type=error_type,
                        message=_safe_error_message(
                            error, error_type=error_type
                        ),
                        traceback_text="".join(
                            traceback.format_exception(
                                type(error), error, error.__traceback__
                            )
                        ),
                    ),
                    terminal_reference=None,
                    evidence=evidence,
                    successors_data=(),
                )
            return None

        object_store = _require_object_store(run_stage)
        with _object_store_context(object_store):
            await checkpoint_executor.run(
                complete_stage,
                workflow_id=workflow_id,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key.value,
                stage_index=persisted_stage_index,
                succeeded=True,
                output_reference=completion.output_reference,
                terminal_summary=build_terminal_outcome_summary(
                    outcome=StageExecutionState.SUCCEEDED.value,
                ),
                terminal_reference=completion.output_reference,
                evidence=None,
                successors_data=_encode_successors(completion.successors),
            )
        return completion.output_reference

    # Dispatcher rejects declarations lacking this package-owned marker.
    setattr(run_stage, _WRAPPED_STAGE_MARKER, True)
    mark_wrapped_recovery_cap(run_stage, max_recovery_attempts)
    return cast("StageWorkflowCallable", run_stage)


def _complete_stage_in_transaction(  # noqa: PLR0913
    connection: Connection,
    *,
    workflow_id: str,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: str,
    stage_index: int,
    succeeded: bool,
    output_reference: str | None,
    terminal_summary: Mapping[str, object],
    terminal_reference: str | None,
    evidence: Jsonable | None,
    successors: tuple[StageSuccessor, ...],
    completed_at: datetime,
    before_next_stage: Callable[[], None] | None = None,
    schema: LedgerSchema | None = None,
) -> None:
    """``before_next_stage`` is a rollback-only test seam."""
    selected_schema = schema or LedgerSchema()
    source = _lock_handoff_source(
        connection,
        workflow_id=workflow_id,
        schema=selected_schema,
    )
    expected = (pipeline_key, pipeline_version, stage_key, stage_index)
    actual = (
        source["pipeline_key"],
        source["pipeline_version"],
        source["stage_key"],
        source["stage_index"],
    )
    if actual != expected:
        raise StageHandoffMismatchError(
            f"workflow stage identity mismatch: expected {expected!r}, "
            f"found {actual!r}"
        )
    if (
        source["state"] == StageExecutionState.CANCELLED.value
        and source["current_attempt"] == source["attempt_number"]
    ):
        # Cancellation wins the row-lock race; late completion cannot rewrite.
        return
    if (
        source["state"] != StageExecutionState.ADMITTED.value
        or source["current_attempt"] != source["attempt_number"]
    ):
        raise StageHandoffMismatchError(
            "workflow attempt is not the current ADMITTED stage attempt"
        )

    stage_execution_id = source["stage_execution_id"]
    evidence_reference: str | None = None
    if succeeded:
        assert output_reference is not None
        if evidence is not None:
            raise ValueError("a succeeded stage cannot store failure evidence")
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            output_reference=output_reference,
            updated_at=completed_at,
            schema=selected_schema,
        )
    else:
        if output_reference is not None:
            raise ValueError("a failed stage cannot store an output reference")
        if evidence is not None:
            reference, _ = _active_object_store().put_enlisted(
                connection,
                STAGE_FAILURE_EVIDENCE_SCHEMA,
                evidence,
            )
            evidence_reference = format_object_reference(reference)
        if successors:
            raise ValueError("a failed stage cannot create successors")
        transition_stage_execution(
            connection,
            stage_execution_id=stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=completed_at,
            schema=selected_schema,
        )

    record_stage_attempt_terminal(
        connection,
        stage_execution_id=stage_execution_id,
        attempt_number=source["attempt_number"],
        terminal_at=completed_at,
        terminal_summary=terminal_summary,
        terminal_reference=terminal_reference,
        evidence_reference=evidence_reference,
        schema=selected_schema,
    )
    if not succeeded:
        return
    if before_next_stage is not None:
        before_next_stage()
    for successor in successors:
        insert_stage_execution(
            connection,
            work_item_id=source["work_item_id"],
            stage_key=successor.stage_key.value,
            stage_index=successor.stage_index,
            input_reference=successor.input_reference,
            barrier=successor.barrier,
            created_at=completed_at,
            schema=selected_schema,
        )


class _StageSuccessorCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    stage_key: str
    stage_index: StrictInt
    input_reference: str
    barrier: StrictBool = False


def _encode_successors(
    successors: tuple[StageSuccessor, ...],
) -> tuple[dict[str, object], ...]:
    return tuple(
        _StageSuccessorCheckpoint(
            stage_key=successor.stage_key.value,
            stage_index=successor.stage_index,
            input_reference=successor.input_reference,
            barrier=successor.barrier,
        ).model_dump(mode="json")
        for successor in successors
    )


def _decode_successors(
    data: tuple[dict[str, object], ...],
) -> tuple[StageSuccessor, ...]:
    return tuple(
        StageSuccessor(
            stage_key=StageKey(item.stage_key),
            stage_index=item.stage_index,
            input_reference=item.input_reference,
            barrier=item.barrier,
        )
        for item in (
            _StageSuccessorCheckpoint.model_validate(raw) for raw in data
        )
    )


def _lock_handoff_source(
    connection: Connection,
    *,
    workflow_id: str,
    schema: LedgerSchema,
) -> RowMapping:
    # Preserve execution-then-attempt lock order to avoid sweep deadlocks.
    executions = schema.stage_executions
    attempts = schema.stage_attempts
    work_items = schema.work_items
    runs = schema.pipeline_runs
    row = (
        connection.execute(
            select(
                attempts.c.attempt_number,
                executions.c.stage_execution_id,
                executions.c.work_item_id,
                executions.c.stage_key,
                executions.c.stage_index,
                executions.c.state,
                executions.c.current_attempt,
                runs.c.pipeline_key,
                runs.c.pipeline_version,
            )
            .select_from(
                attempts.join(
                    executions,
                    attempts.c.stage_execution_id
                    == executions.c.stage_execution_id,
                )
                .join(
                    work_items,
                    executions.c.work_item_id == work_items.c.work_item_id,
                )
                .join(
                    runs,
                    work_items.c.origin_run_key == runs.c.run_key,
                )
            )
            .where(attempts.c.workflow_id == workflow_id)
            .with_for_update(of=executions)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            f"stage attempt workflow does not exist: {workflow_id}"
        )
    # The terminal write re-locks this same attempt without inverting order.
    connection.execute(
        select(attempts.c.stage_attempt_id)
        .where(
            attempts.c.stage_execution_id == row["stage_execution_id"],
            attempts.c.attempt_number == row["attempt_number"],
        )
        .with_for_update(of=attempts)
    ).one()
    return row


def _validate_workflow_args(value: object) -> tuple[object, ...]:
    if not isinstance(value, tuple):
        raise TypeError("stage args_for must return a tuple")
    return value


def _current_workflow_id() -> str:
    workflow_id = DBOS.workflow_id
    if workflow_id is None:
        raise RuntimeError("stage wrapper must run inside a DBOS workflow")
    return workflow_id


def _stage_workflow_name(
    *,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    stage_key: StageKey,
) -> str:
    identity = f"{pipeline_key.value}\0{pipeline_version}\0{stage_key.value}"
    # Truncation affects routing names only; ledger identity uses full digest.
    name_slug = hashlib.sha256(identity.encode()).hexdigest()[:12]
    readable = re.sub(
        r"[^A-Za-z0-9_]", "_", f"{pipeline_key.value}_{stage_key.value}"
    )
    return f"dr_platform_stage_{readable}_v{pipeline_version}_{name_slug}"
