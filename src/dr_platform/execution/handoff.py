from __future__ import annotations

import hashlib
import re
import traceback
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from dbos import DBOS
from sqlalchemy import select

from dr_platform._core.ledger.attempts import record_stage_attempt_terminal
from dr_platform._core.ledger.executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryField,
    TerminalSummaryProducer,
    build_terminal_summary,
)
from dr_platform.admission.runner import AdmissionPayload
from dr_platform.completion.execution import (
    is_run_completion_wrapped,
    wrap_run_completion,
)
from dr_platform.execution._checkpoint import (
    _require_ledger_checkpoint_executor,
)
from dr_platform.execution.failures import StageApplicationFailure
from dr_platform.pipeline.definitions import (
    AsyncWorkflowCallable,
    PipelineDefinition,
    StageDefinition,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform._core.identities import PipelineKey, StageKey


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
) -> tuple[AsyncWorkflowCallable, ...]:
    completion = pipeline.run_completion
    return (
        *(stage.workflow for stage in pipeline.stages),
        *(() if completion is None else (completion.workflow,)),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _safe_error_message(error: BaseException, *, error_type: str) -> str:
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- defend against a broken __str__
        return f"<unprintable {error_type} message>"


def wrap_pipeline_workflows(
    pipeline: PipelineDefinition,
    *,
    max_recovery_attempts: int,
    clock: Callable[[], datetime] = _utc_now,
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
                stage_index=stage_index,
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
    stage_index: int,
    max_recovery_attempts: int,
    clock: Callable[[], datetime],
) -> AsyncWorkflowCallable:
    stage = pipeline.stages[stage_index]
    next_stage = (
        pipeline.stages[stage_index + 1]
        if stage_index + 1 < len(pipeline.stages)
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
        evidence_reference: str | None,
        next_stage_key: str | None,
        next_stage_index: int | None,
    ) -> None:
        # Read nondeterministic time only in the checkpointed transaction.
        _complete_stage_in_transaction(
            cast("Connection", DBOS.sql_session),
            workflow_id=workflow_id,
            pipeline_key=pipeline_key,
            pipeline_version=pipeline_version,
            stage_key=stage_key,
            stage_index=stage_index,
            succeeded=succeeded,
            output_reference=output_reference,
            terminal_summary=terminal_summary,
            terminal_reference=terminal_reference,
            evidence_reference=evidence_reference,
            next_stage_key=next_stage_key,
            next_stage_index=next_stage_index,
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
        try:
            workflow_args = _validate_workflow_args(stage.args_for(payload))
            output_reference = _validate_output_reference(
                await stage.workflow(*workflow_args)
            )
        except Exception as error:  # noqa: BLE001 -- application boundary
            error_type = f"{type(error).__module__}.{type(error).__qualname__}"
            evidence_reference = (
                error.evidence_reference
                if isinstance(error, StageApplicationFailure)
                else None
            )
            await checkpoint_executor.run(
                complete_stage,
                workflow_id=workflow_id,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key.value,
                stage_index=stage_index,
                succeeded=False,
                output_reference=None,
                terminal_summary=build_terminal_summary(
                    outcome=StageExecutionState.FAILED.value,
                    producer=TerminalSummaryProducer.APPLICATION_FAILURE,
                    error_type=error_type,
                    message=_safe_error_message(error, error_type=error_type),
                    traceback_text="".join(
                        traceback.format_exception(
                            type(error), error, error.__traceback__
                        )
                    ),
                ),
                terminal_reference=None,
                evidence_reference=evidence_reference,
                next_stage_key=None,
                next_stage_index=None,
            )
            return None

        await checkpoint_executor.run(
            complete_stage,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key=stage.key.value,
            stage_index=stage_index,
            succeeded=True,
            output_reference=output_reference,
            terminal_summary={
                TerminalSummaryField.OUTCOME: (
                    StageExecutionState.SUCCEEDED.value
                ),
            },
            terminal_reference=output_reference,
            evidence_reference=None,
            next_stage_key=(
                None if next_stage is None else next_stage.key.value
            ),
            next_stage_index=(None if next_stage is None else stage_index + 1),
        )
        return output_reference

    # Dispatcher rejects declarations lacking this package-owned marker.
    setattr(run_stage, _WRAPPED_STAGE_MARKER, True)
    return cast("AsyncWorkflowCallable", run_stage)


def _complete_stage_in_transaction(  # noqa: PLR0912, PLR0913
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
    evidence_reference: str | None,
    next_stage_key: str | None,
    next_stage_index: int | None,
    completed_at: datetime,
    before_next_stage: Callable[[], None] | None = None,
    schema: StagingSchema | None = None,
) -> None:
    """``before_next_stage`` is a rollback-only test seam."""
    selected_schema = schema or StagingSchema()
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
    if succeeded:
        assert output_reference is not None
        if evidence_reference is not None:
            raise ValueError(
                "a succeeded stage cannot store an evidence reference"
            )
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
        if evidence_reference is not None and not evidence_reference.strip():
            raise ValueError(
                "evidence reference must be a non-empty string when present"
            )
        if next_stage_key is not None or next_stage_index is not None:
            raise ValueError("a failed stage cannot create a successor")
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
    if (next_stage_key is None) != (next_stage_index is None):
        raise ValueError("next stage key and index must be supplied together")
    if next_stage_key is None:
        return
    if before_next_stage is not None:
        before_next_stage()
    insert_stage_execution(
        connection,
        work_item_id=source["work_item_id"],
        stage_key=next_stage_key,
        stage_index=cast("int", next_stage_index),
        created_at=completed_at,
        schema=selected_schema,
    )


def _lock_handoff_source(
    connection: Connection,
    *,
    workflow_id: str,
    schema: StagingSchema,
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


def _validate_output_reference(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(
            "stage application logic must return a non-empty "
            "output-reference string"
        )
    return value


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
