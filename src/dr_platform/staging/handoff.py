"""Package-owned DBOS workflows for linear stage execution and handoff.

Applications declare a :class:`PipelineDefinition` with plain stage callables
that return immutable output-reference strings.  ``wrap_pipeline_workflows``
returns the declaration to register with :class:`PipelineRegistry`; its stage
``workflow`` fields are DBOS-registered wrappers while ``args_for`` continues
to produce only the application's positional arguments.

The wrapper catches application exceptions and commits a logical FAILED
outcome before returning normally.  Completion infrastructure errors are not
swallowed: DBOS can recover and replay the checkpointed transaction.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from dbos import DBOS
from sqlalchemy import select

from dr_platform.staging.definitions import (
    PipelineDefinition,
    StageDefinition,
    WorkflowCallable,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import record_stage_attempt_terminal
from dr_platform.staging.stage_executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform.staging.states import StageExecutionState

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform.staging.identities import PipelineKey, StageKey


class StageHandoffMismatchError(RuntimeError):
    """The running workflow does not match its persisted stage attempt."""


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
    clock: Callable[[], datetime] = _utc_now,
) -> PipelineDefinition:
    """Replace application stage callables with package-owned DBOS wrappers.

    The input declaration remains unchanged.  Register and submit against the
    returned declaration so admission enqueues the generated workflows.

    Application stage callables may re-execute: if DBOS recovers a workflow
    that crashed before its completion transaction checkpointed, the stage body
    runs again.  Applications must tolerate at-least-once execution of stage
    bodies; only the completion transaction itself commits exactly once.
    """
    wrapped_stages = tuple(
        StageDefinition(
            key=stage.key,
            queue_name=stage.queue_name,
            workflow=_wrap_stage_workflow(
                pipeline=pipeline,
                stage_index=stage_index,
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
    )


def _wrap_stage_workflow(
    *,
    pipeline: PipelineDefinition,
    stage_index: int,
    clock: Callable[[], datetime],
) -> WorkflowCallable:
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
        next_stage_key: str | None,
        next_stage_index: int | None,
    ) -> None:
        # ``clock()`` is non-deterministic, so it must be read inside the
        # checkpointed transaction rather than in the replayable workflow body.
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
            next_stage_key=next_stage_key,
            next_stage_index=next_stage_index,
            completed_at=clock(),
        )

    complete_stage = DBOS.transaction(
        name=f"{workflow_name}_complete"
    )(_complete_stage_transaction)

    @DBOS.workflow(name=workflow_name)
    def run_stage(*args: object) -> str | None:
        workflow_id = _current_workflow_id()
        try:
            output_reference = _validate_output_reference(
                stage.workflow(*args)
            )
        except Exception as error:  # noqa: BLE001 -- application boundary
            error_type = f"{type(error).__module__}.{type(error).__qualname__}"
            complete_stage(
                workflow_id=workflow_id,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key.value,
                stage_index=stage_index,
                succeeded=False,
                output_reference=None,
                terminal_summary={
                    "outcome": StageExecutionState.FAILED.value,
                    "error_type": error_type,
                    "message": _safe_error_message(
                        error, error_type=error_type
                    ),
                },
                terminal_reference=error_type,
                next_stage_key=None,
                next_stage_index=None,
            )
            return None

        complete_stage(
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key=stage.key.value,
            stage_index=stage_index,
            succeeded=True,
            output_reference=output_reference,
            terminal_summary={"outcome": StageExecutionState.SUCCEEDED.value},
            terminal_reference=output_reference,
            next_stage_key=(
                None if next_stage is None else next_stage.key.value
            ),
            next_stage_index=(
                None if next_stage is None else stage_index + 1
            ),
        )
        return output_reference

    return cast("WorkflowCallable", run_stage)


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
    next_stage_key: str | None,
    next_stage_index: int | None,
    completed_at: datetime,
    before_next_stage: Callable[[], None] | None = None,
    schema: StagingSchema | None = None,
) -> None:
    """Apply completion on an existing transaction.

    ``before_next_stage`` is a test seam for proving rollback between the
    current-stage update and successor insert; production callers omit it.
    """
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
        source["state"] != StageExecutionState.ADMITTED.value
        or source["current_attempt"] != source["attempt_number"]
    ):
        raise StageHandoffMismatchError(
            "workflow attempt is not the current ADMITTED stage attempt"
        )

    stage_execution_id = source["stage_execution_id"]
    if succeeded:
        assert output_reference is not None
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
            .with_for_update(of=(attempts, executions))
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            f"stage attempt workflow does not exist: {workflow_id}"
        )
    return row


def _validate_output_reference(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(
            "stage application logic must return a non-empty "
            "output-reference string"
        )
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
    identity = (
        f"{pipeline_key.value}\0{pipeline_version}\0{stage_key.value}"
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:12]
    readable = re.sub(
        r"[^A-Za-z0-9_]", "_", f"{pipeline_key.value}_{stage_key.value}"
    )
    return f"dr_platform_stage_{readable}_v{pipeline_version}_{digest}"
