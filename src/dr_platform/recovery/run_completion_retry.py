from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import null, select, update

from dr_platform._core.clock import utc_now
from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
    normalize_key,
)
from dr_platform._core.ledger.completion_attempts import (
    RunCompletionAttemptRecord,
    append_run_completion_attempt,
    get_run_completion_attempt,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import (
    RunCompletionExecutionState,
    StageExecutionState,
)
from dr_platform.completion.execution import (
    RunCompletionExecutionRecord,
    RunCompletionPayload,
    decode_run_completion_execution,
    is_run_completion_wrapped,
)
from dr_platform.inspection.statuses import StateCount

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from datetime import datetime

    from dbos import DBOSClient, EnqueueOptions
    from sqlalchemy import Engine

    from dr_platform.pipeline.definitions import RunCompletionDefinition
    from dr_platform.pipeline.registry import PipelineRegistry


@dataclass(frozen=True, slots=True)
class RunCompletionRetryResult:
    execution: RunCompletionExecutionRecord
    new_attempt: RunCompletionAttemptRecord


def retry_run_completion(  # noqa: PLR0913 -- explicit operator boundary
    run_key: RunKey | str,
    *,
    engine: Engine,
    client: DBOSClient,
    registry: PipelineRegistry,
    clock: Callable[[], datetime] = utc_now,
    schema: StagingSchema | None = None,
) -> RunCompletionRetryResult:
    """Only FAILED run completions may prepare a new attempt for enqueue."""
    selected_schema = schema or StagingSchema()
    normalized_run_key = normalize_key(run_key, RunKey)
    execution_record: RunCompletionExecutionRecord
    new_attempt_record: RunCompletionAttemptRecord
    with engine.begin() as connection:
        runs = selected_schema.pipeline_runs
        executions = selected_schema.run_completion_executions
        run_row = (
            connection.execute(
                select(
                    runs.c.campaign_key,
                    runs.c.pipeline_key,
                    runs.c.pipeline_version,
                    runs.c.execution_config_reference,
                    runs.c.manifest_reference,
                    runs.c.membership_digest,
                    runs.c.run_completion_key,
                    runs.c.expected_member_count,
                    runs.c.released_at,
                    runs.c.release_terminal_state_counts,
                    executions.c.run_completion_execution_id,
                    executions.c.current_attempt,
                    executions.c.state,
                    executions.c.enqueued_at,
                )
                .select_from(
                    runs.join(
                        executions,
                        runs.c.run_key == executions.c.run_key,
                    )
                )
                .where(runs.c.run_key == normalized_run_key.value)
                .with_for_update(of=executions)
            )
            .mappings()
            .one_or_none()
        )
        if run_row is None:
            raise LookupError(
                "run completion execution does not exist: "
                f"{normalized_run_key}"
            )
        if run_row["state"] != RunCompletionExecutionState.FAILED.value:
            raise ValueError(
                "only a FAILED run completion execution can be retried"
            )
        if run_row["released_at"] is None:
            raise RuntimeError(
                "FAILED run completion has no barrier release facts"
            )
        previous = get_run_completion_attempt(
            connection,
            run_completion_execution_id=run_row["run_completion_execution_id"],
            attempt_number=run_row["current_attempt"],
            schema=selected_schema,
        )
        if previous is None or previous.terminal_at is None:
            raise RuntimeError(
                "FAILED run completion has no terminal current attempt"
            )
        retried_at = clock()
        # The reset below overwrites enqueued_at, so append-time ordering
        # alone cannot preserve ck_rc_exec_terminal_time; check the clock here.
        if retried_at < previous.terminal_at:
            raise ValueError(
                "retry timestamp cannot precede prior attempt termination"
            )
        pipeline = registry.get(
            key=PipelineKey(run_row["pipeline_key"]),
            version=run_row["pipeline_version"],
        )
        completion = pipeline.run_completion
        if (
            completion is None
            or completion.key.value != run_row["run_completion_key"]
            or not is_run_completion_wrapped(completion)
        ):
            raise RuntimeError(
                "persisted run completion disagrees with registry"
            )
        state_counts = _decode_state_counts(
            run_row["release_terminal_state_counts"]
        )
        new_attempt_record = append_run_completion_attempt(
            connection,
            run_completion_execution_id=run_row["run_completion_execution_id"],
            run_key=normalized_run_key,
            pipeline_key=pipeline.key,
            pipeline_version=pipeline.version,
            completion_key=completion.key,
            created_at=retried_at,
            enqueued_at=retried_at,
            schema=selected_schema,
        )
        updated = (
            connection.execute(
                update(executions)
                .where(
                    executions.c.run_completion_execution_id
                    == run_row["run_completion_execution_id"]
                )
                .values(
                    state=RunCompletionExecutionState.ENQUEUED.value,
                    enqueued_at=retried_at,
                    output_reference=None,
                    error_summary=null(),
                    terminal_at=None,
                )
                .returning(*executions.c)
            )
            .mappings()
            .one()
        )
        execution_record = decode_run_completion_execution(
            updated,
            attempt=new_attempt_record,
        )
        payload = RunCompletionPayload(
            campaign_key=CampaignKey(run_row["campaign_key"]),
            run_key=normalized_run_key,
            pipeline_key=pipeline.key,
            pipeline_version=run_row["pipeline_version"],
            execution_config_reference=run_row["execution_config_reference"],
            manifest_reference=run_row["manifest_reference"],
            membership_digest=run_row["membership_digest"],
            member_count=run_row["expected_member_count"],
            released_at=run_row["released_at"],
            release_terminal_state_counts=state_counts,
        )
        options: EnqueueOptions = {
            "workflow_name": _workflow_name(completion),
            "queue_name": completion.queue_name,
            "workflow_id": new_attempt_record.workflow_id,
        }
        client.enqueue_in_transaction(
            connection, options, payload.model_dump(mode="json")
        )
    return RunCompletionRetryResult(
        execution=execution_record,
        new_attempt=new_attempt_record,
    )


def _workflow_name(completion: RunCompletionDefinition) -> str:
    workflow = completion.workflow
    name = getattr(
        workflow,
        "dbos_function_name",
        getattr(workflow, "__name__", None),
    )
    if not isinstance(name, str) or not name:
        raise TypeError("run completion workflow must expose a DBOS name")
    return name


def _decode_state_counts(
    serialized: object,
) -> tuple[StateCount, ...]:
    if not isinstance(serialized, list):
        raise TypeError("release terminal state counts must be a list")
    return tuple(
        StateCount(
            state=StageExecutionState(
                str(cast("Mapping[str, object]", item)["state"])
            ),
            count=int(str(cast("Mapping[str, object]", item)["count"])),
        )
        for item in serialized
    )
