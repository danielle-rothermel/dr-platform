from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003 -- Pydantic resolves it
from enum import UNIQUE, StrEnum, verify
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from dbos import DBOS
from pydantic import (
    BaseModel,
    ConfigDict,
    StrictInt,
    field_serializer,
    field_validator,
    model_validator,
)
from sqlalchemy import null, select, update

from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunCompletionKey,
    RunKey,
)
from dr_platform._core.ledger.completion_attempts import (
    RunCompletionAttemptRecord,
    get_run_completion_attempt,
    get_run_completion_attempt_by_workflow_id,
    record_run_completion_attempt_terminal,
)
from dr_platform._core.ledger.completion_attempts import (
    run_completion_workflow_id as _run_completion_workflow_id,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import RunCompletionExecutionState
from dr_platform._core.validation import validate_non_empty_string
from dr_platform.execution._checkpoint import (
    _require_ledger_checkpoint_executor,
)
from dr_platform.inspection.statuses import StateCount  # noqa: TC001
from dr_platform.pipeline.definitions import RunCompletionDefinition

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy import Connection, Engine
    from sqlalchemy.engine import RowMapping

RUN_COMPLETION_WORKFLOW_ID_PREFIX = "drp-run-"
RUN_COMPLETION_WORKFLOW_ID_DIGEST_LENGTH = 64
_WRAPPED_COMPLETION_MARKER = "_dr_platform_wrapped_run_completion"


@verify(UNIQUE)
class RunCompletionWorkflowIdField(StrEnum):
    """Persisted wire keys; spell them out at hashing sites, never iterate."""

    RUN_KEY = "run_key"
    PIPELINE_KEY = "pipeline_key"
    PIPELINE_VERSION = "pipeline_version"
    RUN_COMPLETION_KEY = "run_completion_key"


class RunCompletionPayload(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    campaign_key: CampaignKey
    run_key: RunKey
    pipeline_key: PipelineKey
    pipeline_version: StrictInt
    execution_config_reference: str
    manifest_reference: str
    membership_digest: str
    member_count: StrictInt
    released_at: datetime
    release_terminal_state_counts: tuple[StateCount, ...]

    @field_validator("campaign_key", mode="before")
    @classmethod
    def _campaign_key(cls, value: object) -> CampaignKey:
        if isinstance(value, CampaignKey):
            return value
        if not isinstance(value, str):
            raise TypeError("campaign key must be a string")
        return CampaignKey(value)

    @field_validator("run_key", mode="before")
    @classmethod
    def _run_key(cls, value: object) -> RunKey:
        if isinstance(value, RunKey):
            return value
        if not isinstance(value, str):
            raise TypeError("run key must be a string")
        return RunKey(value)

    @field_validator("pipeline_key", mode="before")
    @classmethod
    def _pipeline_key(cls, value: object) -> PipelineKey:
        if isinstance(value, PipelineKey):
            return value
        if not isinstance(value, str):
            raise TypeError("pipeline key must be a string")
        return PipelineKey(value)

    @field_serializer("campaign_key", "run_key", "pipeline_key")
    def _serialize_key(self, value: object) -> str:
        return str(value)

    @model_validator(mode="after")
    def _validate_release_facts(self) -> RunCompletionPayload:
        if self.pipeline_version <= 0:
            raise ValueError("pipeline version must be positive")
        if self.member_count < 0:
            raise ValueError("member count must be non-negative")
        for label, value in (
            ("execution config reference", self.execution_config_reference),
            ("manifest reference", self.manifest_reference),
            ("membership digest", self.membership_digest),
        ):
            validate_non_empty_string(value, label=label)
        expected_states = (
            "succeeded",
            "failed",
            "cancelled",
        )
        actual_states = tuple(
            item.state.value for item in self.release_terminal_state_counts
        )
        if actual_states != expected_states:
            raise ValueError(
                "release terminal state counts must use canonical order"
            )
        if any(item.count < 0 for item in self.release_terminal_state_counts):
            raise ValueError(
                "release terminal state counts must be non-negative"
            )
        if (
            sum(item.count for item in self.release_terminal_state_counts)
            != self.member_count
        ):
            raise ValueError(
                "release terminal state counts must sum to members"
            )
        return self


@dataclass(frozen=True, slots=True)
class RunCompletionExecutionRecord:
    run_completion_execution_id: int
    run_key: RunKey
    current_attempt: int
    workflow_id: str
    state: RunCompletionExecutionState
    enqueued_at: datetime
    output_reference: str | None
    error_summary: Mapping[str, object] | None
    terminal_at: datetime | None


class RunCompletionOutcomeError(RuntimeError):
    pass


def run_completion_workflow_id(
    *,
    run_key: RunKey,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
    attempt_number: int = 1,
) -> str:
    return _run_completion_workflow_id(
        run_key=run_key,
        pipeline_key=pipeline_key,
        pipeline_version=pipeline_version,
        completion_key=completion_key,
        attempt_number=attempt_number,
    )


def is_run_completion_wrapped(completion: RunCompletionDefinition) -> bool:
    return bool(
        getattr(completion.workflow, _WRAPPED_COMPLETION_MARKER, False)
    )


def wrap_run_completion(
    completion: RunCompletionDefinition,
    *,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    max_recovery_attempts: int,
    clock: Callable[[], datetime],
) -> RunCompletionDefinition:
    workflow_name = _run_completion_workflow_name(
        pipeline_key=pipeline_key,
        pipeline_version=pipeline_version,
        completion_key=completion.key,
    )

    def _record_transaction(
        *,
        workflow_id: str,
        succeeded: bool,
        output_reference: str | None,
        error_summary: Mapping[str, object] | None,
    ) -> None:
        record_run_completion_outcome(
            cast("Connection", DBOS.sql_session),
            workflow_id=workflow_id,
            succeeded=succeeded,
            output_reference=output_reference,
            error_summary=error_summary,
            terminal_at=clock(),
        )

    record_outcome = DBOS.transaction(
        isolation_level="READ COMMITTED",
        name=f"{workflow_name}_complete",
    )(_record_transaction)

    @DBOS.workflow(
        name=workflow_name,
        max_recovery_attempts=max_recovery_attempts,
    )
    async def run_completion(payload_data: dict[str, object]) -> str | None:
        checkpoint_executor = _require_ledger_checkpoint_executor(
            run_completion
        )
        workflow_id = _current_workflow_id()
        payload = RunCompletionPayload.model_validate(payload_data)
        try:
            workflow_args = _validate_workflow_args(
                completion.args_for(payload), label="run completion"
            )
            output_reference = _validate_output_reference(
                await completion.workflow(*workflow_args)
            )
        except Exception as error:  # noqa: BLE001 -- application boundary
            error_type = f"{type(error).__module__}.{type(error).__qualname__}"
            await checkpoint_executor.run(
                record_outcome,
                workflow_id=workflow_id,
                succeeded=False,
                output_reference=None,
                error_summary={
                    "error_type": error_type,
                    "message": _safe_error_message(
                        error, error_type=error_type
                    ),
                },
            )
            return None
        await checkpoint_executor.run(
            record_outcome,
            workflow_id=workflow_id,
            succeeded=True,
            output_reference=output_reference,
            error_summary=None,
        )
        return output_reference

    setattr(run_completion, _WRAPPED_COMPLETION_MARKER, True)
    return RunCompletionDefinition(
        key=completion.key,
        queue_name=completion.queue_name,
        workflow=cast("object", run_completion),  # ty: ignore[invalid-argument-type]
        args_for=completion.args_for,
    )


def record_run_completion_outcome(  # noqa: PLR0913
    connection: Connection,
    *,
    workflow_id: str,
    succeeded: bool,
    output_reference: str | None,
    error_summary: Mapping[str, object] | None,
    terminal_at: datetime,
    schema: StagingSchema | None = None,
) -> RunCompletionExecutionRecord:
    selected_schema = schema or StagingSchema()
    attempt = get_run_completion_attempt_by_workflow_id(
        connection,
        workflow_id=workflow_id,
        schema=selected_schema,
    )
    if attempt is None:
        raise LookupError(
            f"run completion workflow does not exist: {workflow_id}"
        )
    executions = selected_schema.run_completion_executions
    row = (
        connection.execute(
            select(executions)
            .where(
                executions.c.run_completion_execution_id
                == attempt.run_completion_execution_id
            )
            .with_for_update(of=executions)
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise LookupError(
            f"run completion execution does not exist: "
            f"{attempt.run_completion_execution_id}"
        )
    existing = _decode_execution(row, attempt=attempt)
    if existing.state is not RunCompletionExecutionState.ENQUEUED:
        if (
            existing.state
            is (
                RunCompletionExecutionState.SUCCEEDED
                if succeeded
                else RunCompletionExecutionState.FAILED
            )
            and existing.output_reference == output_reference
            and dict(existing.error_summary or {}) == dict(error_summary or {})
        ):
            return existing
        raise RunCompletionOutcomeError(
            "run completion already records a different terminal outcome"
        )
    if succeeded:
        reference = validate_non_empty_string(
            output_reference, label="run completion output reference"
        )
        attempt_summary: Mapping[str, object] | None = {
            "outcome": RunCompletionExecutionState.SUCCEEDED.value,
        }
        values = {
            "state": RunCompletionExecutionState.SUCCEEDED.value,
            "output_reference": reference,
            "error_summary": null(),
            "terminal_at": terminal_at,
        }
    else:
        if output_reference is not None or error_summary is None:
            raise ValueError("failed run completion requires only an error")
        attempt_summary = {
            "outcome": RunCompletionExecutionState.FAILED.value,
            **dict(error_summary),
        }
        values = {
            "state": RunCompletionExecutionState.FAILED.value,
            "output_reference": None,
            "error_summary": dict(error_summary),
            "terminal_at": terminal_at,
        }
    updated = (
        connection.execute(
            update(executions)
            .where(
                executions.c.run_completion_execution_id
                == existing.run_completion_execution_id
            )
            .values(**values)
            .returning(*executions.c)
        )
        .mappings()
        .one()
    )
    recorded_attempt = record_run_completion_attempt_terminal(
        connection,
        run_completion_execution_id=existing.run_completion_execution_id,
        attempt_number=existing.current_attempt,
        terminal_at=terminal_at,
        terminal_summary=attempt_summary,
        terminal_reference=workflow_id,
        schema=selected_schema,
    )
    return _decode_execution(
        updated,
        attempt=recorded_attempt,
    )


def inspect_run_completion(
    run_key: RunKey | str,
    *,
    engine: Engine,
    schema: StagingSchema | None = None,
) -> RunCompletionExecutionRecord:
    selected_schema = schema or StagingSchema()
    normalized_run_key = (
        run_key if isinstance(run_key, RunKey) else RunKey(run_key)
    )
    table = selected_schema.run_completion_executions
    with engine.connect() as connection:
        row = (
            connection.execute(
                select(table).where(
                    table.c.run_key == normalized_run_key.value
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise LookupError(
                "run completion execution does not exist: "
                f"{normalized_run_key}"
            )
        attempt = get_run_completion_attempt(
            connection,
            run_completion_execution_id=row["run_completion_execution_id"],
            attempt_number=row["current_attempt"],
            schema=selected_schema,
        )
    if attempt is None:
        raise RuntimeError("run completion current attempt is missing")
    return _decode_execution(row, attempt=attempt)


def _decode_execution(
    row: RowMapping,
    *,
    attempt: RunCompletionAttemptRecord,
) -> RunCompletionExecutionRecord:
    error_summary = row["error_summary"]
    return RunCompletionExecutionRecord(
        run_completion_execution_id=row["run_completion_execution_id"],
        run_key=RunKey(row["run_key"]),
        current_attempt=row["current_attempt"],
        workflow_id=attempt.workflow_id,
        state=RunCompletionExecutionState(row["state"]),
        enqueued_at=row["enqueued_at"],
        output_reference=row["output_reference"],
        error_summary=(
            None
            if error_summary is None
            else MappingProxyType(dict(error_summary))
        ),
        terminal_at=row["terminal_at"],
    )


def _validate_output_reference(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(
            "run completion application logic must return a non-empty "
            "output-reference string"
        )
    return value


def _validate_workflow_args(
    value: object, *, label: str
) -> tuple[object, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{label} args_for must return a tuple")
    return value


def _safe_error_message(error: BaseException, *, error_type: str) -> str:
    try:
        return str(error)
    except Exception:  # noqa: BLE001 -- broken application exception
        return f"<unprintable {error_type} message>"


def _current_workflow_id() -> str:
    workflow_id = DBOS.workflow_id
    if workflow_id is None:
        raise RuntimeError("run completion wrapper requires a DBOS workflow")
    return workflow_id


def _run_completion_workflow_name(
    *,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    completion_key: RunCompletionKey,
) -> str:
    identity = (
        f"{pipeline_key.value}\0{pipeline_version}\0{completion_key.value}"
    )
    slug = hashlib.sha256(identity.encode()).hexdigest()[:12]
    readable = re.sub(
        r"[^A-Za-z0-9_]",
        "_",
        f"{pipeline_key.value}_{completion_key.value}",
    )
    return f"dr_platform_run_completion_{readable}_v{pipeline_version}_{slug}"
