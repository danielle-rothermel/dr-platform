from __future__ import annotations

import inspect
from datetime import datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from dbos import DBOS, DBOSClient, Queue
from dr_serialize import Jsonable
from dr_store.content_addressing import (
    OBJECT_REFERENCE_PREFIX,
    ObjectReference,
    compute_content_hash,
    format_object_reference,
)
from dr_store.object_store import ObjectStore
from dr_store.storage_backends.postgresql import PostgresBackend
from sqlalchemy import Engine, select

from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.evidence import STAGE_FAILURE_EVIDENCE_SCHEMA
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import upsert_stage_control
from dr_platform.admission.runner import run_admission_pass
from dr_platform.execution._object_store import _object_store_context
from dr_platform.execution.failures import StageApplicationFailure
from dr_platform.execution.handoff import (
    StageHandoffMismatchError,
    _complete_stage_in_transaction,
    wrap_pipeline_workflows,
)
from dr_platform.execution.stage_completion import StageSuccessor
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.runtime.dbos import PlatformDbosConfig
from dr_platform.runtime.dispatcher import (
    DispatcherRegistration,
    register_scheduled_dispatcher,
)
from dr_platform.submission.stream import WorkInput
from dr_platform.submission.work_items import stable_random_rank
from tests.conftest import (
    _args_for,
    _as_dbos_client,
    _migrate,
    _RecordingClient,
    dbos_config,
    default_live_dbos_identity,
    submit_items,
)
from tests.conftest import (
    handoff_utc_now as _utc_now,
)
from tests.conftest import (
    recorded_workflow_id as _recorded_workflow_id,
)
from tests.conftest import (
    stage_state_count as _stage_state_count,
)
from tests.conftest import (
    wait_for_handoff as _wait_for,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from dr_platform._core.ledger.schema import LedgerSchema


def _pipeline(
    *,
    key: str,
    stage_logic: tuple[tuple[str, Callable[[str], object]], ...],
) -> PipelineDefinition:
    def as_async(logic: Callable[[str], object]):
        async def run(input_reference: str) -> str | None:
            result = logic(input_reference)
            if inspect.isawaitable(result):
                result = await result
            return cast("str | None", result)

        return run

    return PipelineDefinition(
        key=PipelineKey(key),
        version=1,
        stages=tuple(
            StageDefinition(
                key=StageKey(stage_key),
                queue_name=f"{key}-{stage_key}-queue",
                workflow=as_async(logic),
                args_for=_args_for,
            )
            for stage_key, logic in stage_logic
        ),
    )


def _configure_controls(
    engine: Engine,
    pipeline: PipelineDefinition,
    *,
    capacity: int,
) -> None:
    with engine.begin() as connection:
        for stage in pipeline.stages:
            upsert_stage_control(
                connection,
                pipeline_key=pipeline.key.value,
                pipeline_version=pipeline.version,
                stage_key=stage.key,
                selector={},
                capacity=capacity,
                paused=False,
                updated_at=_utc_now(),
            )


def _submit_items(  # noqa: PLR0913 -- explicit submission facts
    engine: Engine,
    registry: PipelineRegistry,
    pipeline: PipelineDefinition,
    *,
    campaign_key: str,
    run_key: str,
    items: tuple[WorkInput, ...],
    clock: Callable[[], datetime] = _utc_now,
) -> None:
    submit_items(
        campaign_key=campaign_key,
        run_key=run_key,
        pipeline=pipeline.identity,
        execution_config_reference="config:smoke",
        items=items,
        registry=registry,
        engine=engine,
        clock=clock,
    )


def _launch_dbos(
    database_url: str,
    *,
    suffix: str,
    engine: Engine,
    registry: PipelineRegistry,
) -> DispatcherRegistration:
    DBOS(
        config=dbos_config(
            name=f"drp-handoff-{suffix}",
            system_database_url=database_url,
            application_database_url=database_url,
            application_version=f"handoff-{suffix}",
            notification_listener_polling_interval_sec=0.01,
        )
    )
    registration = register_scheduled_dispatcher(
        live_dbos_identity=default_live_dbos_identity(
            app_version=f"handoff-{suffix}"
        ),
        config=PlatformDbosConfig(
            database_url=database_url,
            system_database_url=database_url,
            max_recovery_attempts=1,
        ),
        engine=engine,
        registry=registry,
        sweep_cron=None,
    )
    try:
        DBOS.launch()
    except Exception:
        registration.close()
        raise
    return registration


def _wait_for_workflow_statuses(
    client: DBOSClient,
    workflow_ids: list[str],
    *,
    expected_status: str,
) -> None:
    _wait_for(
        lambda: (
            len(
                client.list_workflows(
                    workflow_ids=workflow_ids,
                    status=expected_status,
                    load_input=False,
                    load_output=False,
                )
            )
            == len(workflow_ids)
        )
    )


def _submit_and_admit_one(
    engine: Engine,
    schema: LedgerSchema,
    pipeline: PipelineDefinition,
    *,
    campaign_key: str,
    run_key: str,
) -> tuple[str, int, int]:
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(engine, pipeline, capacity=1)
    _submit_items(
        engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=run_key,
        items=(
            WorkInput(
                work_key="work",
                input_reference="input",
                labels={},
            ),
        ),
    )
    client = _RecordingClient()
    admitted = run_admission_pass(
        engine,
        client=_as_dbos_client(client),
        registry=registry,
        clock=_utc_now,
    )
    assert admitted.admitted_total == 1
    workflow_id = _recorded_workflow_id(client.enqueued[0])
    with engine.connect() as connection:
        stage_execution_id, work_item_id = connection.execute(
            select(
                schema.stage_executions.c.stage_execution_id,
                schema.stage_executions.c.work_item_id,
            ).where(
                schema.stage_executions.c.state
                == StageExecutionState.ADMITTED.value
            )
        ).one()
    return workflow_id, stage_execution_id, work_item_id


def _handoff_snapshot(
    engine: Engine,
    schema: LedgerSchema,
) -> tuple[tuple[tuple[object, ...], ...], tuple[tuple[object, ...], ...]]:
    with engine.connect() as connection:
        executions = tuple(
            connection.execute(
                select(
                    schema.stage_executions.c.stage_execution_id,
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.stage_index,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.current_attempt,
                    schema.stage_executions.c.output_reference,
                    schema.stage_executions.c.updated_at,
                ).order_by(schema.stage_executions.c.stage_execution_id)
            ).tuples()
        )
        attempts = tuple(
            connection.execute(
                select(
                    schema.stage_attempts.c.stage_attempt_id,
                    schema.stage_attempts.c.attempt_number,
                    schema.stage_attempts.c.workflow_id,
                    schema.stage_attempts.c.terminal_at,
                    schema.stage_attempts.c.terminal_summary,
                    schema.stage_attempts.c.terminal_reference,
                ).order_by(schema.stage_attempts.c.stage_attempt_id)
            ).tuples()
        )
    return executions, attempts


def test_three_stage_pipeline_streams_end_to_end_through_wrapped_workflows(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def prepare(input_reference: str) -> str:
        return f"prepared:{input_reference}"

    def execute(input_reference: str) -> str:
        return f"executed:{input_reference}"

    def score(input_reference: str) -> str:
        return f"scored:{input_reference}"

    declared = _pipeline(
        key=f"evaluation-{suffix}",
        stage_logic=(
            ("prepare", prepare),
            ("execute", execute),
            ("score", score),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared, clock=_utc_now, max_recovery_attempts=1
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=2)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-{suffix}",
        run_key=f"run-{suffix}",
        items=(
            WorkInput(work_key="work-a", input_reference="input:a", labels={}),
            WorkInput(work_key="work-b", input_reference="input:b", labels={}),
        ),
    )
    for stage in pipeline.stages:
        Queue(stage.queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        for stage_index in range(3):
            summary = run_admission_pass(
                pg_engine,
                client=client,
                registry=registry,
                clock=_utc_now,
            )
            assert summary.admitted_total == 2
            _wait_for(
                lambda stage_index=stage_index: (
                    _stage_state_count(
                        pg_engine,
                        schema,
                        stage_index=stage_index,
                        state=StageExecutionState.SUCCEEDED,
                    )
                    == 2
                )
            )

        with pg_engine.connect() as connection:
            rows = (
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.state,
                        schema.stage_executions.c.output_reference,
                    ).order_by(
                        schema.stage_executions.c.work_item_id,
                        schema.stage_executions.c.stage_index,
                    )
                )
                .tuples()
                .all()
            )
            workflow_ids = (
                connection.execute(select(schema.stage_attempts.c.workflow_id))
                .scalars()
                .all()
            )

        assert len(rows) == 6
        assert {row[0] for row in rows} == {0, 1, 2}
        assert {row[1] for row in rows} == {"prepare", "execute", "score"}
        assert all(
            row[2] == StageExecutionState.SUCCEEDED.value for row in rows
        )
        assert all(row[3] for row in rows)
        assert len(workflow_ids) == 6
        _wait_for_workflow_statuses(
            client,
            list(workflow_ids),
            expected_status="SUCCESS",
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


def test_completion_and_next_ready_insert_roll_back_together(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="atomic-handoff",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key="campaign-atomic",
        run_key="run-atomic",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    admission_client = _RecordingClient()
    run_admission_pass(
        pg_engine,
        client=_as_dbos_client(admission_client),
        registry=registry,
        clock=_utc_now,
    )
    workflow_id = _recorded_workflow_id(admission_client.enqueued[0])

    def fail_before_successor() -> None:
        raise RuntimeError("injected between completion and handoff")

    with (
        pytest.raises(RuntimeError, match="injected between"),
        pg_engine.begin() as connection,
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("execute"),
                    stage_index=1,
                    input_reference="output:prepare",
                ),
            ),
            completed_at=_utc_now(),
            before_next_stage=fail_before_successor,
        )

    with pg_engine.connect() as connection:
        execution_rows = (
            connection.execute(
                select(
                    schema.stage_executions.c.stage_key,
                    schema.stage_executions.c.state,
                    schema.stage_executions.c.output_reference,
                )
            )
            .tuples()
            .all()
        )
        terminal_at = connection.execute(
            select(schema.stage_attempts.c.terminal_at)
        ).scalar_one()

    assert execution_rows == [("prepare", "admitted", None)]
    assert terminal_at is None


@pytest.mark.parametrize(
    "mismatch",
    [
        "workflow_id",
        "pipeline_version",
        "stage_key",
        "stage_index",
    ],
)
def test_completion_identity_mismatch_does_not_mutate_state(
    pg_engine: Engine,
    mismatch: str,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="identity-mismatch",
        stage_logic=(
            ("prepare", lambda input_reference: f"prepared:{input_reference}"),
            ("execute", lambda input_reference: f"executed:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-identity-mismatch",
        run_key="run-identity-mismatch",
    )
    before = _handoff_snapshot(pg_engine, schema)
    expected_error = (
        LookupError if mismatch == "workflow_id" else StageHandoffMismatchError
    )

    with (
        pytest.raises(expected_error),
        pg_engine.begin() as connection,
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=(
                "missing-workflow"
                if mismatch == "workflow_id"
                else workflow_id
            ),
            pipeline_key=pipeline.key.value,
            pipeline_version=(
                pipeline.version + 1
                if mismatch == "pipeline_version"
                else pipeline.version
            ),
            stage_key=(
                "wrong-stage" if mismatch == "stage_key" else "prepare"
            ),
            stage_index=1 if mismatch == "stage_index" else 0,
            succeeded=True,
            output_reference="output:prepare",
            terminal_summary={"outcome": "succeeded"},
            terminal_reference="output:prepare",
            evidence=None,
            successors=(
                StageSuccessor(
                    stage_key=StageKey("execute"),
                    stage_index=1,
                    input_reference="output:prepare",
                ),
            ),
            completed_at=_utc_now(),
        )

    assert _handoff_snapshot(pg_engine, schema) == before


_TERMINAL_OBJECT_REFERENCE = (
    "objref://terminal-result/v3"
    "?schema=terminal_result"
    "&schema_version=7"
    "&content_hash="
    "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3"
)


def test_output_reference_is_transported_opaquely_without_parsing(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="opaque-output",
        stage_logic=(
            ("execute", lambda input_reference: f"output:{input_reference}"),
        ),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-opaque-output",
        run_key="run-opaque-output",
    )

    with pg_engine.begin() as connection:
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="execute",
            stage_index=0,
            succeeded=True,
            output_reference=_TERMINAL_OBJECT_REFERENCE,
            terminal_summary={"outcome": "succeeded"},
            terminal_reference=_TERMINAL_OBJECT_REFERENCE,
            evidence=None,
            successors=(),
            completed_at=_utc_now(),
        )

    with pg_engine.connect() as connection:
        stored_output = connection.execute(
            select(schema.stage_executions.c.output_reference)
        ).scalar_one()
        stored_terminal = connection.execute(
            select(schema.stage_attempts.c.terminal_reference)
        ).scalar_one()

    assert stored_output == _TERMINAL_OBJECT_REFERENCE
    assert stored_terminal == _TERMINAL_OBJECT_REFERENCE


def test_application_failure_is_recorded_in_band_and_releases_capacity(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    campaign_key = f"campaign-failure-{suffix}"
    work_keys = ("work-a", "work-b")
    first_work_key = min(
        work_keys,
        key=lambda work_key: stable_random_rank(
            work_identity=CampaignWorkIdentity(
                CampaignKey(campaign_key), WorkKey(work_key)
            )
        ),
    )

    def sometimes_fails(input_reference: str) -> str:
        if input_reference == "input:fail":
            raise RuntimeError("application stage failed")
        return f"output:{input_reference}"

    declared = _pipeline(
        key=f"failure-{suffix}",
        stage_logic=(("execute", sometimes_fails),),
    )
    pipeline = wrap_pipeline_workflows(
        declared, clock=_utc_now, max_recovery_attempts=1
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=campaign_key,
        run_key=f"run-failure-{suffix}",
        items=tuple(
            WorkInput(
                work_key=work_key,
                input_reference=(
                    "input:fail" if work_key == first_work_key else "input:ok"
                ),
                labels={},
            )
            for work_key in work_keys
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        first = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert first.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            failed_workflow_id = connection.execute(
                select(schema.stage_attempts.c.workflow_id)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
        _wait_for_workflow_statuses(
            client,
            [failed_workflow_id],
            expected_status="SUCCESS",
        )

        second = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert second.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.SUCCEEDED,
                )
                == 1
            )
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)


@pytest.mark.parametrize(
    "invalid_output",
    [
        pytest.param("", id="empty-string"),
        pytest.param(7, id="non-string"),
    ],
)
def test_invalid_application_output_lands_failed_without_a_successor(
    clean_pg: str,
    pg_engine: Engine,
    invalid_output: object,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def returns_invalid_output(_input_reference: str) -> object:
        return invalid_output

    declared = _pipeline(
        key=f"invalid-output-{suffix}",
        stage_logic=(
            ("execute", returns_invalid_output),
            ("score", lambda input_reference: f"score:{input_reference}"),
        ),
    )
    pipeline = wrap_pipeline_workflows(
        declared, clock=_utc_now, max_recovery_attempts=1
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-invalid-output-{suffix}",
        run_key=f"run-invalid-output-{suffix}",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            execution_rows = (
                connection.execute(
                    select(
                        schema.stage_executions.c.stage_index,
                        schema.stage_executions.c.stage_key,
                        schema.stage_executions.c.state,
                        schema.stage_executions.c.output_reference,
                    ).order_by(schema.stage_executions.c.stage_index)
                )
                .tuples()
                .all()
            )
            attempt = connection.execute(
                select(
                    schema.stage_attempts.c.workflow_id,
                    schema.stage_attempts.c.terminal_at,
                    schema.stage_attempts.c.terminal_summary,
                    schema.stage_attempts.c.terminal_reference,
                    schema.stage_attempts.c.evidence_reference,
                )
            ).one()
        _wait_for_workflow_statuses(
            client,
            [attempt.workflow_id],
            expected_status="SUCCESS",
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)

    assert execution_rows == [(0, "execute", "failed", None)]
    assert attempt.terminal_at is not None
    if invalid_output == "":
        expected_error_type = "builtins.ValueError"
        expected_message = (
            "stage application logic must return a non-empty "
            "output-reference string"
        )
    else:
        expected_error_type = "builtins.TypeError"
        expected_message = (
            "stage workflow must return str or StageCompletion, "
            "not <class 'int'>"
        )
    assert attempt.terminal_summary == {
        "outcome": "failed",
        "producer": "application_failure",
        "error_type": expected_error_type,
        "message": expected_message,
        "traceback": attempt.terminal_summary["traceback"],
    }
    assert isinstance(attempt.terminal_summary["traceback"], str)
    assert attempt.terminal_reference is None
    assert attempt.evidence_reference is None


def test_application_failure_can_store_evidence_reference(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]
    evidence: Jsonable = {"partial": 1}
    expected_reference = format_object_reference(
        ObjectReference(
            schema=STAGE_FAILURE_EVIDENCE_SCHEMA,
            content_hash=compute_content_hash(evidence),
        )
    )

    def raises_with_evidence(_input_reference: str) -> str:
        raise StageApplicationFailure(
            "partial graph outcome",
            evidence=evidence,
        )

    declared = _pipeline(
        key=f"evidence-failure-{suffix}",
        stage_logic=(("execute", raises_with_evidence),),
    )
    pipeline = wrap_pipeline_workflows(
        declared, clock=_utc_now, max_recovery_attempts=1
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-evidence-failure-{suffix}",
        run_key=f"run-evidence-failure-{suffix}",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            attempt = connection.execute(
                select(
                    schema.stage_attempts.c.workflow_id,
                    schema.stage_attempts.c.terminal_summary,
                    schema.stage_attempts.c.terminal_reference,
                    schema.stage_attempts.c.evidence_reference,
                    schema.stage_executions.c.output_reference,
                ).select_from(
                    schema.stage_attempts.join(
                        schema.stage_executions,
                        schema.stage_attempts.c.stage_execution_id
                        == schema.stage_executions.c.stage_execution_id,
                    )
                )
            ).one()
            workflow_id = attempt.workflow_id
        _wait_for_workflow_statuses(
            client,
            [workflow_id],
            expected_status="SUCCESS",
        )
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)

    assert attempt.output_reference is None
    assert attempt.terminal_reference is None
    assert attempt.evidence_reference == expected_reference
    assert attempt.evidence_reference.startswith(
        f"{OBJECT_REFERENCE_PREFIX}:{STAGE_FAILURE_EVIDENCE_SCHEMA}:"
    )
    assert attempt.terminal_summary["producer"] == "application_failure"
    assert attempt.terminal_summary["message"] == "partial graph outcome"


class _UnprintableError(RuntimeError):
    def __str__(self) -> str:
        raise ValueError("this error message cannot be rendered")


def test_application_failure_with_unprintable_error_lands_failed(
    clean_pg: str,
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    suffix = uuid4().hex[:10]

    def raises_unprintable(_input_reference: str) -> str:
        raise _UnprintableError

    declared = _pipeline(
        key=f"unprintable-{suffix}",
        stage_logic=(("execute", raises_unprintable),),
    )
    pipeline = wrap_pipeline_workflows(
        declared, clock=_utc_now, max_recovery_attempts=1
    )
    registry = PipelineRegistry()
    registry.register(pipeline)
    _configure_controls(pg_engine, pipeline, capacity=1)
    _submit_items(
        pg_engine,
        registry,
        pipeline,
        campaign_key=f"campaign-unprintable-{suffix}",
        run_key=f"run-unprintable-{suffix}",
        items=(
            WorkInput(work_key="work", input_reference="input", labels={}),
        ),
    )
    Queue(pipeline.stages[0].queue_name, polling_interval_sec=0.02)

    registration: DispatcherRegistration | None = None
    try:
        registration = _launch_dbos(
            clean_pg,
            suffix=suffix,
            engine=pg_engine,
            registry=registry,
        )
        client = registration.client
        admitted = run_admission_pass(
            pg_engine,
            client=client,
            registry=registry,
            clock=_utc_now,
        )
        assert admitted.admitted_total == 1
        _wait_for(
            lambda: (
                _stage_state_count(
                    pg_engine,
                    schema,
                    stage_index=0,
                    state=StageExecutionState.FAILED,
                )
                == 1
            )
        )

        with pg_engine.connect() as connection:
            terminal_summary = connection.execute(
                select(schema.stage_attempts.c.terminal_summary)
                .join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
                .where(
                    schema.stage_executions.c.state
                    == StageExecutionState.FAILED.value
                )
            ).scalar_one()
    finally:
        if registration is not None:
            registration.close()
        DBOS.destroy(destroy_registry=True)

    expected_type = (
        f"{_UnprintableError.__module__}.{_UnprintableError.__qualname__}"
    )
    assert terminal_summary["error_type"] == expected_type
    assert terminal_summary["message"] == (
        f"<unprintable {expected_type} message>"
    )


def test_failure_evidence_write_aborts_whole_checkpoint(
    pg_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = _migrate(pg_engine)
    pipeline = _pipeline(
        key="evidence-abort",
        stage_logic=(("prepare", lambda input_reference: input_reference),),
    )
    workflow_id, _stage_execution_id, _work_item_id = _submit_and_admit_one(
        pg_engine,
        schema,
        pipeline,
        campaign_key="campaign-evidence-abort",
        run_key="run-evidence-abort",
    )
    object_store = ObjectStore(PostgresBackend.open_sync(pg_engine))

    def explode(*args: object, **kwargs: object) -> tuple[object, object]:
        raise RuntimeError("evidence store unavailable")

    monkeypatch.setattr(object_store, "put_enlisted", explode)

    with (
        pytest.raises(RuntimeError, match="evidence store unavailable"),
        pg_engine.begin() as connection,
        _object_store_context(object_store),
    ):
        _complete_stage_in_transaction(
            connection,
            workflow_id=workflow_id,
            pipeline_key=pipeline.key.value,
            pipeline_version=pipeline.version,
            stage_key="prepare",
            stage_index=0,
            succeeded=False,
            output_reference=None,
            terminal_summary={"outcome": "failed"},
            terminal_reference=None,
            evidence={"partial": 1},
            successors=(),
            completed_at=_utc_now(),
            schema=schema,
        )

    with pg_engine.connect() as connection:
        row = connection.execute(
            select(
                schema.stage_executions.c.state,
                schema.stage_attempts.c.terminal_at,
                schema.stage_attempts.c.evidence_reference,
            ).select_from(
                schema.stage_attempts.join(
                    schema.stage_executions,
                    schema.stage_attempts.c.stage_execution_id
                    == schema.stage_executions.c.stage_execution_id,
                )
            )
        ).one()

    assert row.state == StageExecutionState.ADMITTED.value
    assert row.terminal_at is None
    assert row.evidence_reference is None
