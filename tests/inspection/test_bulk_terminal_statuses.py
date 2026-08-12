from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import Engine, event, select

from dr_platform._core.identities import PipelineKey, StageKey, WorkKey
from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import transition_stage_execution
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    TerminalSummaryProducer,
    build_terminal_summary,
)
from dr_platform.inspection.statuses import bulk_work_terminal_statuses
from dr_platform.inspection.terminal_filters import TerminalSummaryFilter
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.pipeline.registry import PipelineRegistry
from dr_platform.recovery.retry import retry_stage
from dr_platform.submission.stream import WorkInput
from tests.conftest import NOW, _args_for, _migrate, submit_items
from tests.core.test_evidence import SAMPLE_EVIDENCE_REFERENCE

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform._core.ledger.schema import StagingSchema


async def _workflow(input_reference: str) -> str:
    return f"output:{input_reference}"


def _registry() -> PipelineRegistry:
    registry = PipelineRegistry()
    registry.register(
        PipelineDefinition(
            key=PipelineKey("evaluation"),
            version=1,
            stages=(
                StageDefinition(
                    key=StageKey("execute"),
                    queue_name="execute-queue",
                    workflow=_workflow,
                    args_for=_args_for,
                ),
            ),
        )
    )
    return registry


def _execution_by_work_key(
    engine: Engine,
    schema: StagingSchema,
) -> dict[str, tuple[int, int]]:
    with engine.connect() as connection:
        rows = connection.execute(
            select(
                schema.work_items.c.work_key,
                schema.work_items.c.work_item_id,
                schema.stage_executions.c.stage_execution_id,
            ).select_from(
                schema.work_items.join(
                    schema.stage_executions,
                    schema.work_items.c.work_item_id
                    == schema.stage_executions.c.work_item_id,
                )
            )
        ).tuples()
        return {row[0]: (row[1], row[2]) for row in rows}


def _fail_with_terminal_summary(  # noqa: PLR0913 -- explicit seeded facts
    connection: Connection,
    *,
    stage_execution_id: int,
    state: StageExecutionState,
    producer: TerminalSummaryProducer,
    at: datetime,
    error_type: str | None = None,
    evidence_reference: str | None = None,
) -> None:
    attempt = append_stage_attempt(
        connection,
        stage_execution_id=stage_execution_id,
        created_at=at,
        admitted_at=at,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        new_state=StageExecutionState.ADMITTED,
        updated_at=at,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=stage_execution_id,
        new_state=state,
        updated_at=at + timedelta(seconds=1),
    )
    record_stage_attempt_terminal(
        connection,
        stage_execution_id=stage_execution_id,
        attempt_number=attempt.attempt_number,
        terminal_at=at + timedelta(seconds=1),
        terminal_summary=build_terminal_summary(
            outcome=state.value,
            producer=producer,
            error_type=error_type,
            message="seeded failure",
        ),
        evidence_reference=evidence_reference,
    )


def _seed_terminal_members(engine: Engine, schema: StagingSchema) -> None:
    registry = _registry()
    submit_items(
        campaign_key="campaign-terminal",
        run_key="run-terminal",
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key="work-app-failure",
                input_reference="input:app",
                labels={},
            ),
            WorkInput(
                work_key="work-abandonment",
                input_reference="input:abandon",
                labels={},
            ),
            WorkInput(
                work_key="work-cancellation",
                input_reference="input:cancel",
                labels={},
            ),
        ),
        registry=registry,
        engine=engine,
        clock=lambda: NOW,
    )
    executions = _execution_by_work_key(engine, schema)
    with engine.begin() as connection:
        _fail_with_terminal_summary(
            connection,
            stage_execution_id=executions["work-app-failure"][1],
            state=StageExecutionState.FAILED,
            producer=TerminalSummaryProducer.APPLICATION_FAILURE,
            at=NOW,
            error_type="builtins.RuntimeError",
            evidence_reference=SAMPLE_EVIDENCE_REFERENCE,
        )
        _fail_with_terminal_summary(
            connection,
            stage_execution_id=executions["work-abandonment"][1],
            state=StageExecutionState.FAILED,
            producer=TerminalSummaryProducer.ABANDONMENT,
            at=NOW + timedelta(seconds=1),
        )
        _fail_with_terminal_summary(
            connection,
            stage_execution_id=executions["work-cancellation"][1],
            state=StageExecutionState.CANCELLED,
            producer=TerminalSummaryProducer.CANCELLATION,
            at=NOW + timedelta(seconds=2),
        )


def test_bulk_terminal_statuses_returns_summary_and_evidence_reference(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_terminal_members(pg_engine, schema)

    result = bulk_work_terminal_statuses(
        "campaign-terminal",
        ("work-app-failure",),
        engine=pg_engine,
    )
    status = result.statuses[WorkKey("work-app-failure")]

    assert status.present is True
    assert status.evidence_reference == SAMPLE_EVIDENCE_REFERENCE
    assert status.terminal_summary is not None
    assert (
        status.terminal_summary["producer"]
        == TerminalSummaryProducer.APPLICATION_FAILURE.value
    )
    assert status.terminal_summary["error_type"] == "builtins.RuntimeError"


def test_bulk_terminal_statuses_filters_by_producer(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_terminal_members(pg_engine, schema)
    work_keys = (
        "work-app-failure",
        "work-abandonment",
        "work-cancellation",
        "work-missing",
    )

    result = bulk_work_terminal_statuses(
        "campaign-terminal",
        work_keys,
        engine=pg_engine,
        terminal_filter=TerminalSummaryFilter(
            producer=TerminalSummaryProducer.APPLICATION_FAILURE,
        ),
    )

    assert set(result.statuses) == {
        WorkKey("work-app-failure"),
        WorkKey("work-missing"),
    }
    assert result.statuses[WorkKey("work-app-failure")].present is True
    assert result.statuses[WorkKey("work-missing")].present is False


def test_bulk_terminal_statuses_filters_by_error_type(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_terminal_members(pg_engine, schema)

    matching = bulk_work_terminal_statuses(
        "campaign-terminal",
        ("work-app-failure", "work-abandonment"),
        engine=pg_engine,
        terminal_filter=TerminalSummaryFilter(
            error_type="builtins.RuntimeError",
        ),
    )
    non_matching = bulk_work_terminal_statuses(
        "campaign-terminal",
        ("work-app-failure",),
        engine=pg_engine,
        terminal_filter=TerminalSummaryFilter(
            error_type="builtins.ValueError",
        ),
    )

    assert set(matching.statuses) == {WorkKey("work-app-failure")}
    assert matching.statuses[WorkKey("work-app-failure")].present is True
    assert tuple(non_matching.statuses.items()) == ()


def test_bulk_terminal_statuses_populates_stage_execution_id_for_retry(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_terminal_members(pg_engine, schema)

    status = bulk_work_terminal_statuses(
        "campaign-terminal",
        ("work-app-failure",),
        engine=pg_engine,
    ).statuses[WorkKey("work-app-failure")]

    assert status.stage_execution_id is not None
    retry_stage(
        status.stage_execution_id,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=10),
    )
    refreshed = bulk_work_terminal_statuses(
        "campaign-terminal",
        ("work-app-failure",),
        engine=pg_engine,
    ).statuses[WorkKey("work-app-failure")]
    assert refreshed.state is StageExecutionState.READY


def test_bulk_terminal_statuses_uses_one_query_per_chunk(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    _seed_terminal_members(pg_engine, schema)
    work_keys = tuple(f"work-{index}" for index in range(6))
    registry = _registry()
    submit_items(
        campaign_key="campaign-terminal",
        run_key="run-chunk",
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key=work_key,
                input_reference=f"input:{work_key}",
                labels={},
            )
            for work_key in work_keys
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW + timedelta(seconds=3),
    )

    bulk_queries = 0

    def before_cursor_execute(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal bulk_queries
        normalized = " ".join(str(statement).split())
        if (
            "platform_work_items" in normalized
            and "terminal_summary" in normalized
        ):
            bulk_queries += 1

    event.listen(pg_engine, "before_cursor_execute", before_cursor_execute)
    try:
        result = bulk_work_terminal_statuses(
            "campaign-terminal",
            (*work_keys, "work-app-failure"),
            engine=pg_engine,
            chunk_size=3,
        )
    finally:
        event.remove(pg_engine, "before_cursor_execute", before_cursor_execute)

    assert len(result.statuses) == len(work_keys) + 1
    assert bulk_queries == 3


def test_bulk_terminal_statuses_reports_unadmitted_ready_work_as_present(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    registry = _registry()
    submit_items(
        campaign_key="campaign-ready",
        run_key="run-ready",
        pipeline=PipelineIdentity(PipelineKey("evaluation"), 1),
        execution_config_reference="config:1",
        items=(
            WorkInput(
                work_key="work-ready",
                input_reference="input:ready",
                labels={},
            ),
        ),
        registry=registry,
        engine=pg_engine,
        clock=lambda: NOW,
    )

    status = bulk_work_terminal_statuses(
        "campaign-ready",
        ("work-ready", "work-missing"),
        engine=pg_engine,
    ).statuses

    ready = status[WorkKey("work-ready")]
    missing = status[WorkKey("work-missing")]
    assert ready.present is True
    assert ready.state is StageExecutionState.READY
    assert ready.terminal_summary is None
    assert ready.evidence_reference is None
    assert missing.present is False
