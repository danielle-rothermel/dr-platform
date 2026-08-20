from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dr_platform._core.ledger.attempts import (
    append_stage_attempt,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform._core.ledger.terminal_summary import (
    build_terminal_outcome_summary,
)

if TYPE_CHECKING:
    from sqlalchemy import Connection

FIXTURE_TIMESTAMP = datetime(2026, 7, 17, 12, tzinfo=UTC)


def seed_work_item(  # noqa: PLR0913 -- explicit seed facts
    connection: Connection,
    *,
    campaign_key: str,
    work_key: str,
    run_key: str,
    input_reference: str = "seed",
    schema: LedgerSchema | None = None,
) -> int:
    """Insert pipeline run and work item rows; return ``work_item_id``."""
    selected_schema = schema or LedgerSchema()
    connection.execute(
        selected_schema.pipeline_runs.insert().values(
            run_key=run_key,
            campaign_key=campaign_key,
            pipeline_key="evaluation",
            pipeline_version=1,
            execution_config_reference="config:1",
            expected_member_count=1,
            created_at=FIXTURE_TIMESTAMP,
        )
    )
    return connection.execute(
        selected_schema.work_items.insert()
        .values(
            campaign_key=campaign_key,
            work_key=work_key,
            origin_run_key=run_key,
            input_reference=input_reference,
            labels={},
            rank=1,
        )
        .returning(selected_schema.work_items.c.work_item_id)
    ).scalar_one()


def succeed_stage(  # noqa: PLR0913 -- explicit ledger seed facts
    connection: Connection,
    *,
    work_item_id: int,
    stage_key: str,
    stage_index: int,
    input_reference: str,
    output_reference: str,
    barrier: bool = False,
    schema: LedgerSchema | None = None,
) -> None:
    """Insert one stage execution and drive it to ``SUCCEEDED``."""
    selected_schema = schema or LedgerSchema()
    execution = insert_stage_execution(
        connection,
        work_item_id=work_item_id,
        stage_key=stage_key,
        stage_index=stage_index,
        input_reference=input_reference,
        barrier=barrier,
        created_at=FIXTURE_TIMESTAMP,
        schema=selected_schema,
    )
    append_stage_attempt(
        connection,
        stage_execution_id=execution.stage_execution_id,
        created_at=FIXTURE_TIMESTAMP,
        admitted_at=FIXTURE_TIMESTAMP,
        schema=selected_schema,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=execution.stage_execution_id,
        new_state=StageExecutionState.ADMITTED,
        updated_at=FIXTURE_TIMESTAMP,
        schema=selected_schema,
    )
    transition_stage_execution(
        connection,
        stage_execution_id=execution.stage_execution_id,
        new_state=StageExecutionState.SUCCEEDED,
        output_reference=output_reference,
        updated_at=FIXTURE_TIMESTAMP,
        schema=selected_schema,
    )
    record_stage_attempt_terminal(
        connection,
        stage_execution_id=execution.stage_execution_id,
        attempt_number=1,
        terminal_at=FIXTURE_TIMESTAMP,
        terminal_summary=build_terminal_outcome_summary(
            outcome=StageExecutionState.SUCCEEDED.value,
        ),
        terminal_reference=output_reference,
        schema=selected_schema,
    )


def seed_deferral_episode(  # noqa: PLR0913 -- explicit episode topology
    connection: Connection,
    *,
    row_count: int = 2,
    optim_step_key: str = "optim_step",
    eval_row_key: str = "eval_row",
    fanin_key: str = "eval_fanin",
    campaign_key: str = "campaign-deferral-episode",
    work_key: str = "work-deferral-episode",
    run_key: str = "run-deferral-episode",
    schema: LedgerSchema | None = None,
) -> tuple[int, int, int]:
    """Seed one succeeded deferral episode; return ids and indices."""
    if row_count < 1:
        raise ValueError("row_count must be at least 1")
    work_item_id = seed_work_item(
        connection,
        campaign_key=campaign_key,
        work_key=work_key,
        run_key=run_key,
        schema=schema,
    )
    optim_stage_index = 0
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=optim_step_key,
        stage_index=optim_stage_index,
        input_reference="optim:in:0",
        output_reference="optim:out:0",
        schema=schema,
    )
    for offset in range(1, row_count + 1):
        succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key=eval_row_key,
            stage_index=offset,
            input_reference=f"row:in:{offset}",
            output_reference=f"row:out:{offset}",
            schema=schema,
        )
    fanin_stage_index = row_count + 1
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=fanin_key,
        stage_index=fanin_stage_index,
        input_reference="fanin:in:1",
        output_reference="fanin:out:1",
        barrier=True,
        schema=schema,
    )
    return work_item_id, optim_stage_index, fanin_stage_index


def seed_double_deferral_episode(  # noqa: PLR0913 -- explicit episode topology
    connection: Connection,
    *,
    optim_step_key: str = "optim_step",
    eval_row_key: str = "eval_row",
    fanin_key: str = "eval_fanin",
    campaign_key: str = "campaign-double-deferral",
    work_key: str = "work-double-deferral",
    run_key: str = "run-double-deferral",
    schema: LedgerSchema | None = None,
) -> tuple[int, int, int, int, int]:
    """Seed two back-to-back episodes on one work item."""
    work_item_id = seed_work_item(
        connection,
        campaign_key=campaign_key,
        work_key=work_key,
        run_key=run_key,
        schema=schema,
    )

    o1 = 0
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=optim_step_key,
        stage_index=o1,
        input_reference="optim:in:0",
        output_reference="optim:out:0",
        schema=schema,
    )
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=eval_row_key,
        stage_index=1,
        input_reference="row:in:1",
        output_reference="row:out:1",
        schema=schema,
    )
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=eval_row_key,
        stage_index=2,
        input_reference="row:in:2",
        output_reference="row:out:2",
        schema=schema,
    )
    f1 = 3
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=fanin_key,
        stage_index=f1,
        input_reference="fanin:in:1",
        output_reference="fanin:out:1",
        barrier=True,
        schema=schema,
    )

    o2 = 4
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=optim_step_key,
        stage_index=o2,
        input_reference="optim:in:4",
        output_reference="optim:out:4",
        schema=schema,
    )
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=eval_row_key,
        stage_index=5,
        input_reference="row:in:5",
        output_reference="row:out:5",
        schema=schema,
    )
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=eval_row_key,
        stage_index=6,
        input_reference="row:in:6",
        output_reference="row:out:6",
        schema=schema,
    )
    f2 = 7
    succeed_stage(
        connection,
        work_item_id=work_item_id,
        stage_key=fanin_key,
        stage_index=f2,
        input_reference="fanin:in:2",
        output_reference="fanin:out:2",
        barrier=True,
        schema=schema,
    )
    return work_item_id, o1, f1, o2, f2
