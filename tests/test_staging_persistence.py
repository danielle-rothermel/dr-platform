"""PostgreSQL guarantees for the staged replacement persistence model."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from sqlalchemy import Connection, Engine, inspect, text
from sqlalchemy.exc import IntegrityError

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging.runs import (
    PipelineRunConflictError,
    insert_pipeline_run,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_attempts import (
    append_stage_attempt,
    list_stage_attempts,
)
from dr_platform.staging.stage_executions import (
    StageTransitionError,
    get_stage_execution,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform.staging.states import StageExecutionState
from dr_platform.staging.work_items import insert_work_item
from tests.conftest import engine_dsn

if TYPE_CHECKING:
    from dr_platform.staging.records import StageExecutionRecord

LEGACY_TABLES = {
    "platform_operations",
    "platform_throttle_state",
    "platform_items",
    "platform_item_attempts",
    "platform_enqueue_claims",
    "platform_missing_reobservations",
    "platform_next_attempt_requests",
    "platform_enqueue_compensations",
    "platform_enqueue_compensation_hazards",
}
STAGING_TABLES = {
    "platform_pipeline_runs",
    "platform_work_items",
    "platform_stage_executions",
    "platform_stage_attempts",
    "platform_stage_controls",
}
NOW = datetime(2026, 7, 17, 12, tzinfo=UTC)


def _migrate(engine: Engine) -> None:
    upgrade_platform_schema(engine_dsn(engine))


def _create_stage_execution(
    connection: Connection,
) -> StageExecutionRecord:
    insert_pipeline_run(
        connection,
        run_key="run-1",
        campaign_key="campaign-1",
        pipeline_key="pipeline",
        pipeline_version=1,
        execution_config_reference="config:1",
        created_at=NOW,
    )
    work_item = insert_work_item(
        connection,
        campaign_key="campaign-1",
        work_key="work-1",
        origin_run_key="run-1",
        input_reference="input:1",
        labels={"cohort": "blue"},
    )
    return insert_stage_execution(
        connection,
        work_item_id=work_item.work_item_id,
        stage_key="execute",
        stage_index=0,
        created_at=NOW,
    )


def test_staging_migration_applies_after_the_legacy_chain(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    tables = set(inspect(pg_engine).get_table_names())

    assert tables >= LEGACY_TABLES | STAGING_TABLES


def test_campaign_work_identity_is_unique(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    schema = StagingSchema()
    with pg_engine.begin() as connection:
        connection.execute(
            schema.pipeline_runs.insert().values(
                run_key="run-1",
                campaign_key="campaign-1",
                pipeline_key="pipeline",
                pipeline_version=1,
                execution_config_reference="config:1",
                created_at=NOW,
            )
        )
        connection.execute(
            schema.work_items.insert().values(
                campaign_key="campaign-1",
                work_key="work-1",
                origin_run_key="run-1",
                input_reference="input:1",
                labels={},
                rank=1,
            )
        )

    with (
        pytest.raises(IntegrityError),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            schema.work_items.insert().values(
                campaign_key="campaign-1",
                work_key="work-1",
                origin_run_key="run-1",
                input_reference="input:other",
                labels={"cohort": "other"},
                rank=2,
            )
        )


def test_run_reuse_requires_identical_immutable_provenance(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        first = insert_pipeline_run(
            connection,
            run_key="run-1",
            campaign_key="campaign-1",
            pipeline_key="pipeline",
            pipeline_version=1,
            execution_config_reference="config:1",
            created_at=NOW,
        )
        replayed = insert_pipeline_run(
            connection,
            run_key="run-1",
            campaign_key="campaign-1",
            pipeline_key="pipeline",
            pipeline_version=1,
            execution_config_reference="config:1",
            created_at=NOW + timedelta(seconds=1),
        )

        with pytest.raises(PipelineRunConflictError):
            insert_pipeline_run(
                connection,
                run_key="run-1",
                campaign_key="campaign-1",
                pipeline_key="other-pipeline",
                pipeline_version=2,
                execution_config_reference="config:2",
                created_at=NOW + timedelta(seconds=2),
            )

    assert replayed == first


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("run_key", "run-other"),
        ("campaign_key", "campaign-other"),
        ("pipeline_key", "pipeline-other"),
        ("pipeline_version", 2),
        ("execution_config_reference", "config:other"),
        ("created_at", NOW + timedelta(seconds=1)),
    ],
)
def test_pipeline_run_provenance_rejects_direct_updates(
    pg_engine: Engine,
    field: str,
    changed_value: object,
) -> None:
    _migrate(pg_engine)
    schema = StagingSchema()
    with pg_engine.begin() as connection:
        insert_pipeline_run(
            connection,
            run_key="run-1",
            campaign_key="campaign-1",
            pipeline_key="pipeline",
            pipeline_version=1,
            execution_config_reference="config:1",
            created_at=NOW,
        )

    with (
        pytest.raises(
            IntegrityError,
            match="pipeline run provenance is immutable",
        ),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            schema.pipeline_runs.update()
            .where(schema.pipeline_runs.c.run_key == "run-1")
            .values({field: changed_value})
        )


def test_pipeline_run_allows_submission_completion_update(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    schema = StagingSchema()
    completed_at = NOW + timedelta(seconds=1)
    with pg_engine.begin() as connection:
        insert_pipeline_run(
            connection,
            run_key="run-1",
            campaign_key="campaign-1",
            pipeline_key="pipeline",
            pipeline_version=1,
            execution_config_reference="config:1",
            created_at=NOW,
        )
        stored_completed_at = connection.execute(
            schema.pipeline_runs.update()
            .where(schema.pipeline_runs.c.run_key == "run-1")
            .values(submission_completed_at=completed_at)
            .returning(schema.pipeline_runs.c.submission_completed_at)
        ).scalar_one()

    assert stored_completed_at == completed_at


def test_stage_execution_transitions_reject_terminal_reentry(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        admitted = transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW + timedelta(seconds=1),
        )
        succeeded = transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.SUCCEEDED,
            updated_at=NOW + timedelta(seconds=2),
            output_reference="output:1",
        )

        with pytest.raises(StageTransitionError):
            transition_stage_execution(
                connection,
                stage_execution_id=execution.stage_execution_id,
                new_state=StageExecutionState.ADMITTED,
                updated_at=NOW + timedelta(seconds=3),
            )

    assert (admitted.state, succeeded.state) == (
        StageExecutionState.ADMITTED,
        StageExecutionState.SUCCEEDED,
    )


def test_stage_execution_transition_rejects_stale_updated_at(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    latest = NOW + timedelta(seconds=2)
    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=latest,
        )

        with pytest.raises(
            ValueError,
            match="stage execution updated_at cannot move backwards",
        ):
            transition_stage_execution(
                connection,
                stage_execution_id=execution.stage_execution_id,
                new_state=StageExecutionState.SUCCEEDED,
                updated_at=NOW + timedelta(seconds=1),
                output_reference="output:1",
            )

        unchanged = get_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
        )

    assert unchanged is not None
    assert unchanged.state is StageExecutionState.ADMITTED
    assert unchanged.updated_at == latest


def test_stage_attempt_append_rejects_stale_created_at(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    latest = NOW + timedelta(seconds=2)
    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=latest,
        )

        with pytest.raises(
            ValueError,
            match="stage attempt created_at cannot precede",
        ):
            append_stage_attempt(
                connection,
                stage_execution_id=execution.stage_execution_id,
                created_at=NOW + timedelta(seconds=1),
            )

        unchanged = get_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
        )
        attempts = list_stage_attempts(
            connection,
            stage_execution_id=execution.stage_execution_id,
        )

    assert unchanged is not None
    assert unchanged.current_attempt == 0
    assert unchanged.updated_at == latest
    assert attempts == ()


def test_stage_attempt_terminal_summary_is_recursively_immutable(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        attempt = append_stage_attempt(
            connection,
            stage_execution_id=execution.stage_execution_id,
            created_at=NOW,
            terminal_summary={
                "result": {
                    "events": [{"outcome": "succeeded"}],
                }
            },
        )

    summary = attempt.terminal_summary
    assert summary is not None
    result = summary["result"]
    assert isinstance(result, Mapping)
    events = cast("Mapping[str, object]", result)["events"]
    assert isinstance(events, tuple)
    event = events[0]
    assert isinstance(event, Mapping)

    with pytest.raises(TypeError):
        cast("dict[str, object]", result)["changed"] = True
    with pytest.raises(TypeError):
        cast("dict[str, object]", event)["outcome"] = "changed"


def test_output_reference_is_required_only_for_success_and_preserved(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    schema = StagingSchema()
    with pg_engine.begin() as connection:
        insert_pipeline_run(
            connection,
            run_key="run-output-semantics",
            campaign_key="campaign-1",
            pipeline_key="pipeline",
            pipeline_version=1,
            execution_config_reference="config:1",
            created_at=NOW,
        )
        work_item = insert_work_item(
            connection,
            campaign_key="campaign-1",
            work_key="work-output-semantics",
            origin_run_key="run-output-semantics",
            input_reference="input:1",
            labels={},
        )
        execution = insert_stage_execution(
            connection,
            work_item_id=work_item.work_item_id,
            stage_key="execute",
            stage_index=0,
            created_at=NOW,
        )
        connection.execute(
            schema.stage_executions.update()
            .where(
                schema.stage_executions.c.stage_execution_id
                == execution.stage_execution_id
            )
            .values(output_reference="output:preserved")
        )

        with pytest.raises(ValueError, match="SUCCEEDED transition requires"):
            transition_stage_execution(
                connection,
                stage_execution_id=execution.stage_execution_id,
                new_state=StageExecutionState.SUCCEEDED,
                updated_at=NOW + timedelta(seconds=1),
            )
        with pytest.raises(ValueError, match="only valid for a SUCCEEDED"):
            transition_stage_execution(
                connection,
                stage_execution_id=execution.stage_execution_id,
                new_state=StageExecutionState.ADMITTED,
                updated_at=NOW + timedelta(seconds=1),
                output_reference="output:not-allowed",
            )

        admitted = transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.ADMITTED,
            updated_at=NOW + timedelta(seconds=1),
        )
        failed = transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.FAILED,
            updated_at=NOW + timedelta(seconds=2),
        )
        ready = transition_stage_execution(
            connection,
            stage_execution_id=execution.stage_execution_id,
            new_state=StageExecutionState.READY,
            updated_at=NOW + timedelta(seconds=3),
        )

    assert admitted.output_reference == "output:preserved"
    assert failed.output_reference == "output:preserved"
    assert ready.output_reference == "output:preserved"


def test_partial_ready_admission_index_exists(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    with pg_engine.connect() as connection:
        index_matches = connection.execute(
            text(
                """
                SELECT indexdef LIKE
                    '% USING btree (stage_key, rank) '
                    || 'WHERE (state = ''ready''::text)%'
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                    'platform_ix_stage_executions_ready_admission'
                """
            )
        ).scalar_one()

    assert index_matches is True
