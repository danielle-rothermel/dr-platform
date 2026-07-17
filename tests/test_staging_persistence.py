"""PostgreSQL guarantees for the staged replacement persistence model."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import IntegrityError

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.staging.runs import (
    PipelineRunConflictError,
    insert_pipeline_run,
)
from dr_platform.staging.schema import StagingSchema
from dr_platform.staging.stage_executions import (
    StageTransitionError,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform.staging.states import StageExecutionState
from dr_platform.staging.work_items import insert_work_item
from tests.conftest import engine_dsn

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


def test_stage_execution_transitions_reject_terminal_reentry(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
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
        work_item = insert_work_item(
            connection,
            campaign_key="campaign-1",
            work_key="work-1",
            origin_run_key="run-1",
            input_reference="input:1",
            labels={"cohort": "blue"},
        )
        execution = insert_stage_execution(
            connection,
            work_item_id=work_item.work_item_id,
            stage_key="execute",
            stage_index=0,
            created_at=NOW,
        )
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
