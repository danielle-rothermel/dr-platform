from __future__ import annotations

import pytest
from sqlalchemy import Engine, select

from dr_platform._core.identities import StageKey
from dr_platform._core.ledger.schema import LedgerSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.inspection.barrier_join import resolve_barrier_join_cluster
from dr_platform.inspection.run_members import list_run_members
from dr_platform.inspection.work_items import get_work_item_stages
from dr_platform.runtime.database.migrate import upgrade_platform_schema
from dr_platform.testing import (
    FIXTURE_TIMESTAMP,
    seed_deferral_episode,
    seed_double_deferral_episode,
    seed_work_item,
    succeed_stage,
)
from tests.conftest import _migrate, engine_dsn


def test_seed_deferral_episode_indices_match_topology(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, optim_index, fanin_index = seed_deferral_episode(
            connection,
            row_count=2,
            schema=schema,
        )

    assert optim_index == 0
    assert fanin_index == 3
    assert work_item_id > 0


def test_seed_deferral_episode_rejects_nonpositive_row_count(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with (
        pg_engine.begin() as connection,
        pytest.raises(ValueError, match="row_count must be at least 1"),
    ):
        seed_deferral_episode(connection, row_count=0, schema=schema)


def test_seed_double_deferral_episode_returns_all_indices(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, o1, f1, o2, f2 = seed_double_deferral_episode(
            connection,
            schema=schema,
        )

    assert (work_item_id, o1, f1, o2, f2) == (work_item_id, 0, 3, 4, 7)


def test_seed_deferral_episode_satisfies_cluster_resolution(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, _optim_index, fanin_index = seed_deferral_episode(
            connection,
            schema=schema,
        )

    cluster = resolve_barrier_join_cluster(
        work_item_id,
        fanin_index,
        optim_step_stage_key=StageKey("optim_step"),
        eval_row_stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )
    assert [row.stage_index for row in cluster.eval_rows] == [1, 2]
    assert cluster.fanin.barrier is True


def test_seed_double_deferral_episode_satisfies_cluster_resolution(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, _o1, _f1, o2, f2 = seed_double_deferral_episode(
            connection,
            schema=schema,
        )

    cluster = resolve_barrier_join_cluster(
        work_item_id,
        f2,
        optim_step_stage_key=StageKey("optim_step"),
        eval_row_stage_key=StageKey("eval_row"),
        engine=pg_engine,
    )
    assert cluster.optim_step.stage_index == o2
    assert [row.stage_index for row in cluster.eval_rows] == [5, 6]


def test_succeed_stage_persists_succeeded_row_with_barrier_flag(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id, _, fanin_index = seed_deferral_episode(
            connection,
            row_count=1,
            schema=schema,
        )
        succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="extra",
            stage_index=fanin_index + 1,
            input_reference="extra:in",
            output_reference="extra:out",
            barrier=True,
        )

    with pg_engine.connect() as connection:
        row = (
            connection.execute(
                select(schema.stage_executions).where(
                    schema.stage_executions.c.stage_index == fanin_index + 1
                )
            )
            .mappings()
            .one()
        )
    assert row["state"] == StageExecutionState.SUCCEEDED.value
    assert row["output_reference"] == "extra:out"
    assert row["barrier"] is True


def test_seed_work_item_writes_run_membership(pg_engine: Engine) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = seed_work_item(
            connection,
            campaign_key="campaign-run-membership",
            work_key="work-run-membership",
            run_key="run-run-membership",
            schema=schema,
        )

    members = list_run_members(
        "run-run-membership",
        engine=pg_engine,
        schema=schema,
        limit=10,
    )
    assert len(members) == 1
    assert members[0].work_item_id == work_item_id
    assert members[0].member_ordinal == 0
    assert members[0].work_key.value == "work-run-membership"


def test_succeed_stage_records_terminal_attempt(
    pg_engine: Engine,
) -> None:
    schema = _migrate(pg_engine)
    with pg_engine.begin() as connection:
        work_item_id = seed_work_item(
            connection,
            campaign_key="campaign-terminal-attempt",
            work_key="work-terminal-attempt",
            run_key="run-terminal-attempt",
            schema=schema,
        )
        succeed_stage(
            connection,
            work_item_id=work_item_id,
            stage_key="execute",
            stage_index=0,
            input_reference="stage:in",
            output_reference="stage:out",
            schema=schema,
        )

    summary = get_work_item_stages(
        work_item_id, engine=pg_engine, schema=schema
    )
    attempt = summary[0].attempts[0]
    assert attempt.terminal_at == FIXTURE_TIMESTAMP
    assert attempt.terminal_reference == "stage:out"


def test_seed_deferral_episode_honors_custom_ledger_schema(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    prefix = "fixturetenant"
    schema = LedgerSchema(prefix)
    upgrade_platform_schema(engine_dsn(pg_engine), prefix=prefix)
    with pg_engine.begin() as connection:
        work_item_id, _optim_index, fanin_index = seed_deferral_episode(
            connection,
            campaign_key="campaign-custom-schema",
            work_key="work-custom-schema",
            run_key="run-custom-schema",
            schema=schema,
        )

    default_schema = LedgerSchema()
    with pg_engine.connect() as connection:
        default_count = connection.execute(
            select(default_schema.stage_executions.c.stage_execution_id)
            .select_from(default_schema.stage_executions)
            .where(
                default_schema.stage_executions.c.work_item_id == work_item_id
            )
        ).all()
        prefixed_count = connection.execute(
            select(schema.stage_executions.c.stage_execution_id)
            .select_from(schema.stage_executions)
            .where(schema.stage_executions.c.work_item_id == work_item_id)
        ).all()

    assert default_count == []
    assert len(prefixed_count) == 4
    resolve_barrier_join_cluster(
        work_item_id,
        fanin_index,
        optim_step_stage_key=StageKey("optim_step"),
        eval_row_stage_key=StageKey("eval_row"),
        engine=pg_engine,
        schema=schema,
    )
