from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

import pytest
from alembic import command
from sqlalchemy import (
    BigInteger,
    Boolean,
    Connection,
    DateTime,
    Engine,
    Integer,
    Text,
    bindparam,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.sql.type_api import TypeEngine

from dr_platform._core.ledger.attempts import (
    StageAttemptRecord,
    StageAttemptSequenceError,
    StageAttemptTerminalError,
    append_stage_attempt,
    list_stage_attempts,
    mark_stage_attempt_admitted,
    record_stage_attempt_terminal,
)
from dr_platform._core.ledger.executions import (
    StageExecutionRecord,
    StageTransitionError,
    get_stage_execution,
    insert_stage_execution,
    transition_stage_execution,
)
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import upsert_stage_control
from dr_platform.runtime.database.migrate import (
    PLATFORM_BASELINE_REVISION,
    PLATFORM_HEAD_REVISION,
    _alembic_config,
    upgrade_platform_schema,
)
from dr_platform.submission.runs import (
    PipelineRunConflictError,
    insert_pipeline_run,
)
from dr_platform.submission.work_items import insert_work_item
from tests.conftest import NOW, engine_dsn

STAGING_TABLE_SUFFIXES = (
    "pipeline_runs",
    "run_memberships",
    "run_completion_executions",
    "work_items",
    "stage_executions",
    "stage_attempts",
    "stage_controls",
)
STAGING_TABLES = {f"platform_{suffix}" for suffix in STAGING_TABLE_SUFFIXES}


@dataclass(frozen=True, slots=True)
class _ColumnInventory:
    name: str
    type_semantics: str
    nullable: bool
    default: str | None
    identity: tuple[bool, int, int, bool] | None


@dataclass(frozen=True, slots=True)
class _TableInventory:
    # Column order is semantic only for keys and indexes.
    columns: frozenset[_ColumnInventory]
    primary_key: tuple[str, ...]
    unique_constraints: frozenset[tuple[str, tuple[str, ...]]]
    check_constraints: frozenset[tuple[str, str]]
    foreign_keys: frozenset[
        tuple[
            str,
            tuple[str, ...],
            str,
            tuple[str, ...],
            tuple[tuple[str, str], ...],
        ]
    ]
    indexes: frozenset[
        tuple[
            str,
            tuple[str | None, ...],
            tuple[str, ...],
            bool,
            str,
            str | None,
            tuple[str, ...],
        ]
    ]


@dataclass(frozen=True, slots=True)
class _TriggerInventory:
    name: str
    table: str
    function: str
    timing: str
    orientation: str
    events: tuple[str, ...]


def _migrate(engine: Engine) -> None:
    upgrade_platform_schema(engine_dsn(engine))


def _type_semantics(type_: TypeEngine) -> str:
    if isinstance(type_, JSONB):
        return "jsonb"
    if isinstance(type_, BigInteger):
        return "bigint"
    if isinstance(type_, Integer):
        return "integer"
    if isinstance(type_, Text):
        return "text"
    if isinstance(type_, DateTime):
        timezone = "with" if type_.timezone else "without"
        return f"timestamp {timezone} time zone"
    if isinstance(type_, Boolean):
        return "boolean"
    raise AssertionError(f"unhandled reflected column type: {type_!r}")


def _normalize_name(name: str | None, prefix: str) -> str:
    assert name is not None
    return name.removeprefix(f"{prefix}_")


def _normalize_sql(sql: object, prefix: str) -> str:
    return " ".join(str(sql).split()).replace(f"{prefix}_", "<prefix>_")


def _column_inventory(column: ReflectedColumn) -> _ColumnInventory:
    identity = column.get("identity")
    identity_inventory = (
        None
        if identity is None
        else (
            bool(identity["always"]),
            int(identity["start"]),
            int(identity["increment"]),
            bool(identity["cycle"]),
        )
    )
    default = column.get("default")
    return _ColumnInventory(
        name=column["name"],
        type_semantics=_type_semantics(column["type"]),
        nullable=bool(column["nullable"]),
        default=None if default is None else str(default),
        identity=identity_inventory,
    )


def _table_inventory(
    inspector: Inspector,
    *,
    table: str,
    prefix: str,
) -> _TableInventory:
    primary_key = inspector.get_pk_constraint(table)
    unique_constraints = inspector.get_unique_constraints(table)
    check_constraints = inspector.get_check_constraints(table)
    foreign_keys = inspector.get_foreign_keys(table)
    indexes = inspector.get_indexes(table)

    return _TableInventory(
        columns=frozenset(
            _column_inventory(column)
            for column in inspector.get_columns(table)
        ),
        primary_key=tuple(primary_key["constrained_columns"]),
        unique_constraints=frozenset(
            (
                _normalize_name(constraint["name"], prefix),
                tuple(constraint["column_names"]),
            )
            for constraint in unique_constraints
        ),
        check_constraints=frozenset(
            (
                _normalize_name(constraint["name"], prefix),
                _normalize_sql(constraint["sqltext"], prefix),
            )
            for constraint in check_constraints
        ),
        foreign_keys=frozenset(
            (
                _normalize_name(constraint["name"], prefix),
                tuple(constraint["constrained_columns"]),
                _normalize_name(constraint["referred_table"], prefix),
                tuple(constraint["referred_columns"]),
                tuple(
                    sorted(
                        (str(key), str(value))
                        for key, value in constraint.get(
                            "options",
                            {},
                        ).items()
                    )
                ),
            )
            for constraint in foreign_keys
        ),
        indexes=frozenset(
            (
                _normalize_name(index["name"], prefix),
                tuple(index["column_names"]),
                tuple(
                    _normalize_sql(expression, prefix)
                    for expression in index.get("expressions", ())
                ),
                bool(index["unique"]),
                str(
                    index.get("dialect_options", {}).get(
                        "postgresql_using",
                        "btree",
                    )
                ),
                (
                    None
                    if index.get("dialect_options", {}).get("postgresql_where")
                    is None
                    else _normalize_sql(
                        index.get("dialect_options", {})["postgresql_where"],
                        prefix,
                    )
                ),
                tuple(index.get("include_columns", ())),
            )
            for index in indexes
            if index.get("duplicates_constraint") is None
        ),
    )


def _schema_inventory(
    engine: Engine,
    *,
    prefix: str,
) -> dict[str, _TableInventory]:
    inspector = inspect(engine)
    return {
        suffix: _table_inventory(
            inspector,
            table=f"{prefix}_{suffix}",
            prefix=prefix,
        )
        for suffix in STAGING_TABLE_SUFFIXES
    }


def _trigger_inventory(
    engine: Engine,
    *,
    prefix: str,
) -> frozenset[_TriggerInventory]:
    scoped_tables = tuple(
        f"{prefix}_{suffix}" for suffix in STAGING_TABLE_SUFFIXES
    )
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT
                    trigger.tgname AS trigger_name,
                    relation.relname AS table_name,
                    function.proname AS function_name,
                    trigger.tgtype
                FROM pg_catalog.pg_trigger AS trigger
                JOIN pg_catalog.pg_class AS relation
                    ON relation.oid = trigger.tgrelid
                JOIN pg_catalog.pg_namespace AS namespace
                    ON namespace.oid = relation.relnamespace
                JOIN pg_catalog.pg_proc AS function
                    ON function.oid = trigger.tgfoid
                WHERE NOT trigger.tgisinternal
                    AND namespace.nspname = current_schema()
                    AND relation.relname IN :scoped_tables
                """
            ).bindparams(bindparam("scoped_tables", expanding=True)),
            {"scoped_tables": scoped_tables},
        ).mappings()
        return frozenset(
            _TriggerInventory(
                name=_normalize_name(row["trigger_name"], prefix),
                table=_normalize_name(row["table_name"], prefix),
                function=_normalize_name(row["function_name"], prefix),
                timing=(
                    "instead"
                    if row["tgtype"] & 64
                    else "before"
                    if row["tgtype"] & 2
                    else "after"
                ),
                orientation="row" if row["tgtype"] & 1 else "statement",
                events=tuple(
                    event
                    for event, bit in (
                        ("insert", 4),
                        ("delete", 8),
                        ("update", 16),
                        ("truncate", 32),
                    )
                    if row["tgtype"] & bit
                ),
            )
            for row in rows
        )


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
        expected_member_count=0,
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


def _snapshot_recorded_ledger_rows(
    connection: Connection,
) -> tuple[object, ...]:
    return tuple(
        connection.execute(
            text(
                """
                SELECT
                    to_jsonb(pipeline_run),
                    to_jsonb(work_item),
                    to_jsonb(execution),
                    to_jsonb(attempt),
                    to_jsonb(control)
                FROM platform_pipeline_runs AS pipeline_run
                JOIN platform_work_items AS work_item
                    ON work_item.origin_run_key = pipeline_run.run_key
                JOIN platform_stage_executions AS execution
                    ON execution.work_item_id = work_item.work_item_id
                JOIN platform_stage_attempts AS attempt
                    ON attempt.stage_execution_id =
                        execution.stage_execution_id
                JOIN platform_stage_controls AS control
                    ON control.pipeline_key = pipeline_run.pipeline_key
                    AND control.pipeline_version =
                        pipeline_run.pipeline_version
                    AND control.stage_key = execution.stage_key
                """
            )
        ).one()
    )


def test_fresh_baseline_creates_only_the_staged_work_schema(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)

    tables = set(inspect(pg_engine).get_table_names())
    with pg_engine.connect() as connection:
        installed_revision = connection.execute(
            text("SELECT version_num FROM platform_platform_alembic_version")
        ).scalar_one()

    assert PLATFORM_BASELINE_REVISION == PLATFORM_HEAD_REVISION
    assert installed_revision == PLATFORM_HEAD_REVISION
    assert tables == STAGING_TABLES | {"platform_platform_alembic_version"}


def test_baseline_downgrade_is_irreversible(pg_engine: Engine) -> None:
    _migrate(pg_engine)
    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        append_stage_attempt(
            connection,
            stage_execution_id=execution.stage_execution_id,
            created_at=NOW,
        )
        upsert_stage_control(
            connection,
            pipeline_key="pipeline",
            pipeline_version=1,
            stage_key="execute",
            selector={"cohort": "blue"},
            capacity=1,
            paused=False,
            updated_at=NOW,
        )

    tables_before = set(inspect(pg_engine).get_table_names())
    with pg_engine.connect() as connection:
        installed_revision_before = connection.execute(
            text("SELECT version_num FROM platform_platform_alembic_version")
        ).scalar_one()
        seeded_rows_before = _snapshot_recorded_ledger_rows(connection)

    with pytest.raises(
        NotImplementedError,
        match="baseline migration is irreversible",
    ):
        command.downgrade(
            _alembic_config(engine_dsn(pg_engine), "platform"),
            "base",
        )

    assert set(inspect(pg_engine).get_table_names()) == tables_before
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT version_num FROM platform_platform_alembic_version"
                )
            ).scalar_one()
            == installed_revision_before
        )
        assert _snapshot_recorded_ledger_rows(connection) == seeded_rows_before


def test_custom_prefix_upgrade_matches_runtime_names_and_is_idempotent(
    pg_engine: Engine,
) -> None:
    prefix = "tenant"
    schema = StagingSchema(prefix)

    upgrade_platform_schema(engine_dsn(pg_engine), prefix=prefix)

    expected_tables = set(schema.metadata.tables)
    assert set(inspect(pg_engine).get_table_names()) == expected_tables | {
        "tenant_platform_alembic_version"
    }
    with pg_engine.connect() as connection:
        installed_revision = connection.execute(
            text("SELECT version_num FROM tenant_platform_alembic_version")
        ).scalar_one()
    assert installed_revision == PLATFORM_HEAD_REVISION

    first_inventory = _schema_inventory(pg_engine, prefix=prefix)
    first_triggers = _trigger_inventory(pg_engine, prefix=prefix)
    upgrade_platform_schema(engine_dsn(pg_engine), prefix=prefix)

    assert _schema_inventory(pg_engine, prefix=prefix) == first_inventory
    assert _trigger_inventory(pg_engine, prefix=prefix) == first_triggers
    assert set(inspect(pg_engine).get_table_names()) == expected_tables | {
        "tenant_platform_alembic_version"
    }
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT version_num FROM tenant_platform_alembic_version")
            ).scalar_one()
            == PLATFORM_HEAD_REVISION
        )


def test_migration_and_runtime_schema_inventories_match(
    pg_engine: Engine,
) -> None:
    migrated_prefix = "migrated"
    runtime_prefix = "runtime"
    upgrade_platform_schema(
        engine_dsn(pg_engine),
        prefix=migrated_prefix,
    )
    StagingSchema(runtime_prefix).metadata.create_all(pg_engine)

    assert _schema_inventory(
        pg_engine,
        prefix=migrated_prefix,
    ) == _schema_inventory(pg_engine, prefix=runtime_prefix)
    assert _trigger_inventory(
        pg_engine,
        prefix=migrated_prefix,
    ) == frozenset(
        {
            _TriggerInventory(
                name="guard_pipeline_run_provenance",
                table="pipeline_runs",
                function="guard_pipeline_run_provenance",
                timing="before",
                orientation="row",
                events=("update",),
            ),
            _TriggerInventory(
                name="guard_closed_run_membership",
                table="run_memberships",
                function="guard_closed_run_membership",
                timing="before",
                orientation="row",
                events=("insert", "delete", "update"),
            ),
        }
    )
    assert _trigger_inventory(pg_engine, prefix=runtime_prefix) == frozenset()


def test_upgrade_rejects_conflicting_table_without_destroying_data(
    pg_engine: Engine,
) -> None:
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE platform_pipeline_runs (
                    marker TEXT PRIMARY KEY
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO platform_pipeline_runs (marker)
                VALUES ('preexisting')
                """
            )
        )
    tables_before = set(inspect(pg_engine).get_table_names())

    with pytest.raises(ProgrammingError):
        _migrate(pg_engine)

    assert set(inspect(pg_engine).get_table_names()) == tables_before
    with pg_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT marker FROM platform_pipeline_runs")
            ).scalar_one()
            == "preexisting"
        )


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
                expected_member_count=0,
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


def test_pipeline_run_allows_one_registration_closure_update(
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
        stored_closed_at = connection.execute(
            schema.pipeline_runs.update()
            .where(schema.pipeline_runs.c.run_key == "run-1")
            .values(
                registration_closed_at=completed_at,
                registered_member_count=0,
                created_work_count=0,
                reused_work_count=0,
            )
            .returning(schema.pipeline_runs.c.registration_closed_at)
        ).scalar_one()

    assert stored_closed_at == completed_at


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


def test_stage_attempt_lifecycle_fills_one_row_once(
    pg_engine: Engine,
) -> None:
    _migrate(pg_engine)
    admitted_at = NOW + timedelta(seconds=1)
    terminal_at = NOW + timedelta(seconds=2)

    with pg_engine.begin() as connection:
        execution = _create_stage_execution(connection)
        pending = append_stage_attempt(
            connection,
            stage_execution_id=execution.stage_execution_id,
            created_at=NOW,
        )
        admitted = mark_stage_attempt_admitted(
            connection,
            stage_execution_id=execution.stage_execution_id,
            attempt_number=pending.attempt_number,
            admitted_at=admitted_at,
        )
        terminal = record_stage_attempt_terminal(
            connection,
            stage_execution_id=execution.stage_execution_id,
            attempt_number=pending.attempt_number,
            terminal_at=terminal_at,
            terminal_summary={"outcome": "failed"},
            terminal_reference="builtins.RuntimeError",
        )
        attempts = list_stage_attempts(
            connection,
            stage_execution_id=execution.stage_execution_id,
        )

        with pytest.raises(StageAttemptSequenceError):
            mark_stage_attempt_admitted(
                connection,
                stage_execution_id=execution.stage_execution_id,
                attempt_number=pending.attempt_number,
                admitted_at=terminal_at,
            )
        with pytest.raises(StageAttemptTerminalError):
            record_stage_attempt_terminal(
                connection,
                stage_execution_id=execution.stage_execution_id,
                attempt_number=pending.attempt_number,
                terminal_at=terminal_at,
                terminal_summary={"outcome": "changed"},
            )

    def identity(record: StageAttemptRecord) -> tuple[object, ...]:
        return (
            record.stage_attempt_id,
            record.stage_execution_id,
            record.attempt_number,
            record.workflow_id,
            record.created_at,
        )

    assert identity(pending) == identity(admitted) == identity(terminal)
    assert pending.admitted_at is None
    assert pending.terminal_at is None
    assert admitted.admitted_at == admitted_at
    assert admitted.terminal_at is None
    assert terminal.admitted_at == admitted_at
    assert terminal.terminal_at == terminal_at
    assert terminal.terminal_summary == {"outcome": "failed"}
    assert terminal.terminal_reference == "builtins.RuntimeError"
    assert attempts == (terminal,)


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
