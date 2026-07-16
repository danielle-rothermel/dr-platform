"""Final P1 fixed schema and fresh-lineage contracts."""

from __future__ import annotations

import inspect
from typing import Any, cast

from sqlalchemy import CheckConstraint, Engine, text
from sqlalchemy import inspect as inspect_database

import dr_platform
from tests.conftest import engine_dsn

PREFIX = "p1_contract"
FINAL_TABLE_SUFFIXES = frozenset(
    {
        "operations",
        "items",
        "item_attempts",
        "next_attempt_requests",
        "enqueue_claims",
        "enqueue_compensations",
        "enqueue_compensation_hazards",
        "missing_reobservations",
        "throttle_state",
    }
)
CHANGE_SEQUENCE_SUFFIXES = FINAL_TABLE_SUFFIXES
LIFECYCLE_LEDGER_SUFFIXES = FINAL_TABLE_SUFFIXES

EXPECTED_COLUMN_SUBSETS: dict[str, frozenset[str]] = {
    "operations": frozenset(
        {
            "operation_key",
            "group_key",
            "workflow_role",
            "status",
            "requested_count",
            "registration_page_size",
            "registration_page_count",
            "target_key",
            "target_version",
            "target_contract_digest",
            "platform_cut_version",
            "registration_cursor",
            "registration_lease_id",
            "registration_lease_expires_at",
            "registration_completed_at",
            "retry_policy",
            "spec",
            "metadata",
            "change_seq",
        }
    ),
    "items": frozenset(
        {
            "item_id",
            "operation_key",
            "item_index",
            "item_key",
            "shuffle_rank",
            "service_class",
            "service_priority",
            "spec",
            "insert_status",
            "current_attempt",
            "change_seq",
        }
    ),
    "item_attempts": frozenset(
        {
            "item_id",
            "attempt",
            "workflow_role",
            "execution_key",
            "workflow_id",
            "execution_recipe_digest",
            "enqueue_state",
            "enqueue_try",
            "execution_state",
            "current_claim_id",
            "source_attempt",
            "source_workflow_id",
            "next_attempt_request_id",
            "change_seq",
        }
    ),
    "next_attempt_requests": frozenset(
        {
            "request_id",
            "item_id",
            "request_key",
            "source_attempt",
            "reason",
            "requested_by",
            "max_attempts",
            "effective_max_attempts",
            "disposition",
            "created_attempt",
            "change_seq",
        }
    ),
    "enqueue_claims": frozenset(
        {
            "item_id",
            "attempt",
            "claim_id",
            "workflow_id",
            "enqueue_try",
            "claimed_at",
            "lease_expires_at",
            "enqueue_call_started_at",
            "disposition",
            "replacement_claim_id",
            "change_seq",
        }
    ),
    "enqueue_compensations": frozenset(
        {
            "item_id",
            "attempt",
            "claim_id",
            "workflow_id",
            "reason",
            "cancel_disposition",
            "created_at",
            "resolved_at",
            "first_absent_at",
            "last_absent_at",
            "absence_observation_count",
            "change_seq",
        }
    ),
    "enqueue_compensation_hazards": frozenset(
        {
            "item_id",
            "attempt",
            "claim_id",
            "hazard_seq",
            "workflow_id",
            "cancel_disposition",
            "created_at",
            "resolved_at",
            "change_seq",
        }
    ),
    "missing_reobservations": frozenset(
        {
            "item_id",
            "attempt",
            "last_reobserved_at",
            "observation_count",
            "created_at",
            "change_seq",
        }
    ),
    "throttle_state": frozenset(
        {
            "throttle_key",
            "blocked_until",
            "hold_until",
            "tags",
            "change_seq",
        }
    ),
}

FORBIDDEN_COLUMNS = frozenset(
    {
        "batch_submit_item_id",
        "order_key",
        "fair_order_key",
        "enqueue_status",
        "enqueue_metadata",
    }
)


def _schema(prefix: str = PREFIX) -> Any:
    schema_type = getattr(dr_platform, "PlatformSchema", None)
    assert schema_type is not None, "dr_platform must export PlatformSchema"
    return schema_type(prefix=prefix)


def _upgrade(database_url: str) -> None:
    upgrade = cast("Any", dr_platform.upgrade_platform_schema)
    upgrade(database_url, prefix=PREFIX)


def _table_names(prefix: str = PREFIX) -> set[str]:
    return {f"{prefix}_{suffix}" for suffix in FINAL_TABLE_SUFFIXES}


def _table(schema: Any, suffix: str) -> Any:
    return schema.metadata.tables[f"{schema.prefix}_{suffix}"]


def _constraint_columns(constraint: Any) -> tuple[str, ...]:
    return tuple(column.name for column in constraint.columns)


def test_platform_schema_has_final_tables_and_columns() -> None:
    schema = _schema()

    assert schema.prefix == PREFIX
    assert set(schema.metadata.tables) == _table_names()
    for suffix, expected_columns in EXPECTED_COLUMN_SUBSETS.items():
        columns = set(_table(schema, suffix).columns.keys())
        assert expected_columns <= columns, suffix
        assert FORBIDDEN_COLUMNS.isdisjoint(columns), suffix


def test_item_current_attempt_has_deferred_composite_foreign_key() -> None:
    schema = _schema()
    item_table = _table(schema, "items")
    foreign_keys = tuple(item_table.foreign_key_constraints)
    current_attempt_fk = next(
        constraint
        for constraint in foreign_keys
        if _constraint_columns(constraint) == ("item_id", "current_attempt")
    )

    assert current_attempt_fk.deferrable is True
    assert current_attempt_fk.initially == "DEFERRED"
    assert tuple(
        element.target_fullname for element in current_attempt_fk.elements
    ) == (
        f"{PREFIX}_item_attempts.item_id",
        f"{PREFIX}_item_attempts.attempt",
    )


def test_item_and_request_idempotency_keys_are_unique() -> None:
    schema = _schema()
    items = _table(schema, "items")
    requests = _table(schema, "next_attempt_requests")

    item_unique_sets = {
        frozenset(_constraint_columns(constraint))
        for constraint in items.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    request_unique_sets = {
        frozenset(_constraint_columns(constraint))
        for constraint in requests.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert frozenset({"operation_key", "item_index"}) in item_unique_sets
    assert frozenset({"operation_key", "item_key"}) in item_unique_sets
    assert frozenset({"item_id", "request_key"}) in request_unique_sets


def test_claim_and_compensation_keys_are_exact() -> None:
    schema = _schema()
    claims = _table(schema, "enqueue_claims")
    compensations = _table(schema, "enqueue_compensations")

    assert tuple(column.name for column in claims.primary_key.columns) == (
        "item_id",
        "attempt",
        "claim_id",
    )
    compensation_primary_key = tuple(
        column.name for column in compensations.primary_key.columns
    )
    assert compensation_primary_key == ("item_id", "attempt", "claim_id")
    claim_unique_sets = {
        tuple(_constraint_columns(constraint))
        for constraint in claims.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert (
        "item_id",
        "attempt",
        "claim_id",
        "workflow_id",
    ) in claim_unique_sets
    claim_foreign_key = next(
        constraint
        for constraint in compensations.foreign_key_constraints
        if _constraint_columns(constraint)
        == ("item_id", "attempt", "claim_id", "workflow_id")
        and tuple(element.target_fullname for element in constraint.elements)
        == (
            f"{PREFIX}_enqueue_claims.item_id",
            f"{PREFIX}_enqueue_claims.attempt",
            f"{PREFIX}_enqueue_claims.claim_id",
            f"{PREFIX}_enqueue_claims.workflow_id",
        )
    )
    assert claim_foreign_key is not None


def test_missing_reobservation_fact_is_attempt_scoped_and_indexed() -> None:
    schema = _schema()
    markers = _table(schema, "missing_reobservations")

    assert tuple(column.name for column in markers.primary_key.columns) == (
        "item_id",
        "attempt",
    )
    foreign_key = next(iter(markers.foreign_key_constraints))
    assert tuple(
        element.target_fullname for element in foreign_key.elements
    ) == (
        f"{PREFIX}_item_attempts.item_id",
        f"{PREFIX}_item_attempts.attempt",
    )
    assert foreign_key.ondelete == "RESTRICT"
    index_columns = {
        tuple(column.name for column in index.columns)
        for index in markers.indexes
    }
    assert ("last_reobserved_at", "item_id", "attempt") in index_columns


def test_shared_execution_identity_is_not_unique_per_attempt() -> None:
    schema = _schema()
    attempts = _table(schema, "item_attempts")
    unique_column_sets = {
        frozenset(_constraint_columns(constraint))
        for constraint in attempts.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }

    assert frozenset({"workflow_id"}) not in unique_column_sets
    assert frozenset({"execution_key"}) not in unique_column_sets


def test_schema_enum_checks_use_the_closed_values() -> None:
    from dr_platform import status

    schema = _schema()
    enum_columns = (
        ("operations", "status", status.OperationStatus),
        ("items", "insert_status", status.ItemInsertStatus),
        ("item_attempts", "enqueue_state", status.AttemptEnqueueState),
        ("item_attempts", "execution_state", status.AttemptExecutionState),
        ("enqueue_claims", "disposition", status.EnqueueClaimDisposition),
        (
            "enqueue_compensations",
            "cancel_disposition",
            status.EnqueueCompensationDisposition,
        ),
        (
            "enqueue_compensation_hazards",
            "cancel_disposition",
            status.EnqueueCompensationDisposition,
        ),
    )

    for suffix, column_name, enum_type in enum_columns:
        check_sql = "\n".join(
            str(constraint.sqltext)
            for constraint in _table(schema, suffix).constraints
            if isinstance(constraint, CheckConstraint)
            and column_name in str(constraint.sqltext)
        )
        assert check_sql, f"{suffix}.{column_name} needs a check constraint"
        for value in enum_type:
            assert f"'{value.value}'" in check_sql


def test_every_change_tracked_table_has_change_sequence_first_index() -> None:
    schema = _schema()

    for suffix in CHANGE_SEQUENCE_SUFFIXES:
        table = _table(schema, suffix)
        assert any(
            next(iter(index.columns)).name == "change_seq"
            for index in table.indexes
        ), suffix


def test_migration_api_exposes_upgrade_without_stamping() -> None:
    from dr_platform.db import migrate

    assert not hasattr(migrate, "stamp_platform_schema")
    assert not hasattr(dr_platform, "stamp_platform_schema")
    upgrade_parameters = inspect.signature(
        dr_platform.upgrade_platform_schema
    ).parameters
    assert "prefix" in upgrade_parameters
    assert "naming" not in upgrade_parameters


def test_fresh_upgrade_creates_only_final_tables(pg_engine: Engine) -> None:
    _upgrade(engine_dsn(pg_engine))
    database_inspector = inspect_database(pg_engine)
    application_tables = {
        name
        for name in database_inspector.get_table_names()
        if name.startswith(f"{PREFIX}_")
    }

    assert application_tables == _table_names() | {
        f"{PREFIX}_platform_alembic_version"
    }


def test_fresh_upgrade_installs_change_sequence_ownership(
    pg_engine: Engine,
) -> None:
    _upgrade(engine_dsn(pg_engine))
    expected_tables = {
        f"{PREFIX}_{suffix}" for suffix in CHANGE_SEQUENCE_SUFFIXES
    }

    with pg_engine.connect() as connection:
        sequences = connection.execute(
            text(
                """
                SELECT sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema = current_schema()
                  AND sequence_name LIKE :prefix
                """
            ),
            {"prefix": f"{PREFIX}%change%seq%"},
        ).scalars()
        trigger_tables = connection.execute(
            text(
                """
                SELECT DISTINCT c.relname
                FROM pg_trigger AS t
                JOIN pg_class AS c ON c.oid = t.tgrelid
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE NOT t.tgisinternal
                  AND n.nspname = current_schema()
                  AND c.relname = ANY(:table_names)
                  AND pg_get_triggerdef(t.oid) ILIKE '%change_seq%'
                """
            ),
            {"table_names": sorted(expected_tables)},
        ).scalars()

    assert len(set(sequences)) == 1
    assert set(trigger_tables) == expected_tables


def test_fresh_upgrade_installs_append_only_and_terminal_guards(
    pg_engine: Engine,
) -> None:
    _upgrade(engine_dsn(pg_engine))
    expected_delete_guard_tables = {
        f"{PREFIX}_{suffix}" for suffix in LIFECYCLE_LEDGER_SUFFIXES
    }

    with pg_engine.connect() as connection:
        delete_guard_tables = connection.execute(
            text(
                """
                SELECT DISTINCT c.relname
                FROM pg_trigger AS t
                JOIN pg_class AS c ON c.oid = t.tgrelid
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE NOT t.tgisinternal
                  AND n.nspname = current_schema()
                  AND c.relname = ANY(:table_names)
                  AND pg_get_triggerdef(t.oid) ILIKE '%DELETE%'
                """
            ),
            {"table_names": sorted(expected_delete_guard_tables)},
        ).scalars()
        attempt_trigger_definitions = connection.execute(
            text(
                """
                SELECT pg_get_triggerdef(t.oid),
                       pg_get_functiondef(t.tgfoid)
                FROM pg_trigger AS t
                JOIN pg_class AS c ON c.oid = t.tgrelid
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE NOT t.tgisinternal
                  AND n.nspname = current_schema()
                  AND c.relname = :attempts_table
                """
            ),
            {"attempts_table": f"{PREFIX}_item_attempts"},
        ).all()

    assert set(delete_guard_tables) == expected_delete_guard_tables
    assert any(
        "UPDATE" in trigger_definition.upper()
        and "TERMINAL" in function_definition.upper()
        for trigger_definition, function_definition in (
            attempt_trigger_definitions
        )
    )
