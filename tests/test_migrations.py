"""Fresh platform-schema migration contract."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module

import pytest
from sqlalchemy import Connection, Engine, inspect, text

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.db.schema import (
    MAX_PREFIX_BYTES,
    POSTGRES_IDENTIFIER_MAX_BYTES,
    PlatformSchema,
)

KERNEL_TABLE_SUFFIXES = {
    "operations",
    "items",
    "item_attempts",
    "next_attempt_requests",
    "enqueue_claims",
    "enqueue_compensations",
    "missing_reobservations",
    "throttle_state",
    "platform_alembic_version",
}

CHANGE_TRACKED_SUFFIXES = KERNEL_TABLE_SUFFIXES - {"platform_alembic_version"}

LEDGER_CHANGE_SEQ_QUERIES = {
    "operations": (
        "SELECT change_seq FROM platform_operations WHERE operation_key = 'op'"
    ),
    "items": ("SELECT change_seq FROM platform_items WHERE item_id = 'item'"),
    "enqueue_claims": (
        "SELECT change_seq FROM platform_enqueue_claims "
        "WHERE claim_id = 'claim'"
    ),
    "enqueue_compensations": (
        "SELECT change_seq FROM platform_enqueue_compensations "
        "WHERE claim_id = 'claim'"
    ),
    "next_attempt_requests": (
        "SELECT change_seq FROM platform_next_attempt_requests "
        "WHERE request_id = 'request'"
    ),
}


def _table_columns(engine: Engine, table: str) -> set[str]:
    return {column["name"] for column in inspect(engine).get_columns(table)}


def test_fresh_upgrade_creates_complete_kernel_schema(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))

    tables = set(inspect(pg_engine).get_table_names())
    assert {f"platform_{suffix}" for suffix in KERNEL_TABLE_SUFFIXES} <= tables
    assert not any("batch_submit" in table for table in tables)
    assert "platform_projections" not in tables

    operation_columns = _table_columns(pg_engine, "platform_operations")
    assert {
        "manifest_digest",
        "operation_execution_recipe_digest",
        "target_contract_digest",
        "platform_cut_version",
        "registration_cursor",
        "registration_abandoned_at",
        "retry_policy",
        "active_count",
        "terminal_failed_count",
        "change_seq",
    } <= operation_columns

    item_columns = _table_columns(pg_engine, "platform_items")
    assert {
        "item_id",
        "item_key",
        "shuffle_rank",
        "service_class",
        "service_priority",
        "current_attempt",
        "change_seq",
    } <= item_columns
    assert "enqueue_metadata" not in item_columns

    marker_columns = _table_columns(
        pg_engine, "platform_missing_reobservations"
    )
    assert {
        "item_id",
        "attempt",
        "last_reobserved_at",
        "observation_count",
        "change_seq",
    } <= marker_columns

    attempt_columns = _table_columns(pg_engine, "platform_item_attempts")
    assert {
        "execution_key",
        "workflow_id",
        "execution_recipe_digest",
        "enqueue_state",
        "execution_state",
        "current_claim_id",
        "cancellation_request_id",
        "requested_service_class",
        "effective_service_priority",
        "change_seq",
    } <= attempt_columns

    database_inspector = inspect(pg_engine)
    claim_uniques = database_inspector.get_unique_constraints(
        "platform_enqueue_claims"
    )
    assert any(
        constraint["column_names"]
        == ["item_id", "attempt", "claim_id", "workflow_id"]
        for constraint in claim_uniques
    )
    compensation_foreign_keys = database_inspector.get_foreign_keys(
        "platform_enqueue_compensations"
    )
    assert any(
        foreign_key["constrained_columns"]
        == ["item_id", "attempt", "claim_id", "workflow_id"]
        and foreign_key["referred_columns"]
        == ["item_id", "attempt", "claim_id", "workflow_id"]
        for foreign_key in compensation_foreign_keys
    )


def test_prefix_is_the_only_physical_naming_option(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url), prefix="whetstone")

    tables = set(inspect(pg_engine).get_table_names())
    expected_tables = {
        f"whetstone_{suffix}" for suffix in KERNEL_TABLE_SUFFIXES
    }
    assert expected_tables <= tables
    assert _table_columns(pg_engine, "whetstone_items") >= {
        "item_key",
        "shuffle_rank",
    }


def test_claim_workflow_provenance_upgrades_existing_baseline(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                ALTER TABLE platform_enqueue_compensations
                  DROP CONSTRAINT platform_fk_compensations_claim;
                ALTER TABLE platform_enqueue_claims
                  DROP CONSTRAINT platform_uq_claims_workflow_provenance;
                ALTER TABLE platform_enqueue_compensations
                  ADD CONSTRAINT platform_fk_compensations_claim
                  FOREIGN KEY (item_id, attempt, claim_id)
                  REFERENCES platform_enqueue_claims
                    (item_id, attempt, claim_id)
                  ON DELETE RESTRICT;
                UPDATE platform_platform_alembic_version
                  SET version_num = '0001_platform_baseline';
                """
            )
        )

    upgrade_platform_schema(str(pg_engine.url))

    database_inspector = inspect(pg_engine)
    assert any(
        constraint["column_names"]
        == ["item_id", "attempt", "claim_id", "workflow_id"]
        for constraint in database_inspector.get_unique_constraints(
            "platform_enqueue_claims"
        )
    )


def test_missing_reobservation_schedule_upgrades_existing_schema(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(text("DROP TABLE platform_missing_reobservations"))
        connection.execute(
            text(
                "UPDATE platform_platform_alembic_version "
                "SET version_num = '0003_attempt_retry_reason'"
            )
        )

    upgrade_platform_schema(str(pg_engine.url))

    database_inspector = inspect(pg_engine)
    assert "platform_missing_reobservations" in set(
        database_inspector.get_table_names()
    )
    index_columns = {
        tuple(index["column_names"])
        for index in database_inspector.get_indexes(
            "platform_missing_reobservations"
        )
    }
    assert ("last_reobserved_at", "item_id", "attempt") in index_columns
    assert any(
        foreign_key["constrained_columns"]
        == ["item_id", "attempt", "claim_id", "workflow_id"]
        for foreign_key in database_inspector.get_foreign_keys(
            "platform_enqueue_compensations"
        )
    )


def test_attempt_retry_reason_downgrade_is_explicitly_irreversible() -> None:
    migration = import_module(
        "dr_platform.db.alembic.versions.0003_attempt_retry_reason"
    )

    with pytest.raises(RuntimeError, match="fresh schema"):
        migration.downgrade()


def test_prefix_rejects_generated_identifiers_over_postgres_limit(
    pg_engine: Engine,
) -> None:
    boundary_prefix = "p" * MAX_PREFIX_BYTES
    schema = PlatformSchema(prefix=boundary_prefix)
    generated_metadata_names = {
        table.name for table in schema.metadata.tables.values()
    } | {
        named.name
        for table in schema.metadata.tables.values()
        for named in (*table.constraints, *table.indexes)
        if isinstance(named.name, str)
    }
    migration_names = {
        f"{boundary_prefix}_{suffix}"
        for suffix in (
            "platform_alembic_version",
            "change_seq",
            "assign_change_seq",
            "reject_kernel_delete",
            "reject_terminal_attempt_mutation",
            "00_reject_terminal_attempt_mutation",
            "guard_operation_update",
            "00_guard_operation_update",
            "guard_item_update",
            "00_guard_item_update",
            "guard_enqueue_claim_update",
            "00_guard_enqueue_claim_update",
            "guard_compensation_update",
            "00_guard_compensation_update",
            "guard_next_attempt_request_update",
            "00_guard_next_attempt_request_update",
        )
    }
    generated_names = generated_metadata_names | migration_names
    assert max(len(name.encode()) for name in generated_names) == (
        POSTGRES_IDENTIFIER_MAX_BYTES
    )
    assert all(
        len(name.encode()) <= POSTGRES_IDENTIFIER_MAX_BYTES
        for name in generated_names
    )
    upgrade_platform_schema(str(pg_engine.url), prefix=boundary_prefix)
    assert f"{boundary_prefix}_operations" in set(
        inspect(pg_engine).get_table_names()
    )

    too_long_prefix = "p" * (MAX_PREFIX_BYTES + 1)
    with pytest.raises(ValueError, match="too long"):
        PlatformSchema(prefix=too_long_prefix)
    with pytest.raises(ValueError, match="too long"):
        upgrade_platform_schema(str(pg_engine.url), prefix=too_long_prefix)
    assert not any(
        table.startswith(too_long_prefix)
        for table in inspect(pg_engine).get_table_names()
    )


@pytest.mark.parametrize("prefix", ["", "Upper", "9starts_with_digit", "a-b"])
def test_prefix_rejects_noncanonical_sql_identifiers(prefix: str) -> None:
    with pytest.raises(ValueError, match="lowercase SQL identifier"):
        PlatformSchema(prefix=prefix)


def test_change_sequence_advances_for_insert_and_update(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_throttle_state (
                    throttle_key,
                    consecutive_failures,
                    metadata,
                    tags,
                    updated_at
                ) VALUES ('provider:model', 0, '{}', '{}', now())
                """
            )
        )
        inserted = connection.execute(
            text(
                "SELECT change_seq FROM platform_throttle_state "
                "WHERE throttle_key = 'provider:model'"
            )
        ).scalar_one()
        connection.execute(
            text(
                "UPDATE platform_throttle_state "
                "SET consecutive_failures = 1, updated_at = now() "
                "WHERE throttle_key = 'provider:model'"
            )
        )
        updated = connection.execute(
            text(
                "SELECT change_seq FROM platform_throttle_state "
                "WHERE throttle_key = 'provider:model'"
            )
        ).scalar_one()

    assert updated > inserted


def test_kernel_rows_reject_hard_delete(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_throttle_state (
                    throttle_key,
                    consecutive_failures,
                    metadata,
                    tags,
                    updated_at
                ) VALUES ('provider:model', 0, '{}', '{}', now())
                """
            )
        )

    with (
        pytest.raises(
            Exception, match="kernel lifecycle rows cannot be deleted"
        ),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                "DELETE FROM platform_throttle_state "
                "WHERE throttle_key = 'provider:model'"
            )
        )


def _insert_terminal_attempt_fixture(connection: Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO platform_operations (
                operation_key, group_key, workflow_role, status,
                requested_count, manifest_version, manifest_digest,
                manifest_page_size, manifest_page_count,
                operation_execution_recipe_digest,
                target_key, target_version, target_contract_digest,
                platform_cut_version, registration_cursor, retry_policy,
                inserted_count, already_present_count, enqueued_count,
                workflow_already_present_count, enqueue_failed_count,
                active_count, succeeded_count, terminal_failed_count,
                cancelled_count, spec, metadata, created_at,
                registration_completed_at, updated_at, completed_at
            ) VALUES (
                'op', 'group', 'role', 'succeeded',
                1, 3, 'manifest', 500, 1, 'operation-recipe',
                'target', 1, 'target-contract',
                1, 1, '{"max_attempts": 3, "max_enqueue_tries": 3}',
                1, 0, 1, 0, 0, 0, 1, 0, 0, '{}', '{}', now(),
                now(), now(), now()
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO platform_items (
                item_id, operation_key, item_index, item_key,
                shuffle_rank, service_class, service_priority, spec,
                insert_status, current_attempt, created_at, updated_at
            ) VALUES (
                'item', 'op', 0, 'caller-item',
                1, 'standard', 1000, '{}',
                'inserted', 0, now(), now()
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO platform_item_attempts (
                item_id, attempt, workflow_role, execution_key,
                workflow_id, execution_recipe_digest, enqueue_state,
                enqueue_try, execution_state, source_application_version,
                missing_observation_count, requested_service_class,
                requested_service_priority, effective_service_priority,
                priority_source, created_at, enqueued_at, terminal_at,
                updated_at
            ) VALUES (
                'item', 0, 'role', 'execution', 'workflow',
                'item-recipe', 'enqueued', 1, 'succeeded', 'app-v1',
                0, 'standard', 1000, 1000, 'enqueued_here',
                now(), now(), now(), now()
            )
            """
        )
    )


def test_terminal_attempt_allows_noop_and_rejects_mutation(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with pg_engine.begin() as connection:
        result = connection.execute(
            text(
                "UPDATE platform_item_attempts "
                "SET execution_state = execution_state "
                "WHERE item_id = 'item' AND attempt = 0"
            )
        )
    assert result.rowcount == 0

    with (
        pytest.raises(Exception, match="terminal item attempts are immutable"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                "UPDATE platform_item_attempts "
                "SET execution_state = 'error' "
                "WHERE item_id = 'item' AND attempt = 0"
            )
        )


def test_operation_completion_and_abandonment_require_complete_facts(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with (
        pytest.raises(Exception, match="operations_registration"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                "UPDATE platform_operations SET inserted_count = 0 "
                "WHERE operation_key = 'op'"
            )
        )

    with (
        pytest.raises(Exception, match="operations_registration"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                """
                UPDATE platform_operations
                SET registration_completed_at = NULL,
                    registration_abandoned_by = 'operator',
                    registration_abandonment_reason = 'expired lease',
                    status = 'failed'
                WHERE operation_key = 'op'
                """
            )
        )


@pytest.mark.parametrize(
    "statement",
    [
        (
            "UPDATE platform_operations SET registration_completed_at = "
            "created_at - interval '1 second' WHERE operation_key = 'op'"
        ),
        (
            "UPDATE platform_operations SET registration_completed_at = NULL, "
            "registration_abandoned_at = created_at - interval '1 second', "
            "registration_abandoned_by = 'operator', "
            "registration_abandonment_reason = 'expired lease', "
            "status = 'failed' WHERE operation_key = 'op'"
        ),
        (
            "UPDATE platform_operations SET cancel_requested_at = "
            "created_at - interval '1 second' WHERE operation_key = 'op'"
        ),
        (
            "UPDATE platform_operations SET updated_at = "
            "created_at - interval '1 second' WHERE operation_key = 'op'"
        ),
        (
            "UPDATE platform_operations SET completed_at = "
            "created_at - interval '1 second' WHERE operation_key = 'op'"
        ),
    ],
)
def test_operation_timestamps_cannot_precede_creation(
    pg_engine: Engine,
    statement: str,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with (
        pytest.raises(Exception, match="time"),
        pg_engine.begin() as connection,
    ):
        connection.execute(text(statement))


def test_item_updated_at_cannot_precede_creation(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with (
        pytest.raises(Exception, match="items_updated_time"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                "UPDATE platform_items SET updated_at = "
                "created_at - interval '1 second' WHERE item_id = 'item'"
            )
        )


@pytest.mark.parametrize(
    "timestamp_field",
    ["enqueued_at", "terminal_at", "updated_at"],
)
def test_attempt_timestamps_cannot_precede_creation(
    pg_engine: Engine,
    timestamp_field: str,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    earlier = created_at - timedelta(seconds=1)
    execution_state = (
        "succeeded" if timestamp_field == "terminal_at" else "not_started"
    )
    terminal_at = earlier if timestamp_field == "terminal_at" else None
    enqueued_at = earlier if timestamp_field == "enqueued_at" else created_at
    updated_at = earlier if timestamp_field == "updated_at" else created_at
    with (
        pytest.raises(Exception, match=r"attempts_.*_time"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                """
                INSERT INTO platform_item_attempts (
                    item_id, attempt, workflow_role, execution_key,
                    workflow_id, execution_recipe_digest, enqueue_state,
                    enqueue_try, execution_state, source_attempt,
                    source_workflow_id, retry_reason,
                    source_application_version, missing_observation_count,
                    requested_service_class, requested_service_priority,
                    effective_service_priority, priority_source, created_at,
                    enqueued_at, terminal_at, updated_at
                ) VALUES (
                    'item', 1, 'role', 'execution-1', 'workflow-1',
                    'item-recipe-1', 'enqueued', 1, :execution_state, 0,
                    'workflow', 'domain_outcome', 'app-v1', 0,
                    'standard', 1000, 1000, 'enqueued_here', :created_at,
                    :enqueued_at, :terminal_at, :updated_at
                )
                """
            ),
            {
                "execution_state": execution_state,
                "created_at": created_at,
                "enqueued_at": enqueued_at,
                "terminal_at": terminal_at,
                "updated_at": updated_at,
            },
        )


def _insert_update_guard_fixtures(connection: Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO platform_enqueue_claims (
                item_id, attempt, claim_id, workflow_id, enqueue_try,
                claimed_at, lease_expires_at, enqueue_call_started_at,
                disposition, created_at
            ) VALUES (
                'item', 0, 'claim', 'workflow', 1,
                now(), now() + interval '1 minute', now(),
                'call_started', now()
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO platform_enqueue_compensations (
                item_id, attempt, claim_id, workflow_id, reason,
                cancel_disposition, created_at
            ) VALUES (
                'item', 0, 'claim', 'workflow',
                'invalidated_call_started_claim', 'pending', now()
            )
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO platform_next_attempt_requests (
                request_id, item_id, request_key, source_attempt, reason,
                eligibility_kind, eligibility_record_id, eligibility_digest,
                requested_by, effective_max_attempts, disposition,
                rejection_detail, created_at, resolved_at
            ) VALUES (
                'request', 'item', 'caller-request', 0, 'domain_outcome',
                'generation_run', 'run', 'eligibility-digest',
                'caller', 3, 'ineligible', 'not eligible', now(), now()
            )
            """
        )
    )


def _ledger_change_seqs(connection: Connection) -> dict[str, int]:
    return {
        ledger: connection.execute(text(query)).scalar_one()
        for ledger, query in LEDGER_CHANGE_SEQ_QUERIES.items()
    }


def test_lifecycle_ledger_guards_allow_noops_and_valid_transitions(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        _insert_update_guard_fixtures(connection)
        before = _ledger_change_seqs(connection)

    with pg_engine.begin() as connection:
        noops = {
            "operations": connection.execute(
                text("UPDATE platform_operations SET spec = spec")
            ).rowcount,
            "items": connection.execute(
                text("UPDATE platform_items SET spec = spec")
            ).rowcount,
            "enqueue_claims": connection.execute(
                text(
                    "UPDATE platform_enqueue_claims "
                    "SET workflow_id = workflow_id"
                )
            ).rowcount,
            "enqueue_compensations": connection.execute(
                text(
                    "UPDATE platform_enqueue_compensations "
                    "SET workflow_id = workflow_id"
                )
            ).rowcount,
            "next_attempt_requests": connection.execute(
                text(
                    "UPDATE platform_next_attempt_requests "
                    "SET requested_by = requested_by"
                )
            ).rowcount,
        }
        after = _ledger_change_seqs(connection)

    assert noops == dict.fromkeys(before, 0)
    assert after == before

    with pg_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO platform_enqueue_claims (
                    item_id, attempt, claim_id, workflow_id, enqueue_try,
                    claimed_at, lease_expires_at, disposition, created_at
                ) VALUES (
                    'item', 0, 'new-claim', 'workflow', 2,
                    now(), now() + interval '1 minute', 'claimed', now()
                )
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET disposition = 'call_started',
                    enqueue_call_started_at = now()
                WHERE claim_id = 'new-claim'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET disposition = 'invalidated',
                    invalidated_at = now(),
                    invalidated_by = 'operator',
                    resolved_at = now()
                WHERE claim_id = 'new-claim'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET disposition = 'outcome_recorded', resolved_at = now()
                WHERE claim_id = 'claim'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_compensations
                SET cancel_disposition = 'cancelled', resolved_at = now()
                WHERE claim_id = 'claim'
                """
            )
        )


def test_call_started_claim_can_expire_or_be_replaced_without_losing_fact(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        connection.execute(
            text(
                """
                INSERT INTO platform_enqueue_claims (
                    item_id, attempt, claim_id, workflow_id, enqueue_try,
                    claimed_at, lease_expires_at,
                    enqueue_call_started_at, disposition, created_at
                ) VALUES
                    (
                        'item', 0, 'expired-claim', 'workflow', 1,
                        '2026-01-01T00:00:00Z',
                        '2026-01-01T00:01:00Z',
                        '2026-01-01T00:00:01Z', 'call_started',
                        '2026-01-01T00:00:00Z'
                    ),
                    (
                        'item', 0, 'replacement-claim', 'workflow', 2,
                        '2026-01-01T00:00:02Z',
                        '2026-01-01T00:01:02Z',
                        NULL, 'claimed', '2026-01-01T00:00:02Z'
                    ),
                    (
                        'item', 0, 'replaced-claim', 'workflow', 1,
                        '2026-01-01T00:00:00Z',
                        '2026-01-01T00:01:00Z',
                        '2026-01-01T00:00:01Z', 'call_started',
                        '2026-01-01T00:00:00Z'
                    )
                """
            )
        )
        before = dict(
            connection.execute(
                text(
                    """
                    SELECT claim_id, enqueue_call_started_at
                    FROM platform_enqueue_claims
                    WHERE claim_id IN ('expired-claim', 'replaced-claim')
                    """
                )
            )
            .tuples()
            .all()
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET disposition = 'expired', resolved_at = now()
                WHERE claim_id = 'expired-claim'
                """
            )
        )
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET disposition = 'replaced',
                    replacement_claim_id = 'replacement-claim',
                    resolved_at = now()
                WHERE claim_id = 'replaced-claim'
                """
            )
        )
        after = dict(
            connection.execute(
                text(
                    """
                    SELECT claim_id, enqueue_call_started_at
                    FROM platform_enqueue_claims
                    WHERE claim_id IN ('expired-claim', 'replaced-claim')
                    """
                )
            )
            .tuples()
            .all()
        )

    assert after == before


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        (
            "UPDATE platform_enqueue_claims "
            "SET workflow_id = 'different-workflow'",
            "enqueue Claim identity is immutable",
        ),
        (
            "UPDATE platform_enqueue_claims "
            "SET enqueue_call_started_at = enqueue_call_started_at "
            "+ interval '1 second'",
            "enqueue Claim call-start fact is immutable",
        ),
        (
            "UPDATE platform_enqueue_compensations "
            "SET reason = 'different-reason'",
            "enqueue compensation identity is immutable",
        ),
        (
            "UPDATE platform_next_attempt_requests "
            "SET requested_by = 'different-caller'",
            "next-Attempt request ledger is immutable",
        ),
    ],
)
def test_lifecycle_ledger_guards_reject_immutable_mutation(
    pg_engine: Engine,
    statement: str,
    message: str,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        _insert_update_guard_fixtures(connection)

    with (
        pytest.raises(Exception, match=message),
        pg_engine.begin() as connection,
    ):
        connection.execute(text(statement))


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        (
            "UPDATE platform_operations SET manifest_digest = 'different'",
            "Operation identity fields are immutable",
        ),
        (
            "UPDATE platform_operations SET target_key = 'different'",
            "Operation identity fields are immutable",
        ),
        (
            "UPDATE platform_operations SET spec = '{\"changed\": true}'",
            "Operation identity fields are immutable",
        ),
        (
            "UPDATE platform_operations SET metadata = '{\"changed\": true}'",
            "Operation identity fields are immutable",
        ),
        (
            "UPDATE platform_operations "
            "SET created_at = created_at + interval '1 second'",
            "Operation identity fields are immutable",
        ),
        (
            "UPDATE platform_items SET item_key = 'different'",
            "Item identity fields are immutable",
        ),
        (
            "UPDATE platform_items SET spec = '{\"changed\": true}'",
            "Item identity fields are immutable",
        ),
        (
            "UPDATE platform_items "
            "SET service_class = 'backfill', service_priority = 10000",
            "Item identity fields are immutable",
        ),
        (
            "UPDATE platform_items "
            "SET created_at = created_at + interval '1 second'",
            "Item identity fields are immutable",
        ),
    ],
)
def test_operation_and_item_guards_reject_immutable_mutation(
    pg_engine: Engine,
    statement: str,
    message: str,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with (
        pytest.raises(Exception, match=message),
        pg_engine.begin() as connection,
    ):
        connection.execute(text(statement))


@pytest.mark.parametrize(
    "case",
    [
        (
            "UPDATE platform_enqueue_claims "
            "SET disposition = 'outcome_recorded', resolved_at = now()",
            "SELECT change_seq FROM platform_enqueue_claims",
            "UPDATE platform_enqueue_claims SET change_seq = change_seq",
            "UPDATE platform_enqueue_claims SET invalidated_by = 'changed'",
            "resolved enqueue Claims are immutable",
        ),
        (
            "UPDATE platform_enqueue_compensations "
            "SET cancel_disposition = 'cancelled', resolved_at = now()",
            "SELECT change_seq FROM platform_enqueue_compensations",
            "UPDATE platform_enqueue_compensations "
            "SET change_seq = change_seq",
            "UPDATE platform_enqueue_compensations "
            "SET cancel_disposition = 'observed_terminal'",
            "resolved enqueue compensations are immutable",
        ),
    ],
)
def test_resolved_ledgers_reject_every_non_noop_mutation(
    pg_engine: Engine,
    case: tuple[str, str, str, str, str],
) -> None:
    resolution, select_seq, noop, mutation, message = case
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        _insert_update_guard_fixtures(connection)
        connection.execute(text(resolution))
        before = connection.execute(text(select_seq)).scalar_one()
        noop_result = connection.execute(text(noop))
        after = connection.execute(text(select_seq)).scalar_one()
    assert noop_result.rowcount == 0
    assert after == before

    with (
        pytest.raises(Exception, match=message),
        pg_engine.begin() as connection,
    ):
        connection.execute(text(mutation))


def test_compensation_created_at_is_always_immutable(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        _insert_update_guard_fixtures(connection)

    with (
        pytest.raises(Exception, match="compensation identity is immutable"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                "UPDATE platform_enqueue_compensations "
                "SET created_at = created_at + interval '1 second'"
            )
        )


def test_compensation_fk_rejects_forged_claim_workflow(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        connection.execute(
            text(
                """
                INSERT INTO platform_enqueue_claims (
                    item_id, attempt, claim_id, workflow_id, enqueue_try,
                    claimed_at, lease_expires_at, enqueue_call_started_at,
                    disposition, created_at
                ) VALUES (
                    'item', 0, 'claim', 'workflow', 1,
                    now(), now() + interval '1 minute', now(),
                    'call_started', now()
                )
                """
            )
        )

    with (
        pytest.raises(Exception, match="fk_compensations_claim"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                """
                INSERT INTO platform_enqueue_compensations (
                    item_id, attempt, claim_id, workflow_id, reason,
                    cancel_disposition, created_at
                ) VALUES (
                    'item', 0, 'claim', 'forged-workflow',
                    'invalidated_call_started_claim', 'pending', now()
                )
                """
            )
        )


def test_claimed_disposition_cannot_have_call_started_timestamp(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)

    with (
        pytest.raises(Exception, match="claims_call_started"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                """
                INSERT INTO platform_enqueue_claims (
                    item_id, attempt, claim_id, workflow_id, enqueue_try,
                    claimed_at, lease_expires_at,
                    enqueue_call_started_at, disposition, created_at
                ) VALUES (
                    'item', 0, 'invalid-claim', 'workflow', 1,
                    now(), now() + interval '1 minute', now(),
                    'claimed', now()
                )
                """
            )
        )


def test_claim_invalidation_facts_require_invalidated_disposition(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        _insert_terminal_attempt_fixture(connection)
        _insert_update_guard_fixtures(connection)

    with (
        pytest.raises(Exception, match="claims_invalidation_disposition"),
        pg_engine.begin() as connection,
    ):
        connection.execute(
            text(
                """
                UPDATE platform_enqueue_claims
                SET invalidated_at = now(), invalidated_by = 'operator'
                WHERE claim_id = 'claim'
                """
            )
        )


def test_every_exported_kernel_table_has_change_trigger(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.connect() as connection:
        trigger_tables = set(
            connection.execute(
                text(
                    """
                    SELECT event_object_table
                    FROM information_schema.triggers
                    WHERE trigger_name = 'platform_assign_change_seq'
                    """
                )
            ).scalars()
        )

    assert {
        f"platform_{suffix}" for suffix in CHANGE_TRACKED_SUFFIXES
    } <= trigger_tables


def test_upgrade_accepts_percent_encoded_urls(pg_engine: Engine) -> None:
    url = str(pg_engine.url) + "?options=-csearch_path%3Dpublic"
    upgrade_platform_schema(url)
    assert "platform_operations" in set(inspect(pg_engine).get_table_names())


def test_search_path_schema_has_independent_lineage(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS scratch CASCADE"))
        connection.execute(text("CREATE SCHEMA scratch"))
    scratch_url = (
        str(pg_engine.url) + "?options=-csearch_path%3Dscratch,public"
    )
    upgrade_platform_schema(scratch_url)

    scratch_tables = set(inspect(pg_engine).get_table_names("scratch"))
    assert {f"platform_{suffix}" for suffix in KERNEL_TABLE_SUFFIXES} <= (
        scratch_tables
    )
