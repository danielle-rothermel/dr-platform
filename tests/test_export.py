"""Focused local publication coverage for the P7a export core."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import cast
from unittest.mock import patch

import duckdb
import pytest
from sqlalchemy import (
    Column,
    Connection,
    Engine,
    Integer,
    MetaData,
    Table,
    Text,
    text,
    update,
)

from dr_platform import (
    ExportOptions,
    PlatformSchema,
    export,
    upgrade_platform_schema,
)
from dr_platform.export import (
    ApplicationSnapshot,
    ProjectionSpec,
    _acquire_lease,
    _create_destination_tables,
    _release_lease,
    _stage_and_promote,
    capture_dbos_publication_telemetry,
)
from dr_platform.publication import PostgresPublicationFence
from dr_platform.submission import EXPORT_BARRIER_ADVISORY_KEY
from tests.contracts.test_platform_v6_cancellation import _register_operation


def test_empty_kernel_export_promotes_and_replays(
    pg_engine: Engine, tmp_path
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    database = tmp_path / "kernel.duckdb"
    first = export(
        pg_engine,
        ExportOptions(destination_path=str(database), run_id="first"),
    )

    assert first.snapshot_seq > 0
    assert first.destinations[0].status == "PROMOTED"
    assert set(first.member_counts) == {
        "platform_operations",
        "platform_items",
        "platform_item_attempts",
        "platform_enqueue_claims",
        "platform_next_attempt_requests",
        "platform_enqueue_compensations",
        "platform_throttle_state",
        "platform_missing_reobservations",
    }

    replay = export(
        pg_engine,
        ExportOptions(destination_path=str(database), run_id="replay"),
    )
    assert replay.destinations[0].status == "PROMOTED"
    assert replay.snapshot_seq > first.snapshot_seq
    with duckdb.connect(str(database), read_only=True) as destination:
        pointer = destination.execute(
            "SELECT committed_snapshot_seq FROM __dr_platform_export_state"
        ).fetchone()
    assert pointer == (replay.snapshot_seq,)


def test_projection_full_rebuild_contract_and_dbos_telemetry_are_frozen() -> (
    None
):
    def rebuild(
        connection: Connection, snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, str], ...]:
        del connection, snapshot
        return ()

    spec = ProjectionSpec(
        member="projection",
        columns=("id",),
        unique_key=("id",),
        full_rebuild_builder=rebuild,
    )
    assert spec.full_rebuild_builder is rebuild
    captured: list[dict[str, str | int | float | bool]] = []
    capture_dbos_publication_telemetry(
        lambda attributes: captured.append(dict(attributes)),
        destination_id="postgres-reporting",
        disposition="PROMOTED",
        snapshot_seq=7,
    )
    assert captured == [
        {
            "platform.publication.destination_id": "postgres-reporting",
            "platform.publication.disposition": "PROMOTED",
            "platform.publication.snapshot_seq": 7,
        }
    ]


def test_application_projection_bundle_builds_one_snapshot(
    pg_engine: Engine, tmp_path
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE application_roots (id TEXT PRIMARY KEY)")
        )
        connection.execute(
            text(
                "CREATE TABLE application_children "
                "(id TEXT PRIMARY KEY, root_id TEXT NOT NULL)"
            )
        )
        connection.execute(
            text("INSERT INTO application_roots VALUES ('root')")
        )
        connection.execute(
            text("INSERT INTO application_children VALUES ('child', 'root')")
        )

    snapshots: list[int] = []

    def roots(
        connection: Connection, snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, str], ...]:
        snapshots.append(snapshot.snapshot_seq)
        return tuple(
            {"id": row["id"]}
            for row in connection.execute(
                text("SELECT id FROM application_roots")
            ).mappings()
        )

    def children(
        connection: Connection, snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, str], ...]:
        snapshots.append(snapshot.snapshot_seq)
        return tuple(
            {"id": row["id"], "root_id": row["root_id"]}
            for row in connection.execute(
                text("SELECT id, root_id FROM application_children")
            ).mappings()
        )

    database = tmp_path / "application.duckdb"
    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(database),
            bundle_key="application-fixture",
            full_rebuild=True,
            projections=(
                ProjectionSpec(
                    member="roots",
                    columns=("id",),
                    unique_key=("id",),
                    full_rebuild_builder=roots,
                ),
                ProjectionSpec(
                    member="children",
                    columns=("id", "root_id"),
                    unique_key=("id",),
                    references=(("root_id", "roots", "id"),),
                    full_rebuild_builder=children,
                ),
            ),
        ),
    )

    assert result.destinations[0].status == "PROMOTED", result
    assert snapshots == [result.snapshot_seq, result.snapshot_seq]
    assert dict(result.member_counts) == {"roots": 1, "children": 1}
    with duckdb.connect(str(database), read_only=True) as destination:
        members = destination.execute(
            "SELECT member FROM __dr_platform_export_members "
            "WHERE bundle_key = 'application-fixture' ORDER BY member"
        ).fetchall()
    assert members == [("children",), ("roots",)]


def test_application_bundle_promotes_and_resolves_remote_fence(
    pg_engine: Engine, tmp_path
) -> None:
    """One public export captures once and independently promotes Postgres."""

    upgrade_platform_schema(str(pg_engine.url))
    with pg_engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE remote_application (id TEXT PRIMARY KEY)")
        )
        connection.execute(
            text("INSERT INTO remote_application VALUES ('root')")
        )

    def roots(
        connection: Connection, snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, str], ...]:
        del snapshot
        return tuple(
            {"id": row["id"]}
            for row in connection.execute(
                text("SELECT id FROM remote_application")
            ).mappings()
        )

    fence = PostgresPublicationFence(
        pg_engine,
        destination_id="remote-fixture",
        table_name="remote_fixture_state",
    )
    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(tmp_path / "remote-local.duckdb"),
            bundle_key="remote-application",
            full_rebuild=True,
            projections=(
                ProjectionSpec(
                    member="roots",
                    columns=("id",),
                    unique_key=("id",),
                    full_rebuild_builder=roots,
                ),
            ),
        ),
        remote_destinations=(fence,),
    )

    assert [destination.status for destination in result.destinations] == [
        "PROMOTED",
        "PROMOTED",
    ]
    pin = fence.pin_bundle(bundle_key="remote-application", pin_id="fixture")
    resolved = fence.resolve_pin(pin)
    assert resolved.snapshot_seq == result.snapshot_seq
    assert set(resolved.members) == {"roots"}


def test_application_projection_rejects_missing_builder_and_invalid_closure(
    pg_engine: Engine, tmp_path
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    options = ExportOptions(
        destination_path=str(tmp_path / "invalid.duckdb"),
        bundle_key="application-invalid",
        full_rebuild=True,
        projections=(
            ProjectionSpec(
                member="roots", columns=("id",), unique_key=("id",)
            ),
        ),
    )
    with pytest.raises(ValueError, match="unknown kernel projection members"):
        export(pg_engine, options)


def test_local_lease_fence_and_fault_preserve_pointer(tmp_path) -> None:
    """A stale token and a failed stage cannot replace a readable pointer."""

    database = tmp_path / "fence.duckdb"
    options = ExportOptions(destination_path=str(database), run_id="owner")
    with duckdb.connect(str(database)) as destination:
        _create_destination_tables(destination)
        lease = _acquire_lease(destination, options)
        assert lease is not None
        first_token, _ = lease
        status, bundle, _, _ = _stage_and_promote(
            destination, options, first_token, 1, (), {}
        )
        assert status == "PROMOTED"
        assert bundle is not None

        newer_lease = _acquire_lease(destination, options)
        assert newer_lease is not None
        newer_token, _ = newer_lease
        assert newer_token > first_token
        stale, _, _, _ = _stage_and_promote(
            destination, options, first_token, 2, (), {}
        )
        assert stale == "STALE_PROMOTION"

        invalid = ProjectionSpec(
            member="bad-name",
            columns=("id",),
            unique_key=("id",),
        )
        with pytest.raises(ValueError, match="unsafe identifier"):
            _stage_and_promote(
                destination,
                options,
                newer_token,
                2,
                ((invalid, cast("Table", None)),),
                {"bad-name": [{"id": "one"}]},
            )
        _release_lease(destination, options, newer_token)
        pointer = destination.execute(
            "SELECT committed_snapshot_seq, bundle_id "
            "FROM __dr_platform_export_state"
        ).fetchone()
    assert pointer == (1, bundle)


def test_nonempty_export_preserves_types_and_excludes_opaque_payloads(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="export-operation",
        item_keys=("export-item",),
    )

    incremental_path = tmp_path / "incremental.duckdb"
    item_projection = ProjectionSpec(
        member=schema.items.name,
        columns=tuple(
            column.name
            for column in schema.items.columns
            if column.name != "spec"
        ),
        unique_key=("item_id",),
        references=(
            ("operation_key", schema.operations.name, "operation_key"),
        ),
    )
    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(incremental_path),
            projections=(item_projection,),
        ),
        schema=schema,
    )
    assert result.destinations[0].status == "PROMOTED", result
    assert result.member_counts[schema.operations.name] == 1
    with duckdb.connect(str(incremental_path), read_only=True) as destination:
        operations_table = destination.execute(
            "SELECT table_name FROM __dr_platform_export_members "
            "WHERE member = ?",
            [schema.operations.name],
        ).fetchone()
        assert operations_table is not None
        columns = {
            row[0]: row[1]
            for row in destination.execute(
                f'DESCRIBE "{operations_table[0]}"'
            ).fetchall()
        }
        row = destination.execute(
            f'SELECT status, requested_count FROM "{operations_table[0]}"'  # noqa: S608 -- destination-owned identifier
        ).fetchone()
    assert row == ("enqueuing", 1)
    assert columns["requested_count"] == "BIGINT"
    assert "spec" not in columns
    assert "metadata" not in columns

    _register_operation(
        pg_engine,
        schema,
        operation_key="export-operation-two",
        item_keys=("export-item-two",),
    )
    advanced = export(
        pg_engine,
        ExportOptions(destination_path=str(incremental_path)),
        schema=schema,
    )
    rebuild_path = tmp_path / "rebuild.duckdb"
    rebuilt = export(
        pg_engine,
        ExportOptions(destination_path=str(rebuild_path), full_rebuild=True),
        schema=schema,
    )
    assert dict(rebuilt.member_counts) == dict(advanced.member_counts)
    assert dict(rebuilt.member_checksums) == dict(advanced.member_checksums)

    with pytest.raises(TypeError):
        result.member_counts[schema.operations.name] = 2  # ty: ignore[invalid-assignment]


def test_equal_snapshot_is_idempotent_and_expired_renewal_cannot_promote(
    tmp_path,
) -> None:
    metadata = MetaData()
    source_table = Table(
        "example",
        metadata,
        Column("id", Text, primary_key=True),
        Column("value", Integer, nullable=False),
    )
    spec = ProjectionSpec(
        member="example", columns=("id", "value"), unique_key=("id",)
    )
    members = ((spec, source_table),)
    rows = {"example": [{"id": "one", "value": 1}]}
    database = tmp_path / "idempotence.duckdb"

    with duckdb.connect(str(database)) as destination:
        _create_destination_tables(destination)
        first_options = ExportOptions(
            destination_path=str(database), run_id="first"
        )
        first_lease = _acquire_lease(destination, first_options)
        assert first_lease is not None
        first_status, bundle, _, _ = _stage_and_promote(
            destination, first_options, first_lease[0], 7, members, rows
        )
        assert first_status == "PROMOTED"

        replay_options = ExportOptions(
            destination_path=str(database), run_id="replay"
        )
        replay_lease = _acquire_lease(destination, replay_options)
        assert replay_lease is not None
        replay_status, replay_bundle, _, _ = _stage_and_promote(
            destination, replay_options, replay_lease[0], 7, members, rows
        )
        assert (replay_status, replay_bundle) == ("IDEMPOTENT", bundle)

        stale_options = ExportOptions(
            destination_path=str(database), run_id="stale"
        )
        stale_lease = _acquire_lease(destination, stale_options)
        assert stale_lease is not None
        with patch(
            "dr_platform.export._renew_lease", side_effect=[True, False]
        ):
            stale_status, _, _, _ = _stage_and_promote(
                destination,
                stale_options,
                stale_lease[0],
                8,
                members,
                {"example": [{"id": "two", "value": 2}]},
            )
        assert stale_status == "STALE_PROMOTION"
        pointer = destination.execute(
            "SELECT committed_snapshot_seq, bundle_id "
            "FROM __dr_platform_export_state"
        ).fetchone()
    assert pointer == (7, bundle)


def test_source_barrier_waits_for_committed_writer(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="barrier-operation",
        item_keys=("barrier-item",),
    )
    database = tmp_path / "barrier.duckdb"

    with pg_engine.connect() as writer:
        transaction = writer.begin()
        writer.execute(
            text("SELECT pg_advisory_xact_lock_shared(:key)"),
            {"key": EXPORT_BARRIER_ADVISORY_KEY},
        )
        writer.execute(
            update(schema.operations)
            .where(schema.operations.c.operation_key == "barrier-operation")
            .values(terminal_reason="cut-visible")
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                export,
                pg_engine,
                ExportOptions(destination_path=str(database)),
                schema=schema,
            )
            deadline = time.monotonic() + 2
            waiting = False
            while time.monotonic() < deadline:
                with pg_engine.connect() as observer:
                    waiting = bool(
                        observer.execute(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                                "WHERE locktype = 'advisory' AND NOT granted)"
                            )
                        ).scalar_one()
                    )
                if waiting:
                    break
                time.sleep(0.01)
            assert waiting
            assert not future.done()
            transaction.commit()
            result = future.result(timeout=5)
    assert result.destinations[0].status == "PROMOTED"
    with duckdb.connect(str(database), read_only=True) as destination:
        table_name = destination.execute(
            "SELECT table_name FROM __dr_platform_export_members "
            "WHERE member = ?",
            [schema.operations.name],
        ).fetchone()
        assert table_name is not None
        reason = destination.execute(
            f'SELECT terminal_reason FROM "{table_name[0]}"'  # noqa: S608 -- destination-owned identifier
        ).fetchone()
    assert reason == ("cut-visible",)


def test_structured_failure_preserves_populated_pointer(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="failure-operation",
        item_keys=("failure-item",),
    )
    database = tmp_path / "failure.duckdb"
    first = export(
        pg_engine, ExportOptions(destination_path=str(database)), schema=schema
    )
    assert first.destinations[0].status == "PROMOTED"
    with patch(
        "dr_platform.export._stage_and_promote",
        side_effect=RuntimeError("postgresql://secret@example.invalid/db"),
    ):
        failed = export(
            pg_engine,
            ExportOptions(destination_path=str(database)),
            schema=schema,
        )
    assert failed.destinations[0].status == "FAILED"
    assert failed.destinations[0].error == "RuntimeError"
    with duckdb.connect(str(database), read_only=True) as destination:
        pointer = destination.execute(
            "SELECT committed_snapshot_seq, bundle_id "
            "FROM __dr_platform_export_state"
        ).fetchone()
    assert pointer == (
        first.snapshot_seq,
        first.destinations[0].bundle_id,
    )
