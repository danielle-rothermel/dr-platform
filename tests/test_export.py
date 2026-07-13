"""Focused local publication coverage for the P7a export core."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
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
    ExportReconciliationDependencies,
    PinnedBundleGoneError,
    PlatformSchema,
    ProjectionColumn,
    ProjectionColumnType,
    TargetRegistry,
    pin_local_bundle,
    resolve_local_pin,
    upgrade_platform_schema,
)
from dr_platform import (
    export as _export,
)
from dr_platform.dbos_config import DbosWorkflowStatus
from dr_platform.enqueue_runtime import (
    PhysicalEnqueueDisposition,
    PhysicalEnqueueOutcome,
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
from dr_platform.publication import (
    PostgresPublicationFence,
    RemotePromotionResult,
)
from dr_platform.reconciliation_runtime import (
    ReconcileOptions,
    ReconciliationObservation,
    ReconciliationObservationDisposition,
)
from dr_platform.submission import EXPORT_BARRIER_ADVISORY_KEY
from tests.contracts.test_platform_v6_cancellation import _register_operation
from tests.contracts.test_platform_v6_enqueue_claims import _target


class _QueueLookup:
    def retrieve_queue(self, name: str) -> object:
        del name
        return type(
            "QueueConfiguration",
            (),
            {"database_backed_queue": True, "priority_enabled": True},
        )()


class _LifecycleReader:
    def observe(self, *, workflow_id: str) -> ReconciliationObservation:
        return ReconciliationObservation(
            workflow_id=workflow_id,
            disposition=ReconciliationObservationDisposition.ACTIVE,
            dbos_status=DbosWorkflowStatus.PENDING,
        )

    def read_step_history(
        self, *, workflow_id: str, limit: int = 100
    ) -> tuple[Any, ...]:
        del workflow_id, limit
        return ()


class _EnqueueAdapter:
    def enqueue(self, call):  # type: ignore[no-untyped-def]
        return PhysicalEnqueueOutcome(
            workflow_id=call.workflow_id,
            disposition=PhysicalEnqueueDisposition.ENQUEUED,
            effective_service_priority=call.service_priority,
        )


def _reconciliation(
    engine: Engine,
    *,
    reader: object | None = None,
    page_size: int = 100,
    max_cycles: int = 10,
) -> ExportReconciliationDependencies:
    registry = TargetRegistry()
    registry.register(_target())
    return ExportReconciliationDependencies(
        resolver=registry,
        queue_lookup=_QueueLookup(),
        reader=cast("Any", reader or _LifecycleReader()),
        dbos_engine=engine,
        options=ReconcileOptions(page_size=page_size),
        max_cycles=max_cycles,
        enqueue_adapter=_EnqueueAdapter(),
    )


def export(source: Engine, options: ExportOptions, **kwargs):  # type: ignore[no-untyped-def]
    return _export(
        source,
        options,
        reconciliation=_reconciliation(source),
        **kwargs,
    )


def _text_schema(*names: str) -> tuple[ProjectionColumn, ...]:
    return tuple(
        ProjectionColumn(name=name, type=ProjectionColumnType.TEXT)
        for name in names
    )


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
        "platform_enqueue_compensation_hazards",
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
        column_schema=_text_schema("id"),
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
                    column_schema=_text_schema("id"),
                    unique_key=("id",),
                    full_rebuild_builder=roots,
                ),
                ProjectionSpec(
                    member="children",
                    columns=("id", "root_id"),
                    column_schema=_text_schema("id", "root_id"),
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
                    column_schema=_text_schema("id"),
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


def test_application_bundle_records_motherduck_main_schema(
    pg_engine: Engine, tmp_path
) -> None:
    """MotherDuck unqualified stages live in `main`, not Postgres `public`."""

    upgrade_platform_schema(str(pg_engine.url))

    def roots(
        _connection: Connection, _snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, str], ...]:
        return ({"id": "root"},)

    class CapturingFence(PostgresPublicationFence):
        def promote(self, *, stage, **kwargs):  # type: ignore[no-untyped-def]
            del kwargs
            with self.engine.connect() as connection:
                transaction = connection.begin()
                try:
                    manifest = stage(connection)
                    assert {
                        member.schema_name for member in manifest.members
                    } == {"main"}
                finally:
                    transaction.rollback()
            return RemotePromotionResult(
                disposition="PROMOTED",
                bundle_id="fixture",
                snapshot_seq=1,
            )

    fence = CapturingFence(
        pg_engine,
        destination_id="motherduck-schema-fixture",
        table_name="motherduck_schema_fixture_state",
        kind="motherduck",
    )

    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(tmp_path / "motherduck-schema.duckdb"),
            bundle_key="motherduck-schema",
            full_rebuild=True,
            projections=(
                ProjectionSpec(
                    member="roots",
                    columns=("id",),
                    column_schema=_text_schema("id"),
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


def test_application_projection_types_round_trip_and_aggregate(
    pg_engine: Engine, tmp_path
) -> None:
    upgrade_platform_schema(str(pg_engine.url))
    captured = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)

    def typed_rows(
        _connection: Connection, _snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, object], ...]:
        return (
            {
                "id": "one",
                "count": 2,
                "score": 1.5,
                "enabled": True,
                "captured_at": captured,
                "payload": {"labels": ["a", "b"]},
            },
            {
                "id": "two",
                "count": 4,
                "score": 2.5,
                "enabled": False,
                "captured_at": captured,
                "payload": {"labels": []},
            },
        )

    spec = ProjectionSpec(
        member="typed_rows",
        columns=(
            "id",
            "count",
            "score",
            "enabled",
            "captured_at",
            "payload",
        ),
        column_schema=(
            ProjectionColumn(name="id", type=ProjectionColumnType.TEXT),
            ProjectionColumn(name="count", type=ProjectionColumnType.INTEGER),
            ProjectionColumn(name="score", type=ProjectionColumnType.NUMERIC),
            ProjectionColumn(
                name="enabled", type=ProjectionColumnType.BOOLEAN
            ),
            ProjectionColumn(
                name="captured_at", type=ProjectionColumnType.TIMESTAMP
            ),
            ProjectionColumn(name="payload", type=ProjectionColumnType.JSON),
        ),
        unique_key=("id",),
        full_rebuild_builder=typed_rows,
    )
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id="typed-remote",
        table_name="typed_remote_state",
    )
    database = tmp_path / "typed.duckdb"
    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(database),
            bundle_key="typed",
            full_rebuild=True,
            projections=(spec,),
        ),
        remote_destinations=(fence,),
    )
    assert [item.status for item in result.destinations] == [
        "PROMOTED",
        "PROMOTED",
    ]

    with duckdb.connect(str(database), read_only=True) as connection:
        pointer = connection.execute(
            "SELECT table_name FROM __dr_platform_export_members "
            "WHERE bundle_key = 'typed' AND member = 'typed_rows'"
        ).fetchone()
        assert pointer is not None
        table_name = pointer[0]
        aggregate = connection.execute(
            f'SELECT sum("count"), avg(score) FROM "{table_name}"'  # noqa: S608
        ).fetchone()
        local_row = connection.execute(
            f"SELECT enabled, epoch(captured_at), payload "  # noqa: S608
            f'FROM "{table_name}" '
            "WHERE id = 'one'"
        ).fetchone()
    assert aggregate == (6, 2.0)
    assert local_row == (
        True,
        captured.timestamp(),
        '{"labels":["a","b"]}',
    )

    pin = pin_local_bundle(database, bundle_key="typed", pin_id="typed-local")
    local_table = resolve_local_pin(database, pin).members["typed_rows"]
    with duckdb.connect(str(database), read_only=True) as connection:
        pinned_aggregate = connection.execute(
            f'SELECT sum("count"), avg(score) FROM "{local_table}"'  # noqa: S608
        ).fetchone()
        pinned_query = (
            "SELECT enabled, epoch(captured_at), payload "  # noqa: S608
            f'FROM "{local_table}" '
            "WHERE id = 'one'"
        )
        pinned_row = connection.execute(pinned_query).fetchone()
    assert pinned_aggregate == (6, 2.0)
    assert pinned_row == (
        True,
        captured.timestamp(),
        '{"labels":["a","b"]}',
    )

    pin = fence.pin_bundle(bundle_key="typed", pin_id="typed-fixture")
    remote_table = fence.resolve_pin(pin).members["typed_rows"]
    with pg_engine.connect() as connection:
        aggregate = connection.execute(
            text(f"SELECT sum(count), avg(score) FROM {remote_table}")  # noqa: S608
        ).one()
        remote_row = connection.execute(
            text(
                f"SELECT enabled, captured_at, payload FROM {remote_table} "  # noqa: S608
                "WHERE id = 'one'"
            )
        ).one()
    assert tuple(aggregate) == (6, 2.0)
    assert remote_row[0] is True
    assert remote_row[1] == captured
    assert remote_row[2] == {"labels": ["a", "b"]}

    with duckdb.connect(str(database)) as connection:
        connection.execute(
            f"UPDATE \"{local_table}\" SET score = 9.5 WHERE id = 'one'"  # noqa: S608
        )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        resolve_local_pin(
            database,
            pin_local_bundle(
                database, bundle_key="typed", pin_id="typed-local-tampered"
            ),
        )

    with duckdb.connect(str(database)) as connection:
        connection.execute(
            f"UPDATE \"{local_table}\" SET score = 1.5 WHERE id = 'one'"  # noqa: S608
        )
        manifest_row = connection.execute(
            "SELECT manifest_json FROM __dr_platform_export_bundles "
            "WHERE bundle_key = 'typed'"
        ).fetchone()
        assert manifest_row is not None
        original_manifest = json.loads(manifest_row[0])

    for pin_id, mutate_schema in (
        (
            "typed-local-type-drift",
            lambda schema: [
                {**column, "type": "text"}
                if column["name"] == "score"
                else column
                for column in schema
            ],
        ),
        (
            "typed-local-order-drift",
            lambda schema: [schema[1], schema[0], *schema[2:]],
        ),
    ):
        tampered_manifest = json.loads(json.dumps(original_manifest))
        facts = tampered_manifest["typed_rows"]
        facts["column_schema"] = mutate_schema(facts["column_schema"])
        with duckdb.connect(str(database)) as connection:
            connection.execute(
                "UPDATE __dr_platform_export_bundles SET manifest_json = ? "
                "WHERE bundle_key = 'typed'",
                [json.dumps(tampered_manifest)],
            )
        with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
            resolve_local_pin(
                database,
                pin_local_bundle(database, bundle_key="typed", pin_id=pin_id),
            )


def test_application_projection_schema_and_values_fail_closed(
    pg_engine: Engine, tmp_path
) -> None:
    upgrade_platform_schema(str(pg_engine.url))

    def invalid_value(
        _connection: Connection, _snapshot: ApplicationSnapshot
    ) -> tuple[dict[str, float], ...]:
        return ({"score": float("nan")},)

    with pytest.raises(ValueError, match="schema must type every column"):
        export(
            pg_engine,
            ExportOptions(
                destination_path=str(tmp_path / "missing-schema.duckdb"),
                full_rebuild=True,
                projections=(
                    ProjectionSpec(
                        member="rows",
                        columns=("id",),
                        unique_key=("id",),
                        full_rebuild_builder=invalid_value,
                    ),
                ),
            ),
        )

    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(tmp_path / "invalid-value.duckdb"),
            full_rebuild=True,
            projections=(
                ProjectionSpec(
                    member="rows",
                    columns=("score",),
                    column_schema=(
                        ProjectionColumn(
                            name="score", type=ProjectionColumnType.NUMERIC
                        ),
                    ),
                    unique_key=("score",),
                    full_rebuild_builder=invalid_value,
                ),
            ),
        ),
    )
    assert result.destinations[0].status == "FAILED"
    assert result.destinations[0].error == "ValueError"


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
    assert row == ("running", 1)
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

    mutable_counts = cast("dict[str, int]", result.member_counts)
    with pytest.raises(TypeError):
        mutable_counts[schema.operations.name] = 2


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
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                execution_state="succeeded",
                enqueued_at=text("clock_timestamp()"),
                terminal_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
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


def test_export_drives_terminal_reconciliation_before_capture(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="reconciled-export",
        item_keys=("item",),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                execution_state="active",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
        )

    class TerminalReader(_LifecycleReader):
        def observe(self, *, workflow_id: str) -> ReconciliationObservation:
            return ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.SUCCEEDED,
                dbos_status=DbosWorkflowStatus.SUCCESS,
            )

    database = tmp_path / "reconciled.duckdb"
    result = _export(
        pg_engine,
        ExportOptions(destination_path=str(database)),
        schema=schema,
        reconciliation=_reconciliation(
            pg_engine, reader=TerminalReader(), page_size=1
        ),
    )
    assert result.destinations[0].status == "PROMOTED"
    with duckdb.connect(str(database), read_only=True) as destination:
        table_name = destination.execute(
            "SELECT table_name FROM __dr_platform_export_members "
            "WHERE member = ?",
            [schema.item_attempts.name],
        ).fetchone()
        assert table_name is not None
        assert destination.execute(
            f'SELECT execution_state FROM "{table_name[0]}"'  # noqa: S608
        ).fetchone() == ("succeeded",)

    with pytest.raises(TypeError):
        _export(  # type: ignore[call-arg]
            pg_engine,
            ExportOptions(
                destination_path=str(tmp_path / "missing-dependencies.duckdb")
            ),
        )
    with pytest.raises(ValueError, match="all platform operations"):
        replace(
            _reconciliation(pg_engine),
            options=ReconcileOptions(operation_key="one-operation"),
        )


def test_reconciliation_failure_and_bound_exhaustion_are_destination_outcomes(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="failed-export-reconciliation",
        item_keys=("item",),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            update(schema.item_attempts).values(
                enqueue_state="enqueued",
                execution_state="active",
                enqueued_at=text("clock_timestamp()"),
                effective_service_priority=1000,
                priority_source="enqueued_here",
                updated_at=text("clock_timestamp()"),
            )
        )

    remote = PostgresPublicationFence(
        pg_engine,
        destination_id="remote-reconciliation-failure",
        table_name="remote_reconciliation_failure_state",
    )

    class UnavailableReader(_LifecycleReader):
        def observe(self, *, workflow_id: str) -> ReconciliationObservation:
            del workflow_id
            raise RuntimeError("DBOS lifecycle unavailable")

    failed = _export(
        pg_engine,
        ExportOptions(
            destination_path=str(tmp_path / "failed-reconciliation.duckdb")
        ),
        schema=schema,
        reconciliation=_reconciliation(pg_engine, reader=UnavailableReader()),
        remote_destinations=(remote,),
    )
    assert [outcome.status for outcome in failed.destinations] == [
        "FAILED",
        "FAILED",
    ]
    assert {outcome.error for outcome in failed.destinations} == {
        "RuntimeError"
    }

    class ActiveReader(_LifecycleReader):
        def observe(self, *, workflow_id: str) -> ReconciliationObservation:
            return ReconciliationObservation(
                workflow_id=workflow_id,
                disposition=ReconciliationObservationDisposition.ACTIVE,
                dbos_status=DbosWorkflowStatus.PENDING,
            )

    exhausted = _export(
        pg_engine,
        ExportOptions(destination_path=str(tmp_path / "bounded.duckdb")),
        schema=schema,
        reconciliation=_reconciliation(
            pg_engine,
            reader=ActiveReader(),
            page_size=1,
            max_cycles=2,
        ),
        remote_destinations=(remote,),
    )
    assert [outcome.status for outcome in exhausted.destinations] == [
        "FAILED",
        "FAILED",
    ]
    assert {outcome.error for outcome in exhausted.destinations} == {
        "IncompleteExportReconciliationError"
    }


def test_kernel_remote_stages_every_member_and_retries_independently(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="remote-kernel",
        item_keys=("item",),
    )

    class FailingFence(PostgresPublicationFence):
        def promote(self, **_kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError("transient destination failure")

    failing = FailingFence(
        pg_engine,
        destination_id="remote-kernel",
        table_name="remote_kernel_state",
    )
    options = ExportOptions(
        destination_path=str(tmp_path / "remote-kernel.duckdb"),
        run_id="remote_kernel_retry",
    )
    first = export(
        pg_engine,
        options,
        schema=schema,
        remote_destinations=(failing,),
    )
    assert [item.status for item in first.destinations] == [
        "PROMOTED",
        "FAILED",
    ], first

    recovered = PostgresPublicationFence(
        pg_engine,
        destination_id="remote-kernel",
        table_name="remote_kernel_state",
    )
    retry = export(
        pg_engine,
        options,
        schema=schema,
        remote_destinations=(recovered,),
    )
    assert [item.status for item in retry.destinations] == [
        "PROMOTED",
        "PROMOTED",
    ]
    pin = recovered.pin_bundle(
        bundle_key=options.bundle_key, pin_id="kernel-members"
    )
    resolved = recovered.resolve_pin(pin)
    assert set(resolved.members) == {
        "platform_operations",
        "platform_items",
        "platform_item_attempts",
        "platform_enqueue_claims",
        "platform_next_attempt_requests",
        "platform_enqueue_compensations",
        "platform_enqueue_compensation_hazards",
        "platform_throttle_state",
        "platform_missing_reobservations",
    }


def test_kernel_remote_runs_when_local_destination_is_unavailable(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="remote-with-local-held",
        item_keys=("item",),
    )
    database = tmp_path / "local-held.duckdb"
    with duckdb.connect(str(database)) as destination:
        _create_destination_tables(destination)
        assert (
            _acquire_lease(
                destination,
                ExportOptions(
                    destination_path=str(database), run_id="foreign_owner"
                ),
            )
            is not None
        )

    fence = PostgresPublicationFence(
        pg_engine,
        destination_id="remote-while-local-held",
        table_name="remote_while_local_held_state",
    )
    result = export(
        pg_engine,
        ExportOptions(
            destination_path=str(database), run_id="remote_only_runner"
        ),
        schema=schema,
        remote_destinations=(fence,),
    )
    assert [item.status for item in result.destinations] == [
        "LEASE_HELD",
        "PROMOTED",
    ]
    resolved = fence.resolve_pin(
        fence.pin_bundle(bundle_key="platform-kernel", pin_id="remote-only")
    )
    assert set(resolved.members) == {
        "platform_operations",
        "platform_items",
        "platform_item_attempts",
        "platform_enqueue_claims",
        "platform_next_attempt_requests",
        "platform_enqueue_compensations",
        "platform_enqueue_compensation_hazards",
        "platform_throttle_state",
        "platform_missing_reobservations",
    }
