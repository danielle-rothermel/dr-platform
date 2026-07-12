"""P7b remote fence, compatibility, and pin/cleanup coverage."""
# ruff: noqa: PLR0915, S608

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import Connection, Engine, text

from dr_platform import (
    ExportOptions,
    IncompatibleSnapshotError,
    PinnedBundleGoneError,
    PlatformSchema,
    PostgresPublicationFence,
    RemoteBundleManifest,
    RemoteBundleMember,
    SourceCoordinate,
    capture_source_coordinate,
    check_snapshot_compatibility,
    cleanup_local_bundles,
    export,
    pin_local_bundle,
    require_compatible_snapshot,
    resolve_local_pin,
    upgrade_platform_schema,
)
from tests.contracts.test_platform_v6_cancellation import _register_operation

_EMPTY_CHECKSUM = hashlib.sha256(b"[]").hexdigest()


def _empty_candidate_manifest(
    connection: Connection, table_name: str
) -> RemoteBundleManifest:
    connection.execute(text(f'CREATE TABLE "{table_name}" (id BIGINT)'))
    return RemoteBundleManifest(
        source_families=("application",),
        members=(
            RemoteBundleMember(
                member="member",
                table_name=table_name,
                key_columns=("id",),
                row_count=0,
                checksum=_EMPTY_CHECKSUM,
            ),
        ),
    )


def test_compatibility_uses_truthful_timestamps_not_equal_sequences() -> None:
    captured = datetime(2026, 1, 1, tzinfo=UTC)
    application = SourceCoordinate(
        source_id="application",
        database_server="application-db",
        captured_at=captured,
        snapshot_seq=9,
    )
    dbos = SourceCoordinate(
        source_id="dbos",
        database_server="dbos-db",
        captured_at=captured + timedelta(milliseconds=101),
        snapshot_seq=9,
    )

    result = check_snapshot_compatibility((application, dbos))
    assert result.disposition == "INCOMPATIBLE"
    assert result.observed_skew_ms == pytest.approx(101)
    with pytest.raises(IncompatibleSnapshotError):
        require_compatible_snapshot((application, dbos))

    compatible = require_compatible_snapshot(
        (
            application,
            dbos.model_copy(
                update={"captured_at": captured + timedelta(milliseconds=100)}
            ),
        )
    )
    assert compatible.disposition == "COMPATIBLE"


def test_combined_remote_promotion_fails_before_staging(
    pg_engine: Engine,
) -> None:
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"compatibility-{uuid4().hex}",
        table_name=f"publication_state_{uuid4().hex[:12]}",
    )
    fence.ensure_schema()
    captured = datetime(2026, 1, 1, tzinfo=UTC)
    coordinates = (
        SourceCoordinate(
            source_id="application:one",
            database_server="shared-server",
            captured_at=captured,
        ),
        SourceCoordinate(
            source_id="dbos:one",
            database_server="dbos-server",
            captured_at=captured + timedelta(milliseconds=101),
        ),
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    staged = False

    def stage(_connection: Connection) -> RemoteBundleManifest:
        nonlocal staged
        staged = True
        raise AssertionError("compatibility must be checked before staging")

    with pytest.raises(IncompatibleSnapshotError) as raised:
        fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=lease.fencing_token,
            snapshot_seq=1,
            bundle_id="candidate",
            cursors={},
            source_coordinates=coordinates,
            source_families=("application", "dbos"),
            stage=stage,
        )
    assert raised.value.result.disposition == "INCOMPATIBLE"
    assert not staged

    second_lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert second_lease.fencing_token is not None
    with pytest.raises(IncompatibleSnapshotError) as missing:
        fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=second_lease.fencing_token,
            snapshot_seq=1,
            bundle_id="missing-coordinate",
            cursors={},
            source_coordinates=coordinates[:1],
            source_families=("application", "dbos"),
            stage=stage,
        )
    assert missing.value.result.disposition == "MISSING_COORDINATE"
    assert not staged


def test_combined_remote_promotion_accepts_same_database_source_families(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"same-database-{suffix}",
        table_name=f"same_database_state_{suffix}",
    )
    fence.ensure_schema()
    captured = datetime.now(UTC) - timedelta(seconds=1)
    coordinates = (
        SourceCoordinate(
            source_id="application:shared",
            database_server="shared",
            captured_at=captured,
        ),
        SourceCoordinate(
            source_id="dbos:shared",
            database_server="shared",
            captured_at=captured + timedelta(milliseconds=1),
        ),
    )
    lease = fence.acquire_lease(
        bundle_key="combined", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="owner",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )

    def stage(connection: Connection) -> RemoteBundleManifest:
        connection.execute(text(f'CREATE TABLE "{table_name}" (id BIGINT)'))
        return RemoteBundleManifest(
            source_families=("application", "dbos"),
            members=(
                RemoteBundleMember(
                    member="member",
                    table_name=table_name,
                    key_columns=("id",),
                    row_count=0,
                    checksum=_EMPTY_CHECKSUM,
                ),
            ),
        )

    result = fence.promote(
        bundle_key="combined",
        run_id="owner",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
        bundle_id="same-database",
        cursors={},
        source_coordinates=coordinates,
        source_families=("application", "dbos"),
        stage=stage,
    )
    assert result.disposition == "PROMOTED"


@pytest.mark.parametrize(
    "coordinates",
    [
        (
            SourceCoordinate(
                source_id="application:one",
                database_server="shared",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            SourceCoordinate(
                source_id="application:two",
                database_server="shared",
                captured_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
        ),
        (
            SourceCoordinate(
                source_id="application:one",
                database_server="shared",
                captured_at=datetime.now(UTC) + timedelta(hours=1),
            ),
            SourceCoordinate(
                source_id="dbos:one",
                database_server="shared",
                captured_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        ),
    ],
    ids=("duplicate-family", "future-capture"),
)
def test_combined_remote_promotion_rejects_invalid_coordinates(
    pg_engine: Engine, coordinates: tuple[SourceCoordinate, ...]
) -> None:
    suffix = uuid4().hex[:12]
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"invalid-family-{suffix}",
        table_name=f"invalid_family_state_{suffix}",
    )
    fence.ensure_schema()
    lease = fence.acquire_lease(
        bundle_key="combined", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    staged = False

    def stage(_connection: Connection) -> RemoteBundleManifest:
        nonlocal staged
        staged = True
        raise AssertionError("coordinate validation must precede staging")

    with pytest.raises(IncompatibleSnapshotError):
        fence.promote(
            bundle_key="combined",
            run_id="owner",
            fencing_token=lease.fencing_token,
            snapshot_seq=1,
            bundle_id="invalid",
            cursors={},
            source_coordinates=coordinates,
            source_families=("application", "dbos"),
            stage=stage,
        )
    assert not staged


def test_remote_bad_builder_cannot_name_an_unowned_candidate_table(
    pg_engine: Engine,
) -> None:
    state_table = f"publication_state_{uuid4().hex[:12]}"
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"bad-builder-{uuid4().hex}",
        table_name=state_table,
        kind="motherduck",
    )
    fence.ensure_schema()
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    coordinate = SourceCoordinate(
        source_id="application:server",
        database_server="application-server",
        captured_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="outside its candidate"):
        fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=lease.fencing_token,
            snapshot_seq=1,
            bundle_id="bad",
            cursors={},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=lambda _connection: RemoteBundleManifest(
                source_families=("application",),
                members=(
                    RemoteBundleMember(
                        member="member",
                        table_name="not_candidate_owned",
                        key_columns=("id",),
                        row_count=0,
                        checksum=_EMPTY_CHECKSUM,
                    ),
                ),
            ),
        )
    with pg_engine.connect() as connection:
        committed = connection.execute(
            text(
                f'SELECT committed_snapshot_seq FROM "{state_table}" '
                "WHERE bundle_key = 'analysis'"
            )
        ).scalar_one()
    assert committed == 0


@pytest.mark.parametrize("kind", ["motherduck", "neon"])
def test_postgres_fence_rejects_stale_stage_and_uses_returning(
    pg_engine: Engine, kind: str
) -> None:
    suffix = uuid4().hex[:12]
    state_table = f"publication_state_{suffix}"
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"{kind}-test",
        table_name=state_table,
        kind=cast("Literal['motherduck', 'neon']", kind),
    )
    fence.ensure_schema()
    staged_table = fence.stage_table_name(
        member="member", run_id="owner", fencing_token=1, snapshot_seq=1
    )
    expired_table = fence.stage_table_name(
        member="member", run_id="owner", fencing_token=2, snapshot_seq=1
    )
    promoted_table = fence.stage_table_name(
        member="member", run_id="owner", fencing_token=3, snapshot_seq=1
    )
    replay_table = fence.stage_table_name(
        member="member", run_id="replay", fencing_token=4, snapshot_seq=1
    )
    try:
        first = fence.acquire_lease(
            bundle_key="analysis", run_id="owner", lease_seconds=60
        )
        assert first.disposition == "ACQUIRED"
        assert first.fencing_token == 1
        held = fence.acquire_lease(
            bundle_key="analysis", run_id="other", lease_seconds=60
        )
        assert held.disposition == "LEASE_HELD"
        current = fence.acquire_lease(
            bundle_key="analysis", run_id="owner", lease_seconds=60
        )
        assert current.fencing_token == 2
        assert not fence.renew_lease(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=1,
            lease_seconds=60,
        )

        coordinate = capture_source_coordinate(
            pg_engine, source_id="application", snapshot_seq=1
        )

        def create_stale_stage(
            connection: Connection,
        ) -> RemoteBundleManifest:
            connection.execute(
                text(f'CREATE TABLE "{staged_table}" (id BIGINT)')
            )
            return RemoteBundleManifest(
                source_families=("application",),
                members=(
                    RemoteBundleMember(
                        member="member",
                        table_name=staged_table,
                        key_columns=("id",),
                        row_count=0,
                        checksum=_EMPTY_CHECKSUM,
                    ),
                ),
            )

        stale = fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=1,
            snapshot_seq=1,
            bundle_id="stale",
            cursors={"member": 1},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=create_stale_stage,
        )
        assert stale.disposition == "STALE_PROMOTION"
        with pg_engine.connect() as connection:
            exists = connection.execute(
                text("SELECT to_regclass(:name)"), {"name": staged_table}
            ).scalar_one()
        assert exists is None

        def expire_during_stage(
            connection: Connection,
        ) -> RemoteBundleManifest:
            connection.execute(
                text(f'CREATE TABLE "{expired_table}" (id BIGINT)')
            )
            connection.execute(
                text(
                    f'UPDATE "{state_table}" SET lease_expires_at = '
                    "CURRENT_TIMESTAMP - INTERVAL '1 second' "
                    "WHERE destination_id = :destination "
                    "AND bundle_key = 'analysis'"
                ),
                {"destination": f"{kind}-test"},
            )
            return RemoteBundleManifest(
                source_families=("application",),
                members=(
                    RemoteBundleMember(
                        member="member",
                        table_name=expired_table,
                        key_columns=("id",),
                        row_count=0,
                        checksum=_EMPTY_CHECKSUM,
                    ),
                ),
            )

        expired = fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=2,
            snapshot_seq=1,
            bundle_id="expired-stage",
            cursors={"member": 1},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=expire_during_stage,
        )
        assert expired.disposition == "STALE_PROMOTION"
        current = fence.acquire_lease(
            bundle_key="analysis", run_id="owner", lease_seconds=60
        )
        assert current.fencing_token == 3

        def create_promoted_stage(
            connection: Connection,
        ) -> RemoteBundleManifest:
            connection.execute(
                text(f'CREATE TABLE "{promoted_table}" (id BIGINT)')
            )
            return RemoteBundleManifest(
                source_families=("application",),
                members=(
                    RemoteBundleMember(
                        member="member",
                        table_name=promoted_table,
                        key_columns=("id",),
                        row_count=0,
                        checksum=_EMPTY_CHECKSUM,
                    ),
                ),
            )

        promoted = fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=3,
            snapshot_seq=1,
            bundle_id="bundle-one",
            cursors={"member": 1},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=create_promoted_stage,
        )
        assert promoted.disposition == "PROMOTED"

        replay = fence.acquire_lease(
            bundle_key="analysis", run_id="replay", lease_seconds=60
        )
        assert replay.fencing_token == 4
        idempotent = fence.promote(
            bundle_key="analysis",
            run_id="replay",
            fencing_token=4,
            snapshot_seq=1,
            bundle_id="ignored-replay-stage",
            cursors={"member": 1},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=lambda connection: _empty_candidate_manifest(
                connection, replay_table
            ),
        )
        assert idempotent.disposition == "IDEMPOTENT"
        assert idempotent.bundle_id == "bundle-one"

        pin = fence.pin_bundle(bundle_key="analysis", ttl_seconds=60)
        resolved = fence.resolve_pin(pin)
        assert resolved.bundle_id == "bundle-one"
        assert resolved.members == {"member": f"public.{promoted_table}"}
        with pg_engine.begin() as connection:
            connection.execute(
                text(f'INSERT INTO "{promoted_table}" VALUES (1)')
            )
        with pytest.raises(PinnedBundleGoneError):
            fence.resolve_pin(pin)
        with pg_engine.begin() as connection:
            connection.execute(text(f'DELETE FROM "{promoted_table}"'))

        cleaner = fence.acquire_lease(
            bundle_key="analysis", run_id="cleaner", lease_seconds=60
        )
        assert cleaner.fencing_token == 5
        deleted = fence.cleanup_bundles(
            bundle_key="analysis",
            run_id="cleaner",
            fencing_token=5,
        )
        assert "bundle-one" not in deleted
        assert fence.resolve_pin(pin).bundle_id == "bundle-one"
    finally:
        with pg_engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{staged_table}"'))
            connection.execute(text(f'DROP TABLE IF EXISTS "{expired_table}"'))
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{promoted_table}"')
            )
            connection.execute(text(f'DROP TABLE IF EXISTS "{replay_table}"'))
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{state_table}_pins"')
            )
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{state_table}_bundles"')
            )
            connection.execute(text(f'DROP TABLE IF EXISTS "{state_table}"'))


def test_active_pin_survives_cleanup_then_missing_bundle_is_typed(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(str(pg_engine.url))
    _register_operation(
        pg_engine,
        schema,
        operation_key="pin-operation",
        item_keys=("pin-item",),
    )
    database = tmp_path / "pins.duckdb"
    first = export(
        pg_engine, ExportOptions(destination_path=str(database)), schema=schema
    )
    pin = pin_local_bundle(database, bundle_id=first.destinations[0].bundle_id)
    second = export(
        pg_engine, ExportOptions(destination_path=str(database)), schema=schema
    )
    assert second.snapshot_seq > first.snapshot_seq

    assert cleanup_local_bundles(database) == ()
    resolved = resolve_local_pin(database, pin)
    assert resolved.bundle_id == first.destinations[0].bundle_id
    assert schema.operations.name in resolved.members

    with duckdb.connect(str(database)) as destination:
        destination.execute(
            "UPDATE __dr_platform_export_pins SET expires_at = 0 "
            "WHERE pin_id = ?",
            [pin.pin_id],
        )
    assert cleanup_local_bundles(database) == (
        first.destinations[0].bundle_id,
    )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        resolve_local_pin(database, pin)
