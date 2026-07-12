"""P7b remote fence, compatibility, and pin/cleanup coverage."""

from __future__ import annotations

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
    SourceCoordinate,
    check_snapshot_compatibility,
    cleanup_local_bundles,
    export,
    pin_local_bundle,
    require_compatible_snapshot,
    resolve_local_pin,
    upgrade_platform_schema,
)
from tests.contracts.test_platform_v6_cancellation import _register_operation


def test_compatibility_uses_truthful_timestamps_not_equal_sequences() -> None:
    captured = datetime(2026, 1, 1, tzinfo=UTC)
    application = SourceCoordinate(
        source_id="application",
        captured_at=captured,
        snapshot_seq=9,
    )
    dbos = SourceCoordinate(
        source_id="dbos",
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


@pytest.mark.parametrize("kind", ["motherduck", "neon"])
def test_postgres_fence_rejects_stale_stage_and_uses_returning(
    pg_engine: Engine, kind: str
) -> None:
    suffix = uuid4().hex[:12]
    state_table = f"publication_state_{suffix}"
    staged_table = f"publication_stage_{suffix}"
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"{kind}-test",
        table_name=state_table,
        kind=cast("Literal['motherduck', 'neon']", kind),
    )
    fence.ensure_schema()
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

        def create_stage(connection: Connection) -> None:
            connection.execute(
                text(f'CREATE TABLE "{staged_table}" (id BIGINT)')
            )

        stale = fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=1,
            snapshot_seq=1,
            bundle_id="stale",
            cursors={"member": 1},
            checksums={"member": "one"},
            stage=create_stage,
        )
        assert stale.disposition == "STALE_PROMOTION"
        with pg_engine.connect() as connection:
            exists = connection.execute(
                text("SELECT to_regclass(:name)"), {"name": staged_table}
            ).scalar_one()
        assert exists is None

        promoted = fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=2,
            snapshot_seq=1,
            bundle_id="bundle-one",
            cursors={"member": 1},
            checksums={"member": "one"},
        )
        assert promoted.disposition == "PROMOTED"

        replay = fence.acquire_lease(
            bundle_key="analysis", run_id="replay", lease_seconds=60
        )
        assert replay.fencing_token == 3
        idempotent = fence.promote(
            bundle_key="analysis",
            run_id="replay",
            fencing_token=3,
            snapshot_seq=1,
            bundle_id="ignored-replay-stage",
            cursors={"member": 1},
            checksums={"member": "one"},
        )
        assert idempotent.disposition == "IDEMPOTENT"
        assert idempotent.bundle_id == "bundle-one"
    finally:
        with pg_engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{staged_table}"'))
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
