"""P7b remote fence, compatibility, and pin/cleanup coverage."""
# ruff: noqa: E501, PLR0913, PLR0915, S608

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier, Event
from typing import Literal
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from dr_platform import (
    BundlePin,
    ExportOptions,
    IncompatibleSnapshotError,
    PinnedBundleGoneError,
    PlatformSchema,
    PostgresPublicationFence,
    PreparedStage,
    PublicationOperationIdentity,
    PublicationReceipt,
    RemoteBundleManifest,
    RemoteBundleMember,
    RemotePromotionResult,
    SourceCoordinate,
    backfill_local_protected_integrity,
    capture_source_coordinate,
    check_snapshot_compatibility,
    cleanup_local_bundles,
    pin_local_bundle,
    require_compatible_snapshot,
    resolve_local_pin,
    upgrade_platform_schema,
)
from dr_platform import (
    export as _export,
)
from dr_platform.export import _canonical
from dr_platform.publication import _StalePromotionError
from tests.conftest import engine_dsn, signed_integrity_test_material
from tests.contracts.test_platform_v6_cancellation import _register_operation
from tests.test_export import _reconciliation

_EMPTY_CHECKSUM = hashlib.sha256(b"[]").hexdigest()


def export(source: Engine, options: ExportOptions, **kwargs):  # type: ignore[no-untyped-def]
    if options.integrity_signer is None:
        options = options.model_copy(
            update={"integrity_signer": signed_integrity_test_material()[0]}
        )
    return _export(
        source,
        options,
        reconciliation=_reconciliation(source),
        **kwargs,
    )


def _empty_candidate_manifest(
    connection: Connection, table_name: str, schema_name: str = "public"
) -> RemoteBundleManifest:
    connection.execute(text(f'CREATE TABLE "{table_name}" (id BIGINT)'))
    return RemoteBundleManifest(
        source_families=("application",),
        members=(
            RemoteBundleMember(
                member="member",
                schema_name=schema_name,
                table_name=table_name,
                key_columns=("id",),
                row_count=0,
                checksum=_EMPTY_CHECKSUM,
            ),
        ),
    )


def _promoted_operation_fixture(
    pg_engine: Engine, suffix: str
) -> tuple[
    PostgresPublicationFence,
    PublicationOperationIdentity,
    PublicationReceipt,
]:
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"race-{suffix}",
        table_name=f"race_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"race-{uuid4().hex}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    member = RemoteBundleMember(
        member="member",
        table_name=table_name,
        key_columns=("id",),
        row_count=0,
        checksum=_EMPTY_CHECKSUM,
    )
    promoted = fence.promote(
        bundle_key="analysis",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
        bundle_id=f"bundle-{suffix}",
        cursors={"member": 1},
        source_coordinates=(
            capture_source_coordinate(
                pg_engine, source_id="application", snapshot_seq=1
            ),
        ),
        source_families=("application",),
        stage=lambda connection, _prepared: _empty_candidate_manifest(
            connection, table_name
        ),
        operation_identity=identity,
        stage_plan=RemoteBundleManifest(
            members=(member,), source_families=("application",)
        ),
    )
    assert promoted.receipt is not None
    return fence, identity, promoted.receipt


def test_two_connection_pin_cleanup_race_serializes_at_gate(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, receipt = _promoted_operation_fixture(pg_engine, suffix)
    barrier = Barrier(2)

    def pin() -> str:
        barrier.wait()
        try:
            fence.pin_bundle(bundle_key="analysis", pin_id="racing-reader")
        except PinnedBundleGoneError:
            return "GONE"
        return "PINNED"

    def cleanup() -> str:
        barrier.wait()
        return fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            receipt.stage_plan_digest,
        ).disposition

    with ThreadPoolExecutor(max_workers=2) as executor:
        pin_future = executor.submit(pin)
        cleanup_future = executor.submit(cleanup)
        outcome = (pin_future.result(), cleanup_future.result())
    assert outcome in {
        ("PINNED", "BLOCKED_EXTERNAL_PIN"),
        ("GONE", "CLEANED"),
    }


def test_cleanup_retries_a_real_serialization_conflict(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, receipt = _promoted_operation_fixture(pg_engine, suffix)
    before_gate = Event()
    pin_committed = Event()
    armed = True

    def fault(boundary: str) -> None:
        nonlocal armed
        if boundary == "before_cleanup_gate" and armed:
            armed = False
            before_gate.set()
            assert pin_committed.wait(timeout=5)

    object.__setattr__(fence, "fault_hook", fault)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            fence.cleanup_operation,
            identity.operation_id,
            "cleanup",
            receipt.stage_plan_digest,
        )
        assert before_gate.wait(timeout=5)
        fence.pin_bundle(bundle_key="analysis", pin_id="serialization-reader")
        pin_committed.set()
        result = future.result(timeout=5)
    # The first SERIALIZABLE snapshot saw no pin. This outcome is possible
    # only after PostgreSQL aborts that gate update and Platform retries with a
    # fresh snapshot that observes the committed external pin.
    assert result.disposition == "BLOCKED_EXTERNAL_PIN"


def _paused_stage_publisher(
    pg_engine: Engine, suffix: str
) -> tuple[
    PostgresPublicationFence,
    PublicationOperationIdentity,
    str,
    Event,
    Event,
    Callable[[], str],
]:
    """A publisher whose stage transaction pauses before the gate CAS."""

    signer, key_ring = signed_integrity_test_material()
    before_stage = Event()
    stage_may_proceed = Event()

    def fault(boundary: str) -> None:
        if boundary == "before_stage_gate":
            before_stage.set()
            assert stage_may_proceed.wait(timeout=10)

    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"stage-cleanup-{suffix}",
        table_name=f"stage_cleanup_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
        fault_hook=fault,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"stage-cleanup-{suffix}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt", lease_seconds=60
    )
    assert lease.fencing_token is not None
    token = lease.fencing_token
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt",
        fencing_token=token,
        snapshot_seq=1,
    )
    plan = RemoteBundleManifest(
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

    def publish() -> str:
        return fence.promote(
            bundle_key="analysis",
            run_id="attempt",
            fencing_token=token,
            snapshot_seq=1,
            bundle_id=f"bundle-{suffix}",
            cursors={"member": 1},
            source_coordinates=(
                capture_source_coordinate(
                    pg_engine, source_id="application", snapshot_seq=1
                ),
            ),
            source_families=("application",),
            stage=lambda connection, _prepared: _empty_candidate_manifest(
                connection, table_name
            ),
            operation_identity=identity,
            stage_plan=plan,
        ).disposition

    return fence, identity, table_name, before_stage, stage_may_proceed, publish


def test_live_stage_lease_blocks_concurrent_cleanup(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, _table_name, before_stage, stage_may_proceed, publish = (
        _paused_stage_publisher(pg_engine, suffix)
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        publish_future = executor.submit(publish)
        assert before_stage.wait(timeout=5)
        observation = fence.observe_operation(identity.operation_id)
        assert observation.state == "STAGING"
        blocked = fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            str(observation.stage_plan_digest),
        )
        assert blocked.disposition == "BLOCKED_LEASE_HELD"
        assert fence.observe_operation(identity.operation_id).state == (
            "STAGING"
        )
        stage_may_proceed.set()
        assert publish_future.result(timeout=10) == "PROMOTED"
    final = fence.observe_operation(identity.operation_id)
    assert final.state == "PROMOTED"
    assert final.present_members == ("member",)


def test_expired_stage_lease_cannot_resurrect_after_cleanup(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, table_name, before_stage, stage_may_proceed, publish = (
        _paused_stage_publisher(pg_engine, suffix)
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        publish_future = executor.submit(publish)
        assert before_stage.wait(timeout=5)
        with pg_engine.begin() as connection:
            connection.execute(
                text(
                    f'UPDATE "{fence.table_name}" SET lease_expires_at = '
                    "clock_timestamp() - INTERVAL '1 second' "
                    "WHERE bundle_key = 'analysis'"
                )
            )
        observation = fence.observe_operation(identity.operation_id)
        cleaned = fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            str(observation.stage_plan_digest),
        )
        assert cleaned.disposition == "CLEANED"
        assert cleaned.observation.present_members == ()
        stage_may_proceed.set()
        # The publisher's same-transaction gate CAS finds no live lease, so
        # its pending CREATE TABLE rolls back with the stage transaction.
        assert publish_future.result(timeout=10) == "STALE_PROMOTION"
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regclass(:name)"), {"name": table_name}
            )
            is None
        )
    replay = fence.cleanup_operation(
        identity.operation_id,
        "cleanup",
        str(observation.stage_plan_digest),
    )
    assert replay.disposition == "CLEANED"
    assert replay.observation.present_members == ()


def test_retention_aborts_when_same_owner_reacquires_before_gate(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"retention-race-{suffix}",
        table_name=f"retention_race_state_{suffix}",
        signer=signed_integrity_test_material()[0],
    )
    fence.ensure_schema()
    first = fence.acquire_lease(
        bundle_key="analysis", run_id="retention", lease_seconds=60
    )
    assert first.fencing_token is not None
    first_token = first.fencing_token
    before_gate = Event()
    reacquired = Event()

    def fault(boundary: str) -> None:
        if boundary == "before_retention_gate":
            before_gate.set()
            assert reacquired.wait(timeout=10)

    object.__setattr__(fence, "fault_hook", fault)
    with ThreadPoolExecutor(max_workers=1) as executor:
        cleanup_future = executor.submit(
            fence.cleanup_bundles,
            bundle_key="analysis",
            run_id="retention",
            fencing_token=first_token,
        )
        assert before_gate.wait(timeout=5)
        second = fence.acquire_lease(
            bundle_key="analysis", run_id="retention", lease_seconds=60
        )
        assert second.fencing_token == first_token + 1
        reacquired.set()
        # The stale token's gate update matches zero rows (or hits a
        # serialization conflict whose retry re-reads the lost lease), so
        # retention aborts before any destructive statement.
        with pytest.raises(_StalePromotionError):
            cleanup_future.result(timeout=10)


def test_equal_snapshot_new_operation_commits_superseded_disposition(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, first_identity, receipt = _promoted_operation_fixture(
        pg_engine, suffix
    )
    second_identity = PublicationOperationIdentity(
        operation_id=f"superseded-{uuid4().hex}", attempt_id="attempt-b"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt-b", lease_seconds=60
    )
    assert lease.fencing_token is not None
    token = lease.fencing_token
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt-b",
        fencing_token=token,
        snapshot_seq=1,
    )
    plan = RemoteBundleManifest(
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

    def publish() -> RemotePromotionResult:
        return fence.promote(
            bundle_key="analysis",
            run_id="attempt-b",
            fencing_token=token,
            snapshot_seq=1,
            bundle_id=f"superseded-bundle-{suffix}",
            cursors={"member": 1},
            source_coordinates=(
                capture_source_coordinate(
                    pg_engine, source_id="application", snapshot_seq=1
                ),
            ),
            source_families=("application",),
            stage=lambda connection, _prepared: _empty_candidate_manifest(
                connection, table_name
            ),
            operation_identity=second_identity,
            stage_plan=plan,
        )

    superseded = publish()
    assert superseded.disposition == "SUPERSEDED"
    assert superseded.receipt is None
    assert superseded.bundle_id == f"superseded-bundle-{suffix}"
    assert superseded.stage_plan_digest is not None
    assert superseded.stage_plan_digest != receipt.stage_plan_digest

    observation = fence.observe_operation(second_identity.operation_id)
    assert observation.state == "SUPERSEDED"
    assert observation.owned_bundle_count == 1
    assert observation.present_members == ("member",)
    with pg_engine.connect() as connection:
        pointer_bundle, published_operation = connection.execute(
            text(
                f'SELECT bundle_id, published_operation_id FROM "{fence.table_name}" '
                "WHERE bundle_key = 'analysis'"
            )
        ).one()
    assert pointer_bundle == receipt.bundle_id
    assert published_operation == first_identity.operation_id

    replay = publish()
    assert replay.disposition == "SUPERSEDED"
    assert replay.receipt is None
    assert replay.stage_plan_digest == superseded.stage_plan_digest

    cleaned = fence.cleanup_operation(
        second_identity.operation_id,
        "superseded-cleanup",
        str(superseded.stage_plan_digest),
    )
    assert cleaned.disposition == "CLEANED"
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regclass(:name)"), {"name": table_name}
            )
            is None
        )
        pointer_bundle, published_operation = connection.execute(
            text(
                f'SELECT bundle_id, published_operation_id FROM "{fence.table_name}" '
                "WHERE bundle_key = 'analysis'"
            )
        ).one()
    assert pointer_bundle == receipt.bundle_id
    assert published_operation == first_identity.operation_id
    survivor = fence.observe_operation(first_identity.operation_id)
    assert survivor.state == "PROMOTED"
    assert survivor.current_pointer_relation == "CURRENT"
    assert survivor.present_members == ("member",)


def test_two_connection_successor_cleanup_race_preserves_winner(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, receipt = _promoted_operation_fixture(pg_engine, suffix)
    barrier = Barrier(2)
    coordinate = capture_source_coordinate(
        pg_engine, source_id="application", snapshot_seq=2
    )

    def successor() -> str:
        barrier.wait()
        lease = fence.acquire_lease(
            bundle_key="analysis", run_id="successor", lease_seconds=60
        )
        assert lease.fencing_token is not None
        table_name = fence.stage_table_name(
            member="member",
            run_id="successor",
            fencing_token=lease.fencing_token,
            snapshot_seq=2,
        )
        return fence.promote(
            bundle_key="analysis",
            run_id="successor",
            fencing_token=lease.fencing_token,
            snapshot_seq=2,
            bundle_id=f"successor-{suffix}",
            cursors={"member": 2},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=lambda connection: _empty_candidate_manifest(
                connection, table_name
            ),
        ).disposition

    def cleanup() -> str:
        barrier.wait()
        return fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            receipt.stage_plan_digest,
        ).disposition

    with ThreadPoolExecutor(max_workers=2) as executor:
        successor_future = executor.submit(successor)
        cleanup_future = executor.submit(cleanup)
        successor_outcome = successor_future.result()
        cleanup_outcome = cleanup_future.result()
    assert successor_outcome == "PROMOTED"
    assert cleanup_outcome in {"CLEANED", "BLOCKED_SUCCESSOR"}
    observation = fence.observe_operation(identity.operation_id)
    assert observation.current_pointer_relation == "SUCCESSOR"


def test_publication_operation_persists_receipt_and_cleans_exact_inventory(
    pg_engine: Engine,
) -> None:
    _require_pgcrypto(pg_engine)
    suffix = uuid4().hex[:12]
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"operation-{suffix}",
        table_name=f"operation_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"operation-{uuid4().hex}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id=identity.attempt_id, lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id=identity.attempt_id,
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    plan = RemoteBundleManifest(
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

    def stage(
        connection: Connection, prepared: PreparedStage
    ) -> RemoteBundleManifest:
        assert prepared.table_name("member") == table_name
        return _empty_candidate_manifest(connection, table_name)

    promoted = fence.promote(
        bundle_key="analysis",
        run_id=identity.attempt_id,
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
        bundle_id="operation-bundle",
        cursors={"member": 1},
        source_coordinates=(
            capture_source_coordinate(
                pg_engine, source_id="application", snapshot_seq=1
            ),
        ),
        source_families=("application",),
        stage=stage,
        operation_identity=identity,
        stage_plan=plan,
    )
    assert promoted.receipt is not None
    observation = fence.observe_operation(identity.operation_id)
    assert observation.state == "PROMOTED"
    assert observation.current_pointer_relation == "CURRENT"
    assert observation.present_members == ("member",)
    with pg_engine.connect() as connection:
        owner, published_operation_id = connection.execute(
            text(
                f'SELECT owner, published_operation_id FROM "{fence.table_name}" '
                "WHERE bundle_key = 'analysis'"
            )
        ).one()
    assert owner is None
    assert published_operation_id == identity.operation_id

    replay_lease = fence.acquire_lease(
        bundle_key="analysis", run_id=identity.attempt_id, lease_seconds=60
    )
    assert replay_lease.fencing_token is not None
    replay_table = fence.stage_table_name(
        member="member",
        run_id=identity.attempt_id,
        fencing_token=replay_lease.fencing_token,
        snapshot_seq=1,
    )
    replay = fence.promote(
        bundle_key="analysis",
        run_id=identity.attempt_id,
        fencing_token=replay_lease.fencing_token,
        snapshot_seq=1,
        bundle_id="ignored-replay-bundle",
        cursors={"member": 1},
        source_coordinates=(
            capture_source_coordinate(
                pg_engine, source_id="application", snapshot_seq=1
            ),
        ),
        source_families=("application",),
        stage=lambda *_args: pytest.fail(
            "published operation must not restage"
        ),
        operation_identity=identity,
        stage_plan=plan.model_copy(
            update={
                "members": (
                    plan.members[0].model_copy(
                        update={"table_name": replay_table}
                    ),
                )
            }
        ),
    )
    assert replay.disposition == "IDEMPOTENT"
    assert replay.receipt is not None
    assert (
        replay.receipt.stage_plan_digest == promoted.receipt.stage_plan_digest
    )
    assert (
        fence.cleanup_operation(
            identity.operation_id, "wrong-authority", "wrong-digest"
        ).disposition
        == "AUTHORITY_MISMATCH"
    )

    external = fence.pin_bundle(bundle_key="analysis", pin_id="reader")
    blocked = fence.cleanup_operation(
        identity.operation_id, "cleanup", promoted.receipt.stage_plan_digest
    )
    assert blocked.disposition == "BLOCKED_EXTERNAL_PIN"
    fence.release_pin(external)
    owned = fence.pin_bundle(
        bundle_key="analysis",
        pin_id="operation-pin",
        owner_operation_id=identity.operation_id,
    )
    assert owned.pin_id == "operation-pin"
    cleaned = fence.cleanup_operation(
        identity.operation_id, "cleanup", promoted.receipt.stage_plan_digest
    )
    assert cleaned.disposition == "CLEANED"
    assert cleaned.observation.present_members == ()
    assert (
        fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            promoted.receipt.stage_plan_digest,
        )
        == cleaned
    )
    already = fence.cleanup_operation(
        identity.operation_id,
        "different-request",
        promoted.receipt.stage_plan_digest,
    )
    assert already.disposition == "ALREADY_CLEANED"


@pytest.mark.parametrize(
    ("boundary", "expected_state"),
    [
        ("after_plan_commit", "PLANNED"),
        ("after_each_stage_member", "STAGING"),
        ("after_stage_commit", "STAGING"),
        ("after_promotion_commit", "PROMOTED"),
    ],
)
def test_publication_operation_crash_boundaries_replay_without_orphan(
    pg_engine: Engine, boundary: str, expected_state: str
) -> None:
    _require_pgcrypto(pg_engine)
    suffix = uuid4().hex[:12]
    armed = True

    def fault(observed: str) -> None:
        nonlocal armed
        if armed and observed == boundary:
            armed = False
            raise RuntimeError(boundary)

    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"crash-{suffix}",
        table_name=f"crash_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
        fault_hook=fault,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"crash-{uuid4().hex}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    plan = RemoteBundleManifest(
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

    def stage(
        connection: Connection, prepared: PreparedStage
    ) -> RemoteBundleManifest:
        manifest = _empty_candidate_manifest(connection, table_name)
        fence.stage_member_complete(prepared, "member")
        return manifest

    with pytest.raises(RuntimeError, match=boundary):
        fence.promote(
            bundle_key="analysis",
            run_id="attempt",
            fencing_token=lease.fencing_token,
            snapshot_seq=1,
            bundle_id="crash-bundle",
            cursors={"member": 1},
            source_coordinates=(
                capture_source_coordinate(
                    pg_engine, source_id="application", snapshot_seq=1
                ),
            ),
            source_families=("application",),
            stage=stage,
            operation_identity=identity,
            stage_plan=plan,
        )
    observation = fence.observe_operation(identity.operation_id)
    assert observation.state == expected_state
    if expected_state != "PROMOTED":
        # The crashed attempt's lease is still live, so recovery cleanup is
        # blocked until the lease expires.
        blocked = fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            str(observation.stage_plan_digest),
        )
        assert blocked.disposition == "BLOCKED_LEASE_HELD"
        with pg_engine.begin() as connection:
            connection.execute(
                text(
                    f'UPDATE "{fence.table_name}" SET lease_expires_at = '
                    "clock_timestamp() - INTERVAL '1 second' "
                    "WHERE bundle_key = 'analysis'"
                )
            )
    cleaned = fence.cleanup_operation(
        identity.operation_id, "cleanup", str(observation.stage_plan_digest)
    )
    assert cleaned.disposition == "CLEANED"
    assert fence.observe_operation(identity.operation_id).state == "CLEANED"
    with pg_engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT to_regclass(:name)"), {"name": table_name}
            )
            is None
        )


def test_cleanup_commit_fault_replays_durable_tombstone(
    pg_engine: Engine,
) -> None:
    _require_pgcrypto(pg_engine)
    suffix = uuid4().hex[:12]
    cleanup_fault = False

    def fault(boundary: str) -> None:
        nonlocal cleanup_fault
        if boundary == "after_cleanup_commit" and cleanup_fault:
            cleanup_fault = False
            raise RuntimeError(boundary)

    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"cleanup-crash-{suffix}",
        table_name=f"cleanup_crash_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
        fault_hook=fault,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"cleanup-crash-{uuid4().hex}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    member = RemoteBundleMember(
        member="member",
        table_name=table_name,
        key_columns=("id",),
        row_count=0,
        checksum=_EMPTY_CHECKSUM,
    )
    promoted = fence.promote(
        bundle_key="analysis",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
        bundle_id="cleanup-crash-bundle",
        cursors={"member": 1},
        source_coordinates=(
            capture_source_coordinate(
                pg_engine, source_id="application", snapshot_seq=1
            ),
        ),
        source_families=("application",),
        stage=lambda connection, _prepared: _empty_candidate_manifest(
            connection, table_name
        ),
        operation_identity=identity,
        stage_plan=RemoteBundleManifest(
            members=(member,), source_families=("application",)
        ),
    )
    assert promoted.receipt is not None
    cleanup_fault = True
    with pytest.raises(RuntimeError, match="after_cleanup_commit"):
        fence.cleanup_operation(
            identity.operation_id,
            "cleanup",
            promoted.receipt.stage_plan_digest,
        )
    replay = fence.cleanup_operation(
        identity.operation_id, "cleanup", promoted.receipt.stage_plan_digest
    )
    assert replay.disposition == "CLEANED"
    assert replay.observation.state == "CLEANED"


def test_operation_cleanup_capability_is_fail_closed(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    # An explicit opt-in enables cleanup on both proven backends.
    motherduck = PostgresPublicationFence(
        pg_engine,
        destination_id="motherduck-enabled",
        table_name=f"motherduck_enabled_{suffix}",
        kind="motherduck",
        operation_cleanup_enabled=True,
    )
    assert motherduck.capabilities.operation_cleanup
    # An omitted enablement flag leaves every endpoint fail-closed, so a
    # missing opt-in cannot route destructive cleanup on either backend.
    kinds: tuple[Literal["motherduck", "neon"], ...] = ("motherduck", "neon")
    for kind in kinds:
        disabled = PostgresPublicationFence(
            pg_engine,
            destination_id=f"{kind}-default-disabled",
            table_name=f"{kind}_default_disabled_{suffix}",
            kind=kind,
        )
        assert not disabled.capabilities.operation_cleanup
        with pytest.raises(RuntimeError, match="operation_cleanup_enabled"):
            disabled.cleanup_operation("operation", "request", "digest")


class _FakeDriverError(Exception):
    """Driver-shaped error carrying the sqlstate psycopg exposes."""

    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self._message = message

    def __str__(self) -> str:
        return self._message


def test_retryable_conflict_classification_is_backend_scoped() -> None:
    engine = create_engine("postgresql+psycopg:///never_connected")

    def wrapped(message: str, sqlstate: str) -> DBAPIError:
        return DBAPIError(
            "UPDATE gate", None, _FakeDriverError(message, sqlstate)
        )

    serialization = wrapped("serialization failure", "40001")
    deadlock = wrapped("deadlock detected", "40P01")
    # MotherDuck reports every failure as XXUUU; only its TransactionContext
    # write-write aborts are transient, so classification must read DuckDB's
    # message, and only for the motherduck backend.
    update_conflict = wrapped(
        "TransactionContext Error: Conflict on update!", "XXUUU"
    )
    catalog_conflict = wrapped(
        "TransactionContext Error: Catalog write-write conflict on alter "
        'with "Schema\0main\0main"',
        "XXUUU",
    )
    catalog_missing = wrapped(
        "Catalog Error: Table with name missing does not exist!", "XXUUU"
    )
    parser_error = wrapped(
        'Parser Error: syntax error at or near "SELEKT"', "XXUUU"
    )

    kinds: tuple[Literal["motherduck", "neon"], ...] = ("motherduck", "neon")
    for kind in kinds:
        fence = PostgresPublicationFence(
            engine, destination_id="classification", kind=kind
        )
        assert fence._retryable_database_error(serialization)
        assert fence._retryable_database_error(deadlock)
        assert not fence._retryable_database_error(catalog_missing)
        assert not fence._retryable_database_error(parser_error)
        expected = kind == "motherduck"
        assert fence._retryable_database_error(update_conflict) is expected
        assert fence._retryable_database_error(catalog_conflict) is expected


def test_cleanup_rejects_a_plan_signed_by_an_unknown_key(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    fence, identity, receipt = _promoted_operation_fixture(pg_engine, suffix)
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f'UPDATE "{fence.table_name}_operations" '
                "SET plan_key_id = 'unknown-key' "
                "WHERE operation_id = :operation"
            ),
            {"operation": identity.operation_id},
        )
    assert (
        fence.preflight_cleanup(
            identity.operation_id, receipt.stage_plan_digest
        ).disposition
        == "AUTHORITY_MISMATCH"
    )
    result = fence.cleanup_operation(
        identity.operation_id, "cleanup", receipt.stage_plan_digest
    )
    assert result.disposition == "AUTHORITY_MISMATCH"
    assert fence.observe_operation(identity.operation_id).present_members == (
        "member",
    )


def test_operation_cleanup_blocks_a_newer_current_pointer(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"successor-{suffix}",
        table_name=f"successor_state_{suffix}",
        operation_cleanup_enabled=True,
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    identity = PublicationOperationIdentity(
        operation_id=f"successor-{uuid4().hex}", attempt_id="attempt"
    )
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="attempt", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="attempt",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    prepared = fence.prepare_stage(
        bundle_key="analysis",
        identity=identity,
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
        bundle_id="candidate",
        manifest=RemoteBundleManifest(
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
        ),
    )
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f'UPDATE "{fence.table_name}" SET committed_snapshot_seq = 2, '
                "bundle_id = 'successor', owner = NULL, lease_expires_at = NULL "
                "WHERE bundle_key = 'analysis'"
            )
        )
    preflight = fence.preflight_cleanup(
        identity.operation_id, prepared.plan_digest
    )
    assert preflight.disposition == "BLOCKED_SUCCESSOR"
    assert (
        fence.cleanup_operation(
            identity.operation_id, "cleanup", prepared.plan_digest
        ).disposition
        == "BLOCKED_SUCCESSOR"
    )


def test_ensure_schema_additively_migrates_legacy_rows_fail_closed(
    pg_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    table_name = f"legacy_state_{suffix}"
    destination = f"legacy-{suffix}"
    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f'CREATE TABLE "{table_name}" ('
                "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, committed_snapshot_seq BIGINT NOT NULL DEFAULT 0, "
                "cursors_json TEXT NOT NULL DEFAULT '{}', checksums_json TEXT NOT NULL DEFAULT '{}', bundle_id TEXT, "
                "owner TEXT, lease_expires_at TIMESTAMPTZ, fencing_token BIGINT NOT NULL DEFAULT 0, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(destination_id, bundle_key))"
            )
        )
        connection.execute(
            text(
                f'CREATE TABLE "{table_name}_bundles" ('
                "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, bundle_id TEXT NOT NULL, snapshot_seq BIGINT NOT NULL, "
                "source_coordinates_json TEXT NOT NULL, manifest_json TEXT NOT NULL, status TEXT NOT NULL, owner TEXT NOT NULL, "
                "fencing_token BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                "PRIMARY KEY(destination_id, bundle_key, bundle_id))"
            )
        )
        connection.execute(
            text(
                f'CREATE TABLE "{table_name}_pins" ('
                "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, pin_id TEXT NOT NULL, bundle_id TEXT NOT NULL, "
                "expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                "PRIMARY KEY(destination_id, bundle_key, pin_id))"
            )
        )
        connection.execute(
            text(
                f'INSERT INTO "{table_name}" (destination_id, bundle_key, bundle_id) '
                "VALUES (:destination, 'analysis', 'legacy-bundle')"
            ),
            {"destination": destination},
        )
        connection.execute(
            text(
                f'INSERT INTO "{table_name}_bundles" (destination_id, bundle_key, bundle_id, snapshot_seq, '
                "source_coordinates_json, manifest_json, status, owner, fencing_token) "
                "VALUES (:destination, 'analysis', 'legacy-bundle', 1, '[]', '{}', 'PROMOTED', 'legacy', 1)"
            ),
            {"destination": destination},
        )
        connection.execute(
            text(
                f'INSERT INTO "{table_name}_pins" (destination_id, bundle_key, pin_id, bundle_id, expires_at) '
                "VALUES (:destination, 'analysis', 'legacy-pin', 'legacy-bundle', CURRENT_TIMESTAMP + INTERVAL '1 hour')"
            ),
            {"destination": destination},
        )
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=destination,
        table_name=table_name,
        operation_cleanup_enabled=True,
    )
    fence.ensure_schema()
    with pg_engine.connect() as connection:
        assert connection.execute(
            text(
                f"SELECT pin_kind, owner_operation_id FROM \"{table_name}_pins\" WHERE pin_id = 'legacy-pin'"
            )
        ).one() == ("EXTERNAL", None)
        assert (
            connection.scalar(
                text(
                    f"SELECT operation_id FROM \"{table_name}_bundles\" WHERE bundle_id = 'legacy-bundle'"
                )
            )
            is None
        )
    assert (
        fence.preflight_cleanup("legacy", "digest").disposition == "NOT_FOUND"
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
    _require_pgcrypto(pg_engine)
    suffix = uuid4().hex[:12]
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"same-database-{suffix}",
        table_name=f"same_database_state_{suffix}",
        signer=signer,
        public_key_ring=key_ring,
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


def _motherduck_source_coordinate(
    engine: Engine, snapshot_seq: int
) -> SourceCoordinate:
    """Capture a database-server coordinate from a MotherDuck endpoint.

    MotherDuck's endpoint has no clock_timestamp(); its transaction-stable
    CURRENT_TIMESTAMP is the only database-server clock and is sufficient
    for a single-family cut.
    """

    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT current_database(), CURRENT_TIMESTAMP")
        ).one()
    return SourceCoordinate(
        source_id=f"application:{row[0]}",
        database_server=str(row[0]),
        captured_at=row[1],
        snapshot_seq=snapshot_seq,
    )


def _remote_table_exists(
    engine: Engine,
    kind: Literal["motherduck", "neon"],
    schema_name: str,
    table_name: str,
) -> bool:
    """Catalog existence probe; MotherDuck has no to_regclass."""

    with engine.connect() as connection:
        if kind == "neon":
            return (
                connection.execute(
                    text("SELECT to_regclass(CAST(:name AS TEXT))"),
                    {"name": f"{schema_name}.{table_name}"},
                ).scalar_one()
                is not None
            )
        return (
            connection.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :schema_name "
                    "AND table_name = :table_name"
                ),
                {"schema_name": schema_name, "table_name": table_name},
            ).one_or_none()
            is not None
        )


def _assert_remote_fence_rejects_stale_stage_and_uses_returning(
    engine: Engine, kind: Literal["motherduck", "neon"]
) -> None:
    suffix = uuid4().hex[:12]
    state_table = f"publication_state_{suffix}"
    # MotherDuck's endpoint exposes exactly one writable schema, main;
    # PostgreSQL destinations stage into public. This mirrors export's
    # kind-derived member schema.
    schema = "main" if kind == "motherduck" else "public"
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        engine,
        destination_id=f"{kind}-test",
        table_name=state_table,
        kind=kind,
        signer=signer,
        public_key_ring=key_ring,
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
    successor_table = fence.stage_table_name(
        member="member", run_id="successor", fencing_token=5, snapshot_seq=2
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

        coordinate = (
            _motherduck_source_coordinate(engine, snapshot_seq=1)
            if kind == "motherduck"
            else capture_source_coordinate(
                engine, source_id="application", snapshot_seq=1
            )
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
                        schema_name=schema,
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
        assert not _remote_table_exists(engine, kind, schema, staged_table)

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
                        schema_name=schema,
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
                        schema_name=schema,
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
                connection, replay_table, schema
            ),
        )
        assert idempotent.disposition == "IDEMPOTENT"
        assert idempotent.bundle_id == "bundle-one"

        pin = fence.pin_bundle(bundle_key="analysis", ttl_seconds=60)
        resolved = fence.resolve_pin(pin)
        assert resolved.bundle_id == "bundle-one"
        assert resolved.members == {"member": f"{schema}.{promoted_table}"}

        successor_lease = fence.acquire_lease(
            bundle_key="analysis", run_id="successor", lease_seconds=60
        )
        assert successor_lease.fencing_token == 5
        successor = fence.promote(
            bundle_key="analysis",
            run_id="successor",
            fencing_token=5,
            snapshot_seq=2,
            bundle_id="bundle-two",
            cursors={"member": 2},
            source_coordinates=(
                _motherduck_source_coordinate(engine, snapshot_seq=2)
                if kind == "motherduck"
                else capture_source_coordinate(
                    engine, source_id="application", snapshot_seq=2
                ),
            ),
            source_families=("application",),
            stage=lambda connection: _empty_candidate_manifest(
                connection, successor_table, schema
            ),
        )
        assert successor.disposition == "PROMOTED"
        # The old pin reads bundle-one's attestation rather than the mutable
        # current-state pointer now owned by bundle-two.
        assert fence.resolve_pin(pin).bundle_id == "bundle-one"
        with engine.begin() as connection:
            connection.execute(
                text(f'INSERT INTO "{promoted_table}" VALUES (1)')
            )
        with pytest.raises(PinnedBundleGoneError):
            fence.resolve_pin(pin)
        with engine.begin() as connection:
            connection.execute(text(f'DELETE FROM "{promoted_table}"'))

        cleaner = fence.acquire_lease(
            bundle_key="analysis", run_id="cleaner", lease_seconds=60
        )
        assert cleaner.fencing_token == 6
        deleted = fence.cleanup_bundles(
            bundle_key="analysis",
            run_id="cleaner",
            fencing_token=6,
        )
        assert "bundle-one" not in deleted
        assert fence.resolve_pin(pin).bundle_id == "bundle-one"
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{staged_table}"'))
            connection.execute(text(f'DROP TABLE IF EXISTS "{expired_table}"'))
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{promoted_table}"')
            )
            connection.execute(text(f'DROP TABLE IF EXISTS "{replay_table}"'))
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{successor_table}"')
            )
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{state_table}_pins"')
            )
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{state_table}_bundles"')
            )
            connection.execute(
                text(f'DROP TABLE IF EXISTS "{state_table}_operations"')
            )
            connection.execute(
                text(
                    f'DROP TABLE IF EXISTS "{state_table}_operation_members"'
                )
            )
            connection.execute(text(f'DROP TABLE IF EXISTS "{state_table}"'))


def test_postgres_fence_rejects_stale_stage_and_uses_returning(
    pg_engine: Engine,
) -> None:
    _require_pgcrypto(pg_engine)
    _assert_remote_fence_rejects_stale_stage_and_uses_returning(
        pg_engine, "neon"
    )


@pytest.fixture
def motherduck_engine() -> Iterator[Engine]:
    """Opt-in endpoint used to execute MotherDuck's native sha256 aggregate."""

    url = os.environ.get("DR_PLATFORM_MOTHERDUCK_TEST_DATABASE_URL")
    if url is None:
        pytest.skip(
            "MotherDuck capability test requires "
            "DR_PLATFORM_MOTHERDUCK_TEST_DATABASE_URL"
        )
    # SQLAlchemy's psycopg dialect probes the hstore extension type inside a
    # SAVEPOINT on first connect; MotherDuck's endpoint parses neither, so the
    # probe must be disabled for the connection to initialize at all.
    engine = create_engine(url, use_native_hstore=False)
    try:
        with engine.connect():
            pass
    except SQLAlchemyError as exc:
        engine.dispose()
        pytest.skip(f"MotherDuck unavailable: {exc}")
    yield engine
    engine.dispose()


def test_motherduck_fence_rejects_stale_stage_and_uses_returning(
    motherduck_engine: Engine,
) -> None:
    _assert_remote_fence_rejects_stale_stage_and_uses_returning(
        motherduck_engine, "motherduck"
    )


# The live cleanup proof publishes the same non-empty six-member Analysis
# inventory Whetstone publishes, at the fence protocol level.
_ANALYSIS_SHAPE: tuple[tuple[str, str], ...] = (
    ("experiments", "experiment_id"),
    ("predictions", "prediction_id"),
    ("generation_runs", "generation_run_id"),
    ("score_attempts", "score_attempt_id"),
    ("sweep_metrics", "metric_id"),
    ("failure_metrics", "failure_id"),
)
_ANALYSIS_MEMBER_NAMES = tuple(sorted(name for name, _ in _ANALYSIS_SHAPE))


def _analysis_member_rows(member: str, key: str) -> list[dict[str, object]]:
    return [
        {
            key: f"{member}-{index}",
            "payload": f"{member}-payload-{index}",
            "value": index,
        }
        for index in (1, 2)
    ]


def _analysis_stage_plan(
    fence: PostgresPublicationFence,
    *,
    run_id: str,
    fencing_token: int,
    snapshot_seq: int,
) -> tuple[RemoteBundleManifest, dict[str, tuple[str, list[dict[str, object]]]]]:
    members: list[RemoteBundleMember] = []
    rows_by_member: dict[str, tuple[str, list[dict[str, object]]]] = {}
    for member, key in _ANALYSIS_SHAPE:
        rows = _analysis_member_rows(member, key)
        rows_by_member[member] = (key, rows)
        members.append(
            RemoteBundleMember(
                member=member,
                schema_name="main",
                table_name=fence.stage_table_name(
                    member=member,
                    run_id=run_id,
                    fencing_token=fencing_token,
                    snapshot_seq=snapshot_seq,
                ),
                key_columns=(key,),
                row_count=len(rows),
                checksum=hashlib.sha256(_canonical(rows).encode()).hexdigest(),
            )
        )
    return (
        RemoteBundleManifest(
            members=tuple(members), source_families=("application",)
        ),
        rows_by_member,
    )


def _stage_analysis_members(
    connection: Connection,
    prepared: PreparedStage,
    fence: PostgresPublicationFence,
    plan: RemoteBundleManifest,
    rows_by_member: dict[str, tuple[str, list[dict[str, object]]]],
) -> RemoteBundleManifest:
    for member in plan.members:
        key, rows = rows_by_member[member.member]
        assert prepared.table_name(member.member) == member.table_name
        connection.execute(
            text(
                f'CREATE TABLE "{member.table_name}" '
                f'("{key}" VARCHAR, "payload" VARCHAR, "value" BIGINT)'
            )
        )
        for row in rows:
            connection.execute(
                text(
                    f'INSERT INTO "{member.table_name}" VALUES '
                    "(:key_value, :payload, :value)"
                ),
                {
                    "key_value": row[key],
                    "payload": row["payload"],
                    "value": row["value"],
                },
            )
        fence.stage_member_complete(prepared, member.member)
    return plan


def _disposable_motherduck_fence(
    engine: Engine,
    suffix: str,
    fault_hook: Callable[[str], None] | None = None,
    *,
    operation_cleanup_enabled: bool = True,
) -> PostgresPublicationFence:
    signer, key_ring = signed_integrity_test_material()
    return PostgresPublicationFence(
        engine,
        destination_id=f"drp-cleanup-proof-{suffix}",
        table_name=f"drp_cleanup_proof_{suffix}",
        kind="motherduck",
        operation_cleanup_enabled=operation_cleanup_enabled,
        signer=signer,
        public_key_ring=key_ring,
        fault_hook=fault_hook or (lambda _boundary: None),
    )


def _promote_analysis_operation(
    fence: PostgresPublicationFence,
    engine: Engine,
    identity: PublicationOperationIdentity,
    *,
    bundle_key: str,
    snapshot_seq: int,
    staged_tables: set[str],
) -> RemotePromotionResult:
    lease = fence.acquire_lease(
        bundle_key=bundle_key, run_id=identity.attempt_id, lease_seconds=120
    )
    assert lease.fencing_token is not None
    plan, rows_by_member = _analysis_stage_plan(
        fence,
        run_id=identity.attempt_id,
        fencing_token=lease.fencing_token,
        snapshot_seq=snapshot_seq,
    )
    staged_tables.update(member.table_name for member in plan.members)
    return fence.promote(
        bundle_key=bundle_key,
        run_id=identity.attempt_id,
        fencing_token=lease.fencing_token,
        snapshot_seq=snapshot_seq,
        bundle_id=f"drp-bundle-{identity.operation_id}",
        cursors={member.member: snapshot_seq for member in plan.members},
        source_coordinates=(
            _motherduck_source_coordinate(engine, snapshot_seq=snapshot_seq),
        ),
        source_families=("application",),
        stage=lambda connection, prepared: _stage_analysis_members(
            connection, prepared, fence, plan, rows_by_member
        ),
        operation_identity=identity,
        stage_plan=plan,
    )


def _drop_disposable_motherduck_state(
    engine: Engine, fence: PostgresPublicationFence, staged_tables: set[str]
) -> None:
    with engine.begin() as connection:
        for table_name in sorted(staged_tables):
            connection.execute(
                text(f'DROP TABLE IF EXISTS "main"."{table_name}"')
            )
        for state_suffix in (
            "_pins",
            "_bundles",
            "_operations",
            "_operation_members",
            "",
        ):
            connection.execute(
                text(
                    f'DROP TABLE IF EXISTS "{fence.table_name}{state_suffix}"'
                )
            )


def _assert_zero_operation_state(
    fence: PostgresPublicationFence,
    engine: Engine,
    operation_id: str,
    staged_tables: set[str],
) -> None:
    final = fence.observe_operation(operation_id)
    assert final.state == "CLEANED"
    assert final.present_members == ()
    assert final.owned_pin_ids == ()
    assert final.active_external_pin_ids == ()
    assert final.owned_bundle_count == 0
    assert final.current_pointer_relation == "NONE"
    for table_name in sorted(staged_tables):
        assert not _remote_table_exists(
            engine, "motherduck", "main", table_name
        )


def test_motherduck_operation_cleanup_proves_analysis_lifecycle(
    motherduck_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    bundle_key = "whetstone-analysis"
    conflict_armed: list[bool] = []
    conflict_pins: list[BundlePin] = []

    def fault(boundary: str) -> None:
        if boundary == "before_cleanup_gate" and conflict_armed:
            conflict_armed.clear()
            # A reader pin committed on a second connection mutates the
            # shared gate row mid-cleanup, so the open cleanup transaction's
            # compare-and-set aborts with MotherDuck's optimistic write-write
            # conflict and must retry from a fresh snapshot.
            conflict_pins.append(
                fence.pin_bundle(
                    bundle_key=bundle_key,
                    pin_id=f"drp-conflict-{suffix}",
                    ttl_seconds=600,
                )
            )

    fence = _disposable_motherduck_fence(
        motherduck_engine, suffix, fault_hook=fault
    )
    assert fence.capabilities.operation_cleanup
    assert fence.capabilities.reason is None
    # The omitted flag stays fail-closed against the live endpoint too.
    flagless = _disposable_motherduck_fence(
        motherduck_engine, suffix, operation_cleanup_enabled=False
    )
    assert not flagless.capabilities.operation_cleanup
    with pytest.raises(RuntimeError, match="operation_cleanup_enabled"):
        flagless.cleanup_operation("operation", "request", "digest")

    staged_tables: set[str] = set()
    try:
        fence.ensure_schema()
        identity_a = PublicationOperationIdentity(
            operation_id=f"drp-analysis-a-{suffix}",
            attempt_id=f"drp-attempt-a-{suffix}",
        )
        promoted = _promote_analysis_operation(
            fence,
            motherduck_engine,
            identity_a,
            bundle_key=bundle_key,
            snapshot_seq=1,
            staged_tables=staged_tables,
        )
        assert promoted.disposition == "PROMOTED"
        receipt = promoted.receipt
        assert receipt is not None
        observation = fence.observe_operation(identity_a.operation_id)
        assert observation.state == "PROMOTED"
        assert observation.current_pointer_relation == "CURRENT"
        assert observation.present_members == _ANALYSIS_MEMBER_NAMES
        assert observation.owned_bundle_count == 1
        assert observation.stage_plan_digest == receipt.stage_plan_digest

        # Authority/digest mismatch fails closed without dropping anything.
        wrong_digest = hashlib.sha256(b"wrong-plan").hexdigest()
        assert (
            fence.preflight_cleanup(
                identity_a.operation_id, wrong_digest
            ).disposition
            == "AUTHORITY_MISMATCH"
        )
        assert (
            fence.cleanup_operation(
                identity_a.operation_id,
                f"drp-cleanup-a-{suffix}",
                wrong_digest,
            ).disposition
            == "AUTHORITY_MISMATCH"
        )
        assert (
            fence.observe_operation(identity_a.operation_id).present_members
            == _ANALYSIS_MEMBER_NAMES
        )

        # An active external reader pin blocks the destructive path.
        external = fence.pin_bundle(
            bundle_key=bundle_key,
            pin_id=f"drp-external-{suffix}",
            ttl_seconds=600,
        )
        assert (
            fence.preflight_cleanup(
                identity_a.operation_id, receipt.stage_plan_digest
            ).disposition
            == "BLOCKED_EXTERNAL_PIN"
        )
        assert (
            fence.cleanup_operation(
                identity_a.operation_id,
                f"drp-cleanup-a-{suffix}",
                receipt.stage_plan_digest,
            ).disposition
            == "BLOCKED_EXTERNAL_PIN"
        )
        fence.release_pin(external)

        # A concurrent gate mutation mid-transaction aborts with MotherDuck's
        # XXUUU write-write conflict; the bounded retry re-observes the fresh
        # external pin instead of raising or cleaning past it.
        conflict_armed.append(True)
        conflicted = fence.cleanup_operation(
            identity_a.operation_id,
            f"drp-cleanup-a-{suffix}",
            receipt.stage_plan_digest,
        )
        assert conflicted.disposition == "BLOCKED_EXTERNAL_PIN"
        assert not conflict_armed
        assert len(conflict_pins) == 1
        fence.release_pin(conflict_pins[0])

        # An operation-owned fixture pin is deleted with its operation.
        owned = fence.pin_bundle(
            bundle_key=bundle_key,
            pin_id=f"drp-owned-{suffix}",
            owner_operation_id=identity_a.operation_id,
            ttl_seconds=600,
        )
        assert owned.pin_id == f"drp-owned-{suffix}"

        # A successor pointer blocks the superseded operation's cleanup.
        identity_b = PublicationOperationIdentity(
            operation_id=f"drp-analysis-b-{suffix}",
            attempt_id=f"drp-attempt-b-{suffix}",
        )
        successor = _promote_analysis_operation(
            fence,
            motherduck_engine,
            identity_b,
            bundle_key=bundle_key,
            snapshot_seq=2,
            staged_tables=staged_tables,
        )
        assert successor.disposition == "PROMOTED"
        assert successor.receipt is not None
        assert (
            fence.preflight_cleanup(
                identity_a.operation_id, receipt.stage_plan_digest
            ).disposition
            == "BLOCKED_SUCCESSOR"
        )
        assert (
            fence.cleanup_operation(
                identity_a.operation_id,
                f"drp-cleanup-a-{suffix}",
                receipt.stage_plan_digest,
            ).disposition
            == "BLOCKED_SUCCESSOR"
        )

        # The current operation is cleanable and clears the pointer.
        assert (
            fence.preflight_cleanup(
                identity_b.operation_id, successor.receipt.stage_plan_digest
            ).disposition
            == "ELIGIBLE"
        )
        cleaned_b = fence.cleanup_operation(
            identity_b.operation_id,
            f"drp-cleanup-b-{suffix}",
            successor.receipt.stage_plan_digest,
        )
        assert cleaned_b.disposition == "CLEANED"
        assert cleaned_b.observation.present_members == ()
        with motherduck_engine.connect() as connection:
            pointer = connection.execute(
                text(
                    f'SELECT bundle_id, published_operation_id FROM "{fence.table_name}" '
                    "WHERE destination_id = :destination AND bundle_key = :bundle"
                ),
                {"destination": fence.destination_id, "bundle": bundle_key},
            ).one()
        assert pointer == (None, None)

        # With the pointer cleared the first operation becomes eligible.
        assert (
            fence.preflight_cleanup(
                identity_a.operation_id, receipt.stage_plan_digest
            ).disposition
            == "ELIGIBLE"
        )
        cleaned_a = fence.cleanup_operation(
            identity_a.operation_id,
            f"drp-cleanup-a-{suffix}",
            receipt.stage_plan_digest,
        )
        assert cleaned_a.disposition == "CLEANED"

        # Exact replay returns the durable tombstone result; a different
        # request against the tombstone is typed, never re-destructive.
        replay = fence.cleanup_operation(
            identity_a.operation_id,
            f"drp-cleanup-a-{suffix}",
            receipt.stage_plan_digest,
        )
        assert replay == cleaned_a
        assert (
            fence.cleanup_operation(
                identity_a.operation_id,
                f"drp-cleanup-a-replayed-{suffix}",
                receipt.stage_plan_digest,
            ).disposition
            == "ALREADY_CLEANED"
        )

        # Zero remaining physical tables, member rows, bundle rows, and
        # operation-owned pins; pointer relation is NONE for both operations.
        for operation_id in (
            identity_a.operation_id,
            identity_b.operation_id,
        ):
            _assert_zero_operation_state(
                fence, motherduck_engine, operation_id, staged_tables
            )
        with motherduck_engine.connect() as connection:
            for leftover_table in (fence._bundles_table, fence._pins_table):
                assert (
                    connection.execute(
                        text(
                            f"SELECT count(*) FROM {leftover_table} "
                            "WHERE destination_id = :destination"
                        ),
                        {"destination": fence.destination_id},
                    ).scalar_one()
                    == 0
                )
    finally:
        _drop_disposable_motherduck_state(
            motherduck_engine, fence, staged_tables
        )


def test_motherduck_operation_cleanup_recovers_every_fault_boundary(
    motherduck_engine: Engine,
) -> None:
    suffix = uuid4().hex[:12]
    armed: list[str] = []

    def fault(boundary: str) -> None:
        if armed and boundary == armed[0]:
            armed.clear()
            raise RuntimeError(boundary)

    fence = _disposable_motherduck_fence(
        motherduck_engine, suffix, fault_hook=fault
    )
    staged_tables: set[str] = set()
    try:
        fence.ensure_schema()
        crash_boundaries = (
            ("after_plan_commit", "PLANNED"),
            ("after_each_stage_member", "STAGING"),
            ("after_stage_commit", "STAGING"),
            ("after_promotion_commit", "PROMOTED"),
        )
        for boundary, expected_state in crash_boundaries:
            bundle_key = f"analysis-{boundary}"
            identity = PublicationOperationIdentity(
                operation_id=f"drp-{boundary}-{suffix}",
                attempt_id=f"drp-attempt-{boundary}-{suffix}",
            )
            boundary_tables: set[str] = set()
            armed.append(boundary)
            with pytest.raises(RuntimeError, match=boundary):
                _promote_analysis_operation(
                    fence,
                    motherduck_engine,
                    identity,
                    bundle_key=bundle_key,
                    snapshot_seq=1,
                    staged_tables=boundary_tables,
                )
            staged_tables.update(boundary_tables)
            observation = fence.observe_operation(identity.operation_id)
            assert observation.state == expected_state
            if expected_state != "PROMOTED":
                # The crashed attempt's lease is still live, so recovery is
                # blocked until the lease expires.
                blocked = fence.cleanup_operation(
                    identity.operation_id,
                    f"drp-recover-{boundary}-{suffix}",
                    str(observation.stage_plan_digest),
                )
                assert blocked.disposition == "BLOCKED_LEASE_HELD"
                with motherduck_engine.begin() as connection:
                    connection.execute(
                        text(
                            f'UPDATE "{fence.table_name}" SET lease_expires_at = '
                            "CURRENT_TIMESTAMP - INTERVAL '1 second' "
                            "WHERE destination_id = :destination "
                            "AND bundle_key = :bundle"
                        ),
                        {
                            "destination": fence.destination_id,
                            "bundle": bundle_key,
                        },
                    )
            cleaned = fence.cleanup_operation(
                identity.operation_id,
                f"drp-recover-{boundary}-{suffix}",
                str(observation.stage_plan_digest),
            )
            assert cleaned.disposition == "CLEANED"
            _assert_zero_operation_state(
                fence,
                motherduck_engine,
                identity.operation_id,
                boundary_tables,
            )

        # A crash immediately before the cleanup gate rolls the whole
        # destructive transaction back; nothing is dropped.
        identity = PublicationOperationIdentity(
            operation_id=f"drp-before-gate-{suffix}",
            attempt_id=f"drp-attempt-before-gate-{suffix}",
        )
        gate_tables: set[str] = set()
        promoted = _promote_analysis_operation(
            fence,
            motherduck_engine,
            identity,
            bundle_key="analysis-before-cleanup-gate",
            snapshot_seq=1,
            staged_tables=gate_tables,
        )
        staged_tables.update(gate_tables)
        assert promoted.disposition == "PROMOTED"
        assert promoted.receipt is not None
        armed.append("before_cleanup_gate")
        with pytest.raises(RuntimeError, match="before_cleanup_gate"):
            fence.cleanup_operation(
                identity.operation_id,
                f"drp-recover-before-gate-{suffix}",
                promoted.receipt.stage_plan_digest,
            )
        interrupted = fence.observe_operation(identity.operation_id)
        assert interrupted.state == "PROMOTED"
        assert interrupted.present_members == _ANALYSIS_MEMBER_NAMES
        cleaned = fence.cleanup_operation(
            identity.operation_id,
            f"drp-recover-before-gate-{suffix}",
            promoted.receipt.stage_plan_digest,
        )
        assert cleaned.disposition == "CLEANED"
        _assert_zero_operation_state(
            fence, motherduck_engine, identity.operation_id, gate_tables
        )

        # A crash after the cleanup commit replays the durable tombstone.
        identity = PublicationOperationIdentity(
            operation_id=f"drp-after-commit-{suffix}",
            attempt_id=f"drp-attempt-after-commit-{suffix}",
        )
        commit_tables: set[str] = set()
        promoted = _promote_analysis_operation(
            fence,
            motherduck_engine,
            identity,
            bundle_key="analysis-after-cleanup-commit",
            snapshot_seq=1,
            staged_tables=commit_tables,
        )
        staged_tables.update(commit_tables)
        assert promoted.disposition == "PROMOTED"
        assert promoted.receipt is not None
        armed.append("after_cleanup_commit")
        with pytest.raises(RuntimeError, match="after_cleanup_commit"):
            fence.cleanup_operation(
                identity.operation_id,
                f"drp-recover-after-commit-{suffix}",
                promoted.receipt.stage_plan_digest,
            )
        assert (
            fence.observe_operation(identity.operation_id).state == "CLEANED"
        )
        replay = fence.cleanup_operation(
            identity.operation_id,
            f"drp-recover-after-commit-{suffix}",
            promoted.receipt.stage_plan_digest,
        )
        assert replay.disposition == "CLEANED"
        _assert_zero_operation_state(
            fence, motherduck_engine, identity.operation_id, commit_tables
        )
    finally:
        _drop_disposable_motherduck_state(
            motherduck_engine, fence, staged_tables
        )


def test_active_pin_survives_cleanup_then_missing_bundle_is_typed(
    pg_engine: Engine, tmp_path
) -> None:
    schema = PlatformSchema()
    upgrade_platform_schema(engine_dsn(pg_engine))
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
    resolved = resolve_local_pin(
        database, pin, public_key_ring=signed_integrity_test_material()[1]
    )
    assert resolved.bundle_id == first.destinations[0].bundle_id
    assert schema.operations.name in resolved.members

    # A legacy row is unreadable until a holder of the current fence signs it;
    # the existing pin continues to identify the same immutable bundle.
    with duckdb.connect(str(database)) as destination:
        destination.execute(
            "UPDATE __dr_platform_export_bundles "
            "SET integrity_version = NULL, "
            "integrity_key_id = NULL, integrity_payload_json = NULL, "
            "integrity_signature = NULL, physical_digest_algorithm = NULL "
            "WHERE bundle_id = ?",
            [first.destinations[0].bundle_id],
        )
        destination.execute(
            "UPDATE __dr_platform_export_state SET owner = ?, "
            "lease_expires_at = epoch_ms(now()) + 60000 WHERE bundle_key = ?",
            ["integrity-backfill", "platform-kernel"],
        )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        resolve_local_pin(
            database,
            pin,
            public_key_ring=signed_integrity_test_material()[1],
        )
    assert backfill_local_protected_integrity(
        database,
        signer=signed_integrity_test_material()[0],
        run_id="integrity-backfill",
        fencing_token=2,
    ) == (first.destinations[0].bundle_id,)
    assert (
        resolve_local_pin(
            database, pin, public_key_ring=signed_integrity_test_material()[1]
        ).bundle_id
        == first.destinations[0].bundle_id
    )

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


def _require_pgcrypto(pg_engine: Engine) -> None:
    try:
        with pg_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT encode(digest('', 'sha256'), 'hex')")
                ).scalar_one()
                == hashlib.sha256(b"").hexdigest()
            )
    except SQLAlchemyError as exc:  # pragma: no cover - dev DB privileges
        pytest.skip(f"Postgres pgcrypto unavailable: {exc}")


def _insert_legacy_remote_bundle(
    fence: PostgresPublicationFence,
    *,
    bundle_key: str,
    bundle_id: str,
    snapshot_seq: int,
    fencing_token: int,
) -> None:
    table_name = f"legacy_member_{bundle_id}"
    coordinate = capture_source_coordinate(
        fence.engine, source_id="application", snapshot_seq=snapshot_seq
    )
    manifest = RemoteBundleManifest(
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
    with fence.engine.begin() as connection:
        connection.execute(text(f'CREATE TABLE "{table_name}" (id BIGINT)'))
        connection.execute(
            text(
                f"INSERT INTO {fence._bundles_table} "
                "(destination_id, bundle_key, bundle_id, snapshot_seq, "
                "source_coordinates_json, manifest_json, "
                "status, owner, fencing_token) VALUES "
                "(:destination, :bundle, :bundle_id, :snapshot, :coordinates, "
                ":manifest, 'PROMOTED', 'owner', :token)"
            ),
            {
                "destination": fence.destination_id,
                "bundle": bundle_key,
                "bundle_id": bundle_id,
                "snapshot": snapshot_seq,
                "coordinates": json.dumps(
                    [coordinate.model_dump(mode="json")]
                ),
                "manifest": manifest.model_dump_json(),
                "token": fencing_token,
            },
        )


def test_remote_backfill_signs_legacy_bundle_under_current_fence(
    pg_engine: Engine,
) -> None:
    _require_pgcrypto(pg_engine)
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id="remote-backfill-success",
        table_name="remote_backfill_success_state",
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    _insert_legacy_remote_bundle(
        fence,
        bundle_key="analysis",
        bundle_id="legacy",
        snapshot_seq=1,
        fencing_token=lease.fencing_token,
    )

    assert fence.backfill_protected_integrity(
        bundle_key="analysis",
        run_id="owner",
        fencing_token=lease.fencing_token,
    ) == ("legacy",)
    pin = fence.pin_bundle(
        bundle_key="analysis", bundle_id="legacy", pin_id="legacy-pin"
    )
    assert fence.resolve_pin(pin).bundle_id == "legacy"


def test_remote_pin_authenticates_persisted_provenance(
    pg_engine: Engine,
) -> None:
    _require_pgcrypto(pg_engine)
    suffix = uuid4().hex[:12]
    signer, key_ring = signed_integrity_test_material()
    fence = PostgresPublicationFence(
        pg_engine,
        destination_id=f"provenance-{suffix}",
        table_name=f"provenance_state_{suffix}",
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    table_name = fence.stage_table_name(
        member="member",
        run_id="owner",
        fencing_token=lease.fencing_token,
        snapshot_seq=1,
    )
    coordinate = capture_source_coordinate(
        pg_engine, source_id="application", snapshot_seq=1
    )

    def stage(connection: Connection) -> RemoteBundleManifest:
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

    assert (
        fence.promote(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=lease.fencing_token,
            snapshot_seq=1,
            bundle_id="bundle",
            cursors={},
            source_coordinates=(coordinate,),
            source_families=("application",),
            stage=stage,
        ).disposition
        == "PROMOTED"
    )
    pin = fence.pin_bundle(bundle_key="analysis", pin_id="provenance-pin")
    assert fence.resolve_pin(pin).bundle_id == "bundle"
    with pg_engine.connect() as connection:
        original_manifest = connection.execute(
            text(
                f"SELECT manifest_json FROM {fence._bundles_table} "
                "WHERE destination_id = :destination"
            ),
            {"destination": fence.destination_id},
        ).scalar_one()

    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {fence._bundles_table} SET source_coordinates_json = "
                "CAST(:coordinates AS TEXT) "
                "WHERE destination_id = :destination"
            ),
            {
                "coordinates": json.dumps(
                    [
                        coordinate.model_copy(
                            update={"database_server": "forged-server"}
                        ).model_dump(mode="json")
                    ]
                ),
                "destination": fence.destination_id,
            },
        )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        fence.resolve_pin(pin)

    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {fence._bundles_table} SET source_coordinates_json = "
                "CAST(:coordinates AS TEXT), "
                "manifest_json = CAST(:manifest AS TEXT) "
                "WHERE destination_id = :destination"
            ),
            {
                "coordinates": json.dumps(
                    [coordinate.model_dump(mode="json")]
                ),
                "manifest": json.dumps(
                    {
                        **json.loads(original_manifest),
                        "source_families": ["dbos"],
                    }
                ),
                "destination": fence.destination_id,
            },
        )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        fence.resolve_pin(pin)

    with pg_engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {fence._bundles_table} SET "
                "manifest_json = CAST(:manifest AS TEXT) "
                "WHERE destination_id = :destination"
            ),
            {
                "manifest": original_manifest,
                "destination": fence.destination_id,
            },
        )
    rotated_reader = PostgresPublicationFence(
        pg_engine,
        destination_id=fence.destination_id,
        table_name=fence.table_name,
        public_key_ring={**key_ring, "next-ed25519": "next-public-key"},
    )
    assert rotated_reader.resolve_pin(pin).bundle_id == "bundle"
    revoked_reader = PostgresPublicationFence(
        pg_engine,
        destination_id=fence.destination_id,
        table_name=fence.table_name,
        public_key_ring={"next-ed25519": "next-public-key"},
    )
    with pytest.raises(PinnedBundleGoneError, match="PINNED_BUNDLE_GONE"):
        revoked_reader.resolve_pin(pin)


def test_remote_backfill_rechecks_fence_before_legacy_write(
    pg_engine: Engine,
) -> None:
    signer, key_ring = signed_integrity_test_material()

    class ExpiringFence(PostgresPublicationFence):
        def _with_physical_digests(self, connection, manifest):  # type: ignore[no-untyped-def]
            connection.execute(
                text(
                    f"UPDATE {self._table} SET lease_expires_at = "
                    "CURRENT_TIMESTAMP - INTERVAL '1 second' "
                    "WHERE destination_id = :destination "
                    "AND bundle_key = :bundle"
                ),
                {"destination": self.destination_id, "bundle": "analysis"},
            )
            digest = hashlib.sha256(b"").hexdigest()
            return manifest.model_copy(
                update={
                    "members": tuple(
                        member.model_copy(update={"physical_digest": digest})
                        for member in manifest.members
                    )
                }
            )

    fence = ExpiringFence(
        pg_engine,
        destination_id="remote-backfill-expired",
        table_name="remote_backfill_expired_state",
        signer=signer,
        public_key_ring=key_ring,
    )
    fence.ensure_schema()
    lease = fence.acquire_lease(
        bundle_key="analysis", run_id="owner", lease_seconds=60
    )
    assert lease.fencing_token is not None
    _insert_legacy_remote_bundle(
        fence,
        bundle_key="analysis",
        bundle_id="legacy",
        snapshot_seq=1,
        fencing_token=lease.fencing_token,
    )

    with pytest.raises(_StalePromotionError):
        fence.backfill_protected_integrity(
            bundle_key="analysis",
            run_id="owner",
            fencing_token=lease.fencing_token,
        )
    with pg_engine.connect() as connection:
        integrity_version = connection.execute(
            text(
                f"SELECT integrity_version FROM {fence._bundles_table} "
                "WHERE destination_id = :destination "
                "AND bundle_key = 'analysis' "
                "AND bundle_id = 'legacy'"
            ),
            {"destination": fence.destination_id},
        ).scalar_one()
    assert integrity_version is None
