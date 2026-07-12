"""Focused local publication coverage for the P7a export core."""

from __future__ import annotations

from typing import cast

import duckdb
import pytest
from sqlalchemy import Engine, Table

from dr_platform import ExportOptions, export, upgrade_platform_schema
from dr_platform.export import (
    ProjectionSpec,
    _acquire_lease,
    _create_destination_tables,
    _release_lease,
    _stage_and_promote,
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
        "platform_throttle_state",
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
