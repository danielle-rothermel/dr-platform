"""Alembic lineage: fresh-upgrade path and stamped-baseline path."""

from __future__ import annotations

from sqlalchemy import Engine, inspect, text

from dr_platform import (
    PlatformNaming,
    stamp_platform_schema,
    upgrade_platform_schema,
)

WHETSTONE_NAMING = PlatformNaming(
    prefix="dr_dspy",
    item_key_label="prediction_id",
    order_key_label="fair_order_key",
    group_key_label="experiment_name",
)

BASE_ITEM_COLUMNS = {
    "batch_submit_item_id",
    "operation_key",
    "item_index",
    "insert_status",
    "enqueue_status",
    "enqueue_metadata",
    "failure",
    "created_at",
}
THROTTLE_HEAD_COLUMNS = {
    "throttle_key",
    "blocked_until",
    "consecutive_failures",
    "failure_class",
    "last_error_type",
    "last_message",
    "metadata",
    "updated_at",
    "hold_until",
    "hold_reason",
    "tags",
}


def _table_columns(engine: Engine, table: str) -> set[str]:
    return {column["name"] for column in inspect(engine).get_columns(table)}


def test_fresh_upgrade_head_default_naming(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url))

    tables = set(inspect(pg_engine).get_table_names())
    assert {
        "dr_platform_batch_submit_operations",
        "dr_platform_batch_submit_items",
        "dr_platform_throttle_backoff",
        "dr_platform_projections",
        "dr_platform_platform_alembic_version",
    } <= tables

    assert _table_columns(pg_engine, "dr_platform_batch_submit_items") == (
        BASE_ITEM_COLUMNS | {"item_id", "order_key"}
    )
    assert (
        _table_columns(pg_engine, "dr_platform_throttle_backoff")
        == THROTTLE_HEAD_COLUMNS
    )


def test_fresh_upgrade_head_whetstone_naming(pg_engine: Engine) -> None:
    upgrade_platform_schema(str(pg_engine.url), naming=WHETSTONE_NAMING)

    tables = set(inspect(pg_engine).get_table_names())
    assert {
        "dr_dspy_batch_submit_operations",
        "dr_dspy_batch_submit_items",
        "dr_dspy_throttle_backoff",
        "dr_dspy_projections",
        "dr_dspy_platform_alembic_version",
    } <= tables

    # Whetstone's frozen physical column words survive the naming layer.
    assert _table_columns(pg_engine, "dr_dspy_batch_submit_items") == (
        BASE_ITEM_COLUMNS | {"prediction_id", "fair_order_key"}
    )
    ops_columns = _table_columns(pg_engine, "dr_dspy_batch_submit_operations")
    assert "experiment_name" in ops_columns


def test_stamp_then_upgrade_runs_only_post_baseline(
    pg_engine: Engine,
) -> None:
    """The whetstone adoption path.

    Simulate an app whose baseline-shaped tables already exist from its
    own frozen migration history: run 0001, then wipe the library's
    version table (as if the lineage had never been recorded), stamp
    the baseline, and upgrade. Only 0002 may run — proven by the
    baseline tables surviving untouched and the 0002 additions
    appearing.
    """
    url = str(pg_engine.url)
    upgrade_platform_schema(url, revision="0001_platform_baseline")
    with pg_engine.begin() as connection:
        connection.execute(
            text("DROP TABLE dr_platform_platform_alembic_version")
        )
        # Sentinel row: 0001 re-running would fail on the existing
        # tables; its survival proves the baseline was not re-created.
        connection.execute(
            text(
                "INSERT INTO dr_platform_throttle_backoff "
                "(throttle_key, consecutive_failures, metadata, updated_at) "
                "VALUES ('sentinel', 3, '{}', now())"
            )
        )

    stamp_platform_schema(url)
    upgrade_platform_schema(url)

    columns = _table_columns(pg_engine, "dr_platform_throttle_backoff")
    assert {"hold_until", "hold_reason", "tags"} <= columns
    tables = set(inspect(pg_engine).get_table_names())
    assert "dr_platform_projections" in tables
    with pg_engine.connect() as connection:
        count = connection.execute(
            text(
                "SELECT consecutive_failures FROM "
                "dr_platform_throttle_backoff "
                "WHERE throttle_key = 'sentinel'"
            )
        ).scalar_one()
    assert count == 3
    with pg_engine.connect() as connection:
        tags = connection.execute(
            text(
                "SELECT tags FROM dr_platform_throttle_backoff "
                "WHERE throttle_key = 'sentinel'"
            )
        ).scalar_one()
    assert tags == {}


def test_upgrade_accepts_percent_encoded_urls(pg_engine: Engine) -> None:
    # Percent-encoded query params (search_path options) must survive
    # alembic's configparser interpolation.
    url = str(pg_engine.url) + "?options=-csearch_path%3Dpublic"
    upgrade_platform_schema(url)
    assert "dr_platform_projections" in set(
        inspect(pg_engine).get_table_names()
    )


def test_search_path_schema_does_not_inherit_public_lineage(
    pg_engine: Engine,
) -> None:
    # An adopter with the lineage applied in public must still get real
    # DDL when migrating an isolated scratch schema on the same DB.
    upgrade_platform_schema(str(pg_engine.url))  # public at head
    with pg_engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS scratch CASCADE"))
        connection.execute(text("CREATE SCHEMA scratch"))
    scratch_url = (
        str(pg_engine.url) + "?options=-csearch_path%3Dscratch,public"
    )
    upgrade_platform_schema(scratch_url)

    scratch_tables = set(inspect(pg_engine).get_table_names("scratch"))
    assert {
        "dr_platform_throttle_backoff",
        "dr_platform_projections",
        "dr_platform_platform_alembic_version",
    } <= scratch_tables
