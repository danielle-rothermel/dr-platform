"""Platform baseline: batch operations/items + throttle backoff.

Matches the table/column shapes that stamped-baseline adopters
(whetstone) already have from their own frozen migration history —
those adopters STAMP this revision instead of running it.

Revision ID: 0001_platform_baseline
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from dr_platform.db.schema import (
    BATCH_OPS_COMPLETED_CHECK,
    BATCH_OPS_COUNT_BOUNDS_CHECK,
)
from dr_platform.naming import PlatformNaming

revision = "0001_platform_baseline"
down_revision = None
branch_labels = None
depends_on = None


def _naming() -> PlatformNaming:
    naming = context.config.attributes.get("naming")
    if isinstance(naming, PlatformNaming):
        return naming
    return PlatformNaming()


def upgrade() -> None:
    naming = _naming()
    prefix = naming.prefix

    op.create_table(
        naming.batch_operations_table,
        sa.Column("operation_key", sa.Text, primary_key=True),
        sa.Column(naming.group_key_label, sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("requested_count", sa.Integer, nullable=False),
        sa.Column("inserted_count", sa.Integer, nullable=False),
        sa.Column("already_present_count", sa.Integer, nullable=False),
        sa.Column("enqueued_count", sa.Integer, nullable=False),
        sa.Column("already_scheduled_count", sa.Integer, nullable=False),
        sa.Column("failed_count", sa.Integer, nullable=False),
        sa.Column("spec", JSONB, nullable=False),
        sa.Column("metadata", JSONB, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "status IN ('enqueuing', 'completed', 'partial', 'error')",
            name=f"ck_{prefix}_batch_ops_status",
        ),
        sa.CheckConstraint(
            "requested_count >= 0 AND inserted_count >= 0 "
            "AND already_present_count >= 0 AND enqueued_count >= 0 "
            "AND already_scheduled_count >= 0 AND failed_count >= 0",
            name=f"ck_{prefix}_batch_ops_counts",
        ),
        sa.CheckConstraint(
            BATCH_OPS_COUNT_BOUNDS_CHECK,
            name=f"ck_{prefix}_batch_ops_count_bounds",
        ),
        sa.CheckConstraint(
            BATCH_OPS_COMPLETED_CHECK,
            name=f"ck_{prefix}_batch_ops_completed",
        ),
        sa.CheckConstraint(
            "completed_at IS NULL OR completed_at >= created_at",
            name=f"ck_{prefix}_batch_ops_time_order",
        ),
    )
    op.create_index(
        f"ix_{prefix}_batch_ops_group",
        naming.batch_operations_table,
        [naming.group_key_label],
    )
    op.create_index(
        f"ix_{prefix}_batch_ops_status_lib",
        naming.batch_operations_table,
        ["status"],
    )

    op.create_table(
        naming.batch_items_table,
        sa.Column("batch_submit_item_id", sa.Text, primary_key=True),
        sa.Column(
            "operation_key",
            sa.Text,
            sa.ForeignKey(f"{naming.batch_operations_table}.operation_key"),
            nullable=False,
        ),
        sa.Column("item_index", sa.Integer, nullable=False),
        sa.Column(naming.item_key_label, sa.Text, nullable=False),
        sa.Column(naming.order_key_label, sa.Text, nullable=False),
        sa.Column("insert_status", sa.Text, nullable=False),
        sa.Column("enqueue_status", sa.Text, nullable=False),
        sa.Column("enqueue_metadata", JSONB, nullable=False),
        sa.Column("failure", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "item_index >= 0",
            name=f"ck_{prefix}_batch_items_item_index",
        ),
        sa.CheckConstraint(
            "insert_status IN ('inserted', 'already_present')",
            name=f"ck_{prefix}_batch_items_insert_status",
        ),
        sa.CheckConstraint(
            "enqueue_status IN ('pending', 'claiming', 'enqueued', "
            "'workflow_already_present', 'failed')",
            name=f"ck_{prefix}_batch_items_enqueue_status",
        ),
        sa.CheckConstraint(
            "(enqueue_status = 'failed' OR failure IS NULL) "
            "AND (enqueue_status != 'failed' OR failure IS NOT NULL)",
            name=f"ck_{prefix}_batch_items_enqueue_status_payload",
        ),
        sa.UniqueConstraint(
            "operation_key",
            "item_index",
            name=f"uq_{prefix}_batch_items_operation_index",
        ),
        sa.UniqueConstraint(
            "operation_key",
            naming.item_key_label,
            name=f"uq_{prefix}_batch_items_operation_item",
        ),
    )
    op.create_index(
        f"ix_{prefix}_batch_items_operation_lib",
        naming.batch_items_table,
        ["operation_key"],
    )
    op.create_index(
        f"ix_{prefix}_batch_items_item",
        naming.batch_items_table,
        [naming.item_key_label],
    )
    op.create_index(
        f"ix_{prefix}_batch_items_order",
        naming.batch_items_table,
        [naming.order_key_label],
    )

    op.create_table(
        naming.throttle_backoff_table,
        sa.Column("throttle_key", sa.Text, primary_key=True),
        sa.Column("blocked_until", sa.DateTime(timezone=True)),
        sa.Column("consecutive_failures", sa.Integer, nullable=False),
        sa.Column("failure_class", sa.Text),
        sa.Column("last_error_type", sa.Text),
        sa.Column("last_message", sa.Text),
        sa.Column("metadata", JSONB, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "consecutive_failures >= 0",
            name=f"ck_{prefix}_throttle_backoff_failures",
        ),
    )
    op.create_index(
        f"ix_{prefix}_throttle_blocked_until",
        naming.throttle_backoff_table,
        ["blocked_until"],
    )


def downgrade() -> None:
    naming = _naming()
    op.drop_table(naming.throttle_backoff_table)
    op.drop_table(naming.batch_items_table)
    op.drop_table(naming.batch_operations_table)
