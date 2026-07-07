"""SQLAlchemy table definitions, parameterized by ``PlatformNaming``.

The canonical shapes. Fresh adopters get these verbatim from Alembic
revision 0001; stamped-baseline adopters (whetstone) already have
byte-identical tables from their own frozen history, so for them only
table and column names matter — constraint/index names are
library-generated.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB

from dr_platform.batch_status import (
    BatchItemEnqueueStatus,
    BatchItemInsertStatus,
    BatchOperationStatus,
)
from dr_platform.naming import PlatformNaming

BATCH_OPS_COUNT_BOUNDS_CHECK = """
inserted_count <= requested_count
AND already_present_count <= requested_count
AND enqueued_count <= requested_count
AND failed_count <= requested_count
AND inserted_count + already_present_count <= requested_count
AND enqueued_count + already_scheduled_count + failed_count <= requested_count
""".strip()

BATCH_OPS_COMPLETED_CHECK = """
status != 'completed'
OR (
  completed_at IS NOT NULL
  AND enqueued_count + already_scheduled_count + failed_count = requested_count
)
""".strip()


def enum_check(column_name: str, enum_type: type[StrEnum]) -> str:
    values = ", ".join(f"'{value.value}'" for value in enum_type)
    return f"{column_name} IN ({values})"


class PlatformSchema:
    """The library-owned tables under one naming configuration."""

    def __init__(self, naming: PlatformNaming | None = None) -> None:
        self.naming = naming or PlatformNaming()
        prefix = self.naming.prefix
        self.metadata = MetaData()

        self.batch_operations = Table(
            self.naming.batch_operations_table,
            self.metadata,
            Column("operation_key", Text, primary_key=True),
            Column(self.naming.group_key_label, Text, nullable=False),
            Column("status", Text, nullable=False),
            Column("requested_count", Integer, nullable=False),
            Column("inserted_count", Integer, nullable=False),
            Column("already_present_count", Integer, nullable=False),
            Column("enqueued_count", Integer, nullable=False),
            Column("already_scheduled_count", Integer, nullable=False),
            Column("failed_count", Integer, nullable=False),
            Column("spec", JSONB, nullable=False),
            Column("metadata", JSONB, nullable=False),
            Column("created_at", DateTime(timezone=True), nullable=False),
            Column("completed_at", DateTime(timezone=True)),
            CheckConstraint(
                enum_check("status", BatchOperationStatus),
                name=f"ck_{prefix}_batch_ops_status",
            ),
            CheckConstraint(
                "requested_count >= 0 AND inserted_count >= 0 "
                "AND already_present_count >= 0 AND enqueued_count >= 0 "
                "AND already_scheduled_count >= 0 AND failed_count >= 0",
                name=f"ck_{prefix}_batch_ops_counts",
            ),
            CheckConstraint(
                BATCH_OPS_COUNT_BOUNDS_CHECK,
                name=f"ck_{prefix}_batch_ops_count_bounds",
            ),
            CheckConstraint(
                BATCH_OPS_COMPLETED_CHECK,
                name=f"ck_{prefix}_batch_ops_completed",
            ),
            CheckConstraint(
                "completed_at IS NULL OR completed_at >= created_at",
                name=f"ck_{prefix}_batch_ops_time_order",
            ),
        )

        self.batch_items = Table(
            self.naming.batch_items_table,
            self.metadata,
            Column("batch_submit_item_id", Text, primary_key=True),
            Column(
                "operation_key",
                Text,
                ForeignKey(
                    f"{self.naming.batch_operations_table}.operation_key"
                ),
                nullable=False,
            ),
            Column("item_index", Integer, nullable=False),
            Column(self.naming.item_key_label, Text, nullable=False),
            Column(self.naming.order_key_label, Text, nullable=False),
            Column("insert_status", Text, nullable=False),
            Column("enqueue_status", Text, nullable=False),
            Column("enqueue_metadata", JSONB, nullable=False),
            Column("failure", JSONB),
            Column("created_at", DateTime(timezone=True), nullable=False),
            CheckConstraint(
                "item_index >= 0",
                name=f"ck_{prefix}_batch_items_item_index",
            ),
            CheckConstraint(
                enum_check("insert_status", BatchItemInsertStatus),
                name=f"ck_{prefix}_batch_items_insert_status",
            ),
            CheckConstraint(
                enum_check("enqueue_status", BatchItemEnqueueStatus),
                name=f"ck_{prefix}_batch_items_enqueue_status",
            ),
            CheckConstraint(
                "(enqueue_status = 'failed' OR failure IS NULL) "
                "AND (enqueue_status != 'failed' OR failure IS NOT NULL)",
                name=f"ck_{prefix}_batch_items_enqueue_status_payload",
            ),
            UniqueConstraint(
                "operation_key",
                "item_index",
                name=f"uq_{prefix}_batch_items_operation_index",
            ),
            UniqueConstraint(
                "operation_key",
                self.naming.item_key_label,
                name=f"uq_{prefix}_batch_items_operation_item",
            ),
        )

        self.throttle_backoff = Table(
            self.naming.throttle_backoff_table,
            self.metadata,
            Column("throttle_key", Text, primary_key=True),
            Column("blocked_until", DateTime(timezone=True)),
            Column("consecutive_failures", Integer, nullable=False),
            Column("failure_class", Text),
            Column("last_error_type", Text),
            Column("last_message", Text),
            Column("metadata", JSONB, nullable=False),
            Column("updated_at", DateTime(timezone=True), nullable=False),
            # 0002 additions: operator holds + target tags.
            Column("hold_until", DateTime(timezone=True)),
            Column("hold_reason", Text),
            Column("tags", JSONB, nullable=False, server_default="{}"),
            CheckConstraint(
                "consecutive_failures >= 0",
                name=f"ck_{prefix}_throttle_backoff_failures",
            ),
        )

        self.projections = Table(
            self.naming.projections_table,
            self.metadata,
            Column("projection_name", Text, primary_key=True),
            Column("projection_version", Text, primary_key=True),
            Column("built_at", DateTime(timezone=True), nullable=False),
            Column("row_count", Integer, nullable=False),
        )

        Index(
            f"ix_{prefix}_batch_ops_group",
            self.batch_operations.c[self.naming.group_key_label],
        )
        Index(
            f"ix_{prefix}_batch_ops_status_lib",
            self.batch_operations.c.status,
        )
        Index(
            f"ix_{prefix}_batch_items_operation_lib",
            self.batch_items.c.operation_key,
        )
        Index(
            f"ix_{prefix}_batch_items_item",
            self.batch_items.c[self.naming.item_key_label],
        )
        Index(
            f"ix_{prefix}_batch_items_order",
            self.batch_items.c[self.naming.order_key_label],
        )
        Index(
            f"ix_{prefix}_throttle_blocked_until",
            self.throttle_backoff.c.blocked_until,
        )
