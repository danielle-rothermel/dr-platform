"""Make registration package-owned.

Revision ID: 0004_package_owned_registration
Revises: 0003_validation_ownership
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op

revision = "0004_package_owned_registration"
down_revision = "0003_validation_ownership"
branch_labels = None
depends_on = None

DEFAULT_PREFIX = "platform"
MAX_PREFIX_BYTES = 21
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")

OPERATION_COUNT_CHECK = """
requested_count >= 0
AND inserted_count >= 0
AND enqueued_count >= 0
AND workflow_already_present_count >= 0
AND enqueue_failed_count >= 0
AND active_count >= 0
AND succeeded_count >= 0
AND terminal_failed_count >= 0
AND cancelled_count >= 0
AND inserted_count <= requested_count
AND enqueued_count + workflow_already_present_count + enqueue_failed_count
    <= requested_count
AND active_count + succeeded_count + terminal_failed_count + cancelled_count
    <= requested_count
""".strip()

LEGACY_OPERATION_COUNT_CHECK = """
requested_count >= 0
AND inserted_count >= 0
AND already_present_count >= 0
AND enqueued_count >= 0
AND workflow_already_present_count >= 0
AND enqueue_failed_count >= 0
AND active_count >= 0
AND succeeded_count >= 0
AND terminal_failed_count >= 0
AND cancelled_count >= 0
AND inserted_count + already_present_count <= requested_count
AND enqueued_count + workflow_already_present_count + enqueue_failed_count
    <= requested_count
AND active_count + succeeded_count + terminal_failed_count + cancelled_count
    <= requested_count
""".strip()

REGISTRATION_COMPLETION_CHECK = """
registration_completed_at IS NULL
OR (
  registration_cursor = registration_page_count
  AND inserted_count = requested_count
  AND registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)
""".strip()

LEGACY_REGISTRATION_COMPLETION_CHECK = """
registration_completed_at IS NULL
OR (
  registration_cursor = registration_page_count
  AND inserted_count + already_present_count = requested_count
  AND registration_lease_id IS NULL
  AND registration_lease_expires_at IS NULL
)
""".strip()


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
    if PREFIX_PATTERN.fullmatch(prefix) is None:
        raise ValueError(
            "prefix must be a lowercase SQL identifier using letters, "
            "numbers, or _"
        )
    if len(prefix.encode()) > MAX_PREFIX_BYTES:
        raise ValueError(
            "prefix is too long for generated PostgreSQL identifiers: "
            f"maximum is {MAX_PREFIX_BYTES} ASCII bytes"
        )
    return prefix


def _name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _replace_item_guard(prefix: str, *, legacy: bool) -> None:
    function = _name(prefix, "guard_item_update")
    insert_status_new = (
        "\n                NEW.insert_status," if legacy else ""
    )
    insert_status_old = (
        "\n                OLD.insert_status," if legacy else ""
    )
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION {function}() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW IS NOT DISTINCT FROM OLD THEN
                RETURN NULL;
              END IF;
              IF ROW(
                NEW.item_id,
                NEW.operation_key,
                NEW.item_index,
                NEW.item_key,
                NEW.shuffle_rank,
                NEW.service_class,
                NEW.service_priority,
                NEW.spec,{insert_status_new}
                NEW.created_at
              ) IS DISTINCT FROM ROW(
                OLD.item_id,
                OLD.operation_key,
                OLD.item_index,
                OLD.item_key,
                OLD.shuffle_rank,
                OLD.service_class,
                OLD.service_priority,
                OLD.spec,{insert_status_old}
                OLD.created_at
              ) THEN
                RAISE EXCEPTION 'Item identity fields are immutable';
              END IF;
              RETURN NEW;
            END;
            $$
            """
        )
    )


def upgrade() -> None:
    prefix = _prefix()
    operations = _name(prefix, "operations")
    items = _name(prefix, "items")

    op.drop_constraint(
        _name(prefix, "ck_operations_counts"),
        operations,
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        operations,
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_items_insert_status"),
        items,
        type_="check",
    )

    # A legacy hook could classify an inserted platform Item as already
    # present. Package ownership makes every persisted Item an insertion, so
    # preserve the registered total before removing that distinction.
    operation_counts = sa.table(
        operations,
        sa.column("inserted_count", sa.Integer()),
        sa.column("already_present_count", sa.Integer()),
    )
    op.execute(
        operation_counts.update().values(
            inserted_count=(
                operation_counts.c.inserted_count
                + operation_counts.c.already_present_count
            )
        )
    )
    _replace_item_guard(prefix, legacy=False)
    op.drop_column(operations, "already_present_count")
    op.drop_column(items, "insert_status")

    op.create_check_constraint(
        _name(prefix, "ck_operations_counts"),
        operations,
        OPERATION_COUNT_CHECK,
    )
    op.create_check_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        operations,
        REGISTRATION_COMPLETION_CHECK,
    )


def downgrade() -> None:
    prefix = _prefix()
    operations = _name(prefix, "operations")
    items = _name(prefix, "items")

    op.drop_constraint(
        _name(prefix, "ck_operations_counts"),
        operations,
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        operations,
        type_="check",
    )

    # The removed classification is irrecoverable. A downgrade explicitly
    # represents every registered Item as inserted and no Item as pre-existing.
    op.add_column(
        operations,
        sa.Column(
            "already_present_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.alter_column(
        operations,
        "already_present_count",
        existing_type=sa.Integer(),
        existing_nullable=False,
        server_default=None,
    )
    op.add_column(
        items,
        sa.Column(
            "insert_status",
            sa.Text(),
            nullable=False,
            server_default="inserted",
        ),
    )
    op.alter_column(
        items,
        "insert_status",
        existing_type=sa.Text(),
        existing_nullable=False,
        server_default=None,
    )
    op.create_check_constraint(
        _name(prefix, "ck_items_insert_status"),
        items,
        "insert_status IN ('inserted', 'already_present')",
    )
    op.create_check_constraint(
        _name(prefix, "ck_operations_counts"),
        operations,
        LEGACY_OPERATION_COUNT_CHECK,
    )
    op.create_check_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        operations,
        LEGACY_REGISTRATION_COMPLETION_CHECK,
    )
    _replace_item_guard(prefix, legacy=True)
