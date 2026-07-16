"""Adopt the single-read registration schema.

Revision ID: 0002_single_read_registration
Revises: 0001_platform_baseline
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op

revision = "0002_single_read_registration"
down_revision = "0001_platform_baseline"
branch_labels = None
depends_on = None

DEFAULT_PREFIX = "platform"
MAX_PREFIX_BYTES = 21
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")

REGISTRATION_BOUNDS_CHECK = """
registration_page_size > 0
AND registration_page_count >= 0
AND registration_cursor >= 0
AND registration_cursor <= registration_page_count
AND (
  (requested_count = 0 AND registration_page_count = 0)
  OR (
    requested_count > 0
    AND registration_page_count =
      (requested_count + registration_page_size - 1) / registration_page_size
  )
)
""".strip()

REGISTRATION_COMPLETION_CHECK = """
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


def _replace_operation_guard(prefix: str) -> None:
    function = _name(prefix, "guard_operation_update")
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
                NEW.operation_key,
                NEW.group_key,
                NEW.workflow_role,
                NEW.requested_count,
                NEW.registration_page_size,
                NEW.registration_page_count,
                NEW.target_key,
                NEW.target_version,
                NEW.target_contract_digest,
                NEW.retry_policy,
                NEW.spec,
                NEW.metadata,
                NEW.created_at
              ) IS DISTINCT FROM ROW(
                OLD.operation_key,
                OLD.group_key,
                OLD.workflow_role,
                OLD.requested_count,
                OLD.registration_page_size,
                OLD.registration_page_count,
                OLD.target_key,
                OLD.target_version,
                OLD.target_contract_digest,
                OLD.retry_policy,
                OLD.spec,
                OLD.metadata,
                OLD.created_at
              ) THEN
                RAISE EXCEPTION 'Operation identity fields are immutable';
              END IF;
              RETURN NEW;
            END;
            $$
            """
        )
    )


def upgrade() -> None:
    prefix = _prefix()
    table = _name(prefix, "operations")

    op.drop_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        table,
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_operations_manifest"),
        table,
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_operations_manifest_version"),
        table,
        type_="check",
    )

    op.alter_column(
        table,
        "manifest_page_size",
        new_column_name="registration_page_size",
        existing_type=sa.Integer(),
        existing_nullable=False,
    )
    op.alter_column(
        table,
        "manifest_page_count",
        new_column_name="registration_page_count",
        existing_type=sa.Integer(),
        existing_nullable=False,
    )
    op.drop_column(table, "manifest_version")
    op.drop_column(table, "manifest_digest")
    op.drop_column(table, "operation_execution_recipe_digest")

    op.create_check_constraint(
        _name(prefix, "ck_operations_registration_bounds"),
        table,
        REGISTRATION_BOUNDS_CHECK,
    )
    op.create_check_constraint(
        _name(prefix, "ck_operations_registration_completed"),
        table,
        REGISTRATION_COMPLETION_CHECK,
    )
    _replace_operation_guard(prefix)


def downgrade() -> None:
    raise RuntimeError(
        "0002_single_read_registration is irreversible because the removed "
        "manifest identity digests cannot be reconstructed"
    )
