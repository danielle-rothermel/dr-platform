"""Adopt validation-ownership schema constraints.

Revision ID: 0003_validation_ownership
Revises: 0002_single_read_registration
"""

from __future__ import annotations

import re

import sqlalchemy as sa
from alembic import context, op

revision = "0003_validation_ownership"
down_revision = "0002_single_read_registration"
branch_labels = None
depends_on = None

DEFAULT_PREFIX = "platform"
MAX_PREFIX_BYTES = 21
PREFIX_PATTERN = re.compile(r"[a-z_][a-z0-9_]*")


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


def upgrade() -> None:
    prefix = _prefix()
    op.create_check_constraint(
        _name(prefix, "ck_requests_resolved_time"),
        _name(prefix, "next_attempt_requests"),
        "resolved_at >= created_at",
    )
    op.create_check_constraint(
        _name(prefix, "ck_throttle_state_failure_count"),
        _name(prefix, "throttle_state"),
        "failure_class IS NULL OR consecutive_failures > 0",
    )
    op.drop_constraint(
        _name(prefix, "ck_attempts_workflow"),
        _name(prefix, "item_attempts"),
        type_="check",
    )


def downgrade() -> None:
    prefix = _prefix()
    attempts = _name(prefix, "item_attempts")
    attempts_table = sa.table(
        attempts,
        sa.column("enqueue_state"),
        sa.column("workflow_id"),
    )
    incompatible_attempt_exists = op.get_bind().execute(
        sa.select(
            sa.exists().where(
                attempts_table.c.enqueue_state.in_(
                    ("enqueued", "workflow_already_present")
                ),
                attempts_table.c.workflow_id.is_(None),
            )
        )
    ).scalar_one()
    if incompatible_attempt_exists:
        raise RuntimeError(
            "cannot downgrade validation ownership while an enqueued attempt "
            "has no workflow_id"
        )

    op.create_check_constraint(
        _name(prefix, "ck_attempts_workflow"),
        attempts,
        "enqueue_state NOT IN ('enqueued', 'workflow_already_present') "
        "OR workflow_id IS NOT NULL",
    )
    op.drop_constraint(
        _name(prefix, "ck_throttle_state_failure_count"),
        _name(prefix, "throttle_state"),
        type_="check",
    )
    op.drop_constraint(
        _name(prefix, "ck_requests_resolved_time"),
        _name(prefix, "next_attempt_requests"),
        type_="check",
    )
