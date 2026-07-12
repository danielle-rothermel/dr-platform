# ruff: noqa: S608 -- every interpolated prefix is validated as an identifier
"""Add the internal automatic execution retry reason.

Revision ID: 0003_attempt_retry_reason
Revises: 0002_claim_workflow_provenance
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform.db.schema import DEFAULT_PREFIX, PlatformSchema

revision = "0003_attempt_retry_reason"
down_revision = "0002_claim_workflow_provenance"
branch_labels = None
depends_on = None


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
    PlatformSchema(prefix)
    return prefix


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def upgrade() -> None:
    prefix = _prefix()
    attempts = f"{prefix}_item_attempts"
    check_name = f"{prefix}_ck_attempts_retry_reason"
    _execute(
        f"""
        DO $$
        DECLARE existing_definition text;
        BEGIN
          SELECT pg_get_constraintdef(oid) INTO existing_definition
          FROM pg_constraint
          WHERE conname = '{check_name}'
            AND conrelid = '{attempts}'::regclass;
          IF existing_definition NOT LIKE '%automatic_execution_error%' THEN
            ALTER TABLE {attempts} DROP CONSTRAINT {check_name};
            ALTER TABLE {attempts} ADD CONSTRAINT {check_name}
              CHECK (retry_reason IS NULL OR retry_reason IN (
                'automatic_execution_error',
                'domain_outcome',
                'operator_cancel_retry'
              ));
          END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    prefix = _prefix()
    attempts = f"{prefix}_item_attempts"
    check_name = f"{prefix}_ck_attempts_retry_reason"
    _execute(
        f"""
        ALTER TABLE {attempts} DROP CONSTRAINT {check_name};
        ALTER TABLE {attempts} ADD CONSTRAINT {check_name}
          CHECK (retry_reason IS NULL OR retry_reason IN (
            'domain_outcome', 'operator_cancel_retry'
          ));
        """
    )
