# ruff: noqa: S608 -- every interpolated prefix is validated as an identifier
"""Bind compensation provenance to the Claim workflow identity.

Revision ID: 0002_claim_workflow_provenance
Revises: 0001_platform_baseline
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform.db.schema import DEFAULT_PREFIX, PlatformSchema

revision = "0002_claim_workflow_provenance"
down_revision = "0001_platform_baseline"
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
    claims = f"{prefix}_enqueue_claims"
    compensations = f"{prefix}_enqueue_compensations"
    unique_name = f"{prefix}_uq_claims_workflow_provenance"
    foreign_key_name = f"{prefix}_fk_compensations_claim"
    _execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = '{unique_name}'
              AND conrelid = '{claims}'::regclass
          ) THEN
            ALTER TABLE {claims}
              ADD CONSTRAINT {unique_name}
              UNIQUE (item_id, attempt, claim_id, workflow_id);
          END IF;
        END
        $$
        """
    )
    _execute(
        f"""
        DO $$
        DECLARE
          existing_definition text;
        BEGIN
          SELECT pg_get_constraintdef(oid)
          INTO existing_definition
          FROM pg_constraint
          WHERE conname = '{foreign_key_name}'
            AND conrelid = '{compensations}'::regclass;

          IF existing_definition IS NULL
             OR existing_definition NOT LIKE '%workflow_id%' THEN
            IF existing_definition IS NOT NULL THEN
              ALTER TABLE {compensations}
                DROP CONSTRAINT {foreign_key_name};
            END IF;
            ALTER TABLE {compensations}
              ADD CONSTRAINT {foreign_key_name}
              FOREIGN KEY (item_id, attempt, claim_id, workflow_id)
              REFERENCES {claims}
                (item_id, attempt, claim_id, workflow_id)
              ON DELETE RESTRICT;
          END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    prefix = _prefix()
    claims = f"{prefix}_enqueue_claims"
    compensations = f"{prefix}_enqueue_compensations"
    unique_name = f"{prefix}_uq_claims_workflow_provenance"
    foreign_key_name = f"{prefix}_fk_compensations_claim"
    _execute(
        f"""
        ALTER TABLE {compensations}
          DROP CONSTRAINT {foreign_key_name};
        ALTER TABLE {compensations}
          ADD CONSTRAINT {foreign_key_name}
          FOREIGN KEY (item_id, attempt, claim_id)
          REFERENCES {claims} (item_id, attempt, claim_id)
          ON DELETE RESTRICT;
        ALTER TABLE {claims}
          DROP CONSTRAINT {unique_name};
        """
    )
