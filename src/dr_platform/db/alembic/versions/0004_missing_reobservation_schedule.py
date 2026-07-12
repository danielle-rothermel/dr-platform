# ruff: noqa: S608 -- every interpolated prefix is validated as an identifier
"""Add durable fair scheduling for terminal MISSING re-observation.

Revision ID: 0004_missing_reobserve
Revises: 0003_attempt_retry_reason
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform.db.schema import DEFAULT_PREFIX, PlatformSchema

revision = "0004_missing_reobserve"
down_revision = "0003_attempt_retry_reason"
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
    table = f"{prefix}_missing_reobservations"
    attempts = f"{prefix}_item_attempts"
    schedule_index = f"{prefix}_ix_missing_reobservations_schedule"
    change_index = f"{prefix}_ix_missing_reobservations_change_seq"
    _execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          item_id text NOT NULL,
          attempt integer NOT NULL,
          last_reobserved_at timestamptz NOT NULL,
          observation_count integer NOT NULL,
          created_at timestamptz NOT NULL,
          change_seq bigint NOT NULL,
          CONSTRAINT {prefix}_pk_missing_reobservations
            PRIMARY KEY (item_id, attempt),
          CONSTRAINT {prefix}_fk_missing_reobservations_attempt
            FOREIGN KEY (item_id, attempt)
            REFERENCES {attempts} (item_id, attempt) ON DELETE RESTRICT,
          CONSTRAINT {prefix}_ck_missing_reobservations_count
            CHECK (observation_count > 0)
        );
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = '{prefix}_assign_change_seq'
              AND tgrelid = '{table}'::regclass
          ) THEN
            CREATE TRIGGER {prefix}_assign_change_seq
              BEFORE INSERT OR UPDATE ON {table}
              FOR EACH ROW EXECUTE FUNCTION {prefix}_assign_change_seq();
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = '{prefix}_reject_kernel_delete'
              AND tgrelid = '{table}'::regclass
          ) THEN
            CREATE TRIGGER {prefix}_reject_kernel_delete
              BEFORE DELETE ON {table}
              FOR EACH ROW EXECUTE FUNCTION {prefix}_reject_kernel_delete();
          END IF;
        END
        $$;
        CREATE INDEX IF NOT EXISTS {schedule_index}
          ON {table} (last_reobserved_at, item_id, attempt);
        CREATE INDEX IF NOT EXISTS {change_index}
          ON {table} (change_seq);
        """
    )


def downgrade() -> None:
    prefix = _prefix()
    _execute(f"DROP TABLE {prefix}_missing_reobservations")
