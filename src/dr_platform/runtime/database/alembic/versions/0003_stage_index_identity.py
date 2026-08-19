# ruff: noqa: S608 -- every interpolated identifier uses a prefix validated by
# LedgerSchema; SQL parameters cannot represent DDL identifiers.
from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform._core.ledger.schema import DEFAULT_PREFIX, LedgerSchema

revision = "0003_stage_index_identity"
down_revision = "0002_dr_store_baseline"
branch_labels = None
depends_on = None


def _prefix() -> str:
    raw = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(raw, str):
        raise TypeError("migration prefix must be a string")
    return LedgerSchema(raw).prefix


def _name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _execute(sql: str) -> None:
    op.execute(sa.text(sql))


def upgrade() -> None:
    prefix = _prefix()
    stage_executions = _name(prefix, "stage_executions")
    work_items = _name(prefix, "work_items")
    old_unique = _name(prefix, "uq_stage_executions_work_stage")
    new_unique = _name(prefix, "uq_stage_executions_work_index")

    _execute(
        f"ALTER TABLE {stage_executions} "
        f"ADD COLUMN IF NOT EXISTS input_reference TEXT"
    )
    _execute(
        f"""
        UPDATE {stage_executions} AS executions
        SET input_reference = work_items.input_reference
        FROM {work_items} AS work_items
        WHERE executions.work_item_id = work_items.work_item_id
          AND executions.input_reference IS NULL
        """
    )
    _execute(
        f"ALTER TABLE {stage_executions} "
        f"DROP CONSTRAINT IF EXISTS {old_unique}"
    )
    _execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = '{new_unique}'
          ) THEN
            ALTER TABLE {stage_executions}
            ADD CONSTRAINT {new_unique}
            UNIQUE (work_item_id, stage_index);
          END IF;
        END $$;
        """
    )
    _execute(
        f"ALTER TABLE {stage_executions} "
        f"ADD COLUMN IF NOT EXISTS barrier BOOLEAN NOT NULL DEFAULT false"
    )


def downgrade() -> None:
    raise NotImplementedError("stage index identity migration is irreversible")
