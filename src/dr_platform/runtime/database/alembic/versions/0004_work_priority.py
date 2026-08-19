# ruff: noqa: S608 -- every interpolated identifier uses a prefix validated by
# LedgerSchema; SQL parameters cannot represent DDL identifiers.
from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op

from dr_platform._core.ledger.schema import DEFAULT_PREFIX, LedgerSchema

revision = "0004_work_priority"
down_revision = "0003_stage_index_identity"
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
    work_items = _name(prefix, "work_items")
    stage_executions = _name(prefix, "stage_executions")
    old_index = _name(prefix, "ix_stage_executions_ready_admission")
    new_index = _name(prefix, "ix_stage_executions_ready_admission")
    ck_work_items = _name(prefix, "ck_work_items_priority")
    ck_stage = _name(prefix, "ck_stage_executions_priority")

    _execute(
        f"ALTER TABLE {work_items} "
        f"ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0"
    )
    _execute(
        f"ALTER TABLE {stage_executions} "
        f"ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0"
    )
    _execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = '{ck_work_items}'
          ) THEN
            ALTER TABLE {work_items}
            ADD CONSTRAINT {ck_work_items}
            CHECK (priority >= 0 AND priority <= 2147483647);
          END IF;
        END $$;
        """
    )
    _execute(
        f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = '{ck_stage}'
          ) THEN
            ALTER TABLE {stage_executions}
            ADD CONSTRAINT {ck_stage}
            CHECK (priority >= 0 AND priority <= 2147483647);
          END IF;
        END $$;
        """
    )
    _execute(f"DROP INDEX IF EXISTS {old_index}")
    _execute(
        f"""
        CREATE INDEX IF NOT EXISTS {new_index}
        ON {stage_executions} (stage_key, priority, rank)
        WHERE state = 'ready'
        """
    )
    _execute(
        f"""
        UPDATE {stage_executions} AS executions
        SET priority = work_items.priority
        FROM {work_items} AS work_items
        WHERE executions.work_item_id = work_items.work_item_id
        """
    )


def downgrade() -> None:
    raise NotImplementedError("work priority migration is irreversible")
