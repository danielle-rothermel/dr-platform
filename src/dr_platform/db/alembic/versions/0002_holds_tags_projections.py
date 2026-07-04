"""Operator holds + target tags on the throttle table; projections
registry.

Runs for every adopter — including stamped-baseline ones (whetstone
stamps 0001, then upgrades through this).

Revision ID: 0002_holds_tags_projections
Revises: 0001_platform_baseline
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects.postgresql import JSONB

from dr_platform.naming import PlatformNaming

revision = "0002_holds_tags_projections"
down_revision = "0001_platform_baseline"
branch_labels = None
depends_on = None


def _naming() -> PlatformNaming:
    naming = context.config.attributes.get("naming")
    if isinstance(naming, PlatformNaming):
        return naming
    return PlatformNaming()


def upgrade() -> None:
    naming = _naming()
    op.add_column(
        naming.throttle_backoff_table,
        sa.Column("hold_until", sa.DateTime(timezone=True)),
    )
    op.add_column(
        naming.throttle_backoff_table,
        sa.Column("hold_reason", sa.Text),
    )
    op.add_column(
        naming.throttle_backoff_table,
        sa.Column(
            "tags",
            JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_table(
        naming.projections_table,
        sa.Column("projection_name", sa.Text, primary_key=True),
        sa.Column("projection_version", sa.Text, primary_key=True),
        sa.Column("built_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("row_count", sa.Integer, nullable=False),
    )


def downgrade() -> None:
    naming = _naming()
    op.drop_table(naming.projections_table)
    op.drop_column(naming.throttle_backoff_table, "tags")
    op.drop_column(naming.throttle_backoff_table, "hold_reason")
    op.drop_column(naming.throttle_backoff_table, "hold_until")
