from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from dr_store.storage_backends.postgresql import install_postgres_sync

revision = "0002_dr_store_baseline"
down_revision = "0001_staging_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    engine = op.get_bind().engine
    with engine.connect() as connection:
        installed = connection.execute(
            sa.text("SELECT to_regnamespace('dr_store') IS NOT NULL")
        ).scalar_one()
    if not installed:
        install_postgres_sync(engine)


def downgrade() -> None:
    op.execute(sa.text("DROP SCHEMA IF EXISTS dr_store CASCADE"))
