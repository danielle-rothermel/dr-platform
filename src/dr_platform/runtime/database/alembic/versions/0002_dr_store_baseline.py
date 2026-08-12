from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from dr_store.storage_backends.postgresql import (
    POSTGRES_METADATA,
    POSTGRES_SCHEMA_FORMAT,
)

revision = "0002_dr_store_baseline"
down_revision = "0001_staging_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    connection = op.get_bind()
    installed = connection.execute(
        sa.text("SELECT to_regnamespace('dr_store') IS NOT NULL")
    ).scalar_one()
    if installed:
        return
    connection.execute(sa.text("CREATE SCHEMA dr_store"))
    POSTGRES_METADATA.create_all(connection)
    connection.execute(
        sa.text(
            "INSERT INTO dr_store.schema_format (singleton, format) "
            "VALUES (TRUE, :format)"
        ),
        {"format": POSTGRES_SCHEMA_FORMAT},
    )


def downgrade() -> None:
    raise NotImplementedError("dr_store baseline migration is irreversible")
