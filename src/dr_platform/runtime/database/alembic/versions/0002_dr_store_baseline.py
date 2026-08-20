from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from dr_store import POSTGRES_METADATA, POSTGRES_SCHEMA_FORMAT

revision = "0002_dr_store_baseline"
down_revision = "0001_staging_baseline"
branch_labels = None
depends_on = None

_SCHEMA_FORMAT_TABLE_EXISTS_SQL = sa.text(
    """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'dr_store'
          AND table_name = 'schema_format'
    )
    """
)

_GET_SCHEMA_FORMATS_SQL = sa.text("SELECT format FROM dr_store.schema_format")

_HAS_SCHEMA_FORMAT_MARKER_SQL = sa.text(
    """
    SELECT EXISTS (
        SELECT 1
        FROM dr_store.schema_format
        WHERE singleton IS TRUE
    )
    """
)

_INSERT_SCHEMA_FORMAT_SQL = sa.text(
    """
    INSERT INTO dr_store.schema_format (singleton, format)
    VALUES (TRUE, :format)
    """
)


def _validate_existing_schema_format(connection: sa.Connection) -> None:
    if not connection.execute(_SCHEMA_FORMAT_TABLE_EXISTS_SQL).scalar_one():
        return
    formats = connection.execute(_GET_SCHEMA_FORMATS_SQL).scalars().all()
    if formats and formats != [POSTGRES_SCHEMA_FORMAT]:
        raise RuntimeError(
            "dr_store schema exists with incompatible format; "
            "drop the dr_store namespace before upgrading"
        )


def _ensure_schema_format_marker(connection: sa.Connection) -> None:
    formats = connection.execute(_GET_SCHEMA_FORMATS_SQL).scalars().all()
    if formats != [POSTGRES_SCHEMA_FORMAT]:
        raise RuntimeError(
            "PostgreSQL schema format marker is missing, malformed, or "
            "unsupported"
        )


def upgrade() -> None:
    connection = op.get_bind()
    schema_exists = connection.execute(
        sa.text("SELECT to_regnamespace('dr_store') IS NOT NULL")
    ).scalar_one()

    if schema_exists:
        _validate_existing_schema_format(connection)
    else:
        connection.execute(sa.text("CREATE SCHEMA dr_store"))

    POSTGRES_METADATA.create_all(connection, checkfirst=True)

    if not connection.execute(_HAS_SCHEMA_FORMAT_MARKER_SQL).scalar_one():
        connection.execute(
            _INSERT_SCHEMA_FORMAT_SQL,
            {"format": POSTGRES_SCHEMA_FORMAT},
        )

    _ensure_schema_format_marker(connection)


def downgrade() -> None:
    raise NotImplementedError("dr_store baseline migration is irreversible")
