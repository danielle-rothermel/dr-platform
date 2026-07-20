"""Library-owned schema and its Alembic lineage."""

from dr_platform.db.migrate import upgrade_platform_schema

__all__ = [
    "upgrade_platform_schema",
]
