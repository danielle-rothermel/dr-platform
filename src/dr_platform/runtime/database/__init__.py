"""Library-owned schema and its Alembic lineage."""

from dr_platform.runtime.database.migrate import upgrade_platform_schema

__all__ = [
    "upgrade_platform_schema",
]
