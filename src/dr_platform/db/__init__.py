"""Library-owned schema and its Alembic lineage."""

from dr_platform.db.migrate import upgrade_platform_schema
from dr_platform.db.schema import PlatformSchema

__all__ = [
    "PlatformSchema",
    "upgrade_platform_schema",
]
