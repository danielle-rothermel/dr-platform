from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine

from dr_platform.naming import PlatformNaming


def _naming() -> PlatformNaming:
    naming = context.config.attributes.get("naming")
    if isinstance(naming, PlatformNaming):
        return naming
    return PlatformNaming()


def run_migrations_online() -> None:
    naming = _naming()
    url = context.config.get_main_option("sqlalchemy.url")
    if not url:
        raise ValueError("sqlalchemy.url is required")
    engine = create_engine(url)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            version_table=naming.alembic_version_table,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


run_migrations_online()
