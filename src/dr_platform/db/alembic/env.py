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
    # Pin the version table to the connection's first search_path schema:
    # adopters that isolate work in scratch schemas must not see (or
    # write) a lineage that lives in a fallback schema. Probed on its own
    # connection so the migration connection's transaction stays
    # alembic-owned.
    with engine.connect() as probe:
        current_schema = probe.exec_driver_sql(
            "SELECT current_schema()"
        ).scalar()
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            version_table=naming.alembic_version_table,
            version_table_schema=current_schema,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


run_migrations_online()
