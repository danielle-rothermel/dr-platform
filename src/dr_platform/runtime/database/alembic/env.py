from __future__ import annotations

from alembic import context
from sqlalchemy import create_engine

from dr_platform._core.ledger.schema import DEFAULT_PREFIX


def _prefix() -> str:
    prefix = context.config.attributes.get("prefix", DEFAULT_PREFIX)
    if not isinstance(prefix, str):
        raise TypeError("migration prefix must be a string")
    return prefix


def run_migrations_online() -> None:
    prefix = _prefix()
    url = context.config.get_main_option("sqlalchemy.url")
    if not url:
        raise ValueError("sqlalchemy.url is required")
    engine = create_engine(url)
    with engine.connect() as probe:
        current_schema = probe.exec_driver_sql(
            "SELECT current_schema()"
        ).scalar()
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=None,
            version_table=f"{prefix}_platform_alembic_version",
            version_table_schema=current_schema,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


run_migrations_online()
