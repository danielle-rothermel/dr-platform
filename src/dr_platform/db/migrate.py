"""Programmatic Alembic entrypoint for the fresh platform lineage."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from dr_platform.db.schema import DEFAULT_PREFIX

PLATFORM_BASELINE_REVISION = "0001_platform_baseline"
PLATFORM_HEAD_REVISION = PLATFORM_BASELINE_REVISION

_ALEMBIC_DIR = Path(__file__).resolve().parent / "alembic"


def _alembic_config(database_url: str, prefix: str) -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    config.attributes["prefix"] = prefix
    return config


def upgrade_platform_schema(
    database_url: str,
    *,
    prefix: str = DEFAULT_PREFIX,
    revision: str = PLATFORM_HEAD_REVISION,
) -> None:
    """Upgrade an empty prefix-owned schema to the requested revision."""

    command.upgrade(_alembic_config(database_url, prefix), revision)
