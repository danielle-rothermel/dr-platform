"""Programmatic Alembic entrypoints for the platform lineage.

Fresh adopters: ``upgrade_platform_schema(url, naming=...)``.
Stamped-baseline adopters (tables already exist from their own frozen
history, e.g. whetstone): ``stamp_platform_schema(url, naming=...,
revision=PLATFORM_BASELINE_REVISION)`` once, then
``upgrade_platform_schema`` for everything after the baseline.

The version table is ``{prefix}_platform_alembic_version`` so this
lineage coexists with an app's own Alembic lineage in one database.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from dr_platform.naming import PlatformNaming

PLATFORM_BASELINE_REVISION = "0001_platform_baseline"
PLATFORM_HEAD_REVISION = "head"

_ALEMBIC_DIR = Path(__file__).resolve().parent / "alembic"


def _alembic_config(
    database_url: str,
    naming: PlatformNaming,
) -> Config:
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    # configparser interpolation: raw "%" (e.g. percent-encoded query
    # params like search_path options) must be escaped.
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    config.attributes["naming"] = naming
    return config


def upgrade_platform_schema(
    database_url: str,
    *,
    naming: PlatformNaming | None = None,
    revision: str = PLATFORM_HEAD_REVISION,
) -> None:
    resolved = naming or PlatformNaming()
    command.upgrade(_alembic_config(database_url, resolved), revision)


def stamp_platform_schema(
    database_url: str,
    *,
    naming: PlatformNaming | None = None,
    revision: str = PLATFORM_BASELINE_REVISION,
) -> None:
    resolved = naming or PlatformNaming()
    command.stamp(_alembic_config(database_url, resolved), revision)
