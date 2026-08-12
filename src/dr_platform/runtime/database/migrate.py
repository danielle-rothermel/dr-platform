from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from dr_platform._core.ledger.schema import DEFAULT_PREFIX

PLATFORM_BASELINE_REVISION = "0001_staging_baseline"
PLATFORM_HEAD_REVISION = "0002_dr_store_baseline"

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
    command.upgrade(_alembic_config(database_url, prefix), revision)
