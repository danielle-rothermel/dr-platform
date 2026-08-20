"""Library test surface for consumer integration tests (Postgres-only)."""

from dr_platform.testing._dsn import validate_test_database_url
from dr_platform.testing.admission_payload import admission_payload_for_stage
from dr_platform.testing.deferral_fanout import validate_deferral_fanout
from dr_platform.testing.engine import migrated_engine
from dr_platform.testing.fixtures import (
    FIXTURE_TIMESTAMP,
    seed_deferral_episode,
    seed_deferral_fanout,
    seed_double_deferral_episode,
    seed_work_item,
    succeed_stage,
)

__all__ = [
    "FIXTURE_TIMESTAMP",
    "admission_payload_for_stage",
    "migrated_engine",
    "seed_deferral_episode",
    "seed_deferral_fanout",
    "seed_double_deferral_episode",
    "seed_work_item",
    "succeed_stage",
    "validate_deferral_fanout",
    "validate_test_database_url",
]
