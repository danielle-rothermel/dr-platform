"""Deterministic identity and scheduling recipes for staged work."""

from __future__ import annotations

from dr_serialize import sha256_json_digest

from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.identities import (
    CampaignWorkIdentity,
    StageKey,
    validate_key_value,
)

WORKFLOW_ID_PREFIX = "drp-"
WORKFLOW_ID_DIGEST_LENGTH = 64
SHUFFLE_RANK_BITS = 63
SHUFFLE_RANK_HEX_LENGTH = 16
SHUFFLE_RANK_MAX = (1 << SHUFFLE_RANK_BITS) - 1


def stage_workflow_id(
    *,
    work_identity: CampaignWorkIdentity,
    pipeline_key: str,
    pipeline_version: int,
    stage_key: StageKey | str,
    attempt_number: int,
) -> str:
    """Derive the DBOS workflow ID for one immutable stage attempt."""
    validate_key_value(pipeline_key, label="pipeline key")
    validate_positive_integer(pipeline_version, label="pipeline version")
    normalized_stage_key = (
        stage_key if isinstance(stage_key, StageKey) else StageKey(stage_key)
    )
    validate_positive_integer(attempt_number, label="attempt number")
    digest = sha256_json_digest(
        {
            "campaign_key": work_identity.campaign_key.value,
            "work_key": work_identity.work_key.value,
            "pipeline_key": pipeline_key,
            "pipeline_version": pipeline_version,
            "stage_key": normalized_stage_key.value,
        },
        length=WORKFLOW_ID_DIGEST_LENGTH,
    )
    return f"{WORKFLOW_ID_PREFIX}{digest}-a{attempt_number}"


def stable_random_rank(*, work_identity: CampaignWorkIdentity) -> int:
    """Derive a stable positive signed-63-bit rank for campaign work."""
    digest = sha256_json_digest(
        {
            "campaign_key": work_identity.campaign_key.value,
            "work_key": work_identity.work_key.value,
        },
        length=SHUFFLE_RANK_HEX_LENGTH,
    )
    return (int(digest, 16) % SHUFFLE_RANK_MAX) + 1
