"""Deterministic identity and scheduling recipes for staged work."""

from __future__ import annotations

from enum import UNIQUE, StrEnum, verify

from dr_serialize import sha256_json_digest

from dr_platform.staging.definitions import validate_positive_integer
from dr_platform.staging.identities import (
    CampaignWorkIdentity,
    PipelineKey,
    StageKey,
)

WORKFLOW_ID_PREFIX = "drp-"
WORKFLOW_ID_DIGEST_LENGTH = 64
SHUFFLE_RANK_BITS = 63
SHUFFLE_RANK_HEX_LENGTH = 16
SHUFFLE_RANK_MAX = (1 << SHUFFLE_RANK_BITS) - 1


@verify(UNIQUE)
class IdentityDigestField(StrEnum):
    """Persisted identity contract: these values are the canonical digest
    payload keys behind stored workflow IDs and ranks.

    Never edit a value: every persisted workflow identity is derived from
    these keys, so a change silently invalidates stored identities. Never
    build a payload by iterating this enum: spell the keys out at each call
    site so the wire format stays decoupled from code structure.
    """

    CAMPAIGN_KEY = "campaign_key"
    WORK_KEY = "work_key"
    PIPELINE_KEY = "pipeline_key"
    PIPELINE_VERSION = "pipeline_version"
    STAGE_KEY = "stage_key"


def stage_workflow_id(
    *,
    work_identity: CampaignWorkIdentity,
    pipeline_key: PipelineKey,
    pipeline_version: int,
    stage_key: StageKey,
    attempt_number: int,
) -> str:
    """Derive the DBOS workflow ID for one immutable stage attempt."""
    if not isinstance(pipeline_key, PipelineKey):
        raise TypeError("pipeline_key must be a PipelineKey")
    validate_positive_integer(pipeline_version, label="pipeline version")
    if not isinstance(stage_key, StageKey):
        raise TypeError("stage_key must be a StageKey")
    validate_positive_integer(attempt_number, label="attempt number")
    digest = sha256_json_digest(
        {
            IdentityDigestField.CAMPAIGN_KEY: work_identity.campaign_key.value,
            IdentityDigestField.WORK_KEY: work_identity.work_key.value,
            IdentityDigestField.PIPELINE_KEY: pipeline_key.value,
            IdentityDigestField.PIPELINE_VERSION: pipeline_version,
            IdentityDigestField.STAGE_KEY: stage_key.value,
        },
        length=WORKFLOW_ID_DIGEST_LENGTH,
    )
    return f"{WORKFLOW_ID_PREFIX}{digest}-a{attempt_number}"


def stable_random_rank(*, work_identity: CampaignWorkIdentity) -> int:
    """Derive a stable positive signed-63-bit rank for campaign work."""
    digest = sha256_json_digest(
        {
            IdentityDigestField.CAMPAIGN_KEY: work_identity.campaign_key.value,
            IdentityDigestField.WORK_KEY: work_identity.work_key.value,
        },
        length=SHUFFLE_RANK_HEX_LENGTH,
    )
    return (int(digest, 16) % SHUFFLE_RANK_MAX) + 1
