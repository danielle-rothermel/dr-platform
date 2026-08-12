from __future__ import annotations

from enum import UNIQUE, StrEnum, verify
from typing import TYPE_CHECKING

from dr_serialize import json_hash

if TYPE_CHECKING:
    from dr_platform._core.identities import CampaignWorkIdentity

SHUFFLE_RANK_BITS = 63
SHUFFLE_RANK_HEX_LENGTH = 16
SHUFFLE_RANK_MAX = (1 << SHUFFLE_RANK_BITS) - 1


@verify(UNIQUE)
class WorkRankDigestField(StrEnum):
    """Persisted wire keys; spell them out at hashing sites, never iterate."""

    CAMPAIGN_KEY = "campaign_key"
    WORK_KEY = "work_key"


def stable_random_rank(*, work_identity: CampaignWorkIdentity) -> int:
    """Derive a stable positive signed-63-bit rank for campaign work."""
    digest = json_hash(
        {
            WorkRankDigestField.CAMPAIGN_KEY: (
                work_identity.campaign_key.value
            ),
            WorkRankDigestField.WORK_KEY: work_identity.work_key.value,
        },
        length=SHUFFLE_RANK_HEX_LENGTH,
    )
    return (int(digest, 16) % SHUFFLE_RANK_MAX) + 1
