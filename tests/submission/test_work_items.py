"""Tests for deterministic work-item ranking."""

from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    WorkKey,
)
from dr_platform.submission.work_items import stable_random_rank


def test_stable_random_rank_is_stable_and_work_scoped() -> None:
    first_identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )
    second_identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/beta")
    )

    first = stable_random_rank(work_identity=first_identity)

    assert first == stable_random_rank(work_identity=first_identity)
    assert first != stable_random_rank(work_identity=second_identity)
    assert 1 <= first <= (1 << 63) - 1


def test_stable_random_rank_matches_pinned_golden_value() -> None:
    identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )

    assert stable_random_rank(work_identity=identity) == 3670084909033913430
