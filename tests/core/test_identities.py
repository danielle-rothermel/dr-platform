"""Tests for shared nominal identities."""

from __future__ import annotations

import pytest

from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    RunKey,
    StageKey,
    WorkKey,
)

KeyType = type[CampaignKey | RunKey | WorkKey | StageKey | PipelineKey]


@pytest.mark.parametrize(
    "key_type",
    [CampaignKey, RunKey, WorkKey, StageKey, PipelineKey],
)
def test_identity_keys_reject_empty_or_malformed_values(
    key_type: KeyType,
) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        key_type("")
    with pytest.raises(ValueError, match="only ASCII"):
        key_type("contains spaces")
    with pytest.raises(ValueError, match="at most 128"):
        key_type("a" * 129)


def test_identity_key_types_are_nominal_and_campaign_scoped() -> None:
    campaign_key = CampaignKey("campaign-7")
    work_key = WorkKey("item/alpha")
    identity = CampaignWorkIdentity(campaign_key, work_key)

    assert identity == CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )
    assert CampaignKey("same") != WorkKey("same")
    assert str(identity.campaign_key) == "campaign-7"
    assert str(identity.work_key) == "item/alpha"
