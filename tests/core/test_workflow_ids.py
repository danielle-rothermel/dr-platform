from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.attempts import stage_workflow_id


def test_stage_workflow_id_is_stable_and_attempt_scoped() -> None:
    identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )

    def workflow_id_for(attempt_number: int) -> str:
        return stage_workflow_id(
            work_identity=identity,
            pipeline_key=PipelineKey("evaluation"),
            pipeline_version=1,
            stage_key=StageKey("execute"),
            attempt_number=attempt_number,
        )

    first = workflow_id_for(1)
    repeated = workflow_id_for(1)
    retry = workflow_id_for(2)

    assert first == repeated
    assert first != retry
    assert first.startswith("drp-")
    assert first.endswith("-a1")
    assert retry.endswith("-a2")


def test_stage_workflow_id_matches_pinned_golden_format() -> None:
    identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )

    workflow_id = stage_workflow_id(
        work_identity=identity,
        pipeline_key=PipelineKey("evaluation"),
        pipeline_version=1,
        stage_key=StageKey("execute"),
        attempt_number=1,
    )

    assert workflow_id == (
        "drp-98d03108ccf351d0ddb9fd4b19a8e51d"
        "9ddd2e5171ed49ce6fddc3b313056bdc-a1"
    )
