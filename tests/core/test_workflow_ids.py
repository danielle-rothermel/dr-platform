from dr_platform._core.identities import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineKey,
    RunCompletionKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.attempts import stage_workflow_id
from dr_platform._core.ledger.completion_attempts import (
    run_completion_workflow_id,
)


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
            stage_index=0,
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
        stage_index=0,
        attempt_number=1,
    )

    assert workflow_id == (
        "drp-ec522298fe642c4803ad044e2d76547d"
        "924a518ecf7a3cdb95d18e5b5602d1f5-a1"
    )


def test_stage_workflow_id_scopes_by_stage_index() -> None:
    identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )
    pipeline_key = PipelineKey("evaluation")
    stage_key = StageKey("execute")
    first_index = stage_workflow_id(
        work_identity=identity,
        pipeline_key=pipeline_key,
        pipeline_version=1,
        stage_key=stage_key,
        stage_index=0,
        attempt_number=1,
    )
    loop_index = stage_workflow_id(
        work_identity=identity,
        pipeline_key=pipeline_key,
        pipeline_version=1,
        stage_key=stage_key,
        stage_index=3,
        attempt_number=1,
    )
    retry = stage_workflow_id(
        work_identity=identity,
        pipeline_key=pipeline_key,
        pipeline_version=1,
        stage_key=stage_key,
        stage_index=0,
        attempt_number=2,
    )

    assert first_index != loop_index
    assert first_index != retry
    assert loop_index != retry


def test_completion_workflow_identity_is_stable_and_pipeline_scoped() -> None:
    first = run_completion_workflow_id(
        run_key=RunKey("run-1"),
        pipeline_key=PipelineKey("pipeline-a"),
        pipeline_version=1,
        completion_key=RunCompletionKey("aggregate"),
        attempt_number=1,
    )
    replay = run_completion_workflow_id(
        run_key=RunKey("run-1"),
        pipeline_key=PipelineKey("pipeline-a"),
        pipeline_version=1,
        completion_key=RunCompletionKey("aggregate"),
        attempt_number=1,
    )
    other_pipeline = run_completion_workflow_id(
        run_key=RunKey("run-1"),
        pipeline_key=PipelineKey("pipeline-b"),
        pipeline_version=1,
        completion_key=RunCompletionKey("aggregate"),
        attempt_number=1,
    )
    assert first == replay
    assert first != other_pipeline
    assert first == (
        "drp-run-23c1e871ca241a532a43c70dbee5b25ccdf2a675ae5f8f120f7"
        "812cad09ab907-a1"
    )
