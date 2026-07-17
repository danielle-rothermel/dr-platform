"""Focused tests for the internal rebuild vocabulary."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from dr_platform.staging import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineDefinition,
    RunKey,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkKey,
    stable_random_rank,
    stage_workflow_id,
)

KeyType = type[CampaignKey | RunKey | WorkKey | StageKey]


def _workflow(*args: object) -> object:
    return args


def _args_for(*args: object) -> tuple[object, ...]:
    return args


def _stage(
    key: str,
    *,
    queue_name: str | None = None,
    workflow: Callable[..., object] = _workflow,
) -> StageDefinition:
    return StageDefinition(
        key=key,
        queue_name=queue_name or key,
        workflow=workflow,
        args_for=_args_for,
    )


@pytest.mark.parametrize(
    "key_type",
    [CampaignKey, RunKey, WorkKey, StageKey],
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

    assert identity == CampaignWorkIdentity("campaign-7", "item/alpha")
    assert CampaignKey("same") != WorkKey("same")
    assert str(identity.campaign_key) == "campaign-7"
    assert str(identity.work_key) == "item/alpha"


def test_pipeline_preserves_declared_linear_stage_order() -> None:
    prepare = _stage("prepare")
    execute = _stage("execute")

    pipeline = PipelineDefinition(
        key="evaluation",
        version=1,
        stages=(prepare, execute),
    )

    assert pipeline.identity == ("evaluation", 1)
    assert pipeline.stages == (prepare, execute)
    assert pipeline.stages[0].key == StageKey("prepare")


def test_pipeline_rejects_an_empty_stage_tuple() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        PipelineDefinition(key="evaluation", version=1, stages=())


def test_pipeline_rejects_duplicate_stage_keys() -> None:
    with pytest.raises(ValueError, match="stage keys must be unique"):
        PipelineDefinition(
            key="evaluation",
            version=1,
            stages=(_stage("execute"), _stage("execute")),
        )


def test_stage_execution_state_is_exactly_the_minimal_logical_set() -> None:
    assert {state.name for state in StageExecutionState} == {
        "READY",
        "ADMITTED",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    }
    assert "RUNNING" not in StageExecutionState.__members__


def test_stage_workflow_id_is_stable_and_attempt_scoped() -> None:
    identity = CampaignWorkIdentity("campaign-7", "item/alpha")

    def workflow_id_for(attempt_number: int) -> str:
        return stage_workflow_id(
            work_identity=identity,
            pipeline_key="evaluation",
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


def test_stable_random_rank_is_stable_and_work_scoped() -> None:
    first_identity = CampaignWorkIdentity("campaign-7", "item/alpha")
    second_identity = CampaignWorkIdentity("campaign-7", "item/beta")

    first = stable_random_rank(work_identity=first_identity)

    assert first == stable_random_rank(work_identity=first_identity)
    assert first != stable_random_rank(work_identity=second_identity)
    assert 1 <= first <= (1 << 63) - 1
