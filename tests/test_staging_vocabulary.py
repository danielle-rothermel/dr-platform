"""Focused tests for the internal rebuild vocabulary."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from dr_platform import staging
from dr_platform.staging import (
    CampaignKey,
    CampaignWorkIdentity,
    PipelineDefinition,
    PipelineIdentity,
    PipelineKey,
    PipelineRegistry,
    RunKey,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkKey,
    definitions,
    identities,
    recipes,
    registry,
    stable_random_rank,
    stage_workflow_id,
    states,
)

_STAGING_BINDINGS = {
    "ArgumentsCallable": definitions.ArgumentsCallable,
    "CampaignKey": identities.CampaignKey,
    "CampaignWorkIdentity": identities.CampaignWorkIdentity,
    "PipelineConflictError": registry.PipelineConflictError,
    "PipelineDefinition": definitions.PipelineDefinition,
    "PipelineIdentity": definitions.PipelineIdentity,
    "PipelineKey": identities.PipelineKey,
    "PipelineRegistry": registry.PipelineRegistry,
    "RunKey": identities.RunKey,
    "StageDefinition": definitions.StageDefinition,
    "StageExecutionState": states.StageExecutionState,
    "StageKey": identities.StageKey,
    "WorkKey": identities.WorkKey,
    "WorkflowCallable": definitions.WorkflowCallable,
    "stable_random_rank": recipes.stable_random_rank,
    "stage_workflow_id": recipes.stage_workflow_id,
}


def test_staging_exports_are_the_internal_contract() -> None:
    assert len(staging.__all__) == len(_STAGING_BINDINGS)
    assert set(staging.__all__) == set(_STAGING_BINDINGS)


def test_staging_exports_are_bound_to_the_contract_objects() -> None:
    for name, expected in _STAGING_BINDINGS.items():
        assert getattr(staging, name) is expected


KeyType = type[CampaignKey | RunKey | WorkKey | StageKey | PipelineKey]


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
        key=StageKey(key),
        queue_name=queue_name or key,
        workflow=workflow,
        args_for=_args_for,
    )


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


def test_pipeline_preserves_declared_linear_stage_order() -> None:
    prepare = _stage("prepare")
    execute = _stage("execute")

    pipeline = PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(prepare, execute),
    )

    assert pipeline.identity == PipelineIdentity(PipelineKey("evaluation"), 1)
    assert pipeline.stages == (prepare, execute)
    assert pipeline.stages[0].key == StageKey("prepare")


def test_pipeline_identity_rejects_a_non_pipeline_key() -> None:
    with pytest.raises(TypeError, match="pipeline key must be a PipelineKey"):
        PipelineIdentity("evaluation", 1)  # ty: ignore[invalid-argument-type]


def test_pipeline_identity_rejects_a_non_positive_version() -> None:
    with pytest.raises(ValueError, match="pipeline version must be positive"):
        PipelineIdentity(PipelineKey("evaluation"), 0)


def test_pipeline_identity_round_trips_through_registry() -> None:
    pipeline = PipelineDefinition(
        key=PipelineKey("evaluation"),
        version=1,
        stages=(_stage("execute"),),
    )
    registry = PipelineRegistry()
    registry.register(pipeline)

    identity = pipeline.identity
    twin = PipelineIdentity(PipelineKey("evaluation"), 1)
    assert identity == twin
    assert hash(identity) == hash(twin)
    assert registry.get(key=identity.key, version=identity.version) is pipeline


def test_pipeline_rejects_an_empty_stage_tuple() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        PipelineDefinition(key=PipelineKey("evaluation"), version=1, stages=())


def test_pipeline_rejects_duplicate_stage_keys() -> None:
    with pytest.raises(ValueError, match="stage keys must be unique"):
        PipelineDefinition(
            key=PipelineKey("evaluation"),
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


# Golden identity formats. These literals are the persisted contract behind
# stored workflow IDs and shuffle ranks. A change to a pinned value means
# every persisted workflow identity changes and must be a deliberate,
# reviewed decision -- not an incidental byproduct of a refactor. The
# expected values are hand-pinned literals, never derived from the
# implementation's constants or enum, so the pin stays independent of the
# code it guards.
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


def test_stable_random_rank_matches_pinned_golden_value() -> None:
    identity = CampaignWorkIdentity(
        CampaignKey("campaign-7"), WorkKey("item/alpha")
    )

    assert stable_random_rank(work_identity=identity) == 3670084909033913430
