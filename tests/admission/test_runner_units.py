from __future__ import annotations

from typing import TYPE_CHECKING

from dr_platform._core.identities import StageKey
from dr_platform.admission.runner import (
    StageAdmissionCount,
    _Admit,
    _Candidate,
    _Control,
    _evaluate_candidate,
    _PassTally,
    _SkipFull,
    _StageIdentity,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_DEFAULT_STAGE_IDENTITY = _StageIdentity("evaluation", 1, "execute")


def _candidate(
    *,
    labels: Mapping[str, str],
    stage_execution_id: int = 1,
    rank: int = 1,
    stage_identity: _StageIdentity = _DEFAULT_STAGE_IDENTITY,
) -> _Candidate:
    return _Candidate(
        stage_execution_id=stage_execution_id,
        rank=rank,
        stage_index=0,
        campaign_key="campaign-1",
        work_key="work-0",
        origin_run_key="run-1",
        input_reference="input:0",
        labels=labels,
        pipeline_key=stage_identity[0],
        pipeline_version=stage_identity[1],
        stage_key=stage_identity[2],
    )


def _make_control(
    *,
    control_id: int,
    selector: Mapping[str, str],
    capacity: int,
    paused: bool = False,
    stage_identity: _StageIdentity = _DEFAULT_STAGE_IDENTITY,
) -> _Control:
    return _Control(
        control_id=control_id,
        stage_identity=stage_identity,
        selector=selector,
        capacity=capacity,
        paused=paused,
    )


def test_evaluate_candidate_occupancy_at_capacity_is_full() -> None:
    candidate = _candidate(labels={})
    control = _make_control(control_id=1, selector={}, capacity=2)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 2},
    )

    assert result == _SkipFull(full_control_ids=frozenset({1}))


def test_evaluate_candidate_occupancy_below_capacity_admits() -> None:
    candidate = _candidate(labels={})
    control = _make_control(control_id=1, selector={}, capacity=2)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 1},
    )

    assert result == _Admit(matching=(control,))


def test_evaluate_candidate_empty_selector_matches_any_labels() -> None:
    candidate = _candidate(labels={"cohort": "red", "tier": "gold"})
    control = _make_control(control_id=1, selector={}, capacity=3)

    result = _evaluate_candidate(
        candidate,
        controls=(control,),
        occupancy={1: 0},
    )

    assert result == _Admit(matching=(control,))


def test_evaluate_candidate_reports_only_the_full_matching_control() -> None:
    candidate = _candidate(labels={"cohort": "blue"})
    default = _make_control(control_id=1, selector={}, capacity=5)
    selective = _make_control(
        control_id=2,
        selector={"cohort": "blue"},
        capacity=1,
    )

    result = _evaluate_candidate(
        candidate,
        controls=(default, selective),
        occupancy={1: 0, 2: 1},
    )

    assert result == _SkipFull(full_control_ids=frozenset({2}))


def test_evaluate_candidate_full_but_non_matching_control_does_not_block() -> (
    None
):
    candidate = _candidate(labels={"cohort": "red"})
    default = _make_control(control_id=1, selector={}, capacity=5)
    other = _make_control(
        control_id=2,
        selector={"cohort": "blue"},
        capacity=1,
    )

    result = _evaluate_candidate(
        candidate,
        controls=(default, other),
        occupancy={1: 0, 2: 1},
    )

    assert result == _Admit(matching=(default,))


def test_pass_tally_to_summary_sorts_and_converts_stage_keys() -> None:
    tally = _PassTally()
    tally.record_admitted(_StageIdentity("evaluation", 1, "zeta"))
    tally.record_admitted(_StageIdentity("evaluation", 1, "alpha"))
    tally.skipped_for_capacity += 1
    tally.skipped_for_pause += 1

    summary = tally.to_summary()

    assert summary.skipped_for_capacity == 1
    assert summary.skipped_for_pause == 1
    assert summary.admitted_counts == (
        StageAdmissionCount(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("alpha"),
            count=1,
        ),
        StageAdmissionCount(
            pipeline_key="evaluation",
            pipeline_version=1,
            stage_key=StageKey("zeta"),
            count=1,
        ),
    )
    assert all(
        isinstance(item.stage_key, StageKey)
        for item in summary.admitted_counts
    )
