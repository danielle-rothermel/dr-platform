"""Frozen domain records returned by staging persistence readers."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from dr_platform.staging.identities import (
        CampaignKey,
        RunKey,
        StageKey,
        WorkKey,
    )
    from dr_platform.staging.states import StageExecutionState


def immutable_mapping(value: Mapping[str, str]) -> Mapping[str, str]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True, slots=True)
class PipelineRunRecord:
    run_key: RunKey
    campaign_key: CampaignKey
    pipeline_key: str
    pipeline_version: int
    execution_config_reference: str
    created_at: datetime
    submission_completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class WorkItemRecord:
    work_item_id: int
    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    input_reference: str
    labels: Mapping[str, str]
    rank: int


@dataclass(frozen=True, slots=True)
class StageExecutionRecord:
    stage_execution_id: int
    work_item_id: int
    stage_key: StageKey
    stage_index: int
    state: StageExecutionState
    current_attempt: int
    rank: int
    output_reference: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class StageAttemptRecord:
    stage_attempt_id: int
    stage_execution_id: int
    attempt_number: int
    workflow_id: str
    terminal_summary: Mapping[str, object] | None
    terminal_reference: str | None
    created_at: datetime
    admitted_at: datetime | None
    terminal_at: datetime | None


@dataclass(frozen=True, slots=True)
class StageControlRecord:
    stage_control_id: int
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    selector: Mapping[str, str]
    capacity: int
    paused: bool
    updated_at: datetime
