"""Public staged-work contract for dr-platform."""

from dr_platform.db import upgrade_platform_schema
from dr_platform.dbos_config import (
    PlatformDbosConfig,
    build_platform_dbos_config,
    initialize_dbos_runtime,
)
from dr_platform.staging.admission import (
    AdmissionPayload,
    MissingStageControlError,
    PipelineStageMismatchError,
)
from dr_platform.staging.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.staging.dispatcher import (
    DispatcherRegistration,
    register_scheduled_dispatcher,
)
from dr_platform.staging.handoff import (
    StageHandoffMismatchError,
    wrap_pipeline_workflows,
)
from dr_platform.staging.identities import (
    CampaignKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform.staging.inspection import (
    BulkStatusResult,
    BulkWorkStatus,
    CampaignSummary,
    RunSummary,
    StageExecutionSummary,
    StateCount,
    WorkItemSummary,
    bulk_work_statuses,
    campaign_state_counts,
    get_work_item_stages,
    inspect_campaign,
    list_campaigns,
    list_runs,
    list_work_items,
    read_controls,
    run_state_counts,
)
from dr_platform.staging.operations import (
    CancellationDisposition,
    StageRetryResult,
    WorkCancellationResult,
    WorkflowCanceller,
    cancel_work,
    pause,
    resume,
    retry_stage,
    set_selector_capacity,
    set_stage_capacity,
)
from dr_platform.staging.registry import (
    PipelineConflictError,
    PipelineRegistry,
)
from dr_platform.staging.runs import PipelineRunConflictError
from dr_platform.staging.states import StageExecutionState
from dr_platform.staging.submission import (
    SubmissionReceipt,
    WorkInput,
    submit,
)
from dr_platform.staging.sweep import (
    SweepProjection,
    SweepSummary,
    sweep_abandoned_stages,
)
from dr_platform.staging.work_items import WorkItemConflictError
from dr_platform.telemetry import (
    TelemetryInitializationResult,
    initialize_telemetry_safely,
)

__all__ = [
    "AdmissionPayload",
    "BulkStatusResult",
    "BulkWorkStatus",
    "CampaignKey",
    "CampaignSummary",
    "CancellationDisposition",
    "DispatcherRegistration",
    "MissingStageControlError",
    "PipelineConflictError",
    "PipelineDefinition",
    "PipelineIdentity",
    "PipelineRegistry",
    "PipelineRunConflictError",
    "PipelineStageMismatchError",
    "PlatformDbosConfig",
    "RunKey",
    "RunSummary",
    "StageDefinition",
    "StageExecutionState",
    "StageExecutionSummary",
    "StageHandoffMismatchError",
    "StageKey",
    "StageRetryResult",
    "StateCount",
    "SubmissionReceipt",
    "SweepProjection",
    "SweepSummary",
    "TelemetryInitializationResult",
    "WorkCancellationResult",
    "WorkInput",
    "WorkItemConflictError",
    "WorkItemSummary",
    "WorkKey",
    "WorkflowCanceller",
    "build_platform_dbos_config",
    "bulk_work_statuses",
    "campaign_state_counts",
    "cancel_work",
    "get_work_item_stages",
    "initialize_dbos_runtime",
    "initialize_telemetry_safely",
    "inspect_campaign",
    "list_campaigns",
    "list_runs",
    "list_work_items",
    "pause",
    "read_controls",
    "register_scheduled_dispatcher",
    "resume",
    "retry_stage",
    "run_state_counts",
    "set_selector_capacity",
    "set_stage_capacity",
    "submit",
    "sweep_abandoned_stages",
    "upgrade_platform_schema",
    "wrap_pipeline_workflows",
]
