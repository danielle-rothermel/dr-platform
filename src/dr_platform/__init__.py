"""Public staged-work contract for dr-platform."""

from dr_platform._core.identities import (
    CampaignKey,
    PipelineKey,
    RunKey,
    StageKey,
    WorkKey,
)
from dr_platform._core.ledger.attempts import StageAttemptRecord
from dr_platform._core.ledger.executions import StageExecutionRecord
from dr_platform._core.ledger.schema import StagingSchema
from dr_platform._core.ledger.states import StageExecutionState
from dr_platform.admission.controls import (
    StageControlRecord,
    pause,
    read_controls,
    resume,
    set_selector_capacity,
    set_stage_capacity,
)
from dr_platform.admission.runner import AdmissionPayload
from dr_platform.execution.handoff import (
    StageHandoffMismatchError,
    wrap_pipeline_workflows,
)
from dr_platform.inspection.campaigns import (
    CampaignSummary,
    RunSummary,
    inspect_campaign,
    list_campaigns,
    list_runs,
)
from dr_platform.inspection.statuses import (
    BulkStatusResult,
    BulkWorkStatus,
    StateCount,
    bulk_work_statuses,
    campaign_state_counts,
    run_state_counts,
)
from dr_platform.inspection.work_items import (
    StageExecutionSummary,
    WorkItemSummary,
    get_work_item_stages,
    list_work_items,
)
from dr_platform.pipeline.definitions import (
    PipelineDefinition,
    PipelineIdentity,
    StageDefinition,
)
from dr_platform.pipeline.registry import (
    PipelineConflictError,
    PipelineRegistry,
)
from dr_platform.recovery.cancellation import (
    CancellationDisposition,
    WorkCancellationResult,
    WorkflowCanceller,
    cancel_work,
)
from dr_platform.recovery.retry import StageRetryResult, retry_stage
from dr_platform.recovery.sweep import (
    SweepProjection,
    SweepSummary,
    sweep_abandoned_stages,
)
from dr_platform.runtime.database import upgrade_platform_schema
from dr_platform.runtime.dbos import (
    PlatformDbosConfig,
    build_platform_dbos_config,
    initialize_dbos_runtime,
)
from dr_platform.runtime.dispatcher import (
    DispatcherRegistration,
    UnwrappedPipelineError,
    register_scheduled_dispatcher,
)
from dr_platform.runtime.telemetry import (
    TelemetryInitializationResult,
    initialize_telemetry_safely,
)
from dr_platform.submission.runs import PipelineRunConflictError
from dr_platform.submission.stream import (
    SubmissionReceipt,
    WorkInput,
    submit,
)
from dr_platform.submission.work_items import WorkItemConflictError

__all__ = [
    "AdmissionPayload",
    "BulkStatusResult",
    "BulkWorkStatus",
    "CampaignKey",
    "CampaignSummary",
    "CancellationDisposition",
    "DispatcherRegistration",
    "PipelineConflictError",
    "PipelineDefinition",
    "PipelineIdentity",
    "PipelineKey",
    "PipelineRegistry",
    "PipelineRunConflictError",
    "PlatformDbosConfig",
    "RunKey",
    "RunSummary",
    "StageAttemptRecord",
    "StageControlRecord",
    "StageDefinition",
    "StageExecutionRecord",
    "StageExecutionState",
    "StageExecutionSummary",
    "StageHandoffMismatchError",
    "StageKey",
    "StageRetryResult",
    "StagingSchema",
    "StateCount",
    "SubmissionReceipt",
    "SweepProjection",
    "SweepSummary",
    "TelemetryInitializationResult",
    "UnwrappedPipelineError",
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
