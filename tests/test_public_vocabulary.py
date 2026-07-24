"""Explicit contract for the public root namespace."""

import dr_platform
from dr_platform import db, dbos_config, telemetry
from dr_platform.staging import (
    admission,
    definitions,
    dispatcher,
    handoff,
    identities,
    inspection,
    operations,
    records,
    registry,
    runs,
    schema,
    states,
    submission,
    sweep,
    work_items,
)

_ROOT_BINDINGS = {
    "AdmissionPayload": admission.AdmissionPayload,
    "BulkStatusResult": inspection.BulkStatusResult,
    "BulkWorkStatus": inspection.BulkWorkStatus,
    "CampaignKey": identities.CampaignKey,
    "CampaignSummary": inspection.CampaignSummary,
    "CancellationDisposition": operations.CancellationDisposition,
    "DispatcherRegistration": dispatcher.DispatcherRegistration,
    "PipelineConflictError": registry.PipelineConflictError,
    "PipelineDefinition": definitions.PipelineDefinition,
    "PipelineIdentity": definitions.PipelineIdentity,
    "PipelineKey": identities.PipelineKey,
    "PipelineRegistry": registry.PipelineRegistry,
    "PipelineRunConflictError": runs.PipelineRunConflictError,
    "PlatformDbosConfig": dbos_config.PlatformDbosConfig,
    "RunKey": identities.RunKey,
    "RunSummary": inspection.RunSummary,
    "StageAttemptRecord": records.StageAttemptRecord,
    "StageControlRecord": records.StageControlRecord,
    "StageDefinition": definitions.StageDefinition,
    "StageExecutionRecord": records.StageExecutionRecord,
    "StageExecutionState": states.StageExecutionState,
    "StageExecutionSummary": inspection.StageExecutionSummary,
    "StageHandoffMismatchError": handoff.StageHandoffMismatchError,
    "StageKey": identities.StageKey,
    "StageRetryResult": operations.StageRetryResult,
    "StagingSchema": schema.StagingSchema,
    "StateCount": inspection.StateCount,
    "SubmissionReceipt": submission.SubmissionReceipt,
    "SweepProjection": sweep.SweepProjection,
    "SweepSummary": sweep.SweepSummary,
    "TelemetryInitializationResult": telemetry.TelemetryInitializationResult,
    "UnwrappedPipelineError": dispatcher.UnwrappedPipelineError,
    "WorkCancellationResult": operations.WorkCancellationResult,
    "WorkInput": submission.WorkInput,
    "WorkItemConflictError": work_items.WorkItemConflictError,
    "WorkItemSummary": inspection.WorkItemSummary,
    "WorkKey": identities.WorkKey,
    "WorkflowCanceller": operations.WorkflowCanceller,
    "build_platform_dbos_config": dbos_config.build_platform_dbos_config,
    "bulk_work_statuses": inspection.bulk_work_statuses,
    "campaign_state_counts": inspection.campaign_state_counts,
    "cancel_work": operations.cancel_work,
    "get_work_item_stages": inspection.get_work_item_stages,
    "initialize_dbos_runtime": dbos_config.initialize_dbos_runtime,
    "initialize_telemetry_safely": telemetry.initialize_telemetry_safely,
    "inspect_campaign": inspection.inspect_campaign,
    "list_campaigns": inspection.list_campaigns,
    "list_runs": inspection.list_runs,
    "list_work_items": inspection.list_work_items,
    "pause": operations.pause,
    "read_controls": inspection.read_controls,
    "register_scheduled_dispatcher": dispatcher.register_scheduled_dispatcher,
    "resume": operations.resume,
    "retry_stage": operations.retry_stage,
    "run_state_counts": inspection.run_state_counts,
    "set_selector_capacity": operations.set_selector_capacity,
    "set_stage_capacity": operations.set_stage_capacity,
    "submit": submission.submit,
    "sweep_abandoned_stages": sweep.sweep_abandoned_stages,
    "upgrade_platform_schema": db.upgrade_platform_schema,
    "wrap_pipeline_workflows": handoff.wrap_pipeline_workflows,
}


def test_root_exports_are_the_public_contract() -> None:
    assert dr_platform.__all__ == list(_ROOT_BINDINGS)


def test_root_exports_are_bound_to_the_contract_objects() -> None:
    for name, expected in _ROOT_BINDINGS.items():
        assert getattr(dr_platform, name) is expected
