import re
from pathlib import Path

import dr_platform
from dr_platform._core import identities
from dr_platform._core.ledger import attempts, executions, schema, states
from dr_platform.admission import controls, runner
from dr_platform.execution import handoff
from dr_platform.inspection import campaigns, statuses
from dr_platform.inspection import work_items as inspection_work_items
from dr_platform.pipeline import definitions, registry
from dr_platform.recovery import cancellation, retry, sweep
from dr_platform.runtime import database, dbos, dispatcher, telemetry
from dr_platform.submission import runs, stream
from dr_platform.submission import work_items as submission_work_items

_ROOT_BINDINGS = {
    "AdmissionPayload": runner.AdmissionPayload,
    "BulkStatusResult": statuses.BulkStatusResult,
    "BulkWorkStatus": statuses.BulkWorkStatus,
    "CampaignKey": identities.CampaignKey,
    "CampaignSummary": campaigns.CampaignSummary,
    "CancellationDisposition": cancellation.CancellationDisposition,
    "DispatcherRegistration": dispatcher.DispatcherRegistration,
    "PipelineConflictError": registry.PipelineConflictError,
    "PipelineDefinition": definitions.PipelineDefinition,
    "PipelineIdentity": definitions.PipelineIdentity,
    "PipelineKey": identities.PipelineKey,
    "PipelineRegistry": registry.PipelineRegistry,
    "PipelineRunConflictError": runs.PipelineRunConflictError,
    "PlatformDbosConfig": dbos.PlatformDbosConfig,
    "RunKey": identities.RunKey,
    "RunSummary": campaigns.RunSummary,
    "StageAttemptRecord": attempts.StageAttemptRecord,
    "StageControlRecord": controls.StageControlRecord,
    "StageDefinition": definitions.StageDefinition,
    "StageExecutionRecord": executions.StageExecutionRecord,
    "StageExecutionState": states.StageExecutionState,
    "StageExecutionSummary": inspection_work_items.StageExecutionSummary,
    "StageHandoffMismatchError": handoff.StageHandoffMismatchError,
    "StageKey": identities.StageKey,
    "StageRetryResult": retry.StageRetryResult,
    "StagingSchema": schema.StagingSchema,
    "StateCount": statuses.StateCount,
    "SubmissionReceipt": stream.SubmissionReceipt,
    "SweepProjection": sweep.SweepProjection,
    "SweepSummary": sweep.SweepSummary,
    "TelemetryInitializationResult": telemetry.TelemetryInitializationResult,
    "UnwrappedPipelineError": dispatcher.UnwrappedPipelineError,
    "WorkCancellationResult": cancellation.WorkCancellationResult,
    "WorkInput": stream.WorkInput,
    "WorkItemConflictError": submission_work_items.WorkItemConflictError,
    "WorkItemSummary": inspection_work_items.WorkItemSummary,
    "WorkKey": identities.WorkKey,
    "WorkflowCanceller": cancellation.WorkflowCanceller,
    "build_platform_dbos_config": dbos.build_platform_dbos_config,
    "bulk_work_statuses": statuses.bulk_work_statuses,
    "campaign_state_counts": statuses.campaign_state_counts,
    "cancel_work": cancellation.cancel_work,
    "get_work_item_stages": inspection_work_items.get_work_item_stages,
    "initialize_dbos_runtime": dbos.initialize_dbos_runtime,
    "initialize_telemetry_safely": telemetry.initialize_telemetry_safely,
    "inspect_campaign": campaigns.inspect_campaign,
    "list_campaigns": campaigns.list_campaigns,
    "list_runs": campaigns.list_runs,
    "list_work_items": inspection_work_items.list_work_items,
    "pause": controls.pause,
    "read_controls": controls.read_controls,
    "register_scheduled_dispatcher": dispatcher.register_scheduled_dispatcher,
    "resume": controls.resume,
    "retry_stage": retry.retry_stage,
    "run_state_counts": statuses.run_state_counts,
    "set_selector_capacity": controls.set_selector_capacity,
    "set_stage_capacity": controls.set_stage_capacity,
    "submit": stream.submit,
    "sweep_abandoned_stages": sweep.sweep_abandoned_stages,
    "upgrade_platform_schema": database.upgrade_platform_schema,
    "wrap_pipeline_workflows": handoff.wrap_pipeline_workflows,
}


def test_root_exports_are_the_public_contract() -> None:
    assert len(dr_platform.__all__) == len(_ROOT_BINDINGS)
    assert set(dr_platform.__all__) == set(_ROOT_BINDINGS)


def test_root_exports_are_bound_to_the_contract_objects() -> None:
    for name, expected in _ROOT_BINDINGS.items():
        assert getattr(dr_platform, name) is expected


_VOCAB_HTML = Path(__file__).resolve().parents[1] / ".defs" / "vocab.html"


def _exported_names_from_vocab_sheet() -> list[str]:
    html = _VOCAB_HTML.read_text(encoding="utf-8")
    anchor = 'id="exported-names"'
    assert anchor in html, f"exported-names section not found in {_VOCAB_HTML}"
    section = html[html.index(anchor) :]
    body = re.search(r"<tbody>(.*?)</tbody>", section, re.DOTALL)
    assert body is not None, "exported-names table body not found"
    rows = re.findall(r"<tr>(.*?)</tr>", body.group(1), re.DOTALL)
    assert rows, "exported-names table has no rows"
    names: list[str] = []
    for row in rows:
        cells = re.findall(r"<td>(.*?)</td>", row, re.DOTALL)
        assert len(cells) >= 2, "exported-names row missing a names column"
        names += re.findall(r"<code>([^<]+)</code>", cells[1])
    names = [name.strip() for name in names]
    assert names, "exported-names table parsed no names"
    return names


def test_vocab_sheet_names_column_has_no_duplicates() -> None:
    names = _exported_names_from_vocab_sheet()
    assert len(names) == len(set(names))


def test_vocab_sheet_names_match_root_exports_both_directions() -> None:
    names = set(_exported_names_from_vocab_sheet())
    exported = set(dr_platform.__all__)
    assert names - exported == set(), "vocab sheet names not in __all__"
    assert exported - names == set(), "__all__ names missing from vocab sheet"
    assert names == exported
