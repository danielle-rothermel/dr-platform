# dr-platform

[![CI](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dr-platform.svg)](https://pypi.org/project/dr-platform/)

| [Repo Definitions](https://danielle-rothermel.github.io/dr-platform/) | [dr-serialize v0.1.0](https://github.com/danielle-rothermel/dr-serialize) |
| --- | --- |

**dr-platform durably moves application-owned work through staged pipelines.**
It is built on PostgreSQL and DBOS and organized into six functional areas:

- **[Pipeline definitions](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/pipeline)**
  describe ordered, versioned stages while applications retain ownership of
  stage behavior and the meaning of input and output references.
- **[Submission](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/submission)**
  records streamed work in bounded chunks and organizes it into campaigns and
  runs with stable identities and replay-safe conflict detection.
- **[Admission and controls](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/admission)**
  select ready work fairly within stage and label-specific capacity, with pause
  and resume controls that leave running work uninterrupted.
- **[Execution and handoff](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/execution)**
  run admitted stages durably, recover interrupted workflows, record outcomes,
  and create the next ready stage.
- **[Recovery and operator actions](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/recovery)**
  reconcile abandoned workflows and provide explicit retry and cancellation
  while preserving stage-attempt history.
- **[Inspection](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/inspection)**
  exposes campaigns, runs, work items, stage and attempt history, current state
  counts, and configured controls through bounded readers.
- **Infra**
    - **[Shared core](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/_core)**
      owns nominal identities, immutable values, execution state, and the
      persistence ledger shared by every functional area.
    - **[Runtime](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/runtime)**
      validates PostgreSQL and DBOS colocation, initializes DBOS, schedules
      dispatch, and optionally configures telemetry.
        - **[Database](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/runtime/database)**
          owns the platform schema and migrations.

## Functional Areas

The following abbreviated shapes emphasize stable contracts rather than
implementation details. Application-facing names are exported from
`dr_platform`; infrastructure-only defaults and collaborators are omitted where
they do not clarify the boundary.

### Pipeline definitions

Pipeline declarations are immutable, versioned, linear stage chains. A startup
registry binds each identity to exactly one declaration for submission and
runtime wiring.

```python
@dataclass(frozen=True, slots=True)
class PipelineIdentity:
    key: PipelineKey
    version: int


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
    key: StageKey
    queue_name: str
    workflow: Callable[..., object]
    args_for: Callable[..., tuple[object, ...]]


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineDefinition:
    key: PipelineKey
    version: int
    stages: tuple[StageDefinition, ...]
```

```python
class PipelineRegistry:
    def register(
        self,
        pipeline: PipelineDefinition,
    ) -> PipelineDefinition: ...

    def get(
        self,
        *,
        key: PipelineKey,
        version: int,
    ) -> PipelineDefinition: ...
```

### Submission

Submission accepts an arbitrary iterable of immutable work inputs and commits
it in bounded chunks. Reusing an existing identity is safe only when its
immutable provenance matches the original submission.

```python
@dataclass(frozen=True, slots=True)
class WorkInput:
    work_key: WorkKey
    input_reference: str
    labels: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    run_key: RunKey
    inserted_count: int
    already_existing_count: int
```

```python
def submit(
    *,
    campaign_key: CampaignKey | str,
    run_key: RunKey | str,
    pipeline: PipelineIdentity,
    execution_config_reference: str,
    items: Iterable[WorkInput],
    registry: PipelineRegistry,
    engine: Engine,
) -> SubmissionReceipt: ...
```

### Admission and controls

Admission supplies each selected stage with immutable work context and respects
the most specific matching capacity control. Operators can change capacity or
pause future admissions without preempting work that is already running.

```python
@dataclass(frozen=True, slots=True)
class AdmissionPayload:
    campaign_key: CampaignKey
    work_key: WorkKey
    run_key: RunKey
    input_reference: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    attempt_number: int


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
```

```text
set_stage_capacity(pipeline, stage_key, capacity) -> StageControlRecord
set_selector_capacity(pipeline, stage_key, labels, capacity) -> StageControlRecord
pause(pipeline, stage_key, labels=None) -> StageControlRecord
resume(pipeline, stage_key, labels=None) -> StageControlRecord
read_controls(pipeline, stage_key, labels=None) -> tuple[StageControlRecord, ...]
```

### Execution and handoff

Execution wraps application stage callables in package-owned DBOS workflows
that record one terminal outcome and prepare the next stage transactionally.
Stage bodies must tolerate at-least-once execution across workflow recovery.

```python
class StageExecutionState(StrEnum):
    READY = "ready"
    ADMITTED = "admitted"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
```

```python
class StageHandoffMismatchError(RuntimeError): ...


def wrap_pipeline_workflows(
    pipeline: PipelineDefinition,
) -> PipelineDefinition: ...
```

### Recovery and operator actions

Recovery keeps platform state authoritative while delegating physical workflow
cancellation through a narrow protocol. Retry creates a new attempt, while the
sweeper only projects terminal DBOS abandonment onto admitted work.

```python
class WorkflowCanceller(Protocol):
    def cancel_workflow(
        self,
        workflow_id: str,
        *,
        cancel_children: bool = False,
    ) -> None: ...


class CancellationDisposition(StrEnum):
    CANCELLED_READY = "cancelled_ready"
    CANCELLED_ADMITTED = "cancelled_admitted"
    CANCELLED_FAILED = "cancelled_failed"
    ALREADY_TERMINAL = "already_terminal"
```

```python
@dataclass(frozen=True, slots=True)
class WorkCancellationResult:
    work_item_id: int
    stage_execution: StageExecutionRecord
    disposition: CancellationDisposition
    delegated_workflow_id: str | None


@dataclass(frozen=True, slots=True)
class StageRetryResult:
    stage_execution: StageExecutionRecord
    new_attempt: StageAttemptRecord


@dataclass(frozen=True, slots=True)
class SweepSummary:
    projections: tuple[SweepProjection, ...]
    inspected_count: int
```

```text
cancel_work(work identity, canceller) -> WorkCancellationResult
retry_stage(stage_execution_id) -> StageRetryResult
sweep_abandoned_stages(DBOS client) -> SweepSummary
```

### Inspection

Inspection provides bounded, read-only projections over stable logical
identities rather than exposing database rows. Readers cover both aggregate
status and the complete stage-attempt history of an individual work item.

```python
@dataclass(frozen=True, slots=True)
class CampaignSummary:
    campaign_key: CampaignKey
    created_at: datetime
    run_count: int
    work_item_count: int


@dataclass(frozen=True, slots=True)
class WorkItemSummary:
    work_item_id: int
    campaign_key: CampaignKey
    work_key: WorkKey
    origin_run_key: RunKey
    labels: Mapping[str, str]
    current_stage_execution_id: int
    current_stage_key: StageKey
    current_stage_index: int
    state: StageExecutionState


@dataclass(frozen=True, slots=True)
class StageExecutionSummary:
    execution: StageExecutionRecord
    attempts: tuple[StageAttemptRecord, ...]
```

```text
inspect_campaign(campaign_key) -> CampaignSummary
list_campaigns(cursor=None, limit=...) -> tuple[CampaignSummary, ...]
list_runs(campaign_key, cursor=None, limit=...) -> tuple[RunSummary, ...]
list_work_items(campaign_key, state=None, cursor=None, limit=...) -> tuple[WorkItemSummary, ...]
get_work_item_stages(work_item_id) -> tuple[StageExecutionSummary, ...]
campaign_state_counts(campaign_key) -> tuple[StateCount, ...]
run_state_counts(run_key) -> tuple[StateCount, ...]
bulk_work_statuses(campaign_key, work_keys) -> BulkStatusResult
```
