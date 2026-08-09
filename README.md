# dr-platform

[![CI](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dr-platform.svg)](https://pypi.org/project/dr-platform/)

| [Definitions](https://danielle-rothermel.github.io/dr-platform/) | [Terms source](https://github.com/danielle-rothermel/dr-platform/blob/main/.defs/terms.toml) | [Contracts source](https://github.com/danielle-rothermel/dr-platform/blob/main/.defs/contracts.toml) | [dr-serialize](https://github.com/danielle-rothermel/dr-serialize) |
| --- | --- | --- | --- |

**dr-platform durably moves application-owned work through staged pipelines.**
It is built on PostgreSQL and DBOS and organized into seven functional areas:

`dr-platform` is alpha software. The root `dr_platform` API is the intended
application boundary, but compatibility is not yet promised.

- **[Pipeline definitions](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/pipeline)**
  describe ordered, versioned stages while applications retain ownership of
  stage behavior and the meaning of input and output references.
- **[Submission](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/submission)**
  records streamed work in bounded chunks and organizes it into campaigns and
  runs with stable identities and replay-safe conflict detection.
- **[Admission and controls](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/admission)**
  select ready work in stable randomized order within stage-wide and
  label-specific capacity, with pause and resume controls that leave running
  work uninterrupted.
- **[Execution and handoff](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/execution)**
  make admitted stages DBOS-durable, record outcomes, and create the next ready
  stage transactionally.
- **[Run completion](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/completion)**
  releases one optional durable fan-in operation after a closed run's members
  settle, without adding graph semantics to item pipelines.
- **[Recovery and operator actions](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/recovery)**
  reconcile abandoned workflows and provide explicit retry and cancellation
  while preserving stage-attempt history.
- **[Inspection](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/inspection)**
  exposes campaigns, runs, work items, stage and attempt history, current state
  counts, and bulk work status without exposing persistence rows.
- **Infra**
    - **[Shared core](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/_core)**
      owns nominal identities, immutable values, execution state, and the
      persistence ledger shared across functional areas.
    - **[Runtime](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/runtime)**
      validates PostgreSQL and DBOS colocation, initializes DBOS, schedules
      admission and run-barrier reconciliation independently, and optionally
      configures telemetry.
        - **[Database](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/runtime/database)**
          owns the platform schema and migrations.

## Installation

```console
pip install dr-platform
```

```console
uv add dr-platform
```

`dr-platform` requires Python 3.12 or newer and a PostgreSQL database. The
package pins `dbos[otel]` to the exact release used to validate its recovery
and sweep behavior.

## Functional Areas

The following abbreviated shapes describe the intended application boundary,
not exact call signatures. Application-facing names are exported from
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
    workflow: Callable[..., Awaitable[str | None]]
    args_for: Callable[[AdmissionPayload], tuple[object, ...]]


@dataclass(frozen=True, slots=True, kw_only=True)
class RunCompletionDefinition:
    key: RunCompletionKey
    queue_name: str
    workflow: Callable[..., Awaitable[str | None]]
    args_for: Callable[[RunCompletionPayload], tuple[object, ...]]


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineDefinition:
    key: PipelineKey
    version: int
    stages: tuple[StageDefinition, ...]
    run_completion: RunCompletionDefinition | None = None
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

Submission registers one declared ordered membership in bounded, set-oriented
chunks and closes it after successful stream exhaustion and validation. A
matching closed replay returns its persisted receipt without consuming the
member iterable. Compatible work may belong to more than one run.

```python
@dataclass(frozen=True, slots=True, init=False)
class WorkInput:
    work_key: WorkKey
    input_reference: str
    labels: Mapping[str, str]

    def __init__(
        self,
        *,
        work_key: WorkKey | str,
        input_reference: str,
        labels: Mapping[str, str],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RunMemberInput:
    ordinal: int
    work: WorkInput


@dataclass(frozen=True, slots=True)
class RunRegistrationDeclaration:
    expected_member_count: int
    manifest_reference: str | None = None
    membership_digest: str | None = None


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    run_key: RunKey
    membership_digest: str | None
    registered_member_count: int
    created_work_count: int
    reused_work_count: int
    registration_closed_at: datetime
```

```python
def submit(
    *,
    campaign_key: CampaignKey | str,
    run_key: RunKey | str,
    pipeline: PipelineIdentity,
    execution_config_reference: str,
    declaration: RunRegistrationDeclaration,
    members: Iterable[RunMemberInput],
    registry: PipelineRegistry,
    engine: Engine,
) -> SubmissionReceipt: ...
```

### Admission and controls

Admission supplies each selected stage with immutable work context and respects
every matching capacity control. Operators can change capacity or pause future
admissions without preempting work that is already running.

```python
class AdmissionPayload(BaseModel):
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

Execution wraps async application stage callables in package-owned DBOS workflows
that record one terminal outcome and prepare the next stage transactionally.
Stage bodies must tolerate at-least-once execution across workflow recovery.
Crash recovery requires a worker with the matching executor and application
version and with the workflows registered; cross-version recovery is not
promised.

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

Argument derivation runs inside the durable wrapper, after admission commits.
Applications own and close any loop-affine clients used by their workflows.

### Run completion

A completion-enabled pipeline requires an immutable manifest reference and the
canonical digest of its declared ordered membership. Independent scheduled
barrier reconciliation releases exactly one completion execution after every
member's current stage settles. Release facts remain fixed if a member later
changes state.

```python
class RunCompletionExecutionState(StrEnum):
    ENQUEUED = "enqueued"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RunCompletionPayload(BaseModel):
    campaign_key: CampaignKey
    run_key: RunKey
    pipeline_key: PipelineKey
    pipeline_version: int
    execution_config_reference: str
    manifest_reference: str
    membership_digest: str
    member_count: int
    released_at: datetime
    release_terminal_state_counts: tuple[StateCount, ...]
```

The application validates the manifest digest before aggregation and records
the exact member outcomes and output references consumed in its aggregate
artifact. `inspect_run_completion()` reports durable submission as `ENQUEUED`
until the application records success or failure; it does not project DBOS
runtime state.

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

Inspection provides read-only projections over stable logical identities rather
than exposing database rows. Collection readers are bounded; direct work-item
inspection returns its complete stage-attempt history.

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
bulk_run_state_counts(run_keys) -> Mapping[RunKey, tuple[StateCount, ...] | None]
bulk_work_statuses(campaign_key, work_keys) -> BulkStatusResult
```

## Operational preconditions

The platform tables and the DBOS system schema must share one PostgreSQL
database. Runtime initialization and dispatcher registration validate that
colocation and fail when their URLs identify different databases.

`0001_staging_baseline` is the fresh-schema root of the supported Alembic
chain. This development hard cut has no compatibility or historical backfill
path. Archive any database worth retaining before explicitly resetting it.
Downgrade remains deliberately non-destructive and refuses to delete the
recorded ledger.

Register wrapped workflows, application queues, and the scheduled dispatcher
before `DBOS.launch()`. Admission and run-barrier reconciliation have separate
schedule and batch settings. Keep the returned dispatcher registration alive
while the runtime is active. Production-like deployments must also schedule
`sweep_abandoned_stages`, either through the dispatcher or independently, so
abandoned workflows do not retain admission capacity indefinitely.

Size the schedules independently with
`batch size * 3600 / interval seconds`. For example, a five-second admission
schedule with a batch of 200 has a theoretical ceiling of 144,000 admissions
per hour, giving 44% headroom over a 100,000-admission workload. A five-second
barrier schedule with a batch of 20 similarly provides 14,400 releases per
hour for a 10,000-completion workload. These are sizing examples, not
throughput guarantees; qualify the chosen configuration in its deployment.

## Development

The full suite requires a disposable PostgreSQL database. Create the default
with `createdb dr_platform_test`, or set `DR_PLATFORM_TEST_DATABASE_URL` to a
database whose name ends in `_test`. The suite refuses other database names and
destructively recreates the `public` schema between tests.

Run the local quality gates with:

```console
./pre-check.sh
```
