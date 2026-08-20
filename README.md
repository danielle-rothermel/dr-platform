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
  describe versioned registered stages and application-directed handoff while
  applications retain ownership of stage behavior and the meaning of input and
  output references.
- **[Submission](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/submission)**
  records streamed work in bounded chunks and organizes it into campaigns and
  runs with stable identities and replay-safe conflict detection.
- **[Admission and controls](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/admission)**
  select ready work in stable randomized order within stage-wide and
  label-specific capacity, with pause and resume controls that leave running
  work uninterrupted.
- **[Execution and handoff](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/execution)**
  make admitted stages DBOS-durable, record outcomes—including optional partial
  evidence on application failure—and create the next ready stage
  transactionally.
- **[Run completion](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/completion)**
  releases one optional durable fan-in operation after a closed run's members
  settle, without adding graph semantics to item pipelines.
- **[Recovery and operator actions](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/recovery)**
  reconcile abandoned workflows and provide explicit retry and cancellation
  while preserving stage-attempt history.
- **[Inspection](https://github.com/danielle-rothermel/dr-platform/tree/main/src/dr_platform/inspection)**
  exposes campaigns, runs, paginated run members, work items, stage and attempt
  history with pinned terminal summaries, current state counts, and bulk work
  status without exposing persistence rows or evidence payloads.
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

Pipeline declarations are immutable, versioned sets of uniquely keyed stages
with optional run completion. Registration order defines the default linear
successor for `str` returns; application-directed handoff may fan out, loop
on a `stage_key` at a higher `stage_index`, or join behind an admission
barrier. A startup registry binds each identity to exactly one declaration for
submission and runtime wiring.

#### Handoff semantics

Stage workflows return a non-empty output-reference `str` or a
`StageCompletion` with explicit `successors`. A `str` return is valid only
when the persisted `stage_index` equals the stage's registration position;
otherwise the workflow must return `StageCompletion`. On the linear path, a
`str` return enqueues the next registered stage with that string as the
successor `input_reference` (not the work item's original submission input).
The admission payload carries the persisted `stage_index` and `work_item_id`
so completion identity matches the ledger row and join bodies can read
sibling outputs. Join example:

```python
async def join(payload: AdmissionPayload) -> StageCompletion:
    outputs = list_predecessor_stage_outputs(
        payload.work_item_id,
        payload.stage_index,
        engine=engine,
    )
    refs = "|".join(item.output_reference for item in outputs)
    return StageCompletion(
        output_reference=f"join:{payload.input_reference}:{refs}"
    )
```

This example is valid for a single fan-out episode. Multi-deferral or loop
pipelines must scope reads with ``stage_key`` and ``min_stage_index``; see
**Deferral episodes and barrier fan-in** below.
Fan-out inserts every successor in one handoff transaction. Loops reuse a
`stage_key` at a higher `stage_index`; identity is `(work_item_id,
stage_index)`. Join successors set `barrier=True` on `StageSuccessor`;
admission holds them ready until every lower `stage_index` for the same work
item is `SUCCEEDED`. Join bodies call `list_predecessor_stage_outputs` to read
sibling outputs once admitted.

#### Deferral episodes and barrier fan-in

When a stage defers work it typically fans out branch successors plus one
``barrier=True`` join successor in a single handoff transaction. Indices are
application-chosen and sparse; within one episode they are usually contiguous:

```text
optim_step @ O  →  eval_row @ O+1 .. O+N  →  eval_fanin @ F (= O+N+1)
```

Admission holds the barrier join ready until every lower ``stage_index`` for
the work item is ``SUCCEEDED``. That admission gate is work-item-wide; join
bodies must scope reads to one episode. After multiple deferrals on one work
item, unfiltered predecessor reads include every lower succeeded stage. Use
``list_episode_predecessor_outputs`` to read one episode's branch outputs
(``stage_index > O``, ``stage_index < F``):

```python
async def eval_fanin(payload: AdmissionPayload) -> StageCompletion:
    predecessors = list_episode_predecessor_outputs(
        payload.work_item_id,
        payload.stage_index,
        origin_stage_index=origin_stage_index,  # deferring step at O
        stage_key=STAGE_EVAL_ROW,
        engine=engine,
    )
    # input_reference: per-row admitted payload; output_reference: completion
    ...
```

Carrying ``origin_stage_index`` in the join payload is preferred. When it is
not already available, discover or verify the deferring step with
``resolve_barrier_join_cluster(...).optim_step.stage_index``.

``list_predecessor_stage_outputs`` remains the general reader for unscoped or
custom-bound queries. ``list_stage_executions`` lists executions with the same
exclusive bounds for episode discovery. Its bounds are independently optional
and min-only queries have no implicit upper cap. ``list_predecessor_stage_outputs``
instead defaults the exclusive upper bound to ``below_stage_index`` when
``max_stage_index`` is omitted. ``resolve_barrier_join_cluster`` requires
distinct optim and eval stage keys and validates that the open interval
``(O, F)`` contains only eval-row stages, returning the deferring optim step,
eval rows, and fan-in record. The platform stores and transports references
only; payload meaning stays in the application layer.

Failed or cancelled lower siblings block the join until an operator
`retry_stage`s the sibling, not the join. This is ~80% best-effort behavior:
prefer loud failures and operator recovery over silent corruption.

```python
@dataclass(frozen=True, slots=True)
class PipelineIdentity:
    key: PipelineKey
    version: int


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
    key: StageKey
    queue_name: str
    workflow: Callable[..., Awaitable[str | StageCompletion]]
    args_for: Callable[[AdmissionPayload], tuple[object, ...]]
    label_queue_routes: tuple[LabelQueueRoute, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class LabelQueueRoute:
    selector: Mapping[str, str]
    queue_name: str


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

#### Label queue routing

Stages may declare optional `LabelQueueRoute` selectors for enqueue-time queue
selection. Each route uses a non-empty exact-label predicate; routes overlap
only when shared label keys agree on values. Route queue names must be distinct.
Admission enqueues to the first matching route queue name and otherwise uses the
stage default `queue_name`. Capacity and pause controls remain label-based and
unchanged.

Every distinct queue name referenced by a stage (default plus routes) must be
registered as a DBOS `Queue` on a worker that will dequeue it before
`DBOS.launch()`. The platform cannot detect a missing queue at enqueue time;
misconfiguration leaves work `ADMITTED` with no dequeue.

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
    priority: int = 0

    def __init__(
        self,
        *,
        work_key: WorkKey | str,
        input_reference: str,
        labels: Mapping[str, str],
        priority: int = 0,
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

Use `compute_run_membership_digest(members, expected_member_count=...)` when
constructing a manifest-bound declaration. It validates the canonical
zero-based member order and pins the digest wire format used again at closure.

### Admission and controls

Admission supplies each selected stage with immutable work context and respects
every matching capacity control. Ready work is ordered by `(priority, stable
rank, stage_execution_id)`; lower priority numbers are admitted first and `0`
is the default highest priority. Operators can change capacity or pause future
admissions without preempting work that is already running. Join successors
with `barrier=True` remain `READY` until every lower `stage_index` for the
same work item is `SUCCEEDED`; admission reports skips in
`AdmissionSummary.skipped_for_barrier`.

```python
@dataclass(frozen=True, slots=True)
class AdmissionSummary:
    admitted_counts: tuple[StageAdmissionCount, ...]
    skipped_for_capacity: int
    skipped_for_pause: int
    skipped_for_barrier: int
    unconfigured_stages: tuple[StageIdentityRecord, ...]
    failed_stages: tuple[StageAdmissionFailure, ...]
    mismatched_stages: tuple[StageMismatch, ...]
```

```python
class AdmissionPayload(BaseModel):
    campaign_key: CampaignKey
    work_key: WorkKey
    work_item_id: int
    origin_run_key: RunKey
    input_reference: str
    labels: Mapping[str, str]
    pipeline_key: str
    pipeline_version: int
    stage_key: StageKey
    stage_index: int
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
that record one terminal outcome and insert all successors in one transaction.
Stage bodies must tolerate at-least-once execution across workflow recovery.
Crash recovery requires `DBOS.launch()` on a worker with the matching executor
and application version and with the workflows registered; cross-version
recovery is not promised. Wrapped stage and run completion workflows require an
explicit `max_recovery_attempts` on `PlatformDbosConfig`; DBOS marks a workflow
recovery-exhausted when `recovery_attempts > max_recovery_attempts + 1` at
execution time, after which the sweep projects platform failure for operator
retry.

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


class StageApplicationFailure(Exception):
    def __init__(
        self,
        message: str,
        *,
        evidence: Jsonable | None = None,
    ) -> None: ...


def wrap_pipeline_workflows(
    pipeline: PipelineDefinition,
    *,
    max_recovery_attempts: int,
) -> PipelineDefinition: ...
```

Argument derivation runs inside the durable wrapper, after admission commits.
Applications own and close any loop-affine clients used by their workflows.

#### Preemptible stage bodies

Wrapped application bodies run inside a preemptible DBOS step so operator
cancellation can interrupt in-flight work with roughly one-second latency
(DBOS poll interval). Bodies must re-raise `asyncio.CancelledError`; swallowing
cancellation can still complete the step while the ledger is already
`CANCELLED`. Do not call `DBOS.transaction()` inside a body (DBOS asserts).
Avoid nested DBOS steps inside the body; return values cross the step boundary
via pickle, so keep returns small and serializable.

### Run completion

A completion-enabled pipeline requires an immutable manifest reference and the
canonical digest of its declared ordered membership. Independent scheduled
barrier reconciliation releases exactly one completion execution after every
member's representative stage state settles. Release facts remain fixed if a member later
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
cancellation through a narrow protocol. Retry creates a new attempt for failed
stages or run completions, while the sweeper projects terminal DBOS abandonment
and identity-orphaned pending work onto admitted stages and enqueued run
completions. Pending rows that still
match the live application version and executor identity are skipped so startup
recovery and the recovery cap can settle same-process crashes; projection uses
structural evidence only (no time thresholds). Deploy one live process per
executor id so dead-executor detection remains truthful.

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
class CancelledStageExecution:
    stage_execution: StageExecutionRecord
    disposition: CancellationDisposition
    delegated_workflow_id: str | None


@dataclass(frozen=True, slots=True)
class WorkCancellationResult:
    work_item_id: int
    cancellations: tuple[CancelledStageExecution, ...]
```

`cancel_work` cancels every nonterminal execution (`READY`, `ADMITTED`,
`FAILED`) for the item. When a barrier join is blocked by a FAILED sibling,
retry that sibling execution — `retry_stage` only accepts FAILED rows. A FAILED
lower sibling keeps the run unsealed until that sibling is retried;
`cancel_work` on such an item derives **cancelled** (not failed) under
precedence.

#### Work priority

Each work item carries an optional submission `priority` (default `0`, lower
numbers are preferred). Admission orders ready work by priority before stable
rank. Non-zero priorities are passed to DBOS enqueue; application queues must
set `priority_enabled=True` when priority should affect dequeue order.
`set_work_priority` updates ready and admitted executions and, for admitted
work, the colocated DBOS `workflow_status.priority` row on the current attempt.

#### Dynamic executor identity

When sweep is enabled, pass `LiveDbosIdentity` with either a non-empty static
`executor_ids` set or a `resolve_executor_ids` callable that returns the
current live worker ids (for example from SLURM). Sweep reads application
version and the local process executor id from the live DBOS runtime once per
pass, then unions resolved ids, static `executor_ids`, and the local process
executor id. Identity-orphan projection applies to **`PENDING` DBOS rows
only**; `ENQUEUED` and `DELAYED` queue backlog is left for dequeue and
startup recovery. Blank or whitespace identity fields are treated as absent.
When any identity axis is unavailable, sweep suppresses the
dependent pending projections and sets `identity_unavailable` on the sweep
summary: empty application version suppresses `stale_app_version`; resolver
failure or an empty result, or an executor set that is only the `"local"`
sentinel, suppresses `dead_executor`. Terminal DBOS statuses are still
projected. Rows left for startup recovery remain eligible for the configured
recovery cap. The resolver must be fast and side-effect-free. Distinct
per-process `executor_id` values are required for multi-worker
`dead_executor` detection.

```python
@dataclass(frozen=True, slots=True)
class WorkPriorityResult:
    work_item_id: int
    priority: int
    updated_stage_execution_ids: tuple[int, ...]
    updated_workflow_ids: tuple[str, ...]
```

```python
@dataclass(frozen=True, slots=True)
class StageRetryResult:
    stage_execution: StageExecutionRecord
    new_attempt: StageAttemptRecord


@dataclass(frozen=True, slots=True)
class RunCompletionRetryResult:
    execution: RunCompletionExecutionRecord
    new_attempt: RunCompletionAttemptRecord


@dataclass(frozen=True, slots=True)
class SweepSummary:
    projections: tuple[SweepProjection, ...]
    inspected_count: int
    identity_unavailable: bool = False
```

```text
cancel_work(work identity, canceller) -> WorkCancellationResult
set_work_priority(campaign_key, work_key, priority) -> WorkPriorityResult
retry_stage(stage_execution_id) -> StageRetryResult
retry_run_completion(run_key, DBOS client, registry) -> RunCompletionRetryResult
sweep_abandoned_stages(DBOS client, live_identity) -> SweepSummary
sweep_abandoned_run_completions(DBOS client, live_identity) -> RunCompletionSweepSummary
```

### Inspection

Inspection provides read-only projections over stable logical identities rather
than exposing database rows. Collection readers are bounded; direct work-item
inspection returns its complete stage-attempt history. Run member listing is
paginated by membership ordinal and reports representative stage state without
returning evidence payloads. State counts (`campaign_state_counts`,
`run_state_counts`, `bulk_run_state_counts`) and work-item `current_stage_*`
fields derive from precedence
(`FAILED > CANCELLED > ADMITTED > READY > SUCCEEDED`), not simply the highest
`stage_index`; ties within a precedence band break on `stage_execution_id`.
Representative index: lowest in that state except `SUCCEEDED`, which uses the
highest index.

Terminal attempt summaries use pinned wire keys (`TerminalSummaryField`,
`TerminalSummaryProducer`) with an explicit producer tag on failed,
abandoned, and cancelled attempts, plus optional traceback capture on
application failure. Successful stage attempts record outcome only through
`build_terminal_outcome_summary`. Run-completion attempt summaries use the
same pinned keys without a producer tag; extending producer tagging to run
completions remains deferred until ratified. Applications may attach partial
evidence on failure by raising `StageApplicationFailure` with an optional
strict-JSON evidence payload; the platform writes it through enlisted
`dr-store` in the same checkpoint transaction as the failed attempt and
records the resulting opaque reference separately from success output
references. The platform stores and transports evidence references without
resolving them; out-of-transaction evidence read-back is caller-owned.
`TerminalSummaryFilter` applies exact-match predicates on those pinned keys
over the current attempt. `bulk_work_terminal_statuses` reads terminal facts
for a bounded work-key set in one SELECT per chunk; filtered
`list_run_members` paginates matching run members and includes
`stage_execution_id` for retry eligibility.

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
class RunMemberSummary:
    member_ordinal: int
    work_key: WorkKey
    work_item_id: int
    input_reference: str
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None
    stage_execution_id: int | None = None
    terminal_summary: Mapping[str, object] | None = None
    evidence_reference: str | None = None


@dataclass(frozen=True, slots=True)
class BulkWorkTerminalStatus:
    work_key: WorkKey
    present: bool
    work_item_id: int | None
    stage_execution_id: int | None
    current_stage_key: StageKey | None
    current_stage_index: int | None
    state: StageExecutionState | None
    terminal_summary: Mapping[str, object] | None
    evidence_reference: str | None


@dataclass(frozen=True, slots=True)
class StageExecutionSummary:
    execution: StageExecutionRecord
    attempts: tuple[StageAttemptRecord, ...]
```

```text
inspect_campaign(campaign_key) -> CampaignSummary
list_campaigns(cursor=None, limit=...) -> tuple[CampaignSummary, ...]
list_runs(campaign_key, cursor=None, limit=...) -> tuple[RunSummary, ...]
list_run_members(run_key, cursor=None, limit=..., terminal_filter=None) -> tuple[RunMemberSummary, ...]
list_work_items(campaign_key, state=None, cursor=None, limit=...) -> tuple[WorkItemSummary, ...]
get_work_item_stages(work_item_id) -> tuple[StageExecutionSummary, ...]
list_stage_executions(work_item_id, stage_key=None, min_stage_index=None, max_stage_index=None, state=None) -> tuple[StageExecutionRecord, ...]
list_predecessor_stage_outputs(work_item_id, below_stage_index, stage_key=None, min_stage_index=None, max_stage_index=None) -> tuple[PredecessorStageOutput, ...]
resolve_barrier_join_cluster(work_item_id, fanin_stage_index, optim_step_stage_key, eval_row_stage_key) -> BarrierJoinCluster
campaign_state_counts(campaign_key) -> tuple[StateCount, ...]
run_state_counts(run_key) -> tuple[StateCount, ...]
bulk_run_state_counts(run_keys) -> Mapping[RunKey, tuple[StateCount, ...] | None]
bulk_work_statuses(campaign_key, work_keys) -> BulkStatusResult
bulk_work_terminal_statuses(campaign_key, work_keys, terminal_filter=None) -> BulkTerminalStatusResult
```

## Operational preconditions

The platform tables and the DBOS system schema must share one PostgreSQL
database. Runtime initialization and dispatcher registration validate that
colocation and fail when their URLs identify different databases.

`0001_staging_baseline` is the fresh-schema root of the supported Alembic
chain, and `0004_work_priority` is its head — the revision
`upgrade_platform_schema` installs by default. This development hard cut has
no compatibility or historical backfill path. Archive any database worth
retaining before explicitly resetting it. All four revisions refuse downgrade
outright rather than delete the recorded ledger.

Register wrapped workflows, application queues, and the scheduled dispatcher
before `DBOS.launch()`. Declare every queue name used by a stage default or
label route, and enable `priority_enabled=True` on queues that should honor
work priority. Admission, run-barrier reconciliation, and
abandoned-stage and run-completion sweep have separate schedule and batch
settings; both register by default unless `sweep_cron=None`. Pass
`LiveDbosIdentity` with either a static executor set or a
`resolve_executor_ids` callable at dispatcher registration; sweep reads
application version and the local process executor id from the live DBOS
runtime once per pass after `DBOS.launch()`. Optionally pin both on
`PlatformDbosConfig` when explicit deployment identity is required. Distinct
per-process `executor_id` values are required for multi-worker
`dead_executor` detection. When multiple worker processes run, either disable sweep on all but one
reconciler or supply every live executor ID to the process that owns sweep so
peer work is not projected as `dead_executor`. When identity is unavailable,
sweep suppresses dependent pending abandonment projection and sets
`identity_unavailable` on the sweep summary.
The barrier also has a candidate budget, which
must be at least its release batch size and bounds all evaluated runs, including
ineligible and lock-skipped candidates. A persisted cursor rotates blocked or
failed candidates so later runs can make progress. Keep the returned dispatcher
registration alive while the runtime is active. Each dispatcher pass logs
admission, barrier, and sweep counts at INFO for reconciliation.

Size admission schedules, barrier schedules, DBOS queue concurrency, stage
capacity, and the DBOS application-database pool together using
[whetstone's sizing table](https://github.com/danielle-rothermel/whetstone).
dr-platform exposes dispatcher and runtime knobs; only the application owns
provider, process, and queue configuration.

Per-stage-boundary latency is approximately the admission schedule interval
plus the queue poll interval configured on each application-owned DBOS
`Queue`. Queue poll intervals are part of whetstone's sizing table, not
dr-platform defaults.

## Development

The full suite requires a disposable PostgreSQL database. Create the default
with `createdb dr_platform_test`, or set `DR_PLATFORM_TEST_DATABASE_URL` to a
database whose name ends in `_test`. The suite refuses other database names and
destructively recreates the `public` schema between tests.

Run the local quality gates with:

```console
./pre-check.sh
```
