# Async stages and run fan-in

Status: implemented; clean-tip schema-4 performance qualification passed on
2026-08-09. The
[qualification result](../../qualification/async-stages-and-run-fan-in-results.json)
is the authoritative numeric evidence.

## Planning vocabulary and contracts

The vocabulary in [plan-terms.toml](plan-terms.toml) and standing rules in
[plan-contracts.toml](plan-contracts.toml) hold the planning-stage selections.
This document applies them rather than repeating them.

Their binding forms live in the repository's authoritative `.defs/` sources.

## Purpose and scope

Prepare `dr-platform` to orchestrate high-concurrency research workflows whose
application effects use async storage and synchronous provider or process
clients. Add persisted run membership and a run barrier so an application can
declare one run completion without implementing polling, completeness checks,
or duplicate-launch fencing.

Item pipelines remain linear. Run completion stays outside the item pipeline,
and application semantics and reference resolution stay outside the platform.

The reviewed foundation versions are:

- `dr-store==0.2.0`
- `dr-providers==0.3.0`
- `dr-exec==0.1.7`
- DBOS `2.27.0`, already pinned by `dr-platform`

The packages co-install under Python 3.12 and newer. This work adds no runtime
dependency from `dr-platform` to `dr-store`, `dr-providers`, or `dr-exec`.

### Goals

1. Hard-cut platform-managed application workflows to async.
2. Preserve synchronous argument derivation and ledger transactions.
3. Persist each submission run's ordered membership, including reused work.
4. Bind that membership to an application manifest with one canonical digest.
5. Close registration only after the complete declared membership validates.
6. Release one run completion after the run barrier is satisfied.
7. Preserve immutable release facts even if a member later changes state.
8. Record one run completion outcome under one stable workflow identity.
9. Register each member chunk with set-oriented database work whose statement
   count does not grow with chunk cardinality.
10. Keep application callbacks outside dispatcher transactions.
11. Size admission and run barrier schedules independently from their declared
    service rates.
12. Keep registration writes, dispatcher passes, and inspection reads bounded;
    full-membership validation may stream only the declared run's indexed
    membership.

### Non-goals

- General DAG pipelines or arbitrary dependency graphs.
- Provider, execution, artifact, reward, acceptance, or analysis policy.
- Reference resolution inside the platform.
- Exactly-once external effects.
- Platform storage of every member outcome selected by run completion.
- Run completion retry, cancellation, controls, abandoned workflow projection,
  or attempt history.
- A general member listing or result pagination API.
- Sync and async variants of application workflows.
- Async admission, inspection, or schema APIs without a concrete caller.
- Multi-call run registration.
- In-place migration or historical membership reconstruction from the current
  development schema.

## Proposed flow

```text
run registration declaration
  -> bounded registration of ordered run members
  -> validated membership digest and registration closure
  -> ordinary linear item pipelines
  -> run barrier release with immutable release facts
  -> one async run completion execution
  -> opaque aggregate output reference
```

## 1. Async application workflow boundary

### Declarations

Hard-cut `StageDefinition.workflow` to an async callable:

```python
type AsyncWorkflowCallable = Callable[..., Awaitable[str]]
type ArgumentsCallable = Callable[[AdmissionPayload], tuple[object, ...]]


@dataclass(frozen=True, slots=True, kw_only=True)
class StageDefinition:
    key: StageKey
    queue_name: str
    workflow: AsyncWorkflowCallable
    args_for: ArgumentsCallable
```

Validate the coroutine-function requirement when constructing a pipeline
stage. `args_for` remains synchronous and must be a deterministic, replay-safe,
external-I/O-free, bounded transformation of its compact platform payload.

Admission and run barrier transactions enqueue only validated serialized
platform payloads. `AdmissionPayload` and `RunCompletionPayload` therefore use
closed validation models at the DBOS persistence boundary rather than relying
on internal dataclass serialization.

### Platform-owned wrapper

The wrapped application workflow for a pipeline stage becomes `async def` and:

1. reconstructs the typed admission payload from its validated serialized
   boundary representation;
2. calls `args_for` and validates its tuple result;
3. awaits the application workflow;
4. validates one non-empty output reference;
5. invokes the synchronous stage handoff transaction through the
   dispatcher-owned dedicated checkpoint executor;
6. records an `args_for` or workflow non-cancellation exception as that stage
   execution's application failure;
7. preserves mismatch, late cancellation, and successor-creation behavior;
8. lets `asyncio.CancelledError` follow DBOS cancellation handling; and
9. remains replay-safe under the same stage attempt and workflow identity.

The run completion wrapper follows the same boundary: it reconstructs the run
completion payload, derives and validates application arguments, awaits the
workflow, and records derivation or workflow non-cancellation exceptions as
that run completion execution's application failure.

The SQLAlchemy ledger path remains synchronous. Applications may build
lifecycle-aware async bridges around synchronous `dr-providers` or `dr-exec`
calls, but those adapters and their resource shutdown remain outside this
package.

Synchronous stage and run-completion checkpoint transactions run through one
dispatcher-owned dedicated executor. Their explicit `READ COMMITTED`,
row-locked transactions avoid the `SERIALIZABLE` retry amplification observed
during qualification. Initial measurement through the shared default executor
also showed checkpoint starvation from default-pool contention.

Qualification proves that concurrent async application workflows can reuse one
loop-affine test resource and measures dedicated-executor queue delay,
event-loop lag, ledger-pool wait, handoff throughput, and cancellation cleanup
under an expected burst. Its numeric bounds remain qualification-only evidence,
not standing service-level objectives.

## 2. Run registration and membership

### Declaration and manifest binding

Introduce an immutable run registration declaration:

```python
@dataclass(frozen=True, slots=True)
class RunRegistrationDeclaration:
    expected_member_count: int
    manifest_reference: str | None = None
    membership_digest: str | None = None
```

A pipeline with run completion requires a non-empty manifest reference and
membership digest when the submission run is declared. Reject either missing
value before the first work item or membership write. An item-only pipeline may
omit both or supply both, but cannot supply an unbound manifest or digest.

The membership digest uses one pinned canonical representation containing:

```text
schema/version tag
expected member count
ordered entries of (ordinal, work key, input reference)
```

Use the repository's canonical serialization and hashing path, and pin the
schema tag and field literals with a golden test. The application records the
same digest inside its immutable manifest. Applications use
`compute_run_membership_digest()` rather than reproducing the wire format.

The platform treats the manifest reference as opaque. At closure it computes
the digest from persisted run membership and compares it with the declaration;
it does not load the manifest. Before aggregation, the application loads the
manifest and validates that its recorded digest equals the run completion
payload's digest.

Each submitted member carries its explicit zero-based ordinal:

```python
@dataclass(frozen=True, slots=True)
class RunMemberInput:
    ordinal: int
    work: WorkInput
```

Reject booleans and negative values for counts and ordinals. A nonempty
registration must use contiguous ordinals beginning at zero.

### Persistence

Add the declaration and registration closure facts to `pipeline_runs`. Add a
membership table conceptually shaped as:

```text
run_memberships
  run_key                FK pipeline_runs
  member_ordinal         nonnegative integer
  work_item_id           FK work_items

  PRIMARY KEY (run_key, member_ordinal)
  UNIQUE (run_key, work_item_id)
```

Keep each work item's origin run as immutable creation provenance. Origin does
not substitute for membership.

Indexes must support ordered membership and counts for one submission run and
barrier eligibility without scanning unrelated campaigns. Do not add a reverse
work-item-to-runs index until a reverse lookup or push-based projection requires
it.

Every registration and closure transaction locks the submission run row before
reading or changing membership. Database enforcement prevents changed or new
membership after closure.

### Bounded submission and automatic closure

The existing `submit` operation accepts one complete logical member stream and
commits bounded chunks. Each chunk uses a cardinality-independent number of
database statement executions:

1. validate the declaration against its persisted value;
2. bulk insert candidate work items;
3. bulk read back and validate existing work and origin provenance;
4. bulk insert memberships while detecting ordinal or work conflicts;
5. bulk create first-stage executions for newly created work; and
6. commit before consuming the next chunk.

No registration path executes SQL once per member. The final query shape may
choose its own fixed upper bound; a regression test compares chunks of one and
500 members while excluding transaction-protocol statements.

Reuse adds membership but no duplicate stage execution history. It is permitted
only when pipeline key, pipeline version, and execution-configuration reference
match the work item's origin run exactly.

Registration closes automatically only when:

1. the iterable exhausts normally;
2. the persisted member count equals the declaration;
3. ordinals are contiguous from zero;
4. work items are unique within the submission run; and
5. when a digest was declared, the persisted membership digest equals it.

Perform count, ordinal, and digest validation in one indexed ordered membership
scan. This required O(N) closure work is bounded to the declared submission run
and is not repeated by a matching closed replay.

A generator failure or closure validation failure leaves registration open.
Earlier committed chunks remain an exact resumable prefix, validated with one
set-oriented query per replayed chunk.

A closed submission run whose stored immutable declaration matches, including
its digest when present, returns its persisted closure receipt immediately. It
does not consume the supplied iterable or reread membership. A changed
declaration is rejected before touching the iterable.

Multi-call ingestion is unsupported. A future caller requiring it must justify
a separate explicit closure operation.

An empty declaration with a digest uses the canonical empty membership digest.
If its pipeline declares run completion, its run barrier is immediately
eligible after closure.

`SubmissionReceipt` is a stable closure receipt rather than a report of work
performed by one invocation. It contains at least:

```python
@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    run_key: RunKey
    membership_digest: str | None
    registered_member_count: int
    created_work_count: int
    reused_work_count: int
    registration_closed_at: datetime
```

Persist closure facts that cannot otherwise be returned in O(1). Exact replay
returns the same receipt.

### Inspection by submission run

Counts and status queries for a submission run, and run barrier eligibility,
join through membership rather than origin. They remain aggregate or bounded;
applications use their manifest and application-owned storage for ordered
results.

Add `bulk_run_state_counts(run_keys, chunk_size=...)` following the existing
bulk work-status pattern. It issues one aggregate query per bounded input chunk,
distinguishes missing runs from present empty runs, and returns stable results
in requested-key scope. Documentation directs consumers of `list_runs()` pages
to this bulk function instead of per-run `run_state_counts()` loops.

## 3. Run barrier and run completion

### Pipeline declaration

Declare optional run completion beside the pipeline stage sequence:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class RunCompletionDefinition:
    key: RunCompletionKey
    queue_name: str
    workflow: AsyncWorkflowCallable
    args_for: Callable[[RunCompletionPayload], tuple[object, ...]]


@dataclass(frozen=True, slots=True, kw_only=True)
class PipelineDefinition:
    key: PipelineKey
    version: int
    stages: tuple[StageDefinition, ...]
    run_completion: RunCompletionDefinition | None = None
```

`RunCompletionKey` is nominally distinct from `StageKey`. Adding, removing, or
changing run completion requires a new pipeline version. Registry equality,
conflict checks, wrapping, and dispatcher validation include the entire run
completion definition.

A run completion key's underlying value cannot collide with a pipeline stage
key in the same pipeline identity.

### Barrier reconciliation and release

Register run barrier reconciliation as a scheduled workflow separate from
admission and sweep. It shares the dispatcher-owned DBOS client but has its own
cron or interval, release batch size, candidate budget, workflow name,
diagnostics, and qualification target. The positive candidate budget is at
least the release batch size.

Each bounded pass:

1. acquires the barrier-specific singleton cursor with
   `FOR UPDATE SKIP LOCKED`; a lock loser performs no candidate scan;
2. materializes stable, indexed, keyset-ordered pages of completion candidates
   strictly after the persisted run key;
3. performs one bounded parameterized lateral nonterminal probe per candidate
   and counts every evaluated row against the candidate budget, including
   ineligible and lock-skipped rows;
4. advances across blocked candidates, wrapping at most once and stopping at
   the original cursor boundary so a pass never evaluates a candidate twice;
5. calculates terminal state counts only for locked eligible runs, with one
   grouped query for the page;
6. records immutable `released_at` and release terminal state counts;
7. creates and enqueues each run completion execution transactionally using
   only its compact validated platform payload; and
8. transactionally persists the last examined run key, including after a
   candidate-local release failure, so later candidates make fair progress and
   the failed release remains retryable after rotation.

The expected starting index is equivalent to:

```sql
(work_item_id)
WHERE state IN ('ready', 'admitted')
```

Those are the only nonterminal stage execution states, and linear handoff means
any such execution is current. Keep the standing rule at the level of an
indexed nonterminal anti-join; qualify the final index and query together with
PostgreSQL `EXPLAIN` rather than freezing this exact index shape.

Release is edge-triggered. Later retry or cancellation of a member changes
neither the stored release facts nor the existing run completion execution and
does not launch another one. A revised aggregate requires a new submission run.

Release terminal state counts include `SUCCEEDED`, `FAILED`, and `CANCELLED`
in canonical order, including zeros, and sum to the member count. They describe
the barrier observation; they do not promise that those are the outcomes the
application later consumes.

The reconciliation batch size bounds enqueue work, not completion occupancy.
The application-owned queue controls run completion concurrency. A candidate
failure rolls back that candidate, emits diagnostics, and does not abort the
rest of the selected page. Infrastructure enqueue failures remain retryable,
and registry drift is a deployment error. Do not add durable poison-candidate
suppression; application callbacks do not execute in reconciliation.

### Payload and application result

The platform supplies compact release facts:

```python
class RunCompletionPayload(BaseModel):
    model_config = ConfigDict(frozen=True)

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

Manifest reference and membership digest are nonoptional in this payload. The
application validates the manifest digest, resolves member results from its
own data, and writes an immutable aggregate artifact recording the exact member
outcomes and output references it consumed. The platform stores only the
aggregate artifact's opaque output reference.

Large member sets, detailed outcomes, output references, and credentials never
enter the DBOS workflow arguments.

The barrier transaction enqueues this payload, not application-derived
arguments. The durable wrapper reconstructs it, calls `args_for`, validates the
tuple, and then invokes the application workflow.

### Run completion execution

Use a separate table rather than generalizing stage execution tables. One
record contains:

- the submission run and stable DBOS workflow identity;
- enqueue timestamp;
- platform-recorded state: `ENQUEUED`, `SUCCEEDED`, or `FAILED`;
- optional output reference or safe application error summary; and
- terminal timestamp when the application outcome is recorded.

Pin a dedicated workflow identity recipe over the submission run, pipeline
identity, and run completion key, with a prefix distinct from stage attempts.
Persisting the execution and enqueuing that identity occur in one transaction,
with uniqueness on the submission run and workflow identity. The identity has
no attempt suffix.

`ENQUEUED` means the stable identity was durably submitted but no application
terminal outcome has been recorded. It does not assert that DBOS is currently
executing. `SUCCEEDED` requires a non-empty output reference, and `FAILED`
means application failure only. Inspection exposes the stable DBOS workflow
identity, enqueue timestamp, platform-recorded state, and terminal output or
error.

The async wrapper records at most one application outcome. DBOS replay keeps
the same workflow identity. Runtime cancellation, administrative intervention,
or recovery exhaustion may leave the platform record `ENQUEUED`; operators use
the workflow identity to inspect DBOS directly. Runtime state projection is
future work.

## 4. Failure and replay boundaries

- Stage `args_for` and application workflow exceptions use the existing stage
  execution failure path; cancellation is not rewritten as application failure.
- Run completion `args_for` and application workflow exceptions record one
  `FAILED` outcome; cancellation is not rewritten as application failure.
- A registration chunk is atomic. Earlier chunks survive a later failure and
  remain replayable.
- Closure failure records no closure or release facts.
- Matching closed replay returns the stored receipt without consuming input.
- A nonterminal member prevents release; a later reconciliation page observes
  its terminal transition.
- One reconciliation candidate failure does not abort the selected page;
  retryable infrastructure failure leaves that candidate eligible later.
- Completion failure changes neither membership, member outcomes, nor release
  facts.
- Post-release member changes do not invalidate or repeat completion.

## 5. Fresh schema baseline

Land the target schema as a fresh development baseline. Do not build an
in-place migration, origin-only membership backfill, or nullable legacy
manifest path.

Any existing database with evidence worth retaining must be archived before
the schema is reset. Reset is an explicit owner operation outside the supported
schema API; downgrade and upgrade commands must not delete ledger data.

The fresh baseline contains the complete run declaration, membership, release,
and run completion execution schema and its immutability enforcement. All new
completion-enabled submission runs require a manifest reference and membership
digest from their declaration onward.

## 6. Package boundaries

```text
dr-platform
  owns: membership, barrier, pipeline stage orchestration,
        durable completion outcome

application adapter
  owns: references, resource instances, async bridges, manifest validation,
        aggregate semantics and aggregate artifact provenance

dr-store / dr-providers / dr-exec
  own: their existing storage, provider-call, and process-execution contracts
```

Cross-package qualification may install the reviewed foundations as test-only
inputs. It uses public APIs and must not add alternate implementations inside
`dr-platform`.

## 7. Performance model

Size admission and run completion independently:

```text
required admissions/hour
  = new work items * average pipeline stages executed
  + retries

required completions/hour
  = completion-enabled submission runs

service rate/hour
  = batch size * 3600 / interval seconds
```

Do not infer completion demand from work-item volume; only completion-enabled
submission runs contribute to that rate.

Document and qualify one declared configuration for each scheduled workflow
with operating headroom. Defaults do not promise 100,000 operations per hour,
and no numeric throughput target becomes a standing contract without an
intentional SLO decision.

The following linear costs are intentional:

- one persisted membership edge for each submission-run member;
- one application digest pass while writing the manifest and one indexed
  platform membership scan at closure;
- terminal state counts calculated once for selected releases;
- one durable run completion workflow for each completion-enabled submission
  run; and
- one compact provenance entry or reference for each result consumed by the
  aggregate artifact.

Applications may shard or content-address provenance artifacts, but must not
move large member sets or outputs into platform rows or DBOS workflow arguments.

## 8. Verification plan

### Async boundary

- Reject a synchronous application workflow at declaration time.
- Enqueue validated serialized platform payloads and reconstruct their typed
  values in the durable wrappers.
- Prove admission and run barrier transactions execute no application callback.
- Derive a tuple, await an async application workflow, and persist its output
  reference.
- Record `args_for` exceptions as application failures without leaving the
  candidate eligible for every dispatcher pass.
- Recompute deterministic `args_for` results safely across DBOS replay.
- Preserve atomic stage handoff and current cancellation fencing.
- Recover the same async DBOS stage attempt after interruption.
- Reuse one loop-affine async test resource across concurrent application
  workflows.
- Under expected burst concurrency, measure event-loop lag, dedicated-executor
  queue delay, ledger-pool wait, handoff throughput, and cancellation cleanup.

### Registration and membership

- Require manifest reference and digest before any member write for a
  completion-enabled pipeline; allow an item-only pipeline to omit both.
- Pin the canonical membership digest representation with golden tests.
- Validate persisted membership against the declared digest at closure.
- Have the application reject a manifest carrying a different digest.
- Register new and reused work in independent submission runs.
- Reject reuse with different execution provenance.
- Assert a fixed upper bound on database statements for chunks of one and 500
  members, excluding transaction protocol, with no per-member SQL.
- Resume at every chunk boundary with one set-oriented prefix-validation query
  per replayed chunk.
- Prove one indexed closure scan validates count, ordinals, and digest.
- Return an identical persisted receipt from a matching closed replay without
  reading membership or consuming an iterable that fails if touched.
- Reject changed declaration, ordinal, work facts, duplicate work,
  noncontiguous ordinals, incorrect count, and incorrect digest.
- Leave registration open after generator or closure validation failure.
- Include reused members in counts for each submission run.
- Exercise empty and large memberships with bounded memory and indexed reads.

### Barrier and run completion

- Register admission and run barrier as separate scheduled workflows with
  independent intervals, batch sizes, names, and diagnostics.
- Verify the declared service-rate formula and qualify one configuration for
  each schedule with headroom.
- Withhold release before closure or while any member is nonterminal.
- Materialize an indexed completion-candidate page before bounded parameterized
  lateral eligibility probes; lock only eligible runs and advance by keyset
  across blocked or lock-skipped pages within the pass.
- Calculate terminal counts for the selected page with one grouped query.
- Release once for all-success and mixed terminal outcomes.
- Release every eligible overlapping submission run containing shared work.
- Make concurrent reconcilers create one run completion execution.
- Pin release counts, zero representation, workflow identity, and compact
  payload size.
- Return one member to `READY` after release but before completion starts;
  assert one execution, unchanged release facts, and an aggregate artifact that
  identifies the outcomes and output references actually consumed.
- Record one success or application failure across DBOS replay.
- Show that a durably submitted workflow without a recorded application
  outcome remains truthfully inspectable as `ENQUEUED`.

### Inspection

- Return bulk run state counts with one aggregate query per bounded input
  chunk.
- Distinguish missing submission runs from present empty runs.
- Make a paged `list_runs()` consumer use bulk counts without an N+1 query
  pattern.

### Schema and performance

- Create the complete schema from the fresh baseline on PostgreSQL.
- Verify the supported schema API cannot delete an existing ledger.
- Run against password-authenticated PostgreSQL.
- Qualify the indexed nonterminal anti-join with PostgreSQL `EXPLAIN` for a
  large nonterminal run and a large unrelated backlog.
- Verify reconciliation query and candidate budgets are page-bounded.
- Run the canonical pre-check and built-wheel public-API checks.

## 9. Future options (not current scope)

- Detect decorated async callables beyond
  `inspect.iscoroutinefunction` when a concrete caller requires that broader
  declaration boundary.
- Add independently tunable or multiple checkpoint pools, or change
  application-pool sizing, only if future evidence requires it.
- Interrupt process-local workflow coroutines when cancellation is delegated.
- Add admission payload and label byte limits.
- Choose between enforcing ordinal arrival order and using weaker
  committed-prefix terminology for resumable registration.

## 10. Implementation sequence

1. **Planning vocabulary and contracts**
   - apply the documentation changes below;
   - promote the selected terms and contracts into `.defs/`.
2. **Fresh schema baseline and membership**
   - add declaration, membership, release, and run completion execution
     persistence;
   - implement set-oriented registration and the stable closure receipt;
   - add inspection by submission run and bounded bulk run state counts.
3. **Async application workflows**
   - hard-cut declarations and enqueue validated platform payloads;
   - move stage and completion `args_for` into durable wrappers;
   - add cancellation, recovery, and loop-affine-resource qualification.
4. **Run barrier**
   - register a schedule independent from admission;
   - add materialized candidate paging, bounded parameterized eligibility
     probes, eligible-only locking, grouped release counts, immutable release
     facts, and duplicate-safe transactional enqueue.
5. **Run completion**
   - add declarations, wrapping, guarded outcome recording, and inspection;
   - retain the deliberately limited three-state lifecycle.
6. **Cross-package qualification**
   - exercise a public async storage resource and application-owned bridges
     without adding platform runtime dependencies;
   - qualify declared scheduler rates and burst handoff behavior.
7. **Release documentation**
   - update README examples, operational guidance, changelog, and version.

Each phase remains separately reviewable and leaves the canonical pre-check
green.

## Authoritative documentation changes

During implementation, promote the planning terms and contracts into `.defs/`.
Also:

- amend the existing admission contract so callback failures belong to durable
  execution rather than candidate rollback inside admission;
- map the stable receipt and bulk run-count symbols under the existing
  submission-run vocabulary; and
- amend the platform schema term and contract for the selected fresh
  development baseline: valuable databases are archived before an explicit
  reset, while supported schema commands remain non-destructive.

## Acceptance criteria

The implementation satisfies these criteria. The 2026-08-09 clean-tip
schema-4 result is the authoritative performance evidence for the scheduler,
burst, barrier-plan, and `list_runs()`-plan criteria. Focused repository checks
remain the acceptance evidence for the other functional contracts.

- async application workflows reuse a loop-affine resource safely;
- dispatcher transactions enqueue validated platform payloads without running
  application callbacks;
- registration uses a cardinality-independent statement count per chunk and
  one indexed closure scan;
- every closed submission run has immutable ordered membership matching its
  declared count, ordinals, uniqueness, and canonical digest;
- matching closed replay returns the same closure receipt in O(1) without
  consuming the member iterable;
- completion-enabled runs declare a manifest reference and digest before
  registration begins, and the application validates the loaded manifest
  before aggregation;
- overlapping runs retain independent membership and correct inspection for
  each submission run;
- page consumers obtain run state counts through one aggregate query per
  bounded chunk;
- independent admission and run barrier schedules satisfy their qualified
  service-rate configurations with headroom;
- the run barrier releases one run completion execution with immutable release
  facts, unaffected by later member transitions;
- the application aggregate artifact records the exact outcomes and output
  references it consumed;
- completion inspection truthfully distinguishes a recorded application
  outcome from durable submission alone;
- burst qualification proves the dispatcher-owned dedicated executor's queue,
  pool, loop, handoff, and cleanup behavior under the declared configuration;
- the fresh schema baseline contains no historical backfill or compatibility
  path; and
- focused concurrency, replay, PostgreSQL, query-bound, and built-wheel checks
  pass.
