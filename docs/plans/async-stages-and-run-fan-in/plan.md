# Async stages and run fan-in

Status: draft implementation plan

## Planning vocabulary and contracts

The proposed vocabulary in [plan-terms.toml](plan-terms.toml) and standing
rules in [plan-contracts.toml](plan-contracts.toml) hold definitions and rules
for this planning stage. This document applies them rather than repeating them.

The selected planning vocabulary and contracts must be promoted into `.defs/`
during implementation.

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
- `dr-platform==0.1.1` as the starting point
- DBOS `2.27.0`, already pinned by `dr-platform`

The packages co-install under Python 3.12 and newer. This work adds no runtime
dependency from `dr-platform` to `dr-store`, `dr-providers`, or `dr-exec`.

### Current gaps

- The platform-owned workflow wrapper is synchronous and does not await an
  async application workflow.
- A work item records its origin run, but a submission run does not persist its
  own membership. Reused work is therefore absent from reads for later
  submission runs.
- No durable platform primitive observes a closed run membership, releases one
  run completion after all members settle, and records its outcome.

### Goals

1. Hard-cut platform-managed application workflows to async.
2. Preserve synchronous argument derivation and ledger transactions.
3. Persist each submission run's ordered membership, including reused work.
4. Bind that membership to an application manifest with one canonical digest.
5. Close registration only after the complete declared membership validates.
6. Release one run completion after the run barrier is satisfied.
7. Preserve immutable release facts even if a member later changes state.
8. Record one run completion outcome under one stable workflow identity.
9. Keep registration writes, dispatcher passes, and inspection reads bounded;
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
stage. `args_for` remains synchronous, derives serializable arguments from the
admission payload, and performs no external I/O.

### Platform-owned wrapper

The wrapped application workflow for a pipeline stage becomes `async def` and:

1. awaits the application workflow;
2. validates one non-empty output reference;
3. invokes the existing synchronous stage handoff transaction through
   `asyncio.to_thread`;
4. preserves success, application failure, mismatch, late cancellation, and
   successor-creation behavior;
5. lets `asyncio.CancelledError` follow DBOS cancellation handling; and
6. remains replay-safe under the same stage attempt and workflow identity.

The SQLAlchemy ledger path remains synchronous. Applications may build
lifecycle-aware async bridges around synchronous `dr-providers` or `dr-exec`
calls, but those adapters and their resource shutdown remain outside this
package.

Qualification must prove that concurrent async application workflows can reuse
one loop-affine test resource and that the synchronous handoff does not block
the event loop.

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
same digest inside its immutable manifest.

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

Indexes must support ordered membership and counts for one run, all runs
containing one work item, and barrier eligibility without scanning unrelated
campaigns.

Every registration and closure transaction locks the submission run row before
reading or changing membership. Database enforcement prevents changed or new
membership after closure.

### Bounded submission and automatic closure

The existing `submit` operation accepts one complete logical member stream and
commits bounded chunks. For each chunk, one transaction:

1. validates the declaration against its persisted value;
2. inserts or resolves the work item;
3. inserts or resolves membership at the declared ordinal;
4. rejects an occupied ordinal bound to different work;
5. rejects the same work at another ordinal in the submission run;
6. creates the first stage execution only for a new work item; and
7. commits before consuming the next chunk.

Reuse adds membership but no duplicate stage execution history. It is permitted
only when pipeline key, pipeline version, and execution-configuration reference
match the work item's origin run exactly.

Registration closes automatically only when:

1. the iterable exhausts normally;
2. the persisted member count equals the declaration;
3. ordinals are contiguous from zero;
4. work items are unique within the submission run; and
5. when a digest was declared, the persisted membership digest equals it.

A generator failure or closure validation failure leaves registration open.
Earlier committed chunks remain an exact resumable prefix. Exact replay of
committed chunks and of an identical completed stream is idempotent, including
after closure; any changed declaration or membership is rejected.

Multi-call ingestion is unsupported. A future caller requiring it must justify
a separate explicit closure operation.

An empty declaration with a digest uses the canonical empty membership digest.
If its pipeline declares run completion, its run barrier is immediately
eligible after closure.

`SubmissionReceipt` reports new and existing work items separately from new
and existing memberships.

### Inspection by submission run

Counts and status queries for a submission run, and run barrier eligibility,
join through membership rather than origin. They remain aggregate or bounded;
applications use their manifest and application-owned storage for ordered
results.

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

After the ordinary admission pass, the scheduled dispatcher runs a separately
bounded run barrier reconciliation page. It:

1. selects closed, completion-enabled submission runs in stable order;
2. proves with indexed anti-joins that no member's current stage execution is
   outside `SUCCEEDED`, `FAILED`, or `CANCELLED`;
3. records immutable `released_at` and release terminal state counts;
4. creates and enqueues one run completion execution in the same transaction;
   and
5. relies on persisted uniqueness to make concurrent passes converge.

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
rest of the bounded page.

### Payload and application result

The platform supplies compact release facts:

```python
@dataclass(frozen=True, slots=True)
class RunCompletionPayload:
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

- Application workflow exceptions use the existing stage execution failure
  path;
  cancellation is not rewritten as application failure.
- A run completion application exception records one `FAILED` outcome;
  cancellation is not rewritten as application failure.
- A registration chunk is atomic. Earlier chunks survive a later failure and
  remain replayable.
- Closure failure records no closure or release facts.
- A nonterminal member prevents release; a later reconciliation page observes
  its terminal transition.
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

## 7. Verification plan

### Async boundary

- Reject a synchronous application workflow at declaration time.
- Await an async application workflow and persist its output reference.
- Preserve atomic stage handoff and current cancellation fencing.
- Recover the same async DBOS stage attempt after interruption.
- Reuse one loop-affine async test resource across concurrent application
  workflows.
- Prove synchronous ledger calls do not block the event loop.

### Registration and membership

- Require manifest reference and digest before any member write for a
  completion-enabled pipeline; allow an item-only pipeline to omit both.
- Pin the canonical membership digest representation with golden tests.
- Validate persisted membership against the declared digest at closure.
- Have the application reject a manifest carrying a different digest.
- Register new and reused work in independent submission runs.
- Reject reuse with different execution provenance.
- Resume at every chunk boundary and replay the completed stream after closure.
- Reject changed declaration, ordinal, work facts, duplicate work,
  noncontiguous ordinals, incorrect count, and incorrect digest.
- Leave registration open after generator or closure validation failure.
- Include reused members in counts for each submission run.
- Exercise empty and large memberships with bounded memory and indexed reads.

### Barrier and run completion

- Withhold release before closure or while any member is nonterminal.
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

### Schema and performance

- Create the complete schema from the fresh baseline on PostgreSQL.
- Verify the supported schema API cannot delete an existing ledger.
- Run against password-authenticated PostgreSQL.
- Assert required membership and barrier indexes through query plans.
- Verify reconciliation is page-bounded under unrelated backlog.
- Run the canonical pre-check and built-wheel public-API checks.

## 8. Implementation sequence

1. **Planning vocabulary and contracts**
   - apply the documentation changes below;
   - promote the selected terms and contracts into `.defs/`.
2. **Fresh schema baseline and membership**
   - add declaration, membership, release, and run completion execution
     persistence;
   - update submission receipts and inspection by submission run.
3. **Async application workflows**
   - hard-cut declarations and wrappers;
   - add cancellation, recovery, and loop-affine-resource qualification.
4. **Run barrier**
   - add indexed eligibility queries, immutable release facts, and
     duplicate-safe transactional enqueue.
5. **Run completion**
   - add declarations, wrapping, guarded outcome recording, and inspection;
   - retain the deliberately limited three-state lifecycle.
6. **Cross-package qualification**
   - exercise a public async storage resource and application-owned bridges
     without adding platform runtime dependencies.
7. **Release documentation**
   - update README examples, operational guidance, changelog, and version.

Each phase remains separately reviewable and leaves the canonical pre-check
green.

## Authoritative documentation changes

During implementation, promote the planning terms and contracts into `.defs/`.
Amend the existing platform schema term and contract for the selected fresh
development baseline: valuable databases are archived before an explicit
reset, while supported schema commands remain non-destructive.

## Acceptance criteria

This effort is complete when:

- async application workflows reuse a loop-affine resource safely;
- every closed submission run has immutable ordered membership matching its
  declared count, ordinals, uniqueness, and canonical digest;
- completion-enabled runs declare a manifest reference and digest before
  registration begins, and the application validates the loaded manifest
  before aggregation;
- overlapping runs retain independent membership and correct inspection for
  each submission run;
- the run barrier releases one run completion execution with immutable release
  facts, unaffected by later member transitions;
- the application aggregate artifact records the exact outcomes and output
  references it consumed;
- completion inspection truthfully distinguishes a recorded application
  outcome from durable submission alone;
- the fresh schema baseline contains no historical backfill or compatibility
  path; and
- focused concurrency, replay, PostgreSQL, query-bound, and built-wheel checks
  pass.
