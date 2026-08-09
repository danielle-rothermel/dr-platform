# Async stages and run fan-in

Status: draft for discussion

## Purpose

Prepare `dr-platform` to orchestrate high-concurrency research workflows whose
application effects are implemented by async storage and synchronous provider
or process clients. At the same time, add an exact run-membership and run-level
fan-in primitive so applications do not repeatedly implement polling,
completeness checks, and one-time aggregate launch around the platform.

This plan intentionally keeps application pipelines linear per work item.
Fan-in is a separate run-level completion execution, not a graph edge or a
special provider/evaluation feature.

## Foundation versions reviewed

The initial plan assumes these published foundations:

- `dr-store==0.2.0`
- `dr-providers==0.3.0`
- `dr-exec==0.1.7`
- `dr-platform==0.1.1` as the starting point
- DBOS `2.27.0`, already pinned by `dr-platform`

The packages co-install under Python 3.12 and newer. The relevant foundation
contracts are already present:

- `dr-store` supplies async object storage, an async PostgreSQL backend, batch
  operations, and run/result-granularity artifact bundles.
- `dr-providers` supplies serializable provider-call state, deterministic
  retry transitions, terminal results, and one-invocation evidence.
- `dr-exec` supplies synchronous single-job execution, async bounded pooling,
  importable JSON jobs, and opaque run-record references.

No provider, execution, artifact, reward, experiment, or analysis semantics
should move into `dr-platform` as part of this work.

## Current gaps

### Application stages are synchronous

`StageDefinition.workflow` currently accepts any callable, and the
platform-owned DBOS wrapper is a synchronous function that calls the
application workflow directly. A coroutine therefore becomes an invalid
output rather than being awaited.

This prevents a stage from naturally sharing a loop-affine `asyncpg` pool and
awaiting `dr-store`. It also pushes applications toward per-call event loops or
other resource-lifecycle workarounds.

DBOS 2.27 supports coroutine workflows and durable async sleep, so the current
restriction is in `dr-platform`, not DBOS.

### Run membership is not represented

The vocabulary describes a submission run as having append-only work-item
membership. The schema instead stores one `origin_run_key` on each work item.
When the same campaign/work identity is submitted by a later run, the work item
and stage execution are reused, but the later run has no persisted membership
edge.

Consequences include:

- run counts and statuses describe only origin work;
- a run cannot prove its exact intended member set;
- interrupted submission can be resumed with a truncated or reordered input
  stream without a complete-set identity check; and
- run-level fan-in cannot determine completeness for overlapping runs.

### There is no platform-owned fan-in

The platform can report per-work state, but no durable primitive waits for a
closed run membership set, releases exactly once after all members become
terminal, and records an aggregate execution result. Each consumer would have
to implement its own polling, race handling, retry history, and publication
trigger.

## Goals

1. Make platform-managed application stage workflows async-only.
2. Preserve synchronous, transaction-safe argument derivation and ledger
   mutation where those operations are currently correct.
3. Represent exact append-only membership between runs and work items,
   including work reused across runs.
4. Bind each run to one caller-declared immutable ordered membership set and
   reject incomplete or conflicting completion.
5. Release at most one run-completion execution when registration is complete
   and every declared member is terminal.
6. Give run-completion execution the same durable outcome, attempt, retry,
   cancellation, recovery, admission, and inspection quality as item stages.
7. Preserve opaque references and keep payload resolution application-owned.
8. Keep all scans, registration writes, reconciliation passes, and inspection
   reads bounded or paginated.

## Non-goals

- General DAG pipelines or arbitrary dependency graphs.
- Provider-specific retry, rate-limit, model, or credential policy.
- A `dr-exec` worker pool, process fleet, or batch envelope in the platform.
- Reward, experiment acceptance, statistical aggregation, or evaluation
  semantics.
- Resolving `dr-store`, `dr-exec`, or other references inside the platform.
- Distributed blob storage or a portable `dr-exec` run-record backend.
- Exactly-once external provider or process effects.
- Sync and async variants of every platform API.
- Changing admission, inspection, and migration APIs to async without a
  demonstrated async caller requirement.

## Proposed architecture

```text
caller-prepared run declaration
  -> bounded registration of ordered run members
  -> ordinary linear item pipelines
  -> terminal member states
  -> bounded run-barrier reconciliation
  -> one async run-completion execution
  -> opaque aggregate output reference
```

The platform remains responsible for durable orchestration facts. Applications
remain responsible for storing and resolving inputs, provider-call state,
execution evidence, detailed outputs, and aggregate results.

## 1. Async application stage boundary

### Public contract

Hard-cut `StageDefinition.workflow` to an async callable. Do not retain a sync
workflow compatibility branch while the package is alpha.

The conceptual public shapes are:

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

`args_for` remains synchronous because admission calls it inside the
transaction that prepares and enqueues an attempt. It must derive serializable
workflow arguments from `AdmissionPayload` without external I/O. This plan does
not introduce async work inside that transaction.

Validate the coroutine-function requirement when constructing a stage. A
synchronous callable fails before registration or submission rather than at
workflow execution time.

### Platform-owned wrapper

The wrapped workflow becomes `async def` and:

1. awaits the application workflow;
2. validates one non-empty output-reference string;
3. invokes the existing checkpointed synchronous completion transaction via
   `asyncio.to_thread`;
4. preserves the existing success/failure and successor-creation transaction;
5. does not translate `asyncio.CancelledError` into an application failure;
6. retains current mismatch and late-cancellation fencing; and
7. remains safe under DBOS replay of the same platform attempt.

The transaction itself should remain synchronous SQLAlchemy code. Do not add a
second async ledger implementation.

### Application resource lifecycle

The platform must document and qualify the following wiring boundary:

- async resources such as `asyncpg.Pool` are process-scoped and created on the
  same long-lived DBOS target event loop that uses them;
- application workflow closures or application-owned containers supply those
  resources to stages;
- the platform neither constructs nor closes provider, store, or executor
  clients; and
- shutdown stops admission before application resources are drained and
  closed.

Add an integration test proving that two async DBOS stage invocations can reuse
one loop-affine async resource. Use a small test capability rather than making
`dr-store` a runtime dependency of `dr-platform`.

### Synchronous dependency bridges

Do not change `dr-exec` or `dr-providers` merely to make platform stages async.
Applications may supply small async bridges around their synchronous calls.
Those bridges must be lifecycle-aware:

- use a named bounded executor rather than the event loop's incidental default
  pool when concurrency is material;
- size the provider bridge consistently with `HttpProvider` connection limits;
- on coroutine cancellation, signal the provider cancellation event or
  `dr-exec.CancelToken`;
- await cleanup and evidence finalization instead of abandoning the underlying
  thread; and
- return or persist the foundation's typed result without reclassifying it as
  platform success or failure.

These bridges belong in the consuming application or a future focused adapter
package. They are not part of this platform change.

## 2. Immutable run registration and membership

### Caller-prepared declaration

Require the caller to declare the complete ordered member set before run
registration begins. This matches current experiment workflows, where the
task/sample/configuration grid is known before submission.

Introduce a closed declaration carrying at least:

```python
@dataclass(frozen=True, slots=True)
class RunRegistrationDeclaration:
    expected_member_count: int
    membership_identity_hash: str
    manifest_reference: str
```

The manifest reference remains opaque. The platform does not resolve it. The
membership identity hash binds the exact ordered member facts crossing the
platform boundary.

Each submitted member has an explicit zero-based ordinal:

```python
@dataclass(frozen=True, slots=True)
class RunMemberInput:
    ordinal: int
    work: WorkInput
```

Define and pin one `dr-serialize` identity recipe over:

- ordinal;
- work key;
- input reference; and
- labels in canonical key order.

The run membership identity is a versioned identity document over the ordered
member identity hashes. The final plan must pin its schema, schema version, and
golden vectors before implementation.

### Persistence

Extend `pipeline_runs` with the immutable registration declaration and add a
membership table conceptually shaped as:

```text
run_memberships
  run_key                FK pipeline_runs
  member_ordinal         nonnegative integer
  work_item_id           FK work_items
  member_identity_hash   exact lowercase SHA-256

  PRIMARY KEY (run_key, member_ordinal)
  UNIQUE (run_key, work_item_id)
```

Keep `origin_run_key` as immutable provenance for the run that first created a
work item, unless the implementation plan deliberately replaces it with an
equivalent creation-provenance record. Membership and origin are different
facts.

Indexes must support:

- ordered members for one run;
- all runs containing one work item;
- membership counts for one run; and
- barrier eligibility without scanning unrelated campaigns.

### Registration behavior

For each bounded submission chunk, one transaction:

1. validates the run declaration against the stored immutable declaration;
2. inserts or resolves the campaign/work item;
3. inserts or resolves its run membership at the declared ordinal;
4. rejects a different member at an occupied ordinal;
5. rejects the same work item at a second ordinal in the run;
6. creates the first stage only when the work item itself is new; and
7. commits before consuming the next chunk.

A reused work item still creates an idempotent membership for the current run.
It does not create another item-stage execution.

Reusing work across runs must not silently change its execution provenance.
The implementation should permit membership reuse only when the new run's
pipeline key, pipeline version, and execution-configuration reference exactly
match the work item's origin run. A caller that changes any execution-defining
fact must select a different work identity. This is a deliberate hardening of
the current behavior, which can reuse one work item across differing run
configurations.

Submission completion must atomically verify:

- exactly `expected_member_count` membership rows exist;
- ordinals are contiguous from zero;
- the recomputed ordered membership identity equals the declared identity; and
- the run has not already completed with different facts.

Only then set the existing completion timestamp. A truncated stream cannot
close the run. Reordered, altered, or duplicated inputs fail visibly. Exact
replay of any committed prefix and the complete stream is idempotent.

Preserve the currently valid empty-run case: a declaration with zero members
uses the pinned empty-membership identity and may complete registration. If it
declares run completion, its barrier is immediately eligible after
registration.

Revise `SubmissionReceipt` so it does not conflate work creation with run
membership. It should report new/existing work items and new/existing
memberships separately.

### Run-scoped inspection

All run-scoped counts, lists, bulk status, and completion queries must join
through membership rather than `origin_run_key`.

Expose bounded readers for ordered run members and their current terminal
states. Do not return every member or every output reference in one unbounded
`RunSummary`.

## 3. Run barrier and completion execution

### Keep item pipelines linear

Add an optional run-completion declaration beside the item-stage sequence:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class RunCompletionDefinition:
    key: StageKey
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

The exact final name is an owner decision. Whatever name is selected, the
definitions must state that this is one run-scoped execution after the item
pipeline, not another item stage and not a graph node.

Completion keys must not collide with item-stage keys in one pipeline version.
The registry must include completion definitions in equality and conflict
checks. Wrapping and dispatcher validation must require the completion
workflow to use the package-owned wrapper.

### Barrier eligibility

A run is eligible for completion when:

1. submission/registration is completed;
2. it declares a run-completion definition;
3. every exact membership has a terminal current item stage; and
4. no completion execution already exists for the run.

Terminal means `SUCCEEDED`, `FAILED`, or `CANCELLED`. The platform releases the
completion execution for mixed terminal outcomes. Whether those outcomes make
an experiment acceptable is application policy and must not be embedded in
the barrier.

This distinction is important: platform execution completeness is not domain
acceptance.

### Bounded reconciliation

Extend the scheduled dispatcher with a bounded run-barrier reconciliation pass
instead of adding an unbounded scan to item completion transactions.

The pass should:

- select registration-complete eligible runs in stable order;
- examine a configured bounded candidate page;
- prove no nonterminal member exists with indexed anti-joins;
- insert one ready run-completion execution using a uniqueness constraint;
- isolate malformed registry/declaration candidates from unrelated runs; and
- make concurrent passes converge without duplicate execution.

Eventual release through the scheduled pass is acceptable. Item terminality
and run-completion creation do not need to share one transaction because the
reconciler reads authoritative platform state and insertion is idempotent.

### Completion payload

The platform supplies compact immutable facts, not all member outputs:

```python
@dataclass(frozen=True, slots=True)
class RunCompletionPayload:
    campaign_key: CampaignKey
    run_key: RunKey
    pipeline_key: PipelineKey
    pipeline_version: int
    execution_config_reference: str
    manifest_reference: str
    membership_identity_hash: str
    member_count: int
    terminal_state_counts: tuple[StateCount, ...]
    attempt_number: int
```

The application resolves or pages member results using platform inspection and
its own opaque references. Large result sets and credentials must never enter
DBOS workflow arguments.

### Completion lifecycle

Prefer separate run-completion execution and attempt tables over immediately
generalizing all item-stage tables to a nullable polymorphic scope. The two
scopes have different foreign keys and payloads, and the shared abstraction is
not yet earned.

The run-completion ledger should nevertheless preserve the established
semantics:

- `READY`, `ADMITTED`, `SUCCEEDED`, `FAILED`, and `CANCELLED` logical states;
- append-only attempt ordinals and stable DBOS workflow identities;
- one guarded terminal transaction;
- a non-empty output reference only on success;
- explicit operator retry only from `FAILED`;
- logical cancellation before non-recursive DBOS cancellation delegation;
- abandoned-work sweep projection;
- capacity occupancy while admitted; and
- read-only attempt history and state-count inspection.

Extract shared lifecycle helpers only after the two concrete implementations
show identical logic that can be shared without weakening either foreign-key
or lock-order contract.

For the first version, use the existing stage-control table and require an
empty-selector control for a run-completion key. Label-specific completion
controls should be rejected until runs have an explicit immutable label
contract.

## 4. Failure, cancellation, and replay

### Async item and completion workflows

- Application exceptions become logical `FAILED` outcomes through the existing
  safe error-summary boundary.
- `asyncio.CancelledError` follows DBOS cancellation handling and is not
  rewritten as an application exception.
- A platform cancellation committed before a late workflow return wins.
- DBOS recovery resumes the same platform attempt and workflow identity.
- Operator retry appends a new attempt; it never reopens an old attempt.

### Registration

- A failure before a chunk transaction changes nothing in that chunk.
- A failure after earlier chunks leaves a resumable exact prefix.
- A declaration conflict fails before further membership writes.
- Completion failure leaves the run open and explains count, ordinal, or
  identity disagreement without silently accepting a partial set.

### Barrier

- A run with any nonterminal member remains unreleased.
- A run whose members became terminal before registration completion releases
  only after registration completes.
- Concurrent reconciliation and item handoff either observe nonterminal work or
  create the one completion execution; a later pass repairs the former case.
- Failure or cancellation of the completion workflow does not change member
  outcomes or reopen registration.

## 5. Schema and migration

Add a forward-only Alembic revision after `0001_staging_baseline`.

Recommended migration policy:

1. create membership and run-completion tables and indexes;
2. add immutable run-registration declaration fields;
3. backfill one membership for each existing work item's `origin_run_key`, with
   ordinals in stable existing rank order;
4. compute and store the corresponding membership identity and member count;
5. preserve existing run, item, stage, attempt, control, and output records;
6. update immutability triggers for the new declaration and membership facts;
7. retain irreversible downgrade behavior; and
8. do not delete or reinterpret existing attempt history.

Historical membership in non-origin runs cannot be reconstructed because the
current schema never recorded it. The migration therefore preserves the
current observable run scope: each migrated run contains its origin work only.
It must document that limitation rather than infer membership from unavailable
submission inputs.

If no persisted `dr-platform` database needs preservation, the owner may choose
a fresh-schema hard cut instead. That choice must be explicit before
implementation; the default plan is data-preserving forward migration.

## 6. Package boundaries

`dr-platform` should not add runtime dependencies on `dr-store`,
`dr-providers`, or `dr-exec` for this change.

The intended composition is:

```text
dr-platform
  owns: membership, barrier, admission, durable execution, attempts, controls

application adapter
  owns: reference codecs, async bridges, resource instances, cancellation

dr-store / dr-providers / dr-exec
  own: their existing storage, provider-call, and process-execution contracts
```

Cross-package qualification tests may install the published foundations as
test-only inputs. They must use public APIs and should not become alternate
implementations inside `dr-platform`.

## 7. Verification plan

### Async boundary

- Reject a synchronous stage workflow at declaration time.
- Await an async item stage and persist its output reference.
- Preserve one atomic completion-and-next-stage handoff.
- Record application exceptions without catching cancellation as failure.
- Recover the same async DBOS attempt after interruption.
- Reuse one loop-affine async test resource across concurrent stages.
- Prove completion transaction calls do not block the event loop.

### Membership and registration

- Register a new work item and membership.
- Register an existing campaign/work item into a second run.
- Reject cross-run reuse with a different pipeline or execution configuration.
- Replay exact chunks and the final completion idempotently.
- Resume after interruption at every chunk boundary.
- Reject changed run declaration, ordinal, work facts, or member identity.
- Reject duplicate work at different ordinals.
- Reject truncated completion and noncontiguous ordinals.
- Reject an incorrect final membership identity.
- Preserve caller-declared order in paginated inspection.
- Make run state counts include reused members.
- Complete and inspect the pinned empty-membership case.
- Exercise large registrations with bounded transaction and query counts.

### Barrier and completion

- Do not release before registration completion.
- Do not release while one member is nonterminal.
- Release once when every member succeeds.
- Release once for mixed succeeded, failed, and cancelled members.
- Release after registration when all members were already terminal.
- Release every eligible overlapping run containing one shared work item.
- Make concurrent reconcilers create one completion execution.
- Isolate a missing or conflicting registry definition.
- Enforce completion capacity and pause.
- Persist completion output, failure, retry, cancellation, and attempt history.
- Preserve late-completion and cancellation race behavior.
- Reconcile an abandoned completion workflow through the sweep.

### Migration and performance

- Upgrade a populated `0001` database and verify every existing fact.
- Run against password-authenticated PostgreSQL.
- Assert required membership and barrier indexes through query plans.
- Verify reconciliation work is page-bounded under a large unrelated backlog.
- Verify workflow arguments remain compact as membership grows.
- Run the existing canonical pre-check and built-wheel public API checks.

## 8. Implementation sequence

1. **Finalize owner decisions and vocabulary**
   - settle names, async-only policy, manifest identity, barrier terminality,
     completion control scope, and migration policy;
   - update `.defs/terms.toml` and `.defs/contracts.toml` with the selected
     standing contracts.

2. **Async item stages**
   - hard-cut declarations and wrappers;
   - add cancellation and DBOS recovery tests;
   - qualify one loop-affine async resource.

3. **Exact run membership**
   - add declaration models, identity vectors, membership persistence, and
     forward migration;
   - update submission receipts and every run-scoped query.

4. **Barrier projection**
   - add bounded eligibility queries and idempotent ready-completion creation;
   - extend dispatcher summaries and diagnostics.

5. **Run-completion execution**
   - add declarations, wrapping, admission, attempts, handoff, retry,
     cancellation, sweep, and inspection;
   - preserve separate concrete persistence until shared lifecycle code is
     demonstrably identical.

6. **Cross-package qualification**
   - exercise a public `dr-store` async resource;
   - exercise lifecycle-aware bridges around published `dr-providers` and
     `dr-exec` without adding them as platform runtime dependencies.

7. **Documentation and release**
   - update README examples, operational startup/shutdown guidance, package
     definitions, changelog, and version;
   - describe local/shared-filesystem limits without claiming distributed
     artifact portability.

Each phase should remain separately reviewable and leave the repository's
canonical pre-check green.

## Owner decisions to finalize

The dr-platform planning agent should resolve these explicitly before
implementation:

1. **Async compatibility:** async-only hard cut (recommended) or temporary
   sync/async dual support.
2. **Registration identity:** explicit ordinal plus one pinned ordered identity
   recipe (recommended), or a different complete-set proof.
3. **Barrier policy:** release after all members are terminal regardless of
   outcome (recommended), or select an explicit subset of terminal outcomes
   and thereby make the platform own completion policy.
4. **Completion persistence:** separate run-completion tables (recommended) or
   a broader polymorphic execution-scope refactor.
5. **Completion controls:** empty-selector stage control only (recommended) or
   a new immutable run-label model.
6. **Migration:** data-preserving forward migration (recommended) or an
   explicitly approved fresh-schema hard cut.
7. **Naming:** `RunCompletionDefinition` and related terms, or another name that
   clearly distinguishes the execution from cleanup/finalization and from an
   item stage.
8. **Cross-run work compatibility:** require exact pipeline and execution
   configuration agreement before reusing work (recommended), or redefine work
   execution provenance independently of its origin run.

## Acceptance criteria

This effort is complete when:

- application stage workflows are async and can reuse a loop-affine async
  resource safely;
- every completed submission run has a verified exact ordered membership set;
- overlapping runs retain independent membership and correct status counts;
- an optional run-completion execution is released exactly once after all
  members become terminal;
- run completion has durable attempts, controls, recovery, retry,
  cancellation, and inspection;
- no platform contract interprets provider, process, storage, evaluation, or
  reward meaning;
- migrations preserve existing ledger evidence under the selected migration
  policy; and
- focused concurrency, recovery, PostgreSQL, performance, and built-wheel
  checks pass.
