# Round 1 unified plan review feedback

**Reviewed:** 2026-07-10

**Plan:** [`../plan.md`](../plan.md) (v0)

**Inputs:** [`fable-findings.md`](fable-findings.md),
[`codex-findings.md`](codex-findings.md), the current
`dr-platform` and `whetstone-ai` trees, the installed DBOS 2.26.0 API, and
`/Users/daniellerothermel/drotherm/codex/viz/00-dbos-transact-vs-conductor.html`.

## Executive verdict

The hard-cut direction is still coherent: one kernel submission path, fresh
schemas, a smaller public API, domain-specific state on the whetstone side,
and analytical/detail stores outside operational Postgres are all good target
constraints. The plan is not implementation-ready, however. **Its core retry,
identity, pacing, priority, and export stories each contain an unresolved
state or ownership decision.** Implementing them literally would create silent
duplicate work, permanently stale exports, misleading completion states, or a
submission path that cannot seed the rows its workflows load.

The Conductor comparison adds one useful pre-experiment constraint: resolve
the state-model defects first, then build a compact application-owned operator
surface over public DBOS APIs. Searchable workflow correlation, a typed
operation inspector, optional OTLP traces, and on-demand health reporting are
worth including before serious experiments. Retention, generic replay,
alerts, MCP tools, and a broad control plane are not.

The current code does not resolve these questions for us. `dr-platform` is
still the pre-cut implementation on `07-08-refactor`; `whetstone-ai` has since
moved to its July 9 phase-2 lockstep merge, but the reviewed seams remain:
domain rows are seeded transactionally, generation identity is
content-addressed independently of an operation, workflows load those domain
rows, and throttle checks happen per graph node.

Priority labels below mean:

- **P0 — decision blocker:** settle before revising dependent sections of the
  plan or opening implementation issues.
- **P1 — correctness contract:** specify before implementation starts.
- **P2 — bounded follow-through:** mechanically important, but it can follow
  the P0/P1 decisions.

## 1. P0 — Define a real workflow reconciliation state machine

This is the most important combined finding. The plan says resubmitting an
operation retries terminal workflow failures by incrementing `attempt`, but
the submission pipeline has no step that can observe those failures. A
successful enqueue leaves the item in `ENQUEUED` or
`WORKFLOW_ALREADY_PRESENT`; the existing retry preparation only resets
enqueue failures and stale claims, and the claim query only sees `PENDING`
items (`src/dr_platform/submission.py:476`, `:602`). DBOS workflow state is
currently read only by observability (`src/dr_platform/observability.py:115`).

The plan must define two separate state planes:

1. **Submission/enqueue state:** registration, claim, enqueue, and enqueue
   failure.
2. **Workflow execution state:** active, succeeded, failed, recovery-exhausted,
   cancelled, or missing in DBOS.

Add an explicit reconciliation phase before claim. It should batch-read DBOS
statuses for previously enqueued items and use a compare-and-swap transition
to create a new attempt only when policy permits. The specification must name:

- the exact source-row predicate and CAS predicate;
- the old and new `attempt`, `workflow_id`, claim, failure, and metadata values;
- how concurrent resubmissions avoid minting two replacement attempts;
- how a missing DBOS row is treated;
- whether `ERROR` and `MAX_RECOVERY_ATTEMPTS_EXCEEDED` auto-retry;
- that `CANCELLED` does **not** auto-retry without an explicit operator action;
- whether operation status means enqueue completion or execution completion.

Use DBOS's real normalized status set. In the installed runtime the live
states are `PENDING`, `ENQUEUED`, and `DELAYED`; `ACTIVE` is not a persisted
status (`src/dr_platform/dbos_config.py:33`). `SUCCESS` blocks replacement.
`ERROR`, `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, `CANCELLED`, and `MISSING` require
separate policy decisions rather than one `terminal-failed` bucket.

If the kernel owns creation of new attempts, it must preserve attempt history
rather than only incrementing `items.attempt` and replacing
`items.workflow_id`. Add an append-only `<prefix>_item_attempts` table (or an
equally durable typed record) keyed by `(item_id, attempt)`, carrying:

- `workflow_id` and workflow role;
- normalized DBOS outcome;
- source attempt/workflow, retry reason, and source application version;
- creation, enqueue, and terminal timestamps; and
- the failure/reconciliation facts needed to explain why the next attempt was
  allowed.

The Item row may point at the current attempt for efficient submission, but it
must not be the only attempt history. This gives reconciliation, inspection,
export, and future domain replay one application-owned provenance source even
if DBOS records are later retained or deleted under a different policy.

**Source reviews:** Fable F1, F10, F12; Codex F1, F2.

## 2. P0 — Replace “watermark export” with an end-to-end consistency protocol

The export design currently names a mechanism without defining the state that
makes it correct. Operations have `created_at`/`completed_at`; items have only
`created_at`; both mutate after insertion (`src/dr_platform/db/schema.py:69`,
`:109`; `src/dr_platform/submission.py:660`). A `created_at` watermark would
therefore miss the status, workflow, attempt, metadata, and failure updates
that analysis most needs.

Adding `updated_at` is necessary but not sufficient. The revised plan should
define one complete export protocol:

1. Give every mutable exported kernel row a monotonic change cursor. Prefer a
   database sequence/change number over wall-clock time; if `updated_at` is
   retained, define precision, tie-breaking, and the `(updated_at, primary
   key)` cursor.
2. Capture a high-water mark in a stable source snapshot.
3. Export rows after the previous cursor and at or before that high-water mark.
4. Commit each sink idempotently.
5. Advance sink-specific state only after that sink commits.
6. Define retry behavior when DuckDB succeeds but MotherDuck or Neon fails.
7. Define deletion/tombstone behavior, even if normal flows are append-only.

Export state must identify the destination it describes. One Postgres
watermark cannot safely describe arbitrary local DuckDB files on multiple
machines. Prefer state stored in the DuckDB target itself; otherwise key
Postgres state by a stable sink/database identity and enforce a single-writer
lease.

The plan must also split artifact semantics instead of applying one watermark
story to everything:

- kernel tables: incremental change-cursor upsert;
- DBOS tables: use their actual `updated_at`/primary-key contract, versioned
  against the supported DBOS schema;
- client projections: explicitly full rebuild or an incremental dependency
  contract that handles late-arriving joined rows;
- Neon detail samples: deterministic root-entity sampling cascaded to all
  child rows, not independent row sampling that breaks drill-through joins.

**Source reviews:** Fable F2, F8, F9; Codex F4.

## 3. P0 — Decide the identity and deduplication scope before owning workflow IDs in the kernel

The proposed workflow recipe `(operation_key, item_id, attempt)` changes a
business contract, not just an implementation detail. Today whetstone derives
`generation_run_id` from `(prediction_id, attempt_index)` and derives the DBOS
workflow ID from that generation-run ID
(`../whetstone-ai/src/whetstone/platform/queue_worker.py:100`). Identical
content submitted under different operation keys therefore converges on the
same durable generation run.

The proposed recipe makes executions operation-scoped, while `item_id` already
contains `operation_key`. That removes cross-operation deduplication and risks
two kernel attempt counters describing one whetstone prediction history.

Choose and document one of these models:

- **Content-scoped execution (recommended):** the caller supplies a stable
  execution key or workflow-ID recipe through the enqueue target; the kernel
  owns safe enqueue mechanics but not domain identity. Kernel `attempt` must
  map explicitly to the caller's attempt/generation identity.
- **Operation-scoped execution:** every operation intentionally creates an
  independent execution history. If selected, revise whetstone's
  `generation_run_id` contract, append-only uniqueness, cost expectations,
  and cross-operation observability together.

Do not keep the current text as an implicit choice. It silently selects the
second model while Part 2.3 promises to preserve content-addressed identity.

**Source review:** Fable F4.

## 4. P0 — Reconcile queue topology with per-node throttle domains

The plan claims one queue per throttle domain eliminates sleeping-workflow
slot starvation, but whetstone resolves `throttle_key` per graph node and a
single generation workflow can traverse several providers/models
(`../whetstone-ai/src/whetstone/platform/graph_workflow.py:145`, `:321`). The
proposed `EnqueueTarget` has one queue per operation, so it cannot implement
the stated rule even for the flagship client.

The plan needs an honest ownership choice:

- Keep multi-domain graph workflows and state that in-workflow sleeps can
  occupy shared queue slots. Bound the residual risk with explicit sleep caps,
  queue sizing, observability, and an operator escape hatch; or
- Route each item to a queue with `queue_for(item)`, while acknowledging that
  this only helps items whose entire workflow belongs to one domain; or
- Move throttling below the workflow-slot boundary, which is a larger redesign
  and should not be smuggled into this hard cut.

The first option is the smallest honest revision. Remove the word
“neutralized” unless the selected architecture actually enforces a
workflow-to-domain 1:1 relationship.

**Source review:** Fable F3.

## 5. P0 — Re-evaluate priority as scheduling policy, not a fairness rename

The plan's digest-derived integer priority does not reproduce fairness. DBOS
dequeues strictly by `priority ASC, created_at ASC` in the installed 2.26.0
runtime. Stable pseudo-random priorities mix a finite backlog, but under
sustained arrivals a high-numbered item can be overtaken indefinitely by new
lower-numbered items. This is a starvation distribution, not a fairness
guarantee. This concern is additional to the two original reviews.

Before deleting fairness, define the actual scheduling objective:

- If the goal is approximate cross-operation mixing, assign a small bounded
  number of priority bands and preserve FIFO within a band, or use a queue
  partition/round-robin facility with a verified contract.
- If the goal is caller-controlled urgency, reserve priority for explicit
  service classes and use FIFO as the default.
- If deterministic random order is truly desired, document starvation as an
  accepted tradeoff and prove it with a workload simulation matching expected
  arrival/service rates.

There are also two concrete runtime gaps:

- `pyproject.toml:22` specifies `dbos>=2.25.0`; it is not pinned. The current
  environment has 2.26.0. Replace “the pinned DBOS version” with either an
  exact/compatible pin plus upgrade tests, or a declared supported range with
  contract tests.
- Database-backed `Queue` objects skip enqueue validation before checking
  `priority_enabled`; the kernel cannot rely on `SetEnqueueOptions` to reject
  a misconfigured queue. The current whetstone registration does not enable
  priority (`../whetstone-ai/src/whetstone/platform/queue_worker.py:41`). Make
  queue registration/configuration a cutover prerequisite and explicitly
  inspect the persisted queue configuration before enqueue.

**Source review:** Codex F3.

**Additional review:** digest priority can starve old work; DBOS is a version
floor rather than the pin assumed by the plan.

## 6. P1 — Make every DBOS workflow searchable from platform identity

Before serious experiments, every DBOS workflow must map directly back to the
Operation, Item, and attempt that created it. DBOS 2.26.0 supports
`SetWorkflowAttributes` at enqueue/start time and indexed filtering through
`DBOSClient.list_workflows(attributes=...)`. Use that public surface instead
of encoding all correlation into workflow-ID strings or querying DBOS tables
directly.

The kernel-owned attribute set should be small, stable, and domain-neutral:

- `operation_key`;
- `item_id` and, when distinct and useful, caller `item_key`;
- `attempt`;
- `workflow_role`; and
- `group_key` only if the schema crosswalk retains a stable meaning for it.

Do not duplicate DBOS-native queue name, application version, executor ID,
priority, or parent/fork fields. Provider/model/throttle identity belongs in
whetstone-added attributes, not the kernel, and must exclude secrets,
endpoints, prompts, outputs, and provider payloads. Attributes are searchable
mirrors for correlation, never a second source of truth; the platform tables
and attempt records remain authoritative.

Because attributes and attribute filters are part of the installed 2.26.0
surface, either prove them against the declared `dbos>=2.25.0` lower bound or
narrow the supported DBOS version before making this a contract.

**Additional review:** Conductor-inspired searchable workflow correlation,
validated against the installed DBOS public APIs.

## 7. P1 — Build a typed operation inspector with narrowly guarded control

The existing observability module can load operation snapshots, list recorded
workflow IDs, summarize statuses, and wait for completion
(`src/dr_platform/observability.py:41`, `:97`, `:115`, `:147`). That is a good
base, but it cannot yet answer the pre-experiment operator question: “what is
stuck, why, and what should I do?”

Build typed inspection/query functions in `dr-platform`, returning frozen
Pydantic models, and expose them through a thin whetstone Typer CLI with both
human-readable and JSON output. The initial commands should cover:

- operation list and show;
- item/attempt status and failure detail;
- DBOS workflow and step timelines;
- queue configuration and queued/active age;
- active throttle holds/backoff state; and
- a health summary using the checks in §8.

Use `DBOSClient` public APIs for workflows, steps, queues, and control. Do not
build a generic web console or read DBOS system tables as an application API.

The inspector should be read-only by default. One guarded `cancel operation`
action is worth including before expensive experiments, but only after §1
defines cancellation as a non-auto-retry platform state. Require explicit
confirmation, define whether child workflows are cancelled, update
application-owned state, and document that DBOS cancellation takes effect at
the next step boundary—an already-running provider call may still finish.

Do not expose raw “retry,” `resume_workflow`, or `fork_workflow`. Retry must
create a domain/platform attempt through the reconciliation contract so
identity and provenance remain correct.

**Additional review:** Conductor-inspired operation inspector, narrowed to the
application's Operation/Item/attempt model and DBOS public APIs.

## 8. P1 — Add optional OTLP tracing and on-demand health reporting

Trace correlation is most valuable during the first real experiments, when
workflow shape, throttling, and LM behavior are still being debugged. Add the
`dbos[otel]` dependency and optional configuration for `enable_otlp`, trace
endpoints, and `otel_attribute_format="semconv"`; the system must still run
normally when no exporter is configured.

Enrich workflow/step spans with the same safe correlation vocabulary used by
§6 plus whetstone-owned provider, model, token count, provider cost, and
throttle-delay values where those facts already exist. Do not emit prompts,
outputs, credentials, raw provider metadata, or other sensitive/high-volume
payloads. The application tables remain the durable source for usage/cost;
trace attributes are diagnostic correlation, not accounting storage.

Before adding a metrics backend or alert router, make the inspector compute a
machine-readable health report on demand:

- oldest queued and active workflow age;
- operations with no recent state progress;
- workflow failure/status counts and missing DBOS rows;
- retry/recovery exhaustion;
- active holds and throttle pressure;
- queue configuration, including priority enablement; and
- application-version mismatches across active work.

Persisted alert rules, worker-heartbeat infrastructure, and cost-anomaly
alerts should wait for real workload data to establish useful thresholds.
Cost anomaly analysis belongs primarily in the Analysis Store.

**Additional review:** Conductor-inspired trace and health visibility, reduced
to pre-experiment instrumentation hooks and derived checks.

## 9. P1 — Preserve the transactional seed seam and give caller metadata a real owner

Whetstone's `seed` hook is load-bearing: it writes experiments and prediction
specs in the registration transaction, and the generation workflow later
loads the prediction spec from those domain tables
(`../whetstone-ai/src/whetstone/platform/submission.py:71`, `:127`;
`../whetstone-ai/src/whetstone/platform/graph_workflow.py:128`). Replacing this
with only `EnqueueTarget` would enqueue workflows whose input rows do not
exist.

Keep a named registration hook in the single submission pipeline unless the
plan explicitly redesigns whetstone's workflow inputs and foreign keys. State
its transaction boundary, empty-submission behavior, idempotency contract,
and how its inserted/already-present results feed item accounting.

At the same time, resolve `enqueue_metadata`. Today the kernel owns lease and
workflow keys in it, while whetstone contributes `generation_run_id`
(`src/dr_platform/records.py:41`;
`../whetstone-ai/src/whetstone/platform/submission.py:154`). The new lease
columns remove the kernel's reason to use that JSON, but deleting
`EnqueueOutcome` also deletes the caller's write path.

Recommended split:

- dedicated typed columns for kernel claim/workflow/attempt state;
- caller metadata captured through an explicit item/target callback and never
  cleared by kernel retry transitions; or
- no metadata column at all if every required caller value is derivable from
  stable identity.

**Source reviews:** Fable F5, F6.

## 10. P1 — Write the schema and lifecycle crosswalk before the new baseline

Section 1.3 is not precise enough to implement as a clean baseline. It moves
`group_key` from operations to items, drops operation `metadata`, drops
`item_index`, changes physical table names, and removes ordering fields
without stating which changes are intentional. Those fields currently carry
resubmit validation and deterministic result ordering
(`src/dr_platform/db/schema.py:69`, `:109`).

Add an old-to-new crosswalk for every table column, constraint, index, record
field, protocol property, JSONL field, public return field, and exported name.
For each entry, mark **rename**, **move**, **delete**, or **new**, plus the
behavioral rationale. This is the specification for the new `0001`, not a
mechanical appendix.

The same pass should make lifecycle language truthful:

- Rename `COMPLETED` if it means only “enqueue completed,” or make operation
  status incorporate workflow execution outcomes.
- Define claim, lease, attempt, workflow execution, watermark/cursor, sink,
  and cancellation in `CONTEXT.md`.
- Make empty submission an explicit state transition rather than relying on
  `failed_count >= requested_count` when both are zero
  (`src/dr_platform/batch_status.py:106`). A distinct status is optional; an
  explicit branch and documented reason are not.
- Rename the generic `OperationProgress` helper if it survives, so it does not
  appear to be the progress model for the newly canonical Operation noun.

**Source reviews:** Fable F7, F12, F14.

## 11. P1 — Retain bounded registration and enqueue paging

Removing fairness ordering does not make chunking unnecessary. Today
`chunk_size` bounds registration transaction size, seed-hook batches, enqueue
pages, and result materialization (`src/dr_platform/submission.py:128`,
`:200`). The target system is explicitly for large sweeps, so an unbounded
in-memory `submit` transaction is a regression.

Keep bounded insert and enqueue paging as execution mechanics with no ordering
semantics. Prefer one explicit `SubmitOptions` field if operators need to tune
the bound. Also decide whether `SubmitResult` must materialize every item or
can return counts plus structured failures and stable lookup identifiers.

**Source review:** Fable F13.

## 12. P1 — Make kernel dependency and model-boundary rules internally consistent

The plan's “domain-agnostic kernel” principle conflicts with the current hard
dependency on `dr_providers.FailureClass` in records and backoff policy. Either
declare that enum a neutral shared transport taxonomy and move it to a neutral
package, or define a kernel failure enum and map provider failures at the
whetstone boundary. Leaving the exception undocumented makes a future
non-provider adopter pay for an LM-specific dependency.

The plan's model rule also conflicts with the active repository instruction to
prefer Pydantic `BaseModel` over dataclasses. The current
`ProjectionSpec` already proves that a frozen Pydantic model can carry a build
callable (`src/dr_platform/projections.py:57`). Keep `EnqueueTarget`,
`SubmitOptions`, and the reshaped projection contract as frozen Pydantic
models unless a concrete technical constraint requires otherwise. This is an
additional current-state correction to plan principle 4.

**Source review:** Fable F11.

**Additional review:** replace the proposed dataclass exceptions with the
repository's Pydantic convention.

## 13. P2 — Enumerate the remaining mechanical blast radius

After the decisions above, the plan still needs a concrete cutover checklist:

- Move or inject whetstone's clock before deleting
  `dr_platform.backoff.utc_now`; runtime and tests currently depend on it at
  `../whetstone-ai/src/whetstone/platform/graph_workflow.py:336`, `:416`, and
  `:434`.
- Verify that removing the pandas extra does not leave retained APIs or
  notebooks assuming pandas is installed. “Pandas comes free via DuckDB”
  should not be used as the dependency rationale; declare pandas directly
  wherever it remains a runtime requirement.
- Repoint observability from JSON metadata to typed workflow columns and make
  it consume the same normalized DBOS status policy as reconciliation.
- Add contract tests for status normalization, queue priority configuration,
  concurrent reconcile/resubmit, append-only attempt lineage, identity scope,
  searchable workflow attributes, inspector joins, guarded cancellation,
  optional OTLP configuration, bounded paging, export crash recovery, and
  sink-local cursor state.
- Run the planned Round 2 against the post-July-9 whetstone tree before
  producing implementation issues; Round 1 intentionally did not audit the
  full whetstone redesign.

**Source review:** Codex F5, plus follow-through from the combined findings.

## 14. P2 — Explicitly defer the operator features that need production evidence

The pre-experiment operator layer should remain deliberately smaller than
Conductor. Define these as deferred rather than allowing them to leak into the
initial implementation:

- **Export-aware retention:** retain all DBOS records initially. Measure
  growth, prove full-rebuild/incremental-export equivalence, and exercise sink
  crash recovery before introducing deletion. Later retention must preview
  deletions, use public DBOS APIs, and retain failures longer than successes.
- **Domain replay controls:** preserve source workflow, step, application
  version, status, and retry-reason provenance now in §1, but defer the replay
  command until attempt identity and idempotency are proven. Never make raw
  DBOS fork/resume the platform retry model.
- **Metrics alerts:** keep the derived health report from §8, but defer alert
  routing and thresholds until real runs provide a baseline.
- **Read-only MCP tools:** build these later as a thin adapter over the mature
  typed inspector. Do not create an independent query model.
- **Control-plane features:** do not clone Conductor's distributed recovery
  protocol, permissions/tenant system, or generic console, and do not mutate
  DBOS system tables directly.

These are sequencing boundaries, not rejections of future usefulness. Each
deferred feature should reuse the reconciliation, attempt, inspector, and
export contracts rather than inventing a parallel state model.

**Additional review:** explicit boundary from the Conductor comparison.

## Recommended plan-revision order

The findings are ranked above by failure severity. For revising the document,
the dependency order should be:

1. Decide execution identity scope, attempt ownership, cancellation policy,
   whether Operation tracks enqueue or execution lifecycle, and the
   application-owned attempt-history shape.
2. Specify the reconciliation state machine, append-only attempt provenance,
   and transaction/CAS invariants.
3. Decide throttle topology and the real scheduling objective; then finalize
   `EnqueueTarget` and queue configuration.
4. Produce the schema/lifecycle crosswalk, including attempt records, seed,
   metadata ownership, and searchable workflow attributes.
5. Specify the export cursor, snapshot, sink commit, projection, and sampling
   protocols.
6. Define the typed inspector, guarded cancellation, optional OTLP wiring, and
   derived health-report contracts.
7. Apply bounded paging, dependency-boundary, vocabulary, clock, and model
   corrections; record retention, replay controls, alerts, MCP, and a generic
   control plane as explicit non-goals for the pre-experiment cut.
8. Bump the spec version, append its revision log, update both glossaries, and
   only then write the Round 2 prompts.

Per the [effort process](../../README.md), steps 1, 3, the attempt-history shape in
step 2, and the sink-state choice in step 5 change or extend previously
selected design decisions. They should return to the owner as explicit
questions before the canonical plan is edited.

## Original-finding coverage

| Original finding | Unified section |
|---|---|
| Fable F1, Codex F1 | §1 reconciliation state machine |
| Fable F10, Codex F2 | §1 normalized DBOS status and cancellation policy |
| Fable F2, F8, F9; Codex F4 | §2 export consistency protocol |
| Fable F4 | §3 identity and deduplication scope |
| Fable F3 | §4 queue/throttle topology |
| Codex F3 | §5 priority and queue configuration |
| Fable F5, F6 | §9 seed and metadata ownership |
| Fable F7, F12, F14 | §10 schema and lifecycle crosswalk |
| Fable F13 | §11 bounded submission |
| Fable F11 | §12 dependency boundary |
| Codex F5 | §13 mechanical blast radius |

## Agreed pre-experiment operator additions

| Addition | Unified section |
|---|---|
| Append-only attempt lineage and replay provenance | §1 reconciliation state machine |
| Searchable DBOS workflow attributes | §6 workflow correlation |
| Typed operation inspector | §7 inspector and guarded control |
| Guarded operation cancellation | §7 inspector and guarded control |
| Optional OTLP trace correlation | §8 tracing and health reporting |
| On-demand machine-readable health report | §8 tracing and health reporting |
| Retention, replay controls, alerts, MCP, and control-plane deferrals | §14 explicit sequencing boundary |
