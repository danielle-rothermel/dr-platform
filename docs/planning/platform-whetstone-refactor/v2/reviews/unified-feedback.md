# V2 whole-system convergence — unified feedback

**Reviewed:** 2026-07-10

**Frozen plan:** [`../plan.md`](../plan.md) (v2)

**Inputs:** [Codex findings](codex-findings.md),
[Fable findings](fable-findings.md), the
[v1 unified feedback](../../v1/reviews/unified-feedback.md), both canonical
glossaries, ADRs 0001–0018, current code in `dr-platform`, `whetstone-ai`, and
`unitbench`, and the installed DBOS 2.26.0 package cited by both reviewers.

## Executive verdict

**Gate: `REPEAT_CONVERGENCE`.** This synthesis is deliberately
strict-inclusive: every evidence-backed finding from either reviewer is
retained even when the other reviewer did not identify it or classified the
same boundary as closed. Codex returned `REPEAT_CONVERGENCE` with three
blockers and five architecture-changing findings. Fable returned
`READY_FOR_FOCUSED_AUDITS` with eight bounded corrections. The disagreement is
substantive and must remain visible to the v3 planner rather than being erased
by majority or severity averaging.

V2 materially closes the five owner decisions and most v1 defects. Its new
contracts are much stronger, but five credible issues still change execution
identity, scheduling, cancellation, publication, or Experiment-acceptance
persistence. Implementing v2 literally can link changed content to stale paid
work, fail the mandatory shuffle guarantee on the named DBOS runtime, overlap
a replacement Attempt with an uncancelled provider call, expose mixed
cross-table snapshots, or leave Experiment acceptance without durable and
current semantics. Those are whole-system concerns, so focused audits are
premature.

The v2 plan remains frozen. Create v3 by copying it, preserving every accepted
v2 invariant, and resolving the P0 findings and disagreement decisions below.
V3 must receive another whole-system convergence review before focused audits.

Priority labels mean:

- **P0 — architecture or owner-decision blocker:** resolve before revising
  dependent sections or opening implementation work.
- **P1 — correctness contract:** encode explicitly in v3 before
  implementation; it does not independently reopen an accepted owner boundary.
- **P2 — verification boundary:** keep as a named implementation gate; current
  evidence cannot close it statically.

## What both reviews agree is preserved

Do not reopen these choices merely because v3 is required:

- dr-platform remains the sole Attempt-ordinal authority; domain and operator
  eligibility request later Attempts through one platform transition.
- Operation membership is caller-prepared and Manifest-backed; the platform
  does not add durable input spooling.
- Managed DBOS executions remain top-level-only and cancellation remains
  non-recursive.
- Every destination/artifact retains destination-local leasing and fencing,
  including the local DuckDB OS lock.
- Experiment completeness remains strict by default, with only explicit,
  persisted, stratified, operator-confirmed partial override.
- Operation-row serialization, execution-scoped DBOS attributes, secret-free
  workflow arguments/safe reads, and kernel-owned Export Barrier lock
  acquisition remain accepted.
- Operational Postgres remains durable truth; the two-plane Analysis/Detail
  direction, fresh-schema cut, content-scoped execution identity, append-only
  provenance, deterministic shuffle objective, and destination-local state
  remain accepted architecture.

## P0 findings — resolve in this order

### 1. Bind content identity, Manifest equality, and the execution recipe

V2 defines a Manifest over Item membership but leaves “target identity”
undefined and excludes `EnqueueTarget` callables from serialized state. Current
Whetstone `prediction_id` also omits the complete persisted task/input snapshot,
while RegistrationHook conflict handling may accept an existing domain row
without exact equality. The same Manifest or content-scoped workflow identity
can therefore be reused with changed workflow/argument/domain content and link
to stale DBOS work.

**V3 requirement:** define one versioned `execution_recipe_digest` covering
every immutable input that affects execution: complete domain input snapshot,
workflow implementation/name/version, argument-recipe version, relevant
profile/parser/dataset versions, and other stable target identity. Persist it
with the Operation and Attempts; include it in exact resubmission equality and
content-scoped execution/workflow identity. Require RegistrationHook
`ALREADY_PRESENT` to prove exact canonical domain-row equality. Decide
explicitly whether Whetstone's Prediction ID itself expands to include the
complete content or whether the separate recipe digest supplies that boundary.

**Disagreement:** Codex treats this as an architecture blocker and marks v1
P0-2 still open. Fable considers the Manifest a valid closed membership
authority and did not identify recipe binding. **Synthesis opinion:** Codex is
correct that membership identity and execution identity are separate contracts;
the Manifest mechanics should remain, but v3 must bind them to a durable recipe
before content-scoped deduplication is safe.

**Source:** Codex F1.

### 2. Replace the impossible DBOS 2.26.0 equal-time ordering assumption

V2 requires deterministic same-Service-Class ordering as a blocking safety
property. Installed DBOS 2.26.0 stores millisecond `created_at` values and
dequeues only by `(priority, created_at)`, with no stable third key. Equal-time
rows—and especially multiple dequeuers using `SKIP LOCKED`—therefore have no
deterministic order. This is not merely an unrun live gate; the inspected
runtime lacks the ordering contract v2 requires.

**V3 requirement:** choose an implementable mechanism: pin a DBOS revision
whose dequeue contract includes a stable third key; contribute/vendor the
required DBOS change; or select another queue/scheduling representation that
durably carries `shuffle_rank` without recreating starvation. The contract test
must force identical millisecond timestamps and multiple dequeuers. Do not
weaken the accepted shuffle safety objective to fit the current runtime.

**Disagreement:** Codex marks P2-12 open and the scheduling contract
architecture-changing. Fable treats exact ordering as a correctly preserved
phase-1 gate and leaves it unverified. **Synthesis opinion:** Codex's installed
implementation evidence converts this from an unknown into a known failing
contract; v3 must choose a remedy rather than defer discovery to phase 1.

**Source:** Codex F5.

### 3. Define provider-call quiescence rather than promising physical stop

V2 and gate 4 require cancellation to prove physical stop, but DBOS 2.26.0
cancellation primarily changes workflow state. Whetstone's paid provider
boundary is a synchronous DBOS step, so cancelling the workflow cannot preempt
the in-flight provider call. A confirmed replacement Attempt can overlap that
call and duplicate paid work.

**V3 owner decision:** choose one of two honest contracts:

1. make provider adapters genuinely async/cancellable with a proved
   adapter-level abort contract; or
2. define DBOS cancellation as logical for synchronous paid steps and prohibit
   a replacement Attempt until the prior call reaches a durable observed
   quiescent outcome.

The second is the smaller recommendation unless every supported provider can
prove abort semantics. Gate 4 must inject cancellation during a provider call
and prove no overlapping paid request; observing DBOS `CANCELLED` is
insufficient.

**Disagreement:** Codex marks v1 P0-4 open because physical cost control is
infeasible. Fable considers the reference/topology protocol closed and treats
cancellation gaps as local state-machine corrections. **Synthesis opinion:**
Fable is right that descendant/reference safety is closed, but Codex is right
that v2 overclaims physical cancellation. Preserve the topology design and add
a separate paid-call quiescence contract.

**Source:** Codex F3; related Fable F5.

### 4. Publish consumer-visible table bundles, not independent referential tables

Per-artifact fencing prevents H1 from overwriting H2 for one table, but v2 can
still expose mutually referential tables from different snapshots. Unitbench
already performs cross-table reads. Independent promotion can expose
Predictions at H2 with metrics/details at H1, or advance a root manifest
without all root-cascaded detail rows.

**V3 requirement:** define explicit publication bundles. Build and validate
all mutually referential members for one snapshot, then atomically advance one
bundle manifest/pointer. If physical replacement cannot share one transaction,
retain versioned tables and switch a single reader-visible pointer/view after
all members are ready. Define bundle-token cleanup and retry ownership.
Within the kernel bundle, commit all kernel tables and cursor bookkeeping in
one destination transaction. Between intentionally independent families
(kernel telemetry versus Whetstone projections), state the allowed
`snapshot_seq` skew and require readers either to tolerate or check it.

**Disagreement:** Codex calls for plane-level consumer-visible bundles and
marks P1-9 incomplete. Fable identifies the kernel atomicity ambiguity but
treats cross-family skew as a detectable/tolerable reader concern and marks
P0-3/P1-9 closed. **Synthesis opinion:** use the narrowest correct blend:
atomic bundles for tables whose joins/root closure are promised, one
transaction for kernel-table deltas, and explicit skew semantics rather than
one universal snapshot across unrelated artifact families.

**Sources:** Codex F2; Fable F7.

### 5. Give Experiment acceptance a durable schema, source cut, and current-pointer rule

V2 names a persisted `ExperimentAcceptanceResult` and partial-policy fields but
does not define its table/model identity, foreign keys, append-only transaction,
selected domain rows, snapshot cut, or invalidation/current-pointer semantics.
After a later Attempt, an earlier “complete” result can remain visible without
a reproducible relationship to the current expected set and outcomes.

**V3 requirement:** specify append-only Whetstone acceptance records keyed to
the exact generation/scoring Manifest digests and a domain snapshot/version.
Persist selected Generation Run and Score Attempt IDs, required profile set,
observed matrix, policy version, override/operator facts, and source
Operation/Attempt cuts. Advance one current-acceptance pointer atomically and
define how later Attempts make prior evaluations historical rather than
current. Include this table/model/transaction in the Whetstone schema and
cutover crosswalk.

**Disagreement:** Codex marks P0-5 open and the missing persistence model
architecture-changing. Fable finds the strict/override policy coherent and
closed. **Synthesis opinion:** the owner policy is closed, as Fable says, but
Codex is correct that an implementation-ready plan needs the durable
enforcement shape; this is a persistence change and therefore requires v3
whole-system review.

**Source:** Codex F4.

## P1 findings — encode after the P0 decisions

### 6. Make successive scoring selections distinct Operations

The default Scoring Operation key does not include the frozen candidate
selection. After regeneration creates a new Generation Run, a second scoring
selection derives the same Operation key with a different Manifest and must
hard-conflict. Strict acceptance then cannot score late successful runs through
the documented default flow.

**V3 requirement:** include candidate-selection digest or explicit selection
sequence in the scoring Operation key. State that one Experiment may have
multiple Scoring Operations and derives acceptance across their domain rows.
Add regenerate-then-score-late-runs to gate 3.

**Source:** Fable F2.

### 7. Define the foreign-cancelled shared-execution observation

A new Operation may link a content-scoped DBOS workflow already cancelled by
another Operation. V2 has no local transition for observing DBOS `CANCELLED`
without local cancellation intent, and the closed next-Attempt reason requires
cancellation provenance the new Operation did not create.

**V3 requirement:** transition such an Attempt to local sticky `CANCELLED` with
foreign cancellation provenance, and explicitly allow the confirmed
`OPERATOR_CANCEL_RETRY` request to cite another Operation's recorded
`cancellation_request_id`. Test cancel-then-resubmit across Operations.

**Source:** Fable F1.

### 8. Complete the supposedly total Operation-status function

`ENQUEUED` or `WORKFLOW_ALREADY_PRESENT` with execution
`NOT_STARTED` is the routine post-enqueue/pre-observation state, but it matches
none of v2's precedence clauses. The “total” function therefore fails on the
happy path.

**V3 requirement:** choose one tier explicitly—recommended `RUNNING` once the
current Attempt is confirmed with DBOS—and enumerate confirmed-enqueued/
`NOT_STARTED`, permanent enqueue error, and mixed combinations in the pure
table-driven tests.

**Disagreement:** Codex marks v1 P1-8 closed; Fable marks it open. **Synthesis
opinion:** Fable's state walk is decisive. This is a local correction, not an
owner decision, but v3 must reopen P1-8 in its closure table.

**Source:** Fable F3.

### 9. Preserve a lifecycle wait and name COPRO's Analysis Store refresh/read contract

V2 deletes Whetstone analysis modules and omits a wait primitive from the new
platform seam, while retained COPRO and the e2e smoke import both. Export is
never automatic, so “repoint minimally” does not define how an optimizer waits
for execution and reads fresh candidate results without querying operational
Postgres.

**V3 requirement:** retain/add a typed full-lifecycle wait built on
reconcile/inspection. Define COPRO's explicit export of required Whetstone
projections and pinned Analysis Store snapshot read between iterations, or
another owner-approved domain-result read consistent with the two-plane rule.
Add COPRO and zero-spend e2e smoke to phase exits and enumerate helper/config
migrations.

**Source:** Codex F6.

### 10. Add an operator terminal transition for abandoned partial Registration

A non-empty Operation abandoned after some Manifest pages commit can remain
`REGISTERING` forever: normal cancellation requires registration completion,
hard deletion is prohibited, and resume requires the original source.

**V3 requirement:** after Registration Lease expiry, allow a confirmed
operator transition to `FAILED/registration_abandoned` or equivalent sticky
cancellation. Specify treatment of committed Items, Attempt-0 rows, and domain
hook rows, and add it to the registrar crash matrix.

**Source:** Fable F4.

### 11. Resolve late DBOS terminal results after cancellation intent

If a workflow reaches `SUCCESS` or `ERROR` before physical cancellation,
DBOS will not overwrite it, but v2 does not say whether the local Attempt
honors the observed result or sticky cancel intent. The choice changes
Operation status and later-Attempt eligibility.

**V3 requirement:** record the observed terminal state and cancellation
disposition separately. Recommended: preserve `SUCCEEDED`/`ERROR` when DBOS
terminality wins before cancellation; use `CANCELLED` only when logical
cancellation has no accepted terminal result under the selected quiescence
contract. Pin every race in tests and reconcile it with P0-3.

**Source:** Fable F5.

### 12. Expose requested versus effective priority for shared executions

A later `URGENT` Item that links an already-enqueued STANDARD content-scoped
workflow cannot change DBOS priority, yet v2 can label the local Item urgent
without showing that the execution remains standard.

**V3 requirement:** state that a linked reference inherits enqueue-time
effective priority. Persist/inspect both requested Service Class and effective
execution priority; surface mismatches in inspection/health. Live priority
mutation remains out of scope unless separately designed.

**Source:** Fable F6.

### 13. Define the request ledger's `max_attempts` semantics

The next-Attempt request row persists `max_attempts`, but creation checks the
Operation's immutable `RetryPolicy.max_attempts`; v2 does not say whether the
request value tightens, echoes, or conflicts with policy.

**V3 requirement:** choose one rule. Recommendation: make the request field an
optional tightening bound and require
`source + 1 < min(policy.max_attempts, request.max_attempts)`. Alternatively
remove it as an input and persist an exact policy echo validated for equality.
Align idempotency equality and database checks.

**Source:** Fable F8.

## Reviewer disagreements requiring owner visibility

The v3 planner must surface these differences rather than silently choosing a
reviewer. “Synthesis opinion” is advisory; the owner decides where a genuine
trade-off remains.

| Topic | Codex position | Fable position | Synthesis opinion | V3 owner surface |
| --- | --- | --- | --- | --- |
| Overall gate | Repeat convergence; five architecture-changing issues remain. | Ready for focused audits; findings are bounded completions. | Repeat convergence: credible identity, scheduling, cancellation, publication, and persistence changes satisfy the effort's successor criteria. | Confirm v3 successor and another convergence pass. |
| Manifest closure | P0-2 remains open because membership is not bound to execution recipe/domain-row equality. | P0-2 is closed; Manifest is a genuine immutable membership authority. | Preserve Manifest mechanics, add a separate durable `execution_recipe_digest` and exact domain equality. | Decide whether full task/input content enters Prediction ID or a separate recipe digest. |
| Publication scope | Consumer-visible plane needs a fenced bundle/pointer. | Per-artifact fence is closed; specify kernel transaction and tolerate/check cross-family skew. | Bundle promised referential sets; keep unrelated families independent with explicit skew rules. | Approve bundle boundaries and reader skew policy. |
| Physical cancellation | P0-4 remains open because synchronous provider calls cannot be physically stopped and may overlap retries. | Topology/reference P0-4 is closed; remaining cancellation states are local. | Topology is closed, but paid-call quiescence is a separate architecture decision. | Choose async abort support versus logical cancellation plus quiescence fence. |
| Experiment acceptance | P0-5 remains open without append-only schema/source cut/current pointer. | Policy and canonical decision are closed. | Policy is closed; persistence enforcement is not implementation-ready and changes schema. | Approve acceptance record/current-pointer model. |
| DBOS ordering | Installed 2.26.0 disproves deterministic equal-time ordering; architecture must change. | Ordering is correctly retained as a phase-1 gate and remains unverified. | Treat it as known failing evidence, not an unknown. | Choose patched/new DBOS contract versus another scheduler/queue representation. |
| Status precedence | P1-8 closed. | P1-8 open because confirmed-enqueued/`NOT_STARTED` matches no clause. | Fable is correct; add the missing happy-path clause. | No product choice required; planner should apply and call out the reviewer split. |

## V1 correction disposition

Strict-inclusive disposition of the twelve v1 items:

| V1 item | Unified disposition for v3 |
| --- | --- |
| P0-1 next Attempt | Core authority/ledger/CAS design retained; extend foreign-cancel and quiescence scenarios (P0-3, P1-7, P1-11, P1-13). |
| P0-2 Manifest | Membership mechanics retained; reopen execution-recipe/domain-equality binding (P0-1). |
| P0-3 destination fencing | Per-artifact H1/H2 fence retained; add consumer-visible bundle atomicity (P0-4). |
| P0-4 cancellation topology | Top-level/reference design retained; add paid-call quiescence and complete local outcomes (P0-3, P1-7, P1-11). |
| P0-5 Experiment acceptance | Strict/override policy retained; add durable acceptance schema/current pointer and multi-scoring-Operation flow (P0-5, P1-6). |
| P1-6 Operation serialization | Closed; preserve. |
| P1-7 execution-scoped attributes | Closed; preserve. |
| P1-8 total status precedence | Reopened by the confirmed-enqueued happy-path gap (P1-8). |
| P1-9 detail Attempts snapshot | Snapshot construction retained; bundle/root publication must make it reader-visible atomically (P0-4). |
| P1-10 secret-free payloads/safe reads | Closed; preserve. |
| P1-11 kernel-owned writer lock | Closed; preserve. |
| P2-12 live/version/order/rescore gates | Preserve live MotherDuck/Neon/Vercel/rescore gates; promote DBOS equal-time ordering to P0-2 because installed evidence already disproves it. |

## P2 verification boundaries

Retain these as explicit gates; neither review could close them from the
current trees:

- live MotherDuck conditional Lease/fenced bundle promotion and DuckDB-SQL
  parity through its deployed endpoint;
- live Neon transactional Lease/bundle behavior under pooling;
- local DuckDB OS-lock and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret wiring;
- OTLP initialization/degradation and safe attributes;
- replacement Whetstone rescore-selection parity; and
- COPRO/e2e behavior against the new wait/export/read contracts.

DBOS same-millisecond ordering is not in this list: installed-package evidence
already shows the required deterministic tie-break is absent.

## Source-to-priority map

| Unified item | Codex | Fable | Treatment |
| --- | --- | --- | --- |
| P0-1 execution recipe binding | F1 | — | Retain Codex blocker; explicitly record Manifest-closure disagreement. |
| P0-2 deterministic DBOS ordering | F5 | Unverified/P2 closure | Elevate installed evidence over deferred gate. |
| P0-3 cancellation/quiescence | F3 | F5 adjacent; P0-4 closed | Preserve reference topology, add separate paid-call contract. |
| P0-4 publication bundles | F2 | F7 | Merge; use bundle boundaries plus explicit cross-family skew. |
| P0-5 acceptance persistence | F4 | P0-5 closed | Retain policy, add durable enforcement shape. |
| P1-6 repeated scoring selections | — | F2 | Retain. |
| P1-7 foreign-cancelled reference | — | F1 | Retain. |
| P1-8 total status happy path | P1-8 closed | F3 | Retain Fable correction and record disagreement. |
| P1-9 COPRO/e2e contracts | F6 | — | Retain. |
| P1-10 abandoned Registration | — | F4 | Retain. |
| P1-11 late terminal after cancel | F3 adjacent | F5 | Retain and reconcile with quiescence decision. |
| P1-12 shared execution priority | — | F6 | Retain. |
| P1-13 request maximum bound | — | F8 | Retain. |

## Final disposition

- **V2 status:** reviewed and frozen.
- **Unified gate:** `REPEAT_CONVERGENCE`.
- **Next artifact:** `v3/plan.md`, copied from v2 before revision.
- **Owner-visible decisions for v3:** execution-recipe identity boundary;
  publication bundle boundaries/reader skew; paid-call cancellation versus
  quiescence; acceptance persistence/current pointer; and DBOS scheduling
  remedy.
- **Required review after v3:** another independent whole-system convergence
  pass using the same Codex code/dependency and Fable architecture/domain
  lenses.
- **Implementation:** remains blocked until v3 resolves P0 and passes its
  convergence gate.
