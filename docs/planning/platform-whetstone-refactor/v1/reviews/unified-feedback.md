# V1 whole-system convergence — unified feedback

**Reviewed:** 2026-07-10

**Frozen plan:** [`../plan.md`](../plan.md) (v1)

**Inputs:** [Codex findings](codex-findings.md),
[Fable findings](fable-findings.md), the [v0 unified feedback](../../v0/reviews/unified-feedback.md),
the canonical glossaries and ADRs linked by the plan, and the current code and
DBOS 2.26.0 evidence cited by both reviewers.

## Executive verdict

**Gate: `REPEAT_CONVERGENCE`.** Both reviewers independently reached this
verdict. V1 is materially stronger than v0 and retains a coherent hard-cut
direction, but it is not implementation-ready. Its largest contradiction is
that the platform exclusively owns the only Attempt ordinal while Whetstone's
dominant domain failures finish as successful DBOS executions. The resulting
system cannot regenerate or rescore the failed domain outcome. The same
missing next-Attempt action makes sticky cancellation irreversible for a
content-scoped experiment.

Three other correctness boundaries remain underspecified: concurrent
submitters can create an Operation from an interleaved or truncated input set;
concurrent exporters can promote an older snapshot after a newer one; and
recursive DBOS cancellation can destroy descendant work whose references were
never checked. Implementing v1 literally can therefore suppress valid paid
work, create invalid Operation membership, regress published data, or cancel
shared work.

The v1 plan must remain frozen. Create v2 by copying it and resolving the
findings below in order. The required changes alter Attempt transitions,
persistence/concurrency contracts, cancellation topology, and cross-repository
interfaces, so v2 must receive another whole-system convergence review before
focused audits begin.

Priority labels mean:

- **P0 — decision or architecture blocker:** settle before revising dependent
  sections or opening implementation work.
- **P1 — correctness contract:** encode explicitly before implementation.
- **P2 — verification boundary:** preserve as a named acceptance obligation;
  current evidence cannot close it from static review.

## P0 findings — resolve in this order

### 1. Add one platform-owned, caller-requested next-Attempt transition

V1 permits the platform to advance the Attempt ordinal only after a retryable
DBOS execution failure. Whetstone intentionally catches provider/node and
scoring-harness failures, persists a Generation Run or Score Attempt domain
outcome, and returns normally. DBOS and the platform therefore record
`SUCCESS`, which blocks replacement. Meanwhile v1 deletes Whetstone's
caller-chosen generation and scoring attempt indexes. A domain-failed
generation or harness-failed score can never run again.

Sticky cancellation has the same shape. Ordinary resubmission must not replace
a cancelled execution, but the only action that could deliberately create a
later Attempt is deferred. Cancelling content-scoped work therefore freezes
that experiment identity permanently.

**Recommendation:** retain one platform-owned Attempt lineage; do not restore
a second Whetstone counter and do not misclassify domain outcomes as platform
failures. Add one policy-gated `request_next_attempt`-style transition. The
caller owns eligibility and supplies a typed neutral reason such as
`domain_outcome` or `operator_cancel_retry`; the platform owns ordinal
allocation, CAS, idempotency, provenance, maximum-attempt policy, and workflow
identity. The action must require a terminal current Attempt, a stable request
key, and an explicit policy permitting that source state. Whetstone maps the
new platform ordinal one-to-one to its Generation Run or Score Attempt index.

Specify separately:

- eligible source states and reasons;
- the exact row/CAS predicate and concurrent-request behavior;
- request idempotency and maximum-attempt exhaustion;
- Operation status changes while a requested Attempt is active;
- cancellation interaction and operator confirmation;
- Generation Run and Score Attempt identity recipes; and
- gates proving regeneration after a persisted domain failure, rescoring after
  a harness failure, and an explicit retry after sticky cancellation.

This preserves the previously accepted ownership split: Whetstone never asks
the kernel to interpret a domain outcome, and the kernel remains the only
authority that creates an Attempt.

**Sources:** Codex F1; Fable F1–F2. This is the highest-priority finding
because it breaks the flagship generation and scoring paths under expected,
not exceptional, failures.

### 2. Give registration an immutable manifest and one durable authority

The 500-Item page bound constrains transaction size but does not define one
Operation's complete input set. Two submitters can register different page
orders or truncated sources under the same Operation key, interleave domain
hook writes, silently lose rows to index conflicts, and allow enqueue to begin
before either caller has proven completion. A crashed caller has no durable
source cursor or digest with which to resume safely.

**Recommendation:** establish an immutable registration manifest before any
domain hook or Item write. It must identify the ordered item set with at least
a total count, canonical digest, and stable page boundaries. One registrar
claims the Operation under a durable token/lease, advances a persisted cursor
under CAS, and commits `registration_completed_at` only after every manifest
page and its transactional hook succeed. Enqueue and reconciliation require
that committed completion marker. Every resubmission must prove exact manifest
equality before linking to the Operation.

V2 must choose whether callers materialize the manifest before `submit` or the
platform spools a bounded source into a durable manifest. It must not leave an
unbounded iterator as the authority while transactions are already committing.
Add crash-between-pages, competing-registrar, reordered-input, truncated-input,
hook-conflict, lease-expiry, and exact-resume tests.

**Source:** Codex F3. Ranked second because it can corrupt the authoritative
Operation membership before any workflow begins.

### 3. Fence destination promotion, not only source extraction

The short source Export Barrier creates a stable extraction point, but it is
released before destination synchronization and promotion. Two export runs can
capture H1 then H2, promote H2, and later let the older H1 replace tables or
cursor metadata. A local DuckDB file also does not support arbitrary
multi-process writers.

**Recommendation:** add a destination-local, per-artifact single-writer lease
with a monotonic fencing token held through staging promotion and cursor
commit. Promotion must compare the candidate snapshot/cursor with the current
committed value and reject stale writers. Define lease acquisition, expiry,
renewal, stale-stage cleanup, crash recovery, and full-rebuild ordering for
local DuckDB, MotherDuck, and Neon. Local DuckDB may use an OS/process lock,
but its metadata must still prevent stale promotion; remote stores need a
transactional lease/fence row. Keep the source barrier short.

Tests must deterministically force A(H1), B(H2), B-promotes, A-promotes and
prove that H2 remains visible for every artifact mode and destination.

**Source:** Codex F2. Ranked third because it can silently regress published
analysis/detail data, but requires overlapping exporters rather than ordinary
single-submitter execution.

### 4. Make reference-aware cancellation safe across the complete workflow tree

V1 checks references only for directly referenced workflow IDs and then calls
DBOS recursive cancellation. DBOS 2.26 recursively cancels descendants without
an application reference predicate. A parent may be exclusive while a
content-scoped descendant remains shared by another live Operation.

**Recommendation for the pre-experiment cut:** prohibit shared or
content-addressed DBOS child workflows beneath a platform-managed execution,
enforce that invariant at workflow registration, and cancel only directly
referenced workflows with `cancel_children=False`. The current generation and
scoring implementations are top-level workflows composed from steps rather
than DBOS child workflows (`whetstone/platform/graph_workflow.py:128-203` and
`whetstone/platform/scoring_workflow.py:76-137`), so this is smaller and more
auditable than inventing a race-free graph-cancellation protocol. If child
workflows are required, each child must become a platform-known
execution with references locked and checked individually; do not delegate the
decision to DBOS's recursive cascade.

Specify cancellation's lock order, reference snapshot, race with a newly
created reference, partial physical-cancel failure, repeated request, and
relationship to the next-Attempt action in P0-1. Preserve sticky cancellation:
only an explicit policy-gated request may create later work.

**Sources:** Codex F6; Fable F2. This is an owner-visible safety decision, not
a mechanical change to `cancel_children`.

### 5. Define when an Experiment is valid enough to call complete

V1 says linked generation and scoring Operations must reach “defined terminal
acceptance states” but never defines them. Platform success is deliberately
separate from domain outcome, so `SUCCEEDED` at the DBOS/Operation layer cannot
prove that enough Generation Runs or Score Attempts succeeded. A model- or
provider-correlated partial result can otherwise be labeled a complete
experiment.

**Recommendation:** default to strict completeness: every expected Prediction
must have a domain-successful accepted Generation Run and every required
scoring profile must have an accepted Score Attempt. Any incomplete result is
`PARTIAL`, never silently complete. If partial experiments are useful, expose
an explicit policy containing minimum ratios and per-stratum requirements,
record the policy and observed counts with the Experiment, and require an
operator-confirmed override. A global percentage alone is insufficient
because failures can be model-correlated.

Gate 3 must prove both strict success and a deliberately biased failure case,
including next-Attempt recovery from P0-1. The experiment-facing command must
report platform status and domain acceptance separately.

**Source:** Fable F6. The source review labeled this minor; this synthesis
elevates it to P0 because it is an unresolved product acceptance decision and
controls whether intensive experiment results are trustworthy.

## P1 findings — encode after the P0 decisions

### 6. Serialize Item/Attempt mutation and aggregate recomputation per Operation

The export barrier's shared advisory writer lock does not serialize writers
with each other. Concurrent terminal Item transitions can each recompute from
a snapshot that omits the other and leave stored Operation counts/status
permanently stale.

Require every Item/Attempt mutation transaction to acquire the Operation row
with `SELECT ... FOR UPDATE` before mutation and recomputation. Define a fixed
Operation-key lock order for any multi-Operation transaction. Keep the export
barrier as a separate lock with a separate purpose. Add a production-isolation
test where the last two Items finish concurrently and no later inspector or
reconcile call repairs the result.

**Sources:** Codex F4; Fable F3.

### 7. Make DBOS attributes execution-scoped, not Operation-scoped

A content-scoped DBOS workflow may be referenced by multiple Operations, but
DBOS attributes are created on the workflow and later updates replace the
attribute object. Scalar `operation_key`/`item_id` attributes cannot represent
every reference without losing one Operation or racing updates.

Keep DBOS attributes limited to immutable execution identity, role, and safe
content labels. Resolve Operation → Attempt → workflow through authoritative
platform rows, then query DBOS by workflow ID. Remove the promise that every
Operation is directly searchable through DBOS attributes. Platform reference
rows, not DBOS attributes, are the searchable many-to-one correlation source.

**Source:** Codex F5.

### 8. Give the pure Operation-status function a total precedence order

Large Operations legitimately contain registering, pending/claiming, active,
and cancelling work at once. V1's status clauses overlap but are not ordered,
so conforming implementations can disagree.

Specify and test a total precedence, including all overlapping and terminal
combinations. A reasonable starting order is `REGISTERING > CANCELLING >
ENQUEUEING > RUNNING > terminal derivation`, but v2 must reconcile this with
the next-Attempt transition and Experiment acceptance contract.

**Source:** Fable F4.

### 9. Build detail platform Attempts inside the Whetstone snapshot

`detail_platform_attempts` is promised root-cascade and snapshot completeness,
but kernel Attempt deltas carry neither Prediction roots nor a Whetstone
snapshot ID.

Build this detail table as part of the same Whetstone full-projection snapshot:
join platform Attempts to Prediction roots at snapshot time and stamp the same
snapshot ID. Do not populate it independently from the incremental kernel
artifact while claiming root-cascade completeness.

**Source:** Fable F5.

### 10. Remove secrets from DBOS replay payloads and make safe reads explicit

Generation and scoring currently pass `database_url` as a workflow argument,
so the credential remains durably serialized even if export excludes DBOS
input/output columns. DBOS client listing defaults to loading those payloads.

Require Whetstone workflows to resolve credentials from process configuration
inside the execution boundary; platform-enqueued args contain no secrets. The
kernel's normal DBOS adapter always uses `load_input=False` and
`load_output=False`. Any payload-debug surface must be separately named,
locally guarded, redacted, and absent from normal inspector JSON.

**Source:** Fable F7.

### 11. Put export-barrier lock acquisition inside every owning write function

V1 says every `change_seq` writer takes the shared barrier lock but does not
assign acquisition responsibility. Throttle updates occur through Whetstone
workflow paths and are especially likely to bypass a caller-owned convention.

Every kernel function that mutates a `change_seq`-bearing table must acquire
the effort-specific shared advisory transaction lock internally. Callers do
not opt in. Add a barrier test with a throttle write issued from inside a
workflow step, plus a static stale-write search for direct table mutation.

**Source:** Fable F8.

## P2 verification boundaries

### 12. Preserve live integration unknowns as explicit gates

The reviews could not exercise live MotherDuck/Neon promotion or query parity,
Vercel secret/runtime wiring, or the not-yet-written adapters and schemas.
DBOS same-instant FIFO tie behavior and current rescore-candidate SQL semantics
were not fully exercised. These are not reasons to weaken the contracts above;
they are named implementation gates.

V2 must retain opt-in live adapter parity/promotion tests, exact supported DBOS
version tests, a deterministic same-band ordering test, and Whetstone fixtures
covering existing rescore selection before deleting the old flow.

**Sources:** both verdicts' `Unverified` sections.

## Cross-review synthesis and additional conclusions

The reviews agree on the central retry defect, aggregate race, and convergence
verdict. Their v0-coverage wording differs but their evidence does not:
Fable found every v0 topic represented in v1, while Codex showed that several
represented contracts remain unsatisfied in the new design. Treat these as v1
correctness defects, not evidence that v0 text should be copied back.

The following distinctions should be explicit throughout v2:

1. **Attempt creation authority versus Attempt eligibility.** The platform
   alone creates the next Attempt; retry policy, Whetstone domain eligibility,
   and an operator request are different reasons that may ask it to do so.
2. **Execution terminality versus Experiment acceptance.** A DBOS workflow and
   platform Attempt may succeed while the domain result is unacceptable.
3. **Three independent lock scopes.** The export barrier protects a source
   cut; the Operation row lock serializes membership/state aggregation; the
   destination lease/fence serializes publication. None substitutes for
   another.
4. **Execution identity versus reference identity.** DBOS owns one workflow
   execution; platform tables own the many Operations that reference it.
   Attributes may mirror the execution but cannot encode a mutable reference
   set safely.
5. **Bounded transactions versus bounded input identity.** A 500-row page size
   controls work per transaction; only an immutable manifest defines the
   Operation's complete input.

These are synthesis conclusions rather than additional independent defects.
Encoding them as named invariants will prevent v2 from fixing one finding in a
way that reintroduces another.

## Decisions the v2 author must obtain from the owner

Ask one focused question at a time, with a recommendation and consequences,
before freezing v2:

1. Confirm the single platform-owned `request_next_attempt` seam rather than a
   second Whetstone attempt counter or re-raised domain failures.
2. Choose pre-materialized caller manifests versus platform durable spooling
   for Operation registration; recommend caller manifests for the cleanest
   contract if Whetstone can enumerate its set before submission.
3. Confirm the pre-experiment prohibition on DBOS child workflows and
   non-recursive cancellation; otherwise design reference locking for the
   complete descendant graph.
4. Confirm destination-local lease/fencing for every sink, including a local
   process/OS lock for DuckDB, rather than assuming export invocations never
   overlap.
5. Confirm strict Experiment completeness by default and define any explicit
   partial-acceptance policy or override.

Do not modify canonical ADRs or glossaries until each corresponding decision
is resolved. Then update them once and link them from v2; never patch frozen
v1.

## Source-to-priority map

| Unified item | Codex | Fable | Synthesis treatment |
| --- | --- | --- | --- |
| P0-1 next Attempt | F1 | F1, F2 | Merge domain retry and cancelled retry into one platform transition. |
| P0-2 registration manifest | F3 | — | Retain as an independent submission-integrity blocker. |
| P0-3 destination fencing | F2 | — | Retain as an independent publication-consistency blocker. |
| P0-4 cancellation tree | F6 | F2 | Separate physical cancellation safety from later-Attempt policy. |
| P0-5 Experiment acceptance | — | F6 | Elevate because it determines experiment validity. |
| P1-6 aggregate serialization | F4 | F3 | Deduplicate into one Operation-lock contract. |
| P1-7 execution attributes | F5 | — | Replace Operation attributes with authoritative reference lookup. |
| P1-8 status precedence | — | F4 | Retain. |
| P1-9 detail Attempt snapshot | — | F5 | Retain. |
| P1-10 secret-free replay payloads | — | F7 | Retain and strengthen the normal read-adapter default. |
| P1-11 shared writer-lock ownership | — | F8 | Retain. |
| P2-12 live verification | verdict | verdict | Merge unverified integration boundaries. |

## Final disposition

- **V1 status:** reviewed and frozen.
- **Next artifact:** `v2/plan.md`, copied from v1 before any revision.
- **Required review after v2:** another independent whole-system convergence
  pass using the same Codex code/dependency and Fable architecture/domain
  lenses.
- **No implementation work should begin** until the P0 decisions are resolved,
  v2 is internally consistent, and its convergence gate permits focused audit.
