# V3 whole-system convergence — unified feedback

**Reviewed:** 2026-07-11

**Frozen plan:** [`../plan.md`](../plan.md) (v3)

**Inputs:** [Codex findings](codex-findings.md),
[Claude Fable findings](fable-findings.md), the
[v2 strict-inclusive synthesis](../../v2/reviews/unified-feedback.md), the v3
owner decisions, both canonical glossaries, ADRs 0001–0020, current code in
`dr-platform`, `whetstone-ai`, and `unitbench`, and installed DBOS 2.26.0.

## Executive verdict

**Gate: `REPEAT_CONVERGENCE`.** This synthesis is strict-inclusive: every
evidence-backed finding from either reviewer is retained, including findings
the other reviewer did not identify or classified as a bounded correction.
Codex returned `REPEAT_CONVERGENCE` with two blockers and four findings total.
Claude returned `READY_FOR_FOCUSED_AUDITS` with four bounded findings. The gate
and closure disagreements are substantive and must remain visible to the v4
planner and owner.

V3 closes much of the v2 packet. The owner-narrowed scheduling contract is
implementable on DBOS 2.26.0; Publication Bundle boundaries and skew policy are
coherent; the next-Attempt ledger, foreign-cancellation provenance, total
Operation status, abandoned Registration, requested/effective priority, and
request-bound semantics are materially complete. The two reviewers also agree
that v3 consistently records the owner's decisions to accept paid-call overlap
and same-millisecond DBOS tie nondeterminism.

Four remaining issues nevertheless affect architecture, persistence, or
Experiment validity:

1. the post-submit lifecycle has no restart-safe way to resolve and verify the
   complete execution target and concrete recipe behavior;
2. the acceptance schema cannot represent a missing Generation Run and does
   not invalidate currentness when its pinned platform cut changes;
3. accepted paid-call overlap can return a provider result whose cost never
   reaches Whetstone's durable accounting records; and
4. strict acceptance has no rule selecting one accepted Generation Run when
   multiple successful ordinals exist.

Implementing v3 literally can therefore strand retryable work after process
restart, publish a stale or unrepresentable acceptance result, undercount the
duplicate spend the owner explicitly accepted, or let conforming evaluators
disagree about whether an Experiment is complete. The first three change
runtime registration or durable schema boundaries; the fourth is an unresolved
domain decision. Under the effort's gate rules, v4 and another whole-system
convergence review are required before focused audits.

Priority labels mean:

- **P0 — architecture or owner-decision blocker:** resolve before dependent v4
  text or implementation work.
- **P1 — correctness contract:** encode explicitly in v4; it does not by itself
  reopen an accepted owner boundary.
- **P2 — verification boundary:** preserve as a named implementation gate.

## What both reviews agree is preserved

Do not reopen these choices without new correctness evidence:

- deterministic kernel rank/claim/enqueue mixing is required, while exact
  final DBOS order among same-priority millisecond ties is not;
- Whetstone Analysis and Detail referential sets publish atomically, kernel
  tables and cursors commit together, and independent families use explicit
  reader skew policy;
- dr-platform remains the sole Attempt-ordinal authority;
- caller-prepared Manifest membership, top-level-only managed workflows,
  non-recursive reference-aware cancellation, destination-local fencing, and
  strict Experiment acceptance remain the chosen directions;
- cancellation may overlap synchronous paid work, as explicitly accepted by
  the owner; the defect is hidden or unaccounted work, not overlap itself;
- scoring selections are distinct Operations, foreign cancellation requires
  new local confirmation, and requested priority may differ visibly from the
  shared execution's effective priority; and
- operational Postgres remains durable truth, with fresh schemas, append-only
  provenance, two-plane reads, and no compatibility migration.

## P0 findings — resolve in this order

### 1. Make execution-target and recipe resolution restart-safe

V3 persists recipe digests but leaves the workflow, `execution_for`,
`args_for`, and implied recipe-producing behavior only on the submit-time
`EnqueueTarget`. `reconcile`, `wait_operation`, inspection/export-triggered
reconciliation, and `request_next_attempt` do not accept or resolve that
target. The Manifest also lacks concrete ordered recipe leaves against which
the Operation aggregate can be recomputed. A new process cannot safely resume
an expired Claim or create a later Attempt.

**V4 requirement:** define one immutable persisted target key/version and one
startup registration/lookup contract. Every process that may drive lifecycle
transitions registers the complete workflow, identity, argument, error, and
recipe functions under that key; missing or conflicting registration fails
closed. All lifecycle entry points resolve the target through the same
registry/resolver. Registration recomputes every concrete Item recipe digest
and the ordered Operation aggregate before completion. Restart tests must
resume an expired Claim and create/enqueue automatic and requested Attempts in
a fresh process.

The kernel should own only a minimal frozen recipe envelope and digest
contract. Whetstone should own and validate the domain payload; profile,
parser, dataset, graph, and provider vocabulary must not enter dr-platform's
domain model.

**Disagreement:** Codex calls the missing runtime target/recipe contract a
blocker and marks v2 P0-1, v1 P0-1/P0-2, owner decision 1, and lifecycle wait
open. Claude considers the identity binding closed and identifies only a minor
recipe-ownership ambiguity. **Synthesis opinion:** the selected identity
direction is closed, but Codex is correct that the lifecycle is not
implementable after restart. Claude's opaque-envelope clarification belongs in
the same v4 correction.

**Sources:** Codex F1; Claude F4.

### 2. Make Experiment acceptance representable and current against platform state

`experiment_acceptance_members` places non-null `generation_run_id` in its
primary key, so it cannot represent an expected Prediction with no Generation
Run. V3 also pins platform Operation/Attempt cuts but increments
`acceptance_source_version` only for domain outcomes, Manifest relationships,
and profile changes. A next Attempt, cancellation, or reconciliation change can
leave the current pointer visibly accepted against stale platform state.

**V4 requirement:** key expected cells by identities that exist before an
outcome—Prediction plus required scoring/profile/parser/dataset axes—and make
selected Generation Run and Score Attempt references nullable with explicit
missing/rejected dispositions. Separate expected-generation and
expected-scoring member tables are also viable. Define storage and transaction
ownership for accepted Manifest relationships. Then choose one currentness
mechanism that respects the domain-agnostic kernel: either a complete
same-transaction invalidation seam for every relevant platform mutation or an
atomic platform-cut version that pointer promotion and readers verify. Tests
must cover no Generation Run, no Score Attempt, next-Attempt reactivation,
cancellation, reconciliation change, and mutation racing pointer promotion.

**Disagreement:** Codex calls this a blocker and marks v2 P0-5, v1 P0-5, and
owner decision 4 open. Claude considers the append-only persistence/current
pointer architecture closed and finds a separate accepted-run derivation gap.
**Synthesis opinion:** the append-only direction is closed, but Codex's
PostgreSQL primary-key and stale-platform-cut evidence is decisive. The durable
enforcement model remains architecture-changing and must be corrected in v4.

**Source:** Codex F2.

### 3. Decide whether accepted overlap requires complete durable cost accounting

The owner accepted overlapping provider calls and duplicate spend, while v3
still promises that both observable costs are retained and reconciled. Current
Whetstone performs the synchronous provider call inside one DBOS step but
persists the Node Attempt and cost in a later step. If cancellation arrives
between them, DBOS prevents the later persistence operation; the returned
provider usage may survive only in excluded DBOS replay output.

**Owner decision for v4:** choose between:

1. preserving complete observable-call accounting by adding an idempotent
   provider-call/Node-Attempt ledger write before the paid DBOS step returns; or
2. explicitly accepting that cancellation overlap may also undercount spend
   and narrowing the cost/accounting gate and ADR 0019 accordingly.

**Recommendation:** preserve accounting with the durable in-step ledger. The
owner accepted duplicate spend, not invisible spend, and cost truth is needed
to interpret intensive experiments. V4 must specify idempotency and the
provider-success/database-commit crash gap; tests must assert durable
Whetstone/export totals rather than a mock call counter or replay payload.

**Disagreement:** Codex treats the missing durable accounting boundary as an
architecture-changing major finding and marks cancellation owner decision 3
open in enforcement. Claude considers the accepted-overlap contract closed and
does not identify the later-step persistence loss. **Synthesis opinion:** the
owner's overlap decision remains closed, but its accounting consequence is a
real unresolved owner choice. Codex's live workflow/DBOS evidence should govern.

**Source:** Codex F3.

### 4. Choose the accepted Generation Run when multiple successful ordinals exist

Shared work may finish successfully after another Operation has locally
cancelled it and created a confirmed replacement. Both the old shared ordinal
and the replacement ordinal can produce successful Generation Runs for one
Prediction. V3 never says which run is accepted, so implementations can require
scores for all successes, the latest success, or any success.

**Owner decision for v4:** choose one deterministic domain rule. The viable
choices are all successful runs, the highest successful platform Attempt
ordinal at the evaluation cut, or an explicitly persisted operator/domain
selection. Each changes required scoring cells, cost, and Experiment outcome.

**Recommendation:** select the successful Generation Run with the highest
platform Attempt ordinal at the source cut; record earlier successes as
superseded provenance rather than expected score cells. Apply the same rule
when freezing scoring selection and add the shared-late-success/cancel-retry
walk to gate 3.

**Disagreement:** Claude classifies this as a bounded local derivation rule and
still returns `READY_FOR_FOCUSED_AUDITS`; Codex does not report it. **Synthesis
opinion:** this is owner-visible P0 because it defines strict Experiment
validity and paid scoring scope. The correction can be small after the owner
chooses, but the planner should not silently select the policy.

**Source:** Claude F2.

## P1 correctness contracts

### 5. Prevent enqueue after logical cancellation

DBOS cancellation of an absent workflow row is a silent no-op. A Claim can
commit, cancellation can finalize the local Attempt, and the in-flight claimant
can then create a DBOS workflow with no live platform reference. This is not
the owner-accepted continuing-call overlap; it is a new workflow starting after
cancellation reported success.

V4 must require: claim eligibility excludes cancellation intent; cancellation
invalidates outstanding Claims; a claimant whose outcome CAS loses to
cancellation performs idempotent DBOS cancellation and records compensation;
and finalization distinguishes `NOT_ENQUEUED` from a delivered DBOS cancel.
Add cancel-during-claim and cancel-then-late-enqueue tests.

**Source:** Claude F1.

### 6. Use a genuinely payload-safe DBOS step inspection path

DBOS 2.26.0 `DBOSClient.list_workflow_steps` exposes no `load_output=False` and
the underlying call defaults to loading and deserializing output/error. The
standard inspector cannot both use only that public API and promise it never
loads replay payloads.

V4 must choose one implementable path: omit standard step timelines, pin a
DBOS patch exposing payload controls, or use the already-reviewed
version-specific allowlisted system-schema adapter. **Recommendation:** reuse
the allowlisted adapter and contract-test that input/output/error
deserialization does not occur; this is smaller than a DBOS fork and preserves
the promised timeline.

**Disagreement:** Codex marks v1 P1-10 and the relevant P2 closure open based on
the installed signature. Claude marks both closed, relying on the plan's stated
safe-read rule. **Synthesis opinion:** Codex is correct; installed API behavior
outweighs an unimplemented plan assertion.

**Source:** Codex F4.

### 7. Define one authoritative Whetstone Analysis Bundle inventory

Sections 1.6, 2.4, and 4.1 enumerate different Analysis members, leaving
atomic promotion, Unitbench tables, and COPRO's candidate-score source
ambiguous. V4 should make §4.1 authoritative and reference it elsewhere.

**Recommendation:** include `experiments`, `predictions`, `generation_runs`,
`score_attempts`, `sweep_metrics`, and `failure_metrics` in the Analysis
Bundle; keep node-attempt payload/detail in the Detail Bundle; name
`score_attempts` as COPRO's candidate-level input. The v4 planner should adjust
this inventory if current reader queries prove a different minimum set.

**Source:** Claude F3.

## Reviewer disagreements requiring owner visibility

| Topic | Codex position | Claude position | Synthesis opinion | V4 owner surface |
| --- | --- | --- | --- | --- |
| Overall gate | `REPEAT_CONVERGENCE`; two blockers and three open v2 P0/owner decisions. | `READY_FOR_FOCUSED_AUDITS`; all architecture closed and four bounded corrections remain. | `REPEAT_CONVERGENCE`; restart lifecycle and acceptance currentness change architecture/persistence. | Confirm v4 successor and another convergence pass. |
| Recipe/target closure | Identity and lifecycle remain open without recipe verification and restart-safe target lookup. | P0 identity is closed; only recipe ownership wording is ambiguous. | Identity direction is closed, executable target resolution is not. Merge both corrections. | No product choice required unless a different runtime registry model is desired. |
| Acceptance persistence | Missing-generation cells and platform-cut invalidation make P0-5/current pointer open. | Persistence/current pointer is closed; accepted-run derivation is a local gap. | Append-only direction is closed, schema/currentness is not. | Choose platform-currentness mechanism and accepted-run rule. |
| Cancellation overlap | Accounted-overlap invariant is open because current step ordering loses cost. | Accepted overlap is consistently closed; separate late-enqueue race is local. | Owner's permission to overlap is closed; accounting guarantee remains an owner choice, and late enqueue is mandatory P1. | Preserve durable accounting or explicitly accept undercount. |
| Safe step inspection | Installed public API disproves the no-payload-load path. | Secret-free/safe reads are closed as specified. | Codex's installed API evidence is decisive; choose a different inspection mechanism. | Technical correction; allowlisted adapter recommended. |
| Accepted-run selection | Not reported. | Bounded local rule; highest successful ordinal recommended. | Owner-visible because it changes strict completeness and paid scoring scope. | Choose all successes, highest successful ordinal, or explicit selection. |
| Analysis Bundle inventory | Not reported. | Three inconsistent inventories require local correction. | Retain as a required P1 clarification. | Usually planner-selected from reader evidence. |

## V2 strict-inclusive closure

| V2 item | Unified disposition for v4 |
| --- | --- |
| P0-1 execution recipe/domain equality | Reopened in runtime enforcement: exact domain equality direction retained; add opaque recipe ownership, ordered verification, and restart-safe complete target resolution (P0-1). |
| P0-2 owner-narrowed scheduling | Closed; preserve deterministic kernel mixing and accepted final tie variance. |
| P0-3 accepted cancellation overlap | Owner choice retained; reopen durable accounting enforcement (P0-3) and add the distinct late-enqueue race correction (P1-5). |
| P0-4 Publication Bundles/skew | Closed in architecture; reconcile the Analysis member inventory (P1-7). |
| P0-5 Experiment acceptance | Reopened: fix missing-cell representation, platform-cut currentness, and accepted-run selection (P0-2/P0-4). |
| P1-6 successive scoring selections | Closed; preserve. |
| P1-7 foreign cancellation | Closed; preserve. |
| P1-8 total status | Closed; preserve. |
| P1-9 lifecycle wait/COPRO loop | Contract retained but operationally depends on P0-1 target resolution. |
| P1-10 abandoned Registration | Closed; preserve. |
| P1-11 late terminal after cancel | Closed; preserve; P1-5 is a different pre-enqueue race. |
| P1-12 requested/effective priority | Closed; preserve. |
| P1-13 request maximum bound | Closed; preserve. |
| P2 live gates | Preserve all live gates; move DBOS step inspection from unknown gate to known P1 correction. |

## V1 disposition closure

| V1 item | Unified disposition for v4 |
| --- | --- |
| P0-1 caller-requested next Attempt | Reopened only by restart-safe target resolution; ledger/reason/CAS/bound semantics retained. |
| P0-2 immutable Manifest | Membership mechanics retained; concrete recipe verification/aggregate proof remains open with P0-1. |
| P0-3 destination fencing | Closed; preserve. |
| P0-4 cancellation topology | Core topology closed; add late-enqueue compensation and separately resolve accounting. |
| P0-5 Experiment acceptance | Reopened by P0-2/P0-4. |
| P1-6 Operation serialization | Closed; preserve. |
| P1-7 execution-scoped attributes | Closed; preserve. |
| P1-8 total status precedence | Closed; preserve. |
| P1-9 Detail Attempt snapshot | Closed; preserve with authoritative bundle inventory. |
| P1-10 secret-free payloads/safe reads | Workflow payload direction retained; step inspection path open (P1-6). |
| P1-11 writer-lock ownership | Closed; preserve. |
| P2-12 live verification | Preserve; installed step-inspection mismatch is no longer merely unverified. |

## V4 owner decisions to obtain

Ask one focused question at a time before revising dependent plan/ADR/glossary
text:

1. **Overlap cost truth:** preserve complete durable accounting with an
   in-step provider-call ledger, or explicitly accept undercount after
   cancellation? Recommend the ledger.
2. **Accepted Generation Run:** score all successful runs, select the highest
   successful platform ordinal at the cut, or persist an explicit selection?
   Recommend the highest successful ordinal with earlier successes retained as
   superseded provenance.
3. **Acceptance platform currentness:** add a domain invalidation seam to every
   relevant platform mutation or make currentness depend on an atomically
   checked platform-cut version at promotion/read time? Recommend the checked
   platform-cut version to preserve dr-platform's domain independence.

The runtime target registry, DBOS step adapter, cancellation compensation, and
Analysis inventory are technical corrections the v4 planner may specify
directly unless implementation evidence reveals a new owner trade-off.

## P2 verification boundaries

Retain these blocking implementation gates:

- live MotherDuck conditional Lease/bundle promotion and DuckDB-SQL parity;
- live Neon transactional Lease/bundle behavior under pooling;
- local DuckDB OS-lock and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret wiring;
- OTLP initialization/degradation and safe attributes;
- replacement Whetstone rescore-selection parity;
- COPRO and zero-spend wait/export/pinned-read behavior; and
- same-millisecond multiple-dequeuer variance remaining within the accepted
  kernel-mixing safety bound.

Also load-test, but do not presently treat as a correctness finding, the
Experiment-row lock taken by every Generation Run/Score Attempt insert.

## Source-to-priority map

| Unified item | Codex | Claude | Treatment |
| --- | --- | --- | --- |
| P0-1 target/recipe lifecycle | F1 | F4 | Merge blocker with ownership clarification. |
| P0-2 acceptance representation/currentness | F2 | — | Retain Codex blocker. |
| P0-3 overlap accounting | F3 | — | Elevate to owner-visible P0 because it changes the accepted cost-truth guarantee. |
| P0-4 accepted-run selection | — | F2 | Elevate from local to owner-visible Experiment-validity decision. |
| P1-5 cancel/enqueue race | — | F1 | Retain as mandatory major correctness contract. |
| P1-6 safe step inspection | F4 | closure says closed | Retain installed-API correction. |
| P1-7 Analysis inventory | — | F3 | Retain local correction. |

## Final disposition

- **V3 status:** reviewed and frozen.
- **Unified gate:** `REPEAT_CONVERGENCE`.
- **Next artifact:** `v4/plan.md`, copied from v3 before revision.
- **Owner-visible decisions for v4:** overlap cost truth, accepted Generation
  Run selection, and acceptance platform-currentness mechanism.
- **Required review after v4:** another independent whole-system convergence
  pass using the same Codex code/dependency and Claude architecture/domain
  lenses.
- **Implementation:** remains blocked until v4 resolves P0 and passes its
  convergence gate.
