# Platform and Whetstone refactor

This effort plans the hard-cut refactor across `dr-platform`, `whetstone-ai`,
and the affected `unitbench` boundary before intensive experiments begin.

**Current version:** v6 (`reviewed`)

**Tracker map:** not created. Until Wayfinder is introduced, this index and the
active plan plus canonical ADRs are the navigation surface. V0 review
questions resolved for v1 are recorded in that version and `docs/adr/`.

## Versions

| Version | Status | Review scope | Plan | Unified feedback |
| --- | --- | --- | --- | --- |
| v0 | superseded | `dr-platform` plus immediate downstream impacts | [plan](v0/plan.md) | [feedback](v0/reviews/unified-feedback.md) |
| v1 | superseded | whole-system convergence across `dr-platform`, `whetstone-ai`, `unitbench`, DBOS, and export/runtime boundaries | [plan](v1/plan.md) | [feedback](v1/reviews/unified-feedback.md) |
| v2 | superseded | hybrid whole-system convergence plus explicit v1-correction closure | [plan](v2/plan.md) | [feedback](v2/reviews/unified-feedback.md) |
| v3 | superseded | strict-inclusive v2 closure plus owner-resolved identity, publication, cancellation, acceptance, and scheduling contracts | [plan](v3/plan.md) | [feedback](v3/reviews/unified-feedback.md) |
| v4 | superseded | strict-inclusive v3 closure plus restart-safe targets, representable/current acceptance, cancellation compensation, safe inspection, and the three owner decisions | [plan](v4/plan.md) | [feedback](v4/reviews/unified-feedback.md) |
| v5 | superseded | strict-inclusive v4 closure plus singular Generation membership, ordered deterministic score selection, `PARTIAL` scoring parity, pre-scoring evaluations, and independent local corrections | [plan](v5/plan.md) | [feedback](v5/reviews/unified-feedback.md) |
| v6 | reviewed | bounded v5 closure for durable Claim identity, terminal/run-pinned acceptance, reference-safe compensation, and owner-selected populated-only `PARTIAL` selection | [plan](v6/plan.md) | [feedback](v6/reviews/unified-feedback.md) |

V0 through v5 are immutable and superseded. V6 is reviewed and immutable.
Its gate remains `REPEAT_CONVERGENCE`; this is not
`READY_FOR_FOCUSED_AUDITS`. [PD1](../../adr/0022-implementation-first-final-hard-cut.md)
stops automatic successor generation and selects an implementation-first final
hard cut against v6. [V6-OD1](../../adr/0023-bounded-cross-source-compatibility.md)
selects bounded cross-source compatibility. The owner-question queue is
complete.

V1 review packet: [Codex 5.6 (`sol`, high)](v1/reviews/codex-findings.md),
[Claude Fable 5 (high)](v1/reviews/fable-findings.md), and the
[unified feedback](v1/reviews/unified-feedback.md).

V2 review packet: [Codex 5.6 (`sol`, high)](v2/reviews/codex-findings.md),
[Claude Fable 5 (high)](v2/reviews/fable-findings.md), and the
[strict-inclusive unified feedback](v2/reviews/unified-feedback.md). The
reviewers disagreed on the gate and seven closure/architecture questions; the
synthesis preserves both positions and the synthesis opinion for v3.

V3 review packet: [Codex code/dependency findings](v3/reviews/codex-findings.md),
[Claude architecture/domain findings](v3/reviews/fable-findings.md), and the
[strict-inclusive unified feedback](v3/reviews/unified-feedback.md). Codex
returned `REPEAT_CONVERGENCE`; Claude returned `READY_FOR_FOCUSED_AUDITS`.
The synthesis preserves their gate and closure disagreements plus its own
opinion for the v4 planner.

V4 review packet: [Codex code/dependency findings](v4/reviews/codex-findings.md),
[Claude architecture/domain findings](v4/reviews/fable-findings.md), and the
[strict-inclusive unified feedback](v4/reviews/unified-feedback.md). Both
reviewers returned `REPEAT_CONVERGENCE`; synthesis retained all eight source
findings and selected `REPEAT_CONVERGENCE`.

V5 review packet: [Codex code/dependency findings](v5/reviews/codex-findings.md),
[Claude architecture/domain findings](v5/reviews/fable-findings.md), and the
[strict-inclusive unified feedback](v5/reviews/unified-feedback.md). Both
reviewers returned `REPEAT_CONVERGENCE`; synthesis accepted all five source
findings and selected `REPEAT_CONVERGENCE`.

V6 review packet: [Codex code/dependency findings](v6/reviews/codex-findings.md),
[Claude architecture/domain findings](v6/reviews/fable-findings.md), and the
[strict-inclusive unified feedback](v6/reviews/unified-feedback.md). Both
reviewers returned `REPEAT_CONVERGENCE`; synthesis accepted six source
findings, merged one duplicate, selected `REPEAT_CONVERGENCE`, and stopped
automatic successor generation for process-level review.

## Process

Versions move through `draft` → `in-review` → `reviewed` → `superseded`.
Only `draft` is mutable. Each version's `reviews/` directory contains the exact
prompts, findings, and synthesis that evaluated that plan.

The issue tracker is the live decision store when a Wayfinder map exists. The
version packet is a historical snapshot. [`CONTEXT.md`](../../../CONTEXT.md)
and `docs/adr/` remain living canonical docs outside version packets. Reports,
prototypes, and future handoffs stay temporary; their durable conclusions are
copied into the tracker, active draft, glossary, or an ADR.

## Review strategy

Review is gate-based, not a fixed number of rounds. The v0 platform-focused
review found architecture-wide identity, lifecycle, export, and ownership
defects, so a version does not advance to narrower scopes merely because one
scheduled round finished.

### 1. Whole-system convergence

Each substantially revised draft first receives two independent reviews over
the complete proposed system:

- **Claude:** architecture, domain coherence, ownership, lifecycle, and
  concrete failure scenarios;
- **Codex:** live-code and dependency audit, DBOS/API feasibility,
  transactions/concurrency, and sibling-repository blast radius.

Both prioritize silent-wrongness and architecture-changing findings over
local naming or implementation polish. Their findings are synthesized into
one severity-ordered unified feedback document.

If unified feedback contains a blocker/P0, unresolved owner decision, changed
identity/ownership boundary, new lifecycle or persistence shape, changed
cross-repository interface, or fundamental transaction/export redesign, the
reviewed version is superseded by a successor draft. That successor receives
another whole-system convergence review.

### 2. Focused audits

Focused review begins only when a convergence pass produces:

- no blocker/P0 findings;
- no unresolved product or architecture decisions;
- no finding that changes the core state model or repository ownership; and
- only local corrections, verification gaps, or mechanical blast-radius work.

The focused scopes are:

1. `dr-platform` kernel, persistence, concurrency, and DBOS contracts;
2. Whetstone generation/scoring and experiment-facing flow; and
3. export, Unitbench's two-plane boundary, and deployment/runtime contracts.

A focused finding that changes architecture sends the effort back through a
successor draft and whole-system convergence; it is not patched into the
frozen version under review.

### 3. Final constellation audit

The candidate-final plan receives one cross-repository audit covering all
affected repositories, dependencies and pins, CI/authentication, secrets,
durable names, schema/cutover order, operator controls, and pre-experiment
acceptance gates. Its purpose is alignment and omission detection. Settled
architecture is reopened only when evidence shows a correctness failure.

The stopping condition is therefore not "reviewers found nothing." Broad
review repeats until reviewers stop finding architecture-changing issues;
focused and constellation audits then drive down local correctness and
integration risk.

V2 completed hybrid whole-system convergence review on 2026-07-10. Codex
returned `REPEAT_CONVERGENCE`; Fable returned `READY_FOR_FOCUSED_AUDITS`.
Strict-inclusive synthesis retained every evidence-backed finding and selected
`REPEAT_CONVERGENCE`. V3 incorporated that synthesis and the five owner
decisions, then completed whole-system convergence review on 2026-07-11.
Codex returned `REPEAT_CONVERGENCE`; Claude returned
`READY_FOR_FOCUSED_AUDITS`. Strict-inclusive synthesis selected
`REPEAT_CONVERGENCE`. V4 incorporated that synthesis and the three owner
decisions, then completed whole-system convergence review on 2026-07-11. Both
reviewers and strict-inclusive synthesis selected `REPEAT_CONVERGENCE`.
V5 incorporated the complete v4 synthesis, all four owner decisions, and the
three independent local corrections, then completed whole-system convergence
review on 2026-07-11. Both reviewers and strict-inclusive synthesis selected
`REPEAT_CONVERGENCE`.
V6 incorporated all five accepted v5 synthesis targets and the populated-only
owner decision, then completed whole-system convergence review on 2026-07-11.
The synthesis classified the current method as non-convergent at the agreed
checkpoint and stopped automatic v7 generation. PD1 then selected an
implementation-first final hard cut: every slice is final-intent, old paths are
deleted as their tested replacements land, and rollback uses source control
plus fresh schemas rather than runtime compatibility paths.

Implementation under
[ADR 0022](../../adr/0022-implementation-first-final-hard-cut.md) is complete
(`READY_WITH_EXTERNAL_GATES`; see the
[orchestration reflection](../../implementation/platform-whetstone-v6/orchestration-reflection.md)).
[ADR 0025](../../adr/0025-v6-descope-recovery-automation-and-deferred-work.md)
then descoped automated release recovery, MotherDuck operation-cleanup
enablement, `PARTIAL` promotion, the queued cancellation extensions, and
recurring hosted parity, closing all ambiguous work-in-progress
([execution record](../../implementation/platform-whetstone-v6/descope-cleanup.md)).

**Exact next action:** merge whetstone-ai PR #41 (strict run-schema
isolation), rerun store acceptance, run the 12-cell canary, then the full
locked paid sweep; the remaining open risks (A2 verification, A3 P7/W7, L1 W3,
L2, V1) close on sweep/validation evidence.

## Historical provenance

The [v0 session handoff](v0/session-handoff.md) is retained only as provenance
for the original planning session. It is not the current process contract.
