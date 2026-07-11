# Platform and Whetstone refactor

This effort plans the hard-cut refactor across `dr-platform`, `whetstone-ai`,
and the affected `unitbench` boundary before intensive experiments begin.

**Current version:** v1 (`in-review`)

**Tracker map:** not created. Until Wayfinder is introduced, this index and the
active plan plus canonical ADRs are the navigation surface. V0 review
questions resolved for v1 are recorded in the draft and `docs/adr/`.

## Versions

| Version | Status | Review scope | Plan | Unified feedback |
| --- | --- | --- | --- | --- |
| v0 | superseded | `dr-platform` plus immediate downstream impacts | [plan](v0/plan.md) | [feedback](v0/reviews/unified-feedback.md) |
| v1 | in-review | whole-system convergence across `dr-platform`, `whetstone-ai`, `unitbench`, DBOS, and export/runtime boundaries | [plan](v1/plan.md) | pending |

v0 is immutable and superseded. V1 is frozen while its convergence review is
in progress. Decision-changing findings land only in a successor draft.

V1 review prompts: [Codex 5.6 (`sol`, high)](v1/reviews/codex-prompt.md) and
[Claude Fable 5 (high)](v1/reviews/fable-prompt.md). Findings and unified
feedback are pending; no findings placeholders are created before a review
actually runs.

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

V1 entered whole-system convergence review on 2026-07-10. Its plan is frozen;
the review packet currently contains only the two issued prompts.

## Historical provenance

The [v0 session handoff](v0/session-handoff.md) is retained only as provenance
for the original planning session. It is not the current process contract.
