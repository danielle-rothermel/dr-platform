# Proceed with an implementation-first final hard cut

PD1 selects **IMPLEMENTATION-FIRST FINAL HARD CUT**. V6 is the final
implementation target. Stop automatic convergence-successor generation and
implement the complete replacement architecture incrementally, then perform a
full switchover. This decision is not
`READY_FOR_FOCUSED_AUDITS` and does not claim production-grade convergence.
Preliminary results may be produced from the new implementation as it becomes
usable, but every implementation slice is final-intent rather than knowingly
temporary.

Before each slice lands, create explicit state-machine and scenario matrices
that make its risky boundaries executable and reviewable. Carry the known v6
A1-A3, L1-L2, and V1 findings forward as an open risk register linked to those
matrices, implementation decisions, focused tests, observability and escalation
triggers, and code-review checkpoints. Implement and review real code
incrementally against those artifacts. Preliminary experiments are permitted
only under the following operating constraints:

- do not initially enable policy-accepted `PARTIAL` promotion;
- treat independently captured cross-source telemetry as approximate
  compatibility, not proven same-cut truth, until V6-OD1 is resolved;
- retain pinned bundles or fail closed rather than clean up a possibly active
  pin; and
- prohibit cancellation or retry operations while enqueue-call state is
  uncertain, except in controlled tests.

This is a hard cut. Do not add temporary compatibility modes, shims, dual
reads or writes, fallback orchestration, or retained legacy paths. Delete each
old path in the same implementation change series that introduces its
replacement, after that slice's state-machine/scenario matrix and focused tests
pass. Do not defer all deletion until full convergence. Rollback is through
source control and fresh schemas, not runtime compatibility paths.

V6-OD1 remains the sole product/architecture question. This decision does not
answer it. The remaining v6 findings are implementation risks to close with
executable evidence, not authorization to create another convergence-plan
version.
