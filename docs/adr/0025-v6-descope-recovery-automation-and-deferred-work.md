# Descope recovery automation and close deferred v6 work

Date: 2026-07-13. Status: accepted, executed.

## Decision

Remove or explicitly close every v6 subsystem and question that is not on the
sweep → validation → cutover critical path, instead of deferring it. ADR 0022
pins rollback to source control plus fresh schemas, which makes automated
destructive recovery of remote state redundant; the locked paid sweep never
exercises `PARTIAL` promotion or extended cancellation. The full rationale and
task breakdown are in
[`descope-cleanup.md`](../implementation/platform-whetstone-v6/descope-cleanup.md);
this ADR records the durable outcomes.

## Outcomes

- **D1 — automated release recovery removed.** whetstone-ai PR #36
  (progress-aware marker-owned recovery) closed, branch
  `whetstone/progress-recovery-pr2` deleted (ref retained by the closed PR);
  its live fault-injection validation campaign is cancelled. Main already has
  fail-closed journaled `stores cleanup`/`verify-cleanup`; the supported
  recovery path is the manual runbook, merged as whetstone-ai
  `docs/release-recovery-runbook.md` (PR #42, `aae625a`). Merged PR #35 leaves
  no dead hooks.
- **D2 — MotherDuck `operation_cleanup` enablement stripped, root fixes
  merged.** Consumer analysis proved the production publication/recovery path
  (PR #24 promotion replay) never reads `capabilities.operation_cleanup` and
  `cleanup_operation` had no non-test callers; its only consumer was the
  D1-descoped Whetstone automation. dr-platform PR #25 was stripped to the
  publication root fixes (constraint-free `ensure_schema` migrations,
  kind-aware XXUUU retryable classification, Neon-scoped SERIALIZABLE),
  freshly reviewed clean, and merged as `b6e76be`.
- **D3 — `PARTIAL` promotion and cancellation extensions closed out of
  scope.** Risk A1 closure work is cancelled: policy-accepted `PARTIAL`
  promotion stays disabled permanently for v6. The two queued P5 owner
  decisions — a second cancellation-request representation and extending the
  late-enqueue absence/successor-hazard protocol — are answered "no, by
  descope"; merged behavior is final. ADR 0022's operating constraint
  (no cancellation/retry while enqueue-call state is uncertain) stands.
- **D4 — hosted release parity is a one-time manual cutover gate.** The
  unitbench parity workflow was already `workflow_dispatch`-only; unitbench
  PR #36 (explicit eight-secret mapping, pin advanced to `679177ca`) was
  reviewed and merged (`a3acf8a`). No recurring parity CI exists or will be
  added; the gate runs once at cutover for recorded evidence.
- **D5 — no ambiguous work-in-progress.**
  - Branch `unitbench/v6-parity-closure-v2` (tip `8c656a9`, includes a backup
    commit of previously uncommitted/untracked parity WIP) is **abandoned**:
    pushed for retention, deliberately no PR. Its content is superseded by
    unitbench PR #32's integration and the merged parity lineage (#31–#35).
  - Unitbench draft PRs #26–#30 (the v6 reader stack) were closed as
    **superseded**: PR #32's integration merge already landed their content —
    all 75 files the stack touched are byte-identical between the stack tip
    and `main`.

## Consequences

- The pre-sweep critical path is now: whetstone-ai PR #41 (strict run-schema
  isolation) → acceptance retry → 12-cell canary → full sweep.
- Recovery from a failed release/cutover is a documented operator procedure,
  not code; the criterion-5 operations report incorporates the runbook.
- Risks V1, A3, L1 (W3 adoption), and L2 remain open and are closed by
  sweep/validation evidence, not by this descope.
- Reopening any descoped item requires a new ADR, not a revival of the closed
  branches.
