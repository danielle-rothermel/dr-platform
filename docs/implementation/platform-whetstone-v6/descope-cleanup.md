# Platform/Whetstone v6 descope and cleanup

Decided 2026-07-13. Owner-approved removal of over-engineered subsystems so the
repos only contain in-progress or shipped work — nothing dormant or "deferred."
Rationale: ADR 0022 already pins rollback to source control plus fresh schemas,
which makes automated destructive recovery/cleanup of remote state redundant,
and the locked paid sweep never exercises `PARTIAL` promotion or advanced
cancellation. Anything not on the sweep → validation → cutover critical path is
removed or closed now, not parked.

## Decisions

- **D1 — Close whetstone-ai PR #36 (progress-aware marker-owned recovery).**
  The automated release/cutover recovery machinery (journals, checkpoints,
  ownership-proof deletion) is descoped entirely. Its live fault-injection
  validation campaign is cancelled. Replacement: a manual recovery runbook
  (drop run schema / drop analysis schema + marker / delete bundle / rerun)
  grounded in the real `stores` tooling. The runbook also feeds the criterion-5
  operations report.
- **D2 — Resolve dr-platform PR #25 (MotherDuck operation cleanup opt-in) by
  consumer analysis, then merge.** With D1 removing the Whetstone-side
  consumer, keep the MotherDuck `operation_cleanup` enablement only if the
  merged authoritative publication operation recovery (PR #24) requires it on
  the production publication path (the Analysis dataset lives on MotherDuck).
  If nothing on the production path consumes it, strip the enablement and its
  opt-in plumbing but keep the live-evidence root fixes that publication needs
  regardless (constraint-free `ensure_schema` migrations, kind-aware retryable
  classification, cleanup isolation scoping where still reachable). Either way
  the PR gets one fresh high-effort review before merge.
- **D3 — Close risk A1 and the two queued P5 owner decisions as out of scope,
  by decision rather than silence.** ADR 0022's operating constraints stand
  permanently for v6: policy-accepted `PARTIAL` promotion stays disabled (A1
  closure work cancelled); no second cancellation-request representation will
  be built; the late-enqueue absence/successor-hazard protocol is not extended
  beyond what is merged. Cancellation/retry remains prohibited while
  enqueue-call state is uncertain. Recorded in the risk register and a short
  ADR — the register must read "closed (out of scope)" not "open."
- **D4 — Hosted release parity becomes a one-time, manually dispatched cutover
  gate, not permanent recurring CI.** Keep exactly enough workflow wiring to
  run the parity check once at cutover and record its evidence (this may mean
  merging unitbench PR #36's secret mapping). Remove any recurring triggers
  (schedule / push / PR) from the parity workflow.
- **D5 — No ambiguous work-in-progress.** Every branch/worktree is either an
  open PR on the critical path or explicitly recorded as abandoned. The
  `unitbench-v6-parity-closure-v2` worktree is assessed under D4: review-fix /
  one-time-validation content becomes a PR; recurring-gate machinery is pushed
  as a backup branch, not PR'd, and recorded as abandoned here and in the ADR.

## Explicitly NOT in scope

- whetstone-ai PR #41 (strict run-schema isolation) — critical path, untouched.
- unitbench reader stack PRs #26–#30 — critical path, untouched.
- Merged dr-platform kernel work (fencing, signed bundles, append-only
  triggers, enqueue claims, late-enqueue compensation, publication operation
  recovery) — load-bearing; do not rip out merged reviewed code.
- Risks V1, A3, L1(W3), L2 — exercised by the sweep/validation; stay open.

## Execution

| Task | Repo | Agent | Deliverable |
| --- | --- | --- | --- |
| T1 (D1) | whetstone-ai | Fable worker | PR #36 closed + branch deleted; manual-recovery runbook PR opened |
| T2 (D2) | dr-platform | Fable worker | PR #25 consumer analysis, fresh review, merged (as-is or stripped) |
| T3 (D4, D5) | unitbench | Fable worker | Parity workflow dispatch-only; PR #36 resolved; worktree resolved |
| T4 (D3 + record) | dr-platform | Root session | Risk register updated, descope ADR committed, planning README pointer |

Safety rules for all tasks: branch before changes; no force-push; closing a PR
requires a closing comment pointing at this document; deleting a branch is
allowed only after its PR is closed (GitHub retains the ref); never discard
uncommitted work — commit and push a backup branch first; merges require the
task's fresh review to be clean and CI green.

## Outcomes (2026-07-13)

All tasks executed same day; durable record in
[ADR 0025](../../adr/0025-v6-descope-recovery-automation-and-deferred-work.md).

- **T1** — whetstone-ai PR #36 closed, branch deleted; manual recovery runbook
  merged (PR #42, `aae625a`).
- **T2** — MotherDuck `operation_cleanup` enablement stripped (production
  recovery path proven not to consume it); publication root fixes kept;
  dr-platform PR #25 reviewed clean and merged (`b6e76be`).
- **T3** — parity workflow confirmed dispatch-only; unitbench PR #36 merged
  (`a3acf8a`, pin advance to `679177ca` verified); worktree branch
  `unitbench/v6-parity-closure-v2` backed up/pushed and abandoned without a PR
  (superseded by PR #32's integration).
- **T4** — risk register updated (A1 closed out of scope; A2 annotated), ADR
  0025 written, this record committed.
- **Additional finding executed under D5** — unitbench draft PRs #26–#30
  closed as superseded: PR #32's integration merge already landed the reader
  stack (all 75 touched files byte-identical between stack tip and `main`).
