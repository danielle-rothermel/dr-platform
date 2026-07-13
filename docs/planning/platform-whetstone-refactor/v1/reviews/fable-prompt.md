# V1 whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

Review the frozen v1 Platform and Whetstone refactor plan adversarially. Find
where its architecture, domain model, ownership boundaries, state machines,
failure behavior, or experiment contract is wrong, incomplete, or internally
contradictory. Prioritize defects that can create silent wrongness, ambiguous
authority, duplicate paid work, lost provenance, unsafe recovery, or an
experiment that appears valid when it is not. Do not manufacture findings for
volume, and do not re-litigate an owner-resolved choice unless concrete code or
design evidence shows it cannot satisfy its stated invariant.

This is one of two independent whole-system convergence reviews. Your primary
lens is architecture and domain coherence, but every finding must still be
grounded in the current repositories and applicable dependency behavior.

## Frozen review target

- Plan: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md`
- Effort index and review gates: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- V0 review synthesis, for promised correction coverage only: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/reviews/unified-feedback.md`
- Canonical platform vocabulary: `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md`
- Canonical Whetstone vocabulary: `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Canonical decisions: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md` through `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0017-fresh-schemas-without-data-migration.md`

The prompt was issued on 2026-07-10 against these repository revisions:

- `dr-platform`: `841c9e1` on `07-08-refactor`;
- `whetstone-ai`: `6ff95c7` on `codex/versioned-planning-docs`; and
- `unitbench`: `cafd493` on `codex/versioned-planning-docs`.

At the start of the review, record each current HEAD, branch, and dirty/clean
status. If a repository has moved, review its current working tree and call
out drift that changes a plan assumption. The plan itself remains frozen.

## Required inspection surface

Read the plan, index, both glossaries, all applicable ADRs, and the v0 unified
feedback before forming findings. Then inspect current code, tests,
dependencies, configuration, and directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository proven directly affected by imports, durable data,
  workflow/schema names, export consumers, or deployment configuration.

Use repository graph tooling if it is present and current, but verify important
claims in raw code. Do not treat a generated graph as authoritative over the
working tree.

## Architecture interrogation

The list is a floor, not a checklist ceiling:

1. **Domain and ownership coherence.** Test every responsibility assigned to
   `dr-platform`, DBOS, Whetstone, Unitbench, Postgres, DuckDB/MotherDuck, and
   Neon. Find shared authority, missing authority, or a layer that must know a
   domain concept the plan says it does not own.
2. **Identity and lineage.** Walk Operation, Item, Attempt, workflow,
   Prediction, Generation Run, Score Attempt, experiment, and export identity.
   Prove which values are caller keys versus derived/generated IDs, which are
   immutable, and how idempotency and reference sharing behave across retries,
   rescoring, cancellation, and recovery.
3. **Lifecycle completeness.** Independently reconstruct all enqueue,
   execution, retry, recovery, missing, cancellation, and terminal transitions.
   Look for states with no legal successor, two authorities for one transition,
   false terminality, ambiguous aggregation, or an event that cannot be
   represented in the append-only ledger.
4. **Failure scenarios.** Walk crashes and races before and after every
   persistence/enqueue/provider/scoring/export boundary, including shared
   references, sticky cancellation, state-sensitive missing, recovery
   exhaustion, and late results. Require deterministic outcomes rather than
   eventual hand-waving.
5. **Shared Operation primitive.** Determine whether generation and scoring
   genuinely use one reusable platform lifecycle without erasing their domain
   results, provenance, cost/accounting, or custom work. Identify surviving
   Whetstone orchestration that should move to the primitive or legitimate
   domain logic that the plan accidentally pushes into the kernel.
6. **Scheduling contract.** Verify that urgency and mandatory deterministic
   shuffle are separate and composable, that grouped model inputs cannot
   recreate the historically harmful all-one-model-first ordering, and that
   page boundaries, retries, and multiple service classes preserve the stated
   requirement.
7. **Export and two-plane truth.** Test whether operational Postgres remains
   the sole durable authority while analysis and detail stores are rebuildable.
   Examine barriers, cursors, partial publication, sampling roots, referential
   closure, local versus deployed compute, and what a reader observes during
   failures or refreshes.
8. **Operator and experiment contract.** Ask whether an operator can explain,
   stop, resume, and verify a run without direct DBOS mutation, and whether the
   pre-experiment gates prevent plausible invalid or misleading experiments.
   Check observable distinctions between platform success and domain outcome.
9. **Cutover coherence.** Walk repository order, dependency pins, schema
   creation, durable names, deployment/environment changes, clean rollback,
   and the first real experiment. Find circular sequencing or a step that
   assumes a downstream contract before it exists.
10. **Principles versus details.** Find any section that violates the plan's
    own one-happy-path, domain-agnostic-kernel, vocabulary, model-boundary, or
    two-plane principles. Also flag important behavior left only in prose with
    no implementable invariant or acceptance proof.
11. **V0 coverage.** Compare v1 against every actionable v0 unified-feedback
    item in preserved priority order. Report anything declared resolved that
    remains semantically open, contradictory, or unverifiable.

## Review rules

- Read-only review: do not edit plan, code, ADRs, or context documents; do not
  commit, stage, push, or create branches.
- Git status, branch, log, and revision inspection are allowed and required.
- Static inspection, fast searches, dependency/API inspection, and focused
  read-only commands are allowed. Do not run broad suites or commands expected
  to exceed about two minutes.
- Every defect must cite the relevant plan/ADR contract and current
  file-and-line or dependency evidence. Put unsupported concerns under
  verification gaps rather than presenting them as findings.
- Rank by consequence. Merge duplicates. Distinguish an architectural defect
  from implementation work or tests the plan already requires.
- Classify every finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.

## Output

Write only the review result to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/fable-findings.md`

Use this structure:

```markdown
# V1 convergence findings — Claude Fable 5 architecture and domain audit

## Review baseline
- **Date:** YYYY-MM-DD
- **dr-platform:** <branch>, <HEAD>, <dirty/clean>
- **whetstone-ai:** <branch>, <HEAD>, <dirty/clean>
- **unitbench:** <branch>, <HEAD>, <dirty/clean>

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Class:** architecture-changing | owner-decision | local-correction | verification-gap
- **Plan contract:** §x.y and/or ADR NNNN
- **Evidence:** absolute/path:line — what current code or dependency actually does
- **Consequence:** concrete failure if implementation follows v1
- **Required plan change:** exact invariant, boundary, or step that must change

## V0 coverage gaps
- <none, or a priority-numbered item with evidence>

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <which gate condition is or is not present>
- **Unverified:** <anything not verifiable and why>
```

Use `REPEAT_CONVERGENCE` if any blocker, unresolved owner decision,
architecture-changing finding, persistence/identity/lifecycle redesign, or
changed cross-repository interface remains. Use `READY_FOR_FOCUSED_AUDITS`
only when remaining work is local correction or bounded verification.
