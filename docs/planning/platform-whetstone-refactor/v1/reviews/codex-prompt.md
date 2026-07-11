# V1 whole-system convergence review — code and dependency audit (Codex 5.6, `sol`, high)

Review the frozen v1 Platform and Whetstone refactor plan adversarially. Find
places where it is factually wrong, infeasible against current APIs, incomplete
at a transaction or concurrency boundary, or inconsistent across repositories.
Prioritize defects that can cause silent wrongness, duplicate paid work, lost
work, unrecoverable state, or an unsafe experiment cutover. Do not manufacture
findings for volume, and do not re-litigate an owner-resolved choice unless
current evidence shows that choice cannot satisfy its stated contract.

This is one of two independent whole-system convergence reviews. Your primary
lens is live code and dependency feasibility, but your scope is the complete
proposed system rather than only `dr-platform`.

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

Treat those as the named baseline, not an excuse to inspect stale code. At the
start of the review, record each current HEAD, branch, and dirty/clean status.
If a repository has moved, audit the current working tree and identify drift
that changes a plan claim or contract. Do not include unrelated local changes
in a proposed correction.

## Required inspection surface

Read the plan, index, both glossaries, all applicable ADRs, and the v0 unified
feedback before forming findings. Then inspect current code, tests,
dependencies, configuration, and directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository that current imports, package metadata, workflow
  names, schemas, exports, or deployment configuration prove is directly
  affected.

Inspect the installed DBOS 2.26 API and implementation actually selected by
the repository environment. Verify signatures and semantics rather than
reasoning from memory or newer online docs.

## Audit questions

The list is a floor, not a checklist ceiling:

1. **Factual and blast-radius audit.** Verify claimed callers, deletions,
   renames, dependencies, workflow names, schema names, and current Whetstone
   generation/scoring behavior. Find consumers or durable contracts the plan
   missed.
2. **DBOS feasibility.** Verify queue configuration, priority behavior,
   enqueue options, workflow IDs, recovery/introspection, cancellation,
   workflow attributes, transaction integration, and system-table reads
   against installed DBOS 2.26. Flag any contract that requires an API DBOS
   does not expose or semantics it does not guarantee.
3. **Transactions and concurrency.** Trace registration, Item/Attempt claims,
   enqueue, reconciliation, retry, cancellation, missing detection, Operation
   aggregation, and export barriers. Check every CAS predicate, unique/FK/check
   constraint, lock boundary, commit point, crash window, and retry path.
4. **Identity and idempotency.** Prove that content-scoped execution identity,
   platform-owned attempt ordinals, operation references, generation/scoring
   linking, and workflow IDs prevent duplicate paid work without suppressing
   valid retries or rescoring.
5. **Scheduling and pacing.** Verify mandatory deterministic shuffle is
   preserved through 500-Item paging and enqueue order, independently of
   urgency. Test the plan's assumptions about DBOS FIFO/priority behavior,
   multi-domain slot occupancy, runtime concurrency, and pacing state.
6. **Execution versus domain outcome.** Check that generation and scoring can
   share the Operation lifecycle while retaining typed domain results,
   append-only provenance, rescore behavior, recovery, and accounting. Find
   any surviving Whetstone control plane that conflicts with the target.
7. **Export and analysis stores.** Validate `change_seq`, snapshot/barrier
   ordering, staging/promotion, destination-local cursors, full-rebuild versus
   incremental equivalence, root-cascade detail sampling, DBOS payload
   exclusion, and retry behavior for DuckDB, MotherDuck, and Neon.
8. **Unitbench and deployment.** Verify local native DuckDB versus deployed
   MotherDuck/Neon adapter feasibility, remote-compute policy gates, Vercel
   constraints, query/schema parity, secrets, and the clean-cut assumptions.
9. **Acceptance and operability.** Determine whether the listed tests,
   inspector/control surfaces, observability, health checks, migration order,
   rollback rules, and pre-experiment gates can actually prove the stated
   invariants.
10. **V0 coverage.** Compare v1 against every actionable v0 unified-feedback
    item in its preserved priority order. Report any item that is only claimed
    resolved but remains mechanically or semantically open.

## Review rules

- Read-only review: do not edit plan, code, ADRs, or context documents; do not
  commit, stage, push, or create branches.
- Git status, branch, log, and revision inspection are allowed and required.
- Static inspection, fast searches, installed-package introspection, and
  focused read-only commands are allowed. Do not run broad suites or commands
  expected to exceed about two minutes.
- Ground every finding in both the relevant plan/ADR contract and current
  file-and-line or installed-API evidence. A speculative risk without evidence
  belongs under verification gaps, not as a defect.
- Rank findings by consequence, not ease of repair. Merge duplicates and
  distinguish a plan defect from an implementation task the plan already
  specifies correctly.
- Classify each finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.

## Output

Write only the review result to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/codex-findings.md`

Use this structure:

```markdown
# V1 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** YYYY-MM-DD
- **dr-platform:** <branch>, <HEAD>, <dirty/clean>
- **whetstone-ai:** <branch>, <HEAD>, <dirty/clean>
- **unitbench:** <branch>, <HEAD>, <dirty/clean>
- **DBOS:** <installed version and inspected package path>

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Class:** architecture-changing | owner-decision | local-correction | verification-gap
- **Plan contract:** §x.y and/or ADR NNNN
- **Evidence:** absolute/path:line — what current code or API actually does
- **Consequence:** concrete failure if implementation follows v1
- **Required plan change:** exact contract or step that must change

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
