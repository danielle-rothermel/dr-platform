# V3 hybrid whole-system convergence review — code and dependency audit (Codex 5.6, `sol`, high)

**Execution precondition:** run this prompt only after the effort index and v3
plan status are changed from `draft` to `in-review` without changing plan
content. If the plan content or repository revisions move, refresh the baseline
before review rather than auditing a mutable target.

Review the frozen v3 Platform and Whetstone refactor plan adversarially. Find
places where it is factually wrong, infeasible against current APIs,
underspecified at a transaction/concurrency boundary, or inconsistent across
repositories. Prioritize defects that can cause silent wrongness, unaccounted
paid work, lost work, stale publication, credential exposure, unrecoverable
state, or an unsafe experiment cutover. Do not manufacture findings for
volume, and do not re-litigate an owner-resolved choice unless current evidence
shows that choice cannot satisfy its stated invariant.

This is one of two independent hybrid whole-system convergence reviews. Your
primary lens is live code, dependency, transaction, and runtime feasibility,
but your scope is the complete proposed system. The review is hybrid:

1. first reconstruct and audit v3 as a fresh design, without using the v2
   findings as an answer key; then
2. explicitly prove or disprove closure of every v2 strict-inclusive P0/P1/P2
   item, every v1 disposition, and all five v3 owner decisions.

The closure pass is a floor, not a limit. New defects count even when every v2
item is nominally addressed.

Two owner choices intentionally narrow earlier synthesis invariants: duplicate
provider spend may occur when a replacement overlaps a logically cancelled
synchronous call, and final DBOS order may vary among same-priority,
same-millisecond ties. Do not report either accepted behavior as a finding by
itself. Do report any contradiction, hidden/unaccounted overlap, false claim of
provider abort, failure to preserve deterministic kernel mixing, or another
stated invariant those choices make infeasible.

## Frozen review target

- Plan: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md`
- Effort index and review gates: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- V2 strict-inclusive unified feedback, for correction closure:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/reviews/unified-feedback.md`
- V2 source findings, for evidence, disagreement, and priority provenance:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/reviews/fable-findings.md`
- V1 unified feedback, for historical disposition closure:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/unified-feedback.md`
- Canonical platform vocabulary:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md`
- Canonical Whetstone vocabulary:
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Canonical decisions:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md`
  through
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0020-append-only-experiment-acceptance.md`

V0, v1, v2, their review packets, and the reviewed v3 target are immutable
during this review.

## Issued baseline

The prompt was issued on 2026-07-10 against:

- `dr-platform`: branch `07-08-refactor`,
  `7b9b340fd8f2717e44de36804396077b7beeb661`;
- `whetstone-ai`: branch `codex/versioned-planning-docs`,
  `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`;
- `unitbench`: branch `codex/versioned-planning-docs`,
  `cafd493ab9e9c1940106037209b1b218097f847e`; and
- installed DBOS 2.26.0 at
  `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`.

At issuance, `dr-platform` is intentionally dirty only in the approved v3
plan/index, canonical glossary/ADR updates, immutable v1/v2 review results, and
the two v3 prompts; no application code is modified. `whetstone-ai` is intentionally
dirty only in its approved canonical `CONTEXT.md` update. `unitbench` is clean.
The two v3 prompt files themselves become expected review-packet drift.

At review start, record each current HEAD, branch, and full dirty/clean status.
If a repository moved, audit the current working tree and identify drift that
changes a plan claim. Do not treat the approved planning/canonical-doc drift as
an implementation, and do not fold unrelated local changes into a proposed
correction.

## Required inspection surface

Read v3, the index, both glossaries, all applicable ADRs, and then perform the
fresh audit before using the v2 packet for the explicit closure pass. Inspect
current code, tests, dependencies, lockfiles, configuration, migrations, and
directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository that imports/package metadata, workflow names,
  schemas, exports, or deployment configuration prove is directly affected.

Inspect the installed DBOS 2.26.0 API and implementation actually selected by
the repository environment. Verify signatures, status values, queue ordering,
attribute behavior, client load defaults, cancellation, and system-schema
assumptions from the installed package rather than memory or newer docs.

## Fresh audit questions

The list is a floor, not a checklist ceiling:

1. **Factual and blast-radius audit.** Verify every claimed caller, deletion,
   rename, dependency, pinned revision, workflow/queue name, schema/table name,
   current Whetstone generation/scoring behavior, rescore-selection behavior,
   Unitbench query surface, and Vercel/runtime assumption. Find consumers or
   durable contracts v3 missed.
2. **Manifest registration.** Prove the caller-prepared Manifest recipe is
   deterministic and implementable with current `dr-serialize`; exact equality
   is sufficient; page descriptors cannot be reordered/truncated; registrar
   Lease/token/cursor and completion CAS have complete predicates; hook and Item
   insertion share the promised transaction; crash/expiry recovery cannot
   duplicate domain rows; and enqueue is impossible before completion. Prove
   concrete Item execution recipes cover full canonical domain input and every
   behavior-affecting version, their ordered Operation aggregate is
   non-circular, cross-Operation dedup remains valid, and `ALREADY_PRESENT`
   cannot accept unequal domain content.
3. **Next-Attempt transition.** Trace eligible source states/reasons, request
   identity, request-ledger uniqueness, maximum-attempt math, exact Item CAS,
   identical/different concurrent requests, source advancement, Operation
   reactivation, cancellation provenance, and content identity. Prove current
   Generation Run and Score Attempt recipes can map the platform ordinal
   one-to-one without suppressing valid regeneration/rescoring. Verify the
   optional request bound only tightens immutable policy and foreign
   cancellation provenance cannot authorize work without new confirmation.
4. **DBOS feasibility.** Verify normalized statuses, queue registration and
   `priority_enabled`, enqueue identity/options, execution-scoped attributes,
   safe `load_input=False`/`load_output=False` inspection, workflow/step
   introspection, top-level-only topology, and non-recursive cancellation
   against installed DBOS 2.26.0. Flag any required API or atomicity DBOS does
   not expose.
5. **Reference-aware cancellation.** Trace candidate workflow discovery,
   workflow advisory locks, Operation/Item/Attempt lock order, the exclusivity
   predicate, unresolved DBOS-cancellation guards, new-reference races, partial
   DBOS failure, foreign cancellation, repeat requests, late terminal results,
   topology drift, and explicit later-Attempt authorization. Prove
   `cancel_children=True` is never needed or called, and prove accepted paid
   overlap is visible/accounted without being mistaken for upstream abort.
6. **Transactions and aggregate correctness.** Check every lock order, row
   predicate, unique/FK/check constraint, isolation assumption, commit point,
   crash window, and retry path across registration, enqueue, reconciliation,
   automatic retry, requested Attempts, cancellation, and stored Operation
   aggregation. Test the total status precedence against overlapping states and
   the last-two-Items-finish race.
7. **Identity and correlation.** Prove the separation between DBOS execution
   identity and authoritative many-Operation references. Verify attributes are
   immutable execution facts, platform lookups remain complete, and shared
   workflows do not lose searchable references or corrupt local outcomes.
8. **Scheduling and pacing.** Verify Manifest/page boundaries preserve
   deterministic shuffle; service urgency remains independent; DBOS 2.26.0
   same-Service-Class/same-timestamp behavior matches the explicitly accepted
   tie-local nondeterminism; requested/effective priority is inspectable;
   concurrent Operations do not reintroduce harmful model blocking; and multi-domain
   workflow sleeps remain honestly bounded and observable.
9. **Export consistency and publication.** Independently validate `change_seq`,
   the source Export Barrier, internal writer-lock ownership, source
   `snapshot_seq`, destination/bundle Lease acquisition and renewal, fencing
   token allocation, monotonic promotion CAS, stage cleanup ownership,
   full-rebuild ordering, and crash behavior. Prove A(H1), B(H2), B-promotes,
   A-rejected for local DuckDB, MotherDuck, and Neon without assuming an API or
   transaction behavior those stores lack. Prove Analysis and Detail pointers
   cannot expose partial member sets, kernel cursors commit with all tables,
   and cross-family readers implement their declared skew policy.
10. **Two-plane projections and secrets.** Verify Whetstone full snapshots
    build `detail_platform_attempts` with the same Prediction root/snapshot;
    root sampling stays referentially complete; DBOS replay payloads remain
    excluded; workflow arguments no longer need credentials; and normal
    inspector/export paths cannot load or leak inputs, outputs, DSNs, prompts,
    or provider payloads.
11. **Experiment and Unitbench boundary.** Verify strict completeness is
    computable from current domain records and Manifests, partial overrides are
    persisted/stratified/operator-confirmed, platform status remains separate,
   and biased missing results cannot pass. Prove append-only acceptance rows,
   exact multi-Scoring-Operation membership, source-version invalidation, and
   current-pointer CAS cannot publish a stale evaluation. Check local DuckDB versus deployed
    MotherDuck/Neon adapters, remote-compute policy, query/schema parity,
    Vercel Node/runtime bundling, server-only secrets, and independent
    fail-closed behavior.
12. **Cutover and proof.** Walk the migration order, dependency/pin changes,
    fresh schemas, durable names, deletion timing, current rescore parity,
    operator controls, rollback, and every pre-experiment gate. Identify any
   circular sequencing or acceptance test that cannot prove its invariant on
   the named environment. In particular, ensure old analysis helpers survive
   until COPRO and the zero-spend smoke pass typed wait, explicit export, and
   pinned Analysis Bundle reads.

## Explicit v2 strict-inclusive closure pass

After the fresh audit, compare v3 against the v2 synthesis in exact order:

1. P0-1 complete execution recipe and exact domain equality;
2. P0-2 implementable scheduling contract, evaluated against the owner's
   narrowed deterministic-kernel-mixing requirement;
3. P0-3 cancellation semantics, evaluated against accepted paid overlap;
4. P0-4 consumer-visible Publication Bundles and cross-family skew;
5. P0-5 append-only Experiment acceptance and current pointer;
6. P1-6 successive scoring selections;
7. P1-7 foreign-cancelled shared execution;
8. P1-8 total Operation status;
9. P1-9 lifecycle wait and COPRO export/read loop;
10. P1-10 abandoned partial Registration;
11. P1-11 late DBOS terminal result after cancellation intent;
12. P1-12 requested versus effective priority;
13. P1-13 request-ledger maximum bound; and
14. every retained P2 live verification boundary.

Also verify every row of the v1 disposition table and all five v3 owner
decisions across the plan, both glossaries, and ADRs 0001–0020. A topic is not
closed because §4.9/§4.10 says it is; trace the actual contract and evidence.
Report every open or contradictory item as a normal ranked finding and in the
closure matrices.

## Review rules

- Read-only review except for the required findings file: do not edit the
  frozen plan, index, code, ADRs, glossaries, prompts, or earlier review
  packets; do not commit, stage, push, create branches, or create worktrees.
- Git status, branch, log, revision, and read-only diff inspection are allowed
  and required.
- Static inspection, fast searches, installed-package introspection, and
  focused read-only commands are allowed. Do not run broad suites or commands
  expected to exceed about two minutes.
- Ground every finding in the relevant plan/ADR contract and current
  file-and-line or installed-API evidence. A speculative risk without evidence
  belongs under `Unverified`, not as a defect.
- Rank findings by consequence, not repair effort. Merge duplicates and
  distinguish a plan defect from implementation work or a live gate v3 already
  specifies correctly.
- Classify each finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.
- Do not weaken an accepted invariant merely because live credentials are
  unavailable; preserve it as an unverified gate unless evidence disproves the
  design.

## Output

Write the review result—and no other file—to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/reviews/codex-findings.md`

Use this structure:

```markdown
# V3 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** YYYY-MM-DD
- **dr-platform:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **whetstone-ai:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **unitbench:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **DBOS:** <installed version and inspected package path>

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Class:** architecture-changing | owner-decision | local-correction | verification-gap
- **Plan contract:** §x.y and/or ADR NNNN
- **Evidence:** absolute/path:line — what current code or API actually does
- **Consequence:** concrete failure if implementation follows v3
- **Required plan change:** exact contract or step that must change

## V2 strict-inclusive closure
| V2 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P1-13 and P2 | yes/no | plan/ADR/code evidence; every `no` also appears as a finding |

## V1 disposition closure
| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P2-12 | yes/no | verify the retained/extended disposition rather than copying §4.9 |

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <which gate condition is or is not present>
- **Unverified:** <anything not verifiable and why>
```

Use `REPEAT_CONVERGENCE` if any blocker, unresolved owner decision,
architecture-changing finding, persistence/identity/lifecycle redesign, or
changed cross-repository interface remains. Use `READY_FOR_FOCUSED_AUDITS`
only when every v2 P0 is closed, no architecture decision remains, and all
remaining work is local correction or bounded verification.
