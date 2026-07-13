# V6 hybrid whole-system convergence review — code and dependency audit (Codex 5.6, `sol`, high)

**Execution precondition:** this prompt is prepared for a future review. V6 is
currently `draft`; do not run this review until the effort index and `plan.md`
are `in-review`, every manifest-declared document is frozen without content
edits, and the future `reviews/review-baseline.json` has been captured.
Immediately before review, run and require success from:

```bash
uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py \
  /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6 \
  --require-prompts --require-baseline
```

Abort if the manifest, any declared document, either prompt, any repository
revision or material dirty-path set, the reviewer configuration/output path,
or the installed DBOS package differs from the issued baseline. Never review
a drifted target.

Adversarially review the complete frozen v6 Platform/Whetstone system. Your
primary lens is live code, dependency and installed-API feasibility,
transactions, concurrency, and cross-repository blast radius. Prioritize
silent wrongness, lost or duplicated paid work, stale acceptance/publication,
unsafe cancellation, irrecoverable state, and cutover gates that cannot prove
their invariant. Do not manufacture findings or re-litigate an owner choice
unless evidence shows it is contradictory or infeasible.

This is one of two hybrid whole-system reviews. Its fresh and closure phases
share one model context, so independence is weaker than two isolated passes.
Record that limitation in the findings. Stabilize and preserve the fresh
finding set and severity order before opening closure material; closure
evidence may add findings but must not alter, normalize, or erase the fresh
set.

## Frozen review target

- Manifest:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/plan-manifest.json`
- Normative documents in the manifest's exact ordered read sequence:
  1. `plan.md` — system overview, invariants, lifecycle, phase gates, and review protocol;
  2. `contracts/platform.md` — dr-platform kernel contract;
  3. `contracts/whetstone.md` — Whetstone domain and platform-boundary contract;
  4. `contracts/publication.md` — export, publication, and two-plane reader contract; and
  5. `contracts/delivery.md` — migration, verification, cutover, rollback, and deferral contract.
- Closure-only declared document: `traceability.md` — prior-review
  provenance, owner decisions, revision history, and source coverage.
- Effort index:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- Closure-only v5 synthesis:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/unified-feedback.md`
- Closure-only v5 source findings:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/fable-findings.md`
- Canonical vocabularies:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md` and
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`.
- Canonical decisions:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md`
  through
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0021-append-only-enqueue-claim-identity.md`.

V0 through v5, all earlier review packets, and frozen v6 are immutable during
this review.

## Issued baseline

The future file
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/review-baseline.json`
is the **exclusive issuance truth**. Do not infer issuance from this prompt,
conversation history, live Git state, traceability, or an earlier packet. It
must hash `plan-manifest.json`, every declared document in manifest order,
both prompts, repository branches/HEADs/material dirty paths, and the selected
DBOS version, installed path, and content. It must identify this lane's
reviewer/model/effort and exact findings path.

After successful validation, independently calculate and report the SHA-256
digests of `review-baseline.json` and `plan-manifest.json`. Any mismatch is an
abort, including repository, output-path, reviewer, or DBOS mismatch.

## Required inspection surface and phase order

### Phase 1 — fresh normative audit

Read `plan-manifest.json`, then only the five normative documents in their
declared order. Read the effort index, both canonical vocabularies, applicable
ADRs, and live evidence. Do **not** open `traceability.md`, any v5 prompt,
finding, synthesis, or older review material until the fresh findings and
severities are stabilized.

Independently inspect current code, tests, dependency declarations, lockfiles,
configuration, migrations, docs, CI, and deployment/runtime surfaces in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository proved affected by imports, package metadata,
  durable names, schemas, exports, CI, or deployment configuration.

Inspect the exact installed DBOS package selected by the baseline, expected to
be DBOS 2.26.0 under the recorded `dr-platform/.venv` path. Verify public
signatures, persisted statuses, queue priority/dequeue behavior, workflow
identity/options/attributes, payload-loading defaults, cancellation/topology,
and every allowlisted system-schema assumption from installed source. Do not
substitute memory, web documentation, or a different version.

The fresh audit must cover at least:

1. Current callers and blast radius for every deletion, rename, export,
   dependency/pin, workflow/queue/table/durable name, schema, CI/auth change,
   Whetstone generation/rescore behavior, Unitbench query, and Vercel runtime.
2. Manifest registration, recipe/target identity, exact equality, paging,
   Leases/cursors/completion CAS, crash resume, and enqueue prohibition before
   completion.
3. Attempt authority, request-ledger idempotency, automatic/requested bounds,
   concurrent requests, content identity, foreign cancellation provenance,
   and aggregate reactivation.
4. DBOS feasibility: normalized states, priority-enabled queues, enqueue
   identity/options, immutable attributes, payload-disabled reads,
   payload-excluding step inspection, top-level-only workflows, and strictly
   non-recursive cancellation.
5. Transaction and crash correctness across registration, Claim/enqueue,
   reconciliation, retry, cancellation, compensation, aggregation,
   Experiment evaluation/promotion, export, and destination publication.
   Check every lock order, CAS/unique/FK/check predicate, external-call
   boundary, and recovery owner.
6. Append-only Claim identity: every expired, replaced, invalidated, and
   call-started Claim remains reconstructible by `(item_id, attempt,
   claim_id)` after Attempt terminalization; compensation exact replay,
   multiple stale claimants, and Claim replacement cannot invent or lose the
   causal key.
7. Reference-safe compensation: both claimant and reconciliation issuers take
   the workflow-reference/Operation lock order, recheck exclusivity, persist
   `SKIPPED_SHARED` without physical cancellation, and reach bounded,
   replay-safe `NO_WORKFLOW_FOUND` resolution without permanently blocking a
   legitimate new reference.
8. Strict acceptance readiness: all accepted-relationship Operations are
   terminal, every selected domain winner binds its exact terminal platform
   Attempt in `SUCCEEDED` with DBOS `SUCCESS`, and persistence-before-return,
   crash, recovery exhaustion, terminalization races, and platform-cut races
   cannot expose a false current pointer.
9. Run-pinned scoring: each cell first pins the selected accepted Generation
   Run, other-run candidates persist as `SUPERSEDED_GENERATION`, and only then
   newest-successful-relationship/highest-successful-Attempt reduction is
   total for stale-run, regeneration, equal-ordinal, concurrent, and
   populated-`PARTIAL`/`SUCCESS` coexistence scenarios.
10. Owner-selected populated-only `PARTIAL` behavior: the exact persisted
    predicate `terminal_submission_text IS NOT NULL AND
    terminal_submission_text ~ '[^[:space:]]'` governs selection digest,
    Item identity, Manifest membership, retry, outcomes, and the deliberately
    narrowed deletion-parity boundary for empty, space-only, tab-only,
    newline-only, and populated rows. It remains separate from strict
    Generation acceptance.
11. Experiment identity/currentness, singular Generation membership,
    pre-scoring durable `PARTIAL`, missing-cell representation, ordered
    Scoring relationships, append-only evaluation, Operation-before-
    Experiment locking, and atomic promotion/read cut checks.
12. Publication and secrets: source barrier/change sequence, destination
    Leases/fences, monotonic promotion, stale-writer rejection, atomic
    Analysis/Detail bundles, cursor commits, root-cascade completeness,
    skew policy, payload exclusion, and server-only DSNs.
13. Operability and cutover: bounded inspection/control/reconciliation,
    paid-call overlap/accounting honesty, phase dependencies, deletion gates,
    private dependency auth, live MotherDuck/Neon/DuckDB/Vercel obligations,
    COPRO and zero-spend continuity, and rollback without mixed schemas.

Explicitly ask whether v6 finally closes the two repeatedly concentrated
boundaries: Claim/cancellation safety and strict Experiment acceptance. A new
architecture-changing defect in either boundary, or in a subsystem earlier
reviews treated as settled, is evidence of **possible non-convergence**. Call
it out under that term and explain the repeated or reopened invariant; do not
automatically normalize it as an expected local correction merely because v6
is described as bounded.

### Phase 2 — strict-inclusive v5 closure

Only after Phase 1 is stable, open `traceability.md`, the v5 unified feedback,
and both v5 source-findings files. Reconstruct every source concern and prove
closure against normative v6, live code and dependencies, the vocabularies,
and ADRs. Traceability is provenance, not proof. Preserve disagreements and
source attribution.

Give a separate closure row to all five v5 source findings:

1. Codex F1 — Claim identity needed for claimant-death compensation is lost
   after terminalization or Claim replacement (v5 A1);
2. Codex F2 — strict acceptance can promote before exact platform/DBOS
   terminal success (v5 L1);
3. Codex F3 — populated-only `PARTIAL` eligibility contradicts legacy
   status-only candidate parity (resolved by V5-OD1);
4. Fable F1 — scoring-cell selection is neither pinned to the accepted
   Generation Run nor total over run-distinct candidates (v5 L2); and
5. Fable F2 — compensation can cancel content-shared work and lacks a
   decidable missing-workflow hazard resolution (v5 L3).

Separately prove V5-OD1 itself: populated-only selection is one exact,
end-to-end observable rule and the legacy parity claim is explicitly narrowed.
Regression-audit every v5 row that declared a v4/v3/v2/v1 concern closed or
retained as a verification gate. Every `no`, contradiction, architecture-
level uncertainty, or reopened settled subsystem must appear as a ranked
finding and in the closure matrices.

## Review rules

- This is read-only except for one output. Write only
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/codex-findings.md`.
  Do not create repository scratch files or mutate code, plans, manifests,
  traceability, prompts, baselines, the other findings lane, synthesis,
  indexes, ADRs, vocabularies, configs, locks, or any earlier packet.
- Do not stage, commit, push, create/switch branches, or create worktrees.
  Read-only Git inspection is required.
- Use live code and installed DBOS evidence. Focused read-only commands are
  allowed; do not run broad or long suites.
- Each defect needs a normative contract, concrete failure scenario, and
  exact absolute file:line or installed-API evidence. Unsupported risks go in
  `Unverified` with the exact proof required.
- Rank by consequence, merge only true duplicates, and distinguish design
  defects from implementation work and correctly retained gates.
- Use exactly one class: `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.

## Output

Write exactly one file:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/codex-findings.md`

Use this validator-compatible structure. Every numbered finding must repeat
the complete exact label set:

```markdown
# V6 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** YYYY-MM-DD
- **Packet validation:** <exact successful command>
- **Review baseline SHA-256:** <digest>
- **Plan manifest SHA-256:** <digest>
- **Independence limitation:** hybrid fresh and closure phases shared one model context
- **dr-platform:** <branch, HEAD, dirty/clean and relevant drift>
- **whetstone-ai:** <branch, HEAD, dirty/clean and relevant drift>
- **unitbench:** <branch, HEAD, dirty/clean and relevant drift>
- **DBOS:** <installed version and inspected package path>

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Class:** architecture-changing | owner-decision | local-correction | verification-gap
- **Plan contract:** <normative path and heading and/or ADR>
- **Failure scenario:** <concrete violating sequence>
- **Evidence:** <absolute path:line or installed API evidence>
- **Affected repositories:** <exact repositories and boundary>
- **Required correction:** <exact invariant/schema/transaction/step change>
- **Closure impact:** <v5 source finding, V5-OD1, or earlier row reopened; otherwise none>
- **Convergence signal:** possible non-convergence | bounded new issue | none

## V5 source-finding closure
| V5 source finding | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| Codex F1 — durable Claim identity | yes/no | normative and live evidence; every no is a finding |
| Codex F2 — terminal execution before strict promotion | yes/no | normative and live evidence; every no is a finding |
| Codex F3 — populated-PARTIAL parity contradiction | yes/no | normative and live evidence; every no is a finding |
| Fable F1 — accepted-run-pinned total scoring selection | yes/no | normative and live evidence; every no is a finding |
| Fable F2 — reference-safe compensation and hazard resolution | yes/no | normative and live evidence; every no is a finding |

## V5 owner-decision closure
| Item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| V5-OD1 — populated-only PARTIAL selection | yes/no | normative and live evidence; every no is a finding |

## Earlier closure regression audit
| Prior item | Still closed? | Evidence or remaining gap |
| --- | --- | --- |
| every v5 prior-closure row and retained owner policy | yes/no | evidence; every no is a finding |

## Concentrated-boundary convergence assessment
- **Cancellation boundary:** converged | possible non-convergence — <evidence>
- **Acceptance boundary:** converged | possible non-convergence — <evidence>
- **Previously settled subsystems:** stable | possible non-convergence — <evidence>

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <mechanical gate conditions>
- **Unverified:** <bounded gaps and exact proof needed>
```

Select `REPEAT_CONVERGENCE` if any blocker/P0 remains, an owner decision is
unresolved, a finding changes architecture, identity, ownership, persistence,
lifecycle, state, or a cross-repository interface, a fundamental transaction/
concurrency/publication/export/cutover redesign remains, a core invariant is
contradictory, or possible non-convergence is evidenced by an architecture
finding in a concentrated or previously settled boundary. Select
`READY_FOR_FOCUSED_AUDITS` only when all five v5 source findings and V5-OD1
are closed with normative and live evidence, no owner/architecture decision
remains, the core state/ownership model is stable, and all remaining work is a
local correction or bounded verification gap.
