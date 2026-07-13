# V5 hybrid whole-system convergence review — code and dependency audit (Codex 5.6, `sol`, high)

**Execution precondition:** this prompt is prepared for a future review. V5 is
currently `draft`; do not run this review until the effort index and `plan.md`
are `in-review`, the packet is frozen without content edits, and the future
`reviews/review-baseline.json` has been captured. Immediately before review,
run and require success from:

```bash
uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py \
  /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5 \
  --require-prompts --require-baseline
```

Abort if the manifest, a declared document, either prompt, a repository
revision or material dirty-path set, or the installed DBOS package differs from
the issued baseline. Do not review a drifted target.

Adversarially review the complete frozen v5 Platform/Whetstone system. Your
primary lens is live code, installed dependencies, transactions, concurrency,
and cross-repository feasibility. Prioritize silent wrongness, lost or
duplicated work, stale acceptance/publication, unsafe paid-work handling,
credential exposure, irrecoverable state, and cutover gates that cannot prove
their invariant. Do not manufacture findings or re-litigate an owner choice
unless evidence shows it is contradictory or infeasible.

This is one of two hybrid whole-system reviews. It combines two phases in one
model context, so it is less independent than two isolated passes. Record that
shared-context independence limitation in the findings. Stabilize the fresh
findings before opening closure material; closure evidence may add findings but
must not alter or erase the fresh set.

## Frozen review target

- Manifest:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/plan-manifest.json`
- Normative documents, in the manifest's exact ordered read sequence:
  1. `plan.md` — system overview, invariants, lifecycle, phase gates, and review protocol;
  2. `contracts/platform.md` — dr-platform kernel contract;
  3. `contracts/whetstone.md` — Whetstone domain and platform-boundary contract;
  4. `contracts/publication.md` — export, publication, and two-plane reader contract; and
  5. `contracts/delivery.md` — migration, verification, cutover, rollback, and deferral contract.
- Closure-only declared document: `traceability.md` — v4 closure, owner
  decisions, earlier provenance, revision history, and source coverage.
- Effort index:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- Closure-only v4 synthesis:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/unified-feedback.md`
- Closure-only v4 source findings:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/fable-findings.md`
- Canonical vocabularies:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md` and
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Canonical decisions: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md`
  through `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0020-append-only-experiment-acceptance.md`.

V0 through v4, all earlier review packets, and the frozen v5 target are
immutable during the review.

## Issued baseline

The future file
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/review-baseline.json`
is the **exclusive issued baseline**. Do not infer issuance from this prompt,
conversation history, current Git state, traceability, or an earlier packet.
It must hash `plan-manifest.json`, every declared document in manifest order,
both prompts, repository branches/HEADs/material dirty paths, and the selected
DBOS version, installed path, and content digest. It must identify this lane's
reviewer/model/effort and exact findings path.

After validation, independently calculate and report the SHA-256 digests of
`review-baseline.json` and `plan-manifest.json`. Any mismatch is an abort,
including a repository, output-path, reviewer, or DBOS mismatch.

## Required inspection surface and phase order

### Phase 1 — fresh normative audit

Read `plan-manifest.json`, then only the five normative documents in their
declared order. Read the effort index, both glossaries, applicable ADRs, and
live evidence. Do **not** open `traceability.md`, v4 prompts, v4 findings, v4
unified feedback, or older review material until fresh findings and severities
are stabilized.

Independently inspect current code, tests, dependency declarations, lockfiles,
configuration, migrations, docs, and deployment/runtime surfaces in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository that live imports, package metadata, durable names,
  schemas, exports, CI, or deployment configuration prove is affected.

Inspect the exact installed DBOS package selected by the baseline, expected to
be DBOS 2.26.0 under the recorded `dr-platform/.venv` path. Verify behavior
from installed source: public signatures, persisted statuses, queue priority
configuration and dequeue order, workflow ID/options and attributes, client
payload-loading defaults, cancellation/topology behavior, and every
allowlisted system-schema assumption. Do not substitute memory or newer docs.

The audit must cover at least:

1. Current callers and blast radius for every deletion, rename, public export,
   dependency/pin, workflow/queue/table/durable name, schema, CI/auth change,
   Whetstone generation/rescore behavior, Unitbench query, and Vercel runtime.
2. Manifest/target correctness: digest implementability with `dr-serialize`,
   recipe completeness, exact equality, page/Lease/cursor/completion CAS,
   RegistrationHook atomicity, crash resume, target resolution in a fresh
   process, and enqueue prohibition before completion.
3. Attempt authority and idempotency: automatic versus requested Attempts,
   request ledgers, source-state and maximum-bound math, concurrent requests,
   content identity, foreign-cancellation provenance, and aggregate reactivation.
4. DBOS feasibility: normalized statuses, priority-enabled queues, enqueue
   identity/options, immutable execution attributes, payload-disabled reads,
   payload-excluding step inspection, top-level-only workflows, and strictly
   non-recursive cancellation.
5. Transaction/concurrency correctness across registration, Claim/enqueue,
   reconciliation, retry, cancellation, claimant-death compensation,
   Operation aggregation, Experiment invalidation/evaluation/promotion, and
   destination publication. Check every lock order, isolation assumption,
   CAS/unique/FK/check predicate, external-call boundary, and crash window.
6. The v5 Generation membership rule: exactly one accepted Generation
   Operation/Manifest per Experiment, exact replay, typed unequal conflict,
   immutable membership, new Experiment identity for growth, and a truly
   Item-lineage-local highest-successful-Attempt selection.
7. The v5 scoring rule: monotonic accepted-Scoring-relationship ordinals,
   newest relationship containing a success then highest successful Attempt,
   behavior for failure-success, success-success, equal Attempt ordinals with
   different recipes, concurrent acceptance, complete candidate/supersession
   provenance, and identity sensitivity to ordered relationships and inputs.
8. Populated `PARTIAL` Generation Run parity: current selector equivalence,
   scoring eligibility, separation from strict Generation acceptance, and the
   deletion gate retaining the old path until parity is demonstrated.
9. Pre-scoring evaluation: valid canonical empty scoring set, durable
   `PARTIAL`, explicit `MISSING_SCORE` rows, correct identity/cuts, later
   relationship invalidation, and append-only replacement without rewrite.
10. Cancellation and paid work: reference locks, exclusivity, racing links,
    Claim invalidation, `NOT_ENQUEUED`, late enqueue, claimant death after
    enqueue before losing CAS, bounded discovery and idempotent compensation,
    logical provider cancellation, accepted undercount, and immutable terminal
    outcomes without a false upstream-abort or total-billing claim.
11. Acceptance currentness and identity: Experiment-scoped Prediction identity,
    one owning Experiment, domain source version, sorted platform cut vector,
    Operation-before-Experiment locking, promotion/read checks, missing cells,
    strict versus override policy, and stale-pointer races.
12. Export/publication and secrets: source barrier/change sequence, destination
    leases/fences, monotonic promotion, A(H1)/B(H2) stale-writer rejection,
    atomic Analysis/Detail bundles, kernel cursor atomicity, skew policies,
    root-cascade detail, safe DBOS telemetry, MotherDuck/Neon/DuckDB feasibility,
    and server-only Unitbench adapters.
13. Scheduling, pacing, inspection, cutover, rollback, phase ordering, live
    verification gates, COPRO and zero-spend wait/export/pinned-read flow, and
    whether every deletion is sequenced after its replacement proof.

### Phase 2 — strict-inclusive v4 closure

Only after Phase 1 is stable, read `traceability.md`, then v4 unified feedback
and both v4 source findings. The v4 synthesis is not an answer key. Prove
closure from v5 normative text, live evidence, glossaries, and ADRs. Every
`no`, contradiction, or unverifiable closure claim must appear as a ranked
finding as appropriate.

Give every v4 source finding a closure row, including duplicate source items:

1. Codex F1 and Fable F1 — singular/plural Generation membership and
   cross-Operation Attempt ordering (A1/OD1);
2. Codex F2 — deterministic Score Attempt selection (OD2);
3. Codex F3 — populated `PARTIAL` rescore parity (OD3);
4. Fable F2 — claimant death after enqueue before outcome CAS (L1);
5. Fable F3 — Experiment-scoped Prediction identity (L2);
6. Fable F4 — pre-scoring evaluation semantics (OD4); and
7. Fable F5 — the retained `classify_error` seam (L3).

Explicitly verify all four owner decisions selected for v5: singular fixed
Generation membership; deterministic ordered Scoring-relationship selection;
populated-`PARTIAL` scoring eligibility distinct from strict acceptance; and
durable pre-scoring `PARTIAL` evaluation. Verify L1–L3 independently:
claimant-death compensation discovery, Experiment scope pinned in
`prediction_id`, and `enqueue_failure_from_whetstone_exception` retained as the
named injected classifier. Also prove every v4 prior-closure row remains
closed; a v5 correction must not regress v3/v2/v1 closure or accepted owner
policies. Closure claims in traceability are evidence pointers, not proof.

## Review rules

- Read-only review except for the one required findings file. Write only
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/codex-findings.md`.
  Do not create repository scratch files or mutate code, plans, manifests,
  traceability, prompts, baselines, findings from another lane, synthesis,
  indexes, ADRs, glossaries, configs, locks, or earlier packets.
- Do not stage, commit, push, create/switch branches, or create worktrees.
  Read-only Git inspection is required.
- Focused searches and installed-package introspection are allowed. Do not run
  broad suites or commands expected to exceed roughly two minutes.
- Ground each defect in a normative contract and precise live file/line or
  installed-API evidence. Put unsupported but plausible risks in `Unverified`.
- Merge true duplicates. Distinguish a plan defect from implementation work or
  a correctly retained live verification gate.
- Rank by consequence. Use exactly one class: `architecture-changing`,
  `owner-decision`, `local-correction`, or `verification-gap`.

## Output

Write exactly one file:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/codex-findings.md`

Use this validator-compatible structure. Repeat the complete exact label set
for every numbered finding:

```markdown
# V5 convergence findings — Codex 5.6 code and dependency audit

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
- **Closure impact:** <v4 source finding, owner decision, L1-L3, or prior row reopened; otherwise none>

## V4 source-finding closure
| V4 source finding | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| Codex F1 | yes/no | normative and live evidence; every no is a finding |
| Codex F2 | yes/no | normative and live evidence; every no is a finding |
| Codex F3 | yes/no | normative and live evidence; every no is a finding |
| Fable F1 | yes/no | normative and live evidence; every no is a finding |
| Fable F2 | yes/no | normative and live evidence; every no is a finding |
| Fable F3 | yes/no | normative and live evidence; every no is a finding |
| Fable F4 | yes/no | normative and live evidence; every no is a finding |
| Fable F5 | yes/no | normative and live evidence; every no is a finding |

## V5 owner-decision and local-correction closure
| Item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| OD1 — singular Generation membership | yes/no | normative and live evidence; every no is a finding |
| OD2 — deterministic scoring selection | yes/no | normative and live evidence; every no is a finding |
| OD3 — populated-PARTIAL scoring eligibility | yes/no | normative and live evidence; every no is a finding |
| OD4 — durable pre-scoring PARTIAL evaluation | yes/no | normative and live evidence; every no is a finding |
| L1 — claimant-death compensation | yes/no | normative and live evidence; every no is a finding |
| L2 — Experiment-scoped Prediction identity | yes/no | normative and live evidence; every no is a finding |
| L3 — retained classifier seam | yes/no | normative and live evidence; every no is a finding |

## Earlier closure regression audit
| Prior item | Still closed? | Evidence or remaining gap |
| --- | --- | --- |
| every v4 prior-closure row and retained owner policy | yes/no | evidence; every no is a finding |

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <mechanical gate conditions>
- **Unverified:** <bounded gaps and exact proof needed>
```

Select `REPEAT_CONVERGENCE` if any blocker/P0 remains, any owner decision is
unresolved, a finding changes architecture, identity, ownership, persistence,
lifecycle, state, or a cross-repository interface, a fundamental transaction/
concurrency/publication/export/cutover redesign remains, or a contradiction
makes a core invariant infeasible. Select `READY_FOR_FOCUSED_AUDITS` only when
every previous P0 and every v4 source finding is closed with evidence, all four
owner decisions and L1–L3 are coherent, no owner/architecture decision remains,
the core state/ownership model is stable, and all remaining work is a local
correction or bounded verification gap.
