# V5 hybrid whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

**Execution precondition:** this prompt is prepared for a future review. V5 is
currently `draft`; run it only after the effort index and `plan.md` are
`in-review`, every manifest-declared document is frozen, and the future
`reviews/review-baseline.json` has been captured. Immediately before review,
run and require success from:

```bash
uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py \
  /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5 \
  --require-prompts --require-baseline
```

Abort on any mismatch in the manifest, declared documents, prompts,
repository revisions/material dirty paths, reviewer configuration, output
path, or installed DBOS package. Never review a drifted target.

Adversarially review the complete frozen v5 system. Your primary lens is
architecture, domain coherence, authority and ownership, lifecycle, and
concrete end-to-end failure scenarios. Prioritize contradictions that permit
silent wrongness, ambiguous truth, lost/duplicated paid work, stale acceptance
or publication, unsafe recovery, or an incoherent cutover. Do not manufacture
findings or re-litigate an owner choice unless a concrete scenario proves it
cannot satisfy its own stated invariant.

This is one of two hybrid whole-system reviews. The fresh and closure phases
share one model context, which is weaker independence than two isolated
reviewers. State that limitation in the findings. Stabilize the fresh finding
set and severity order before opening closure evidence; later material may add
findings but must not rewrite or erase the fresh set.

## Frozen review target

- Manifest:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/plan-manifest.json`
- Normative documents in exact manifest order:
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
- Closure-only source findings:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/fable-findings.md`
- Canonical vocabularies:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md` and
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Canonical decisions: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md`
  through `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0020-append-only-experiment-acceptance.md`.

V0 through v4, all earlier review packets, and frozen v5 are immutable.

## Issued baseline

The future file
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/review-baseline.json`
is the **exclusive issued baseline**. Do not infer the issuance snapshot from
this prompt, conversation history, live Git state, traceability, or prior
packets. It must hash the manifest, every declared document in order, both
prompts, repository branch/HEAD/material dirty paths, and the selected DBOS
version, installed path, and content. It must name Claude Fable 5/high and the
exact findings output path for this lane.

After successful validation, independently record SHA-256 digests for the
baseline and manifest. Any baseline mismatch is an abort condition.

## Required inspection surface and phase order

### Phase 1 — fresh normative audit

Read `plan-manifest.json`, then only the five normative documents in declared
order. Read the effort index, glossaries, applicable ADRs, and live evidence.
Do **not** open `traceability.md` or any v4/older prompt, finding, or synthesis
until the fresh findings and severities have stabilized.

Inspect architecture-relevant live code, tests, schemas, dependencies,
lockfiles, configuration, migrations, docs, and deployments in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository shown by imports, package metadata, durable names,
  schemas, export consumers, CI, or deployment configuration to be affected.

Inspect the exact baseline-selected installed DBOS package, expected to be
2.26.0 in `dr-platform/.venv`, wherever its behavior defines feasibility.
Confirm lifecycle statuses, priority/dequeue semantics, attributes, payload
loading, cancellation/topology, and system-schema assumptions from installed
source rather than memory.

Interrogate at least these architecture questions with concrete failure
walkthroughs:

1. **Ubiquitous language and authority.** Is each identity, ordinal, lifecycle
   transition, source of truth, mutable pointer, publication cursor, and
   operator action owned by exactly one component? Do Operation/Item/Attempt,
   Prediction/Generation Run/Score Attempt/Experiment, DBOS execution, and
   Analysis/Detail concepts remain distinct across all documents and code?
2. **One lifecycle.** Walk new submission through Manifest registration,
   target resolution, Claim/enqueue, reconciliation/retry, domain outcome,
   Experiment evaluation, export, publication, and Unitbench/COPRO reads.
   Find alternative paths, circular prerequisites, ambiguous terminal states,
   and states with no recovery owner.
3. **Restart and identity coherence.** Prove a fresh process can resolve the
   persisted target and recipe, resume or advance work, preserve content
   identity, and avoid embedding domain nouns or secrets in the kernel/DBOS.
4. **Generation membership.** Walk first acceptance, exact replay, unequal
   second relationship, Experiment growth, partial registration, and two
   successful Attempts. Prove exactly one Generation Operation/Manifest is a
   coherent invariant from schema through evaluation and that highest ordinal
   is total within the sole Item lineage.
5. **Scoring ownership and selection.** Walk overlapping logical cells across
   several scoring relationships for success-success, failure-success,
   equal-ordinal/different-recipe, and concurrent evaluation. Prove the
   newest-successful-relationship/highest-successful-Attempt policy is total,
   durable, reproducible, and identity-bearing with no reader recomputation.
6. **`PARTIAL` semantics.** Walk a populated `PARTIAL` Generation Run through
   current rescore candidate selection, scoring execution, strict acceptance,
   and explicit override. Ensure eligibility is preserved without silently
   redefining domain success or the expected matrix.
7. **Pre-scoring Experiment state.** Walk evaluation immediately after the
   Generation relationship and after the first scoring relationship. Prove
   empty scoring membership, missing cells, identity, source invalidation,
   history, and current-pointer semantics are coherent and inspectable.
8. **Acceptance currentness.** Walk late Generation/Score outcomes, required-
   profile changes, next Attempts, cancellation/reconciliation, and races
   between platform mutation and pointer promotion/read. Check Experiment-
   scoped Prediction identity and lock order. No stale evaluation may appear
   current and no domain invalidation may leak into the agnostic kernel.
9. **Cancellation.** Walk exclusive/shared/racing references, cancel during a
   Claim, enqueue after invalidation, claimant death before its losing CAS,
   bounded compensation discovery, late terminal outcomes, foreign
   cancellation, confirmed replacement, and synchronous provider overlap.
   Distinguish logical cancellation, provider abort, outcome-linked accounting,
   and total billing evidence.
10. **Transactions and crash ownership.** For registration, Attempt creation,
    cancellation, aggregation, acceptance, export, and destination promotion,
    identify the authoritative row, lock/CAS/fence, external-call boundary,
    crash successor, replay key, and terminal repair. Look for cross-system
    atomicity claims that no owner can enforce.
11. **Publication and readers.** Walk source cuts, independent kernel/DBOS/
    Analysis/Detail bundles, Lease/fence promotion, root closure, skew policy,
    partial destination failure, stale writers, local/remote compute policy,
    server secrets, and independent Analysis/Detail failure. Ensure consumer
    promises match actual atomic boundaries.
12. **Operability.** Determine whether inspection, health, wait, cancellation,
    retry authorization, reconciliation, export, and rollback let an operator
    diagnose and safely move every nonterminal/degraded state without raw DBOS
    mutation or payload exposure.
13. **Cutover coherence.** Walk phases and deletion gates across dr-platform,
    whetstone-ai, unitbench, exact DBOS 2.26.0, private dependency auth,
    MotherDuck, Neon, DuckDB, Vercel, COPRO, and zero-spend e2e. Ensure old
    mechanisms survive until replacement parity is proven and failures route
    back to design rather than weakening invariants.

### Phase 2 — strict-inclusive v4 closure

Only after Phase 1 stabilizes, open `traceability.md`, v4 unified feedback,
and both v4 source findings. Reconstruct each source concern and prove closure
against normative v5, live architecture, vocabularies, and ADRs. Traceability
is provenance, not proof. Preserve disagreements and duplicate attribution.

Give a separate closure row to every source finding:

1. Codex F1 — plural Generation relationships versus one Manifest and
   cross-Operation ordinal ties;
2. Fable F1 — the same membership defect and Experiment-growth scenario;
3. Codex F2 — nondeterministic Score Attempt selection;
4. Codex F3 — `PARTIAL` rescore parity contradiction;
5. Fable F2 — claimant death after enqueue before losing outcome CAS;
6. Fable F3 — unpinned Experiment-scoped Prediction identity;
7. Fable F4 — inability to represent a pre-scoring evaluation; and
8. Fable F5 — ambiguous retained `classify_error` seam.

Require closure of all four v5 owner decisions: singular Generation
membership; deterministic ordered scoring selection; populated-`PARTIAL`
scoring eligibility distinct from strict acceptance; and pre-scoring durable
`PARTIAL` evaluation. Require closure of L1–L3: bounded claimant-death
compensation, explicit Experiment scope in `prediction_id`, and retention of
`enqueue_failure_from_whetstone_exception` as the injected classifier.

Then regression-audit every v4 row that declared a v3/v2/v1 concern closed or
retained as a verification gate, plus the earlier accepted owner policies.
Every `no`, contradiction, or architecture-level uncertainty must appear as a
ranked finding and in the matrices.

## Review rules

- This is read-only except for one output. Write only
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/fable-findings.md`.
  Do not create repository scratch files or mutate code, plans, manifests,
  traceability, prompts, baselines, the other findings lane, synthesis,
  indexes, ADRs, glossaries, configs, locks, or any earlier packet.
- Do not stage, commit, push, create/switch branches, or create worktrees.
  Read-only Git inspection is required.
- Use live code and installed DBOS evidence for architectural claims. Focused
  read-only commands are allowed; do not run broad or long suites.
- Each defect needs a normative contract, a concrete failure scenario, and
  exact live file/line or installed-API evidence. Unsupported risks belong in
  `Unverified` with the exact proof required.
- Rank by consequence, merge only true duplicates, and distinguish design
  defects from implementation work or correctly retained verification gates.
- Use exactly one class: `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.

## Output

Write exactly one file:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/reviews/fable-findings.md`

Use this validator-compatible structure. Every numbered finding must repeat
the entire exact label set:

```markdown
# V5 convergence findings — Claude Fable 5 architecture and domain audit

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

Select `REPEAT_CONVERGENCE` when any blocker/P0 remains, an owner decision is
unresolved, a finding changes architecture, identity, ownership, persistence,
lifecycle, state, or a cross-repository interface, a fundamental transaction/
concurrency/publication/export/cutover redesign remains, or a contradiction
makes a core invariant infeasible. Select `READY_FOR_FOCUSED_AUDITS` only when
every prior P0 and every v4 source finding is closed with evidence, all four
owner decisions and L1–L3 are coherent, no owner or architecture decision
remains, the core state/ownership model is stable, and all remaining work is a
local correction or bounded verification gap.
