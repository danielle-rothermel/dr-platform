# V6 hybrid whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

**Execution precondition:** this prompt is prepared for a future review. V6 is
currently `draft`; run it only after the effort index and `plan.md` are
`in-review`, every manifest-declared document is frozen without content edits,
and the future `reviews/review-baseline.json` has been captured. Immediately
before review, run and require success from:

```bash
uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py \
  /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6 \
  --require-prompts --require-baseline
```

Abort on any mismatch in the manifest, a declared document, either prompt,
repository revisions/material dirty paths, reviewer configuration/output
path, or the installed DBOS package. Never review a drifted target.

Adversarially review the complete frozen v6 system. Your primary lens is
architecture, domain coherence, authority and ownership, lifecycle, and
concrete end-to-end failure scenarios. Prioritize contradictions that permit
silent wrongness, ambiguous truth, lost or duplicated paid work, stale
acceptance/publication, unsafe recovery, or an incoherent cutover. Do not
manufacture findings or re-litigate an owner choice unless a concrete scenario
proves it cannot satisfy its stated invariant.

This is one of two hybrid whole-system reviews. The fresh and closure phases
share one model context, which is weaker independence than two isolated
passes. State that limitation in the findings. Stabilize and preserve the
fresh finding set and severity order before opening closure evidence; later
material may add findings but must not rewrite, normalize, or erase the fresh
set.

## Frozen review target

- Manifest:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/plan-manifest.json`
- Normative documents in exact manifest order:
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

V0 through v5, all earlier review packets, and frozen v6 are immutable.

## Issued baseline

The future file
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/review-baseline.json`
is the **exclusive issuance truth**. Do not infer the issued snapshot from this
prompt, conversation history, live Git state, traceability, or an earlier
packet. It must hash `plan-manifest.json`, every declared document in order,
both prompts, repository branches/HEADs/material dirty paths, and the selected
DBOS version, installed path, and content. It must identify Claude Fable
5/high and the exact findings output path for this lane.

After successful validation, independently record SHA-256 digests for the
baseline and manifest. Any repository, output-path, reviewer, dependency, or
content mismatch is an abort condition.

## Required inspection surface and phase order

### Phase 1 — fresh normative audit

Read `plan-manifest.json`, then only the five normative documents in declared
order. Read the effort index, both canonical vocabularies, applicable ADRs,
and live evidence. Do **not** open `traceability.md`, any v5 prompt, finding,
synthesis, or older review material until the fresh findings and severities
have stabilized.

Inspect architecture-relevant live code, tests, schemas, dependencies,
lockfiles, configuration, migrations, docs, CI, and deployments in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository shown by imports, package metadata, durable names,
  schemas, export consumers, CI, or deployment configuration to be affected.

Inspect the exact baseline-selected installed DBOS package, expected to be
2.26.0 in `dr-platform/.venv`, wherever behavior defines feasibility. Confirm
lifecycle states, queue priority/dequeue semantics, attributes, payload
loading, cancellation/topology, and system-schema assumptions from installed
source rather than memory, web documentation, or another version.

Interrogate at least these architecture questions with concrete failure
walkthroughs:

1. **Ubiquitous language and authority.** Does each identity, ordinal,
   lifecycle transition, source of truth, mutable pointer, publication cursor,
   and operator action have exactly one owner? Do Operation/Item/Attempt/
   Claim, Prediction/Generation Run/Score Attempt/Experiment, DBOS execution,
   and Analysis/Detail concepts remain distinct across documents and code?
2. **One lifecycle.** Walk submission through Manifest registration, target
   resolution, Claim/enqueue, reconciliation/retry, domain outcome,
   Experiment evaluation, export, publication, and Unitbench/COPRO reads.
   Find alternative paths, circular prerequisites, ambiguous terminal states,
   and states with no recovery owner.
3. **Restart and identity coherence.** Prove a fresh process can resolve the
   persisted target/recipe, resume work, preserve content identity, retain all
   causal Claim identities, and keep domain nouns and secrets out of the
   kernel/DBOS boundary.
4. **Claim and cancellation ownership.** Walk cancellation during Claim,
   terminalization before late enqueue, Lease expiry/replacement, several
   stale claimants, claimant death after enqueue before outcome CAS, a newly
   racing shared reference, `SKIPPED_SHARED`, and repeated absence ending in
   `NO_WORKFLOW_FOUND`. Prove exact replay, reference exclusivity, immutable
   terminal Attempts, and a decidable link guard without false provider-abort
   or complete-billing claims.
5. **Generation membership.** Walk first acceptance, exact replay, unequal
   second relationship, Experiment growth, partial registration, and multiple
   successes. Prove exactly one Generation Operation/Manifest and one Item
   lineage per accepted cell remain coherent from schema through evaluation.
6. **Scoring ownership and selection.** Walk a stale scored success followed
   by regeneration, success-success, failure-success, equal ordinal with
   different recipes, populated-`PARTIAL` plus `SUCCESS`, and concurrent
   relationship/evaluation races. Prove every cell first pins its selected
   accepted Generation Run; other-run candidates remain
   `SUPERSEDED_GENERATION`; relationship and Attempt precedence then form one
   total, durable, identity-bearing reduction.
7. **Populated-only `PARTIAL` semantics.** Apply the exact persisted predicate
   `terminal_submission_text IS NOT NULL AND terminal_submission_text ~
   '[^[:space:]]'` to empty, space-only, tab-only, newline-only, and populated
   rows. Prove selection, identity, retry, Manifest, outcome, and narrowed
   deletion parity share the same boundary, while strict Generation
   acceptance remains a separate policy.
8. **Pre-scoring Experiment state.** Walk evaluation with no Scoring
   relationship and after the first relationship. Prove empty scoring
   membership, explicit missing cells, identity, history, source invalidation,
   and current-pointer semantics are coherent and inspectable.
9. **Acceptance readiness and currentness.** Walk domain persistence before
   workflow return, post-persistence crash, recovery exhaustion, late
   Generation/Score outcomes, next Attempts, cancellation/reconciliation,
   policy changes, and races between platform mutation and promotion/read.
   Every selected winner must bind exact terminal platform/DBOS success; no
   nonterminal or stale evaluation may appear current.
10. **Transactions and crash ownership.** For registration, Claim creation,
    Attempt creation, cancellation, aggregation, acceptance, export, and
    destination promotion, identify the authoritative row, lock/CAS/fence,
    external-call boundary, crash successor, replay key, and terminal repair.
    Reject cross-system atomicity claims no owner can enforce.
11. **Publication and readers.** Walk source cuts, kernel/DBOS/Analysis/Detail
    bundle independence, destination Leases/fences, root closure, skew policy,
    partial failure, stale writers, compute policy, server-only secrets, and
    independent Analysis/Detail failure. Match consumer promises to actual
    atomic boundaries.
12. **Operability.** Determine whether safe inspection, health, wait,
    cancellation, retry authorization, reconciliation, export, and rollback
    let an operator diagnose and safely move every nonterminal/degraded state
    without raw DBOS mutation or payload exposure.
13. **Cutover coherence.** Walk phase and deletion gates across dr-platform,
    whetstone-ai, unitbench, exact DBOS 2.26.0, private dependency auth,
    MotherDuck, Neon, DuckDB, Vercel, COPRO, and zero-spend e2e. Old mechanisms
    must survive until replacement parity is proved; failures route back to
    design rather than weakening invariants.

Explicitly decide whether v6 finally closes the repeatedly concentrated
Claim/cancellation and strict-acceptance boundaries. If another architecture-
changing defect appears in either boundary, or in a subsystem earlier reviews
treated as settled, call it **possible non-convergence** and explain the
repeated or reopened domain/ownership invariant. Do not automatically
normalize it as expected local refinement merely because v6 is bounded.

### Phase 2 — strict-inclusive v5 closure

Only after Phase 1 stabilizes, open `traceability.md`, v5 unified feedback,
and both v5 source findings. Reconstruct every concern and prove closure
against normative v6, live architecture, vocabularies, and ADRs. Traceability
is provenance, not proof. Preserve reviewer disagreements and source
attribution.

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

Separately prove V5-OD1: populated-only selection is one exact end-to-end
observable rule and legacy parity is explicitly narrowed. Regression-audit
every v5 row that declared a v4/v3/v2/v1 concern closed or retained as a gate.
Every `no`, contradiction, architecture-level uncertainty, or reopened
settled subsystem must appear as a ranked finding and in the matrices.

## Review rules

- This is read-only except for one output. Write only
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/fable-findings.md`.
  Do not create repository scratch files or mutate code, plans, manifests,
  traceability, prompts, baselines, the other findings lane, synthesis,
  indexes, ADRs, vocabularies, configs, locks, or any earlier packet.
- Do not stage, commit, push, create/switch branches, or create worktrees.
  Read-only Git inspection is required.
- Use live code and installed DBOS evidence for architectural claims. Focused
  read-only commands are allowed; do not run broad or long suites.
- Each defect needs a normative contract, concrete failure scenario, and
  exact absolute file:line or installed-API evidence. Unsupported risks go in
  `Unverified` with the exact proof required.
- Rank by consequence, merge only true duplicates, and distinguish design
  defects from implementation work and correctly retained gates.
- Use exactly one class: `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.

## Output

Write exactly one file:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v6/reviews/fable-findings.md`

Use this validator-compatible structure. Every numbered finding must repeat
the complete exact label set:

```markdown
# V6 convergence findings — Claude Fable 5 architecture and domain audit

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

Select `REPEAT_CONVERGENCE` when any blocker/P0 remains, an owner decision is
unresolved, a finding changes architecture, identity, ownership, persistence,
lifecycle, state, or a cross-repository interface, a fundamental transaction/
concurrency/publication/export/cutover redesign remains, a core invariant is
contradictory, or possible non-convergence is evidenced by an architecture
finding in a concentrated or previously settled boundary. Select
`READY_FOR_FOCUSED_AUDITS` only when all five v5 source findings and V5-OD1
are closed with normative and live evidence, no owner/architecture decision
remains, the core state/ownership model is stable, and all remaining work is a
local correction or bounded verification gap.
