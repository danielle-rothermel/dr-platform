# V2 hybrid whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

Review the frozen v2 Platform and Whetstone refactor plan adversarially. Find
where its architecture, domain model, ownership boundaries, state machines,
failure behavior, or Experiment contract is wrong, incomplete, or internally
contradictory. Prioritize defects that can create silent wrongness, ambiguous
authority, duplicate paid work, lost provenance, unsafe recovery, regressed
published truth, or an Experiment that appears valid when it is not. Do not
manufacture findings for volume, and do not re-litigate an owner-resolved
choice unless concrete code or design evidence shows it cannot satisfy its
stated invariant.

This is one of two independent hybrid whole-system convergence reviews. Your
primary lens is architecture, domain coherence, ownership, and concrete
failure scenarios, but every finding must remain grounded in current
repositories and applicable dependency behavior. The review is hybrid:

1. first reconstruct and challenge v2 as a fresh system, without using the v1
   findings as an answer key; then
2. explicitly prove or disprove closure of every v1 P0/P1/P2 item and all five
   owner decisions.

The closure pass is a floor, not a limit. New defects count even when every v1
item is nominally addressed.

## Frozen review target

- Plan: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md`
- Effort index and review gates: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- V1 unified feedback, for correction closure:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/unified-feedback.md`
- V1 source findings, for scenario and priority provenance:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/reviews/fable-findings.md`
- V0 unified feedback, for historical coverage only:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/reviews/unified-feedback.md`
- Canonical platform vocabulary:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md`
- Canonical Whetstone vocabulary:
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Canonical decisions:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0001-content-scoped-execution-identity.md`
  through
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0018-strict-experiment-acceptance.md`

V0, v1, their review packets, and v2 are immutable during this review.

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

At issuance, `dr-platform` is intentionally dirty only in the approved v2
plan/index, canonical glossary/ADR updates, and the uncommitted v1 review
results; no application code is modified. `whetstone-ai` is intentionally
dirty only in its approved canonical `CONTEXT.md` update. `unitbench` is clean.
The two v2 prompt files themselves become expected review-packet drift.

At review start, record each current HEAD, branch, and full dirty/clean status.
If a repository moved, audit its current working tree and identify drift that
changes a plan assumption. The plan and canonical decision set remain frozen;
do not confuse approved documentation drift with implementation evidence.

## Required inspection surface

Read v2, the index, both glossaries, and all applicable ADRs, then perform the
fresh architecture reconstruction before using the v1 packet for the explicit
closure pass. Inspect current code, tests, dependencies, configuration,
migrations, and directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository proven directly affected by imports, durable data,
  workflow/schema names, export consumers, or deployment configuration.

Use repository graph tooling if it is present and current, but verify important
claims in raw code. Do not treat generated documentation or the plan's own
resolution tables as authoritative over implementable contracts.

## Fresh architecture interrogation

The list is a floor, not a checklist ceiling:

1. **Ubiquitous language and authority.** Test every responsibility assigned
   to dr-platform, DBOS, Whetstone, Unitbench, operational Postgres,
   DuckDB/MotherDuck, and Neon. Verify the five named distinctions—Attempt
   authority versus eligibility, execution terminality versus Experiment
   acceptance, three lock scopes, execution identity versus reference
   identity, and transaction pages versus Manifest identity—remain true in
   every detailed section. Find shared, circular, or missing authority.
2. **Manifest-backed Registration.** Decide whether the caller-prepared
   Manifest is genuinely the immutable Operation membership authority rather
   than a digest-shaped restatement of a mutable source. Walk preparation,
   canonical identity, page boundaries, registrar ownership, hook atomicity,
   crash/resume, expiry, exact resubmission, empty submission, and the moment
   enqueue becomes legal. Challenge truncation, reordering, changed domain rows,
   and two callers with equal Operation keys but unequal sources.
3. **Attempt lineage and eligibility.** Reconstruct automatic platform retry
   and caller-requested next Attempts independently. Verify the request ledger
   represents authorization as well as execution provenance; reason/source
   combinations are complete and closed; domain policy stays outside the
   kernel; the platform remains the sole ordinal allocator; exhaustion is
   meaningful; and different Operations can share execution without sharing
   the wrong local outcome.
4. **Concrete Whetstone recovery scenarios.** Walk a provider/node domain
   failure that returns DBOS success, a scoring harness failure that returns
   DBOS success, two concurrent requests for the same failed result, an
   exhausted request, cancellation followed by confirmed retry, and a shared
   ordinal already executed by another Operation. Require one deterministic
   owner and outcome at every step; reject prose that assumes a later adapter
   will decide unspecified semantics.
5. **Lifecycle and aggregate completeness.** Independently reconstruct all
   registration, enqueue, execution, retry, request, missing, cancellation,
   and terminal transitions. Test the total Operation-status precedence against
   mixtures that occur naturally in large paged Operations. Find states with
   no successor, multiple successors/owners, false terminality, or append-only
   history that cannot explain the current pointer and aggregate.
6. **Reference-aware cancellation topology.** Test whether top-level-only
   workflow topology is a real enforceable invariant rather than convention.
   Walk candidate discovery, workflow/reference locks, new references, shared
   work, partial physical failure, repeated requests, topology drift, late
   success, and explicit later authorization. Verify non-recursive cancellation
   preserves both sticky local intent and paid work owned by another Operation.
7. **Generation and scoring as one primitive.** Determine whether both roles
   genuinely use one reusable platform Operation lifecycle without erasing
   Whetstone's selection, domain result, provenance, profile/dataset identity,
   cost/accounting, or append-only semantics. Identify surviving parallel
   orchestration that conflicts with the target and legitimate domain logic
   the plan accidentally pushes into dr-platform.
8. **Experiment validity.** Treat strict completeness as a product/domain
   contract, not a reporting detail. Prove the expected Prediction set and
   required score cells are stable and enumerable; accepted domain outcomes
   are unambiguous; a model/task/profile-correlated failure remains partial;
   overrides cannot hide bias behind one ratio; operator confirmation and
   observed strata are durable; and re-evaluation does not rewrite history.
9. **Scheduling and pacing.** Verify urgency, shuffle, Manifest order, original
   result order, retry order, and queue FIFO assumptions are distinct and
   composable. Challenge page boundaries, concurrent Operations, sustained
   urgency, and multi-domain sleeps. The pre-experiment gate must prevent the
   historically harmful one-model-first failure rather than merely observe it.
10. **Three-lock export truth.** Trace the source Export Barrier, per-Operation
    mutation lock, and destination Publication Fence separately. Walk two
    exporters through H1/H2 inversion, Lease expiry and renewal, stale-stage
    cleanup, full rebuild, partial destination success, and crash before/during/
    after promotion. Verify operational Postgres remains durable truth while
    each destination is monotonic and rebuildable.
11. **Two-plane reader contract and confidentiality.** Test root-cascade
    completeness, snapshot-built platform Attempts, intentionally sensitive
    Whetstone detail, excluded DBOS replay payloads, credential resolution,
    local/deployed adapter parity, remote-compute confirmation, Vercel runtime,
    and independent store failure. Find any reader that can observe a mixed
    snapshot, bypass a policy, or require a secret in an unsafe plane.
12. **Operator contract.** Ask whether an operator can explain, stop, retry,
    and validate a run using typed application state without raw DBOS mutation.
    Check previews, confirmation, idempotent request identity, partial failure,
    health output, and the difference between execution status, domain outcome,
    and Experiment acceptance.
13. **Cutover coherence.** Walk repository order, dependency pins, fresh
    schemas, durable names, secret/environment changes, deletion timing,
    rollback, and the first expensive Experiment. Find circular sequencing,
    a gate that depends on already-deleted behavior, or a clean-cut assumption
    contradicted by current consumers.
14. **Principles versus mechanisms.** Find any section that violates the
    one-happy-path, domain-agnostic-kernel, vocabulary, Pydantic-boundary,
    content-scoped-identity, append-only, or two-plane principles. Flag
    important behavior left as a slogan, callback convention, or acceptance
    wish rather than an implementable invariant.

## Explicit v1 closure pass

After the fresh audit, compare v2 against the v1 unified feedback in its exact
priority order:

1. P0-1 caller-requested next Attempt;
2. P0-2 immutable registration Manifest;
3. P0-3 destination fencing;
4. P0-4 complete cancellation topology;
5. P0-5 Experiment acceptance;
6. P1-6 Operation mutation/aggregate serialization;
7. P1-7 execution-scoped DBOS attributes;
8. P1-8 total Operation-status precedence;
9. P1-9 detail platform Attempts inside the Whetstone snapshot;
10. P1-10 secret-free DBOS payloads and explicit safe reads;
11. P1-11 kernel-owned shared writer-lock acquisition; and
12. P2-12 live integration/version/order/rescore verification boundaries.

Also verify that each owner decision is expressed consistently in v2, both
glossaries, and applicable ADRs, without one artifact silently narrowing or
broadening another. A topic is not closed merely because §4.9 says it is.
Report every open or contradictory item as a normal ranked finding and in the
closure matrix.

## Review rules

- Read-only review except for the required findings file: do not edit the
  frozen plan, index, code, ADRs, glossaries, prompts, or earlier review
  packets; do not commit, stage, push, create branches, or create worktrees.
- Git status, branch, log, revision, and read-only diff inspection are allowed
  and required.
- Static inspection, fast searches, dependency/API inspection, and focused
  read-only commands are allowed. Do not run broad suites or commands expected
  to exceed about two minutes.
- Every defect must cite the relevant plan/ADR contract and current
  file-and-line or dependency evidence. Put unsupported concerns under
  `Unverified` instead of presenting them as findings.
- Rank by consequence. Merge duplicates. Distinguish an architectural defect
  from implementation work or a live gate v2 already specifies correctly.
- Classify every finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.
- Do not weaken an accepted invariant merely because a live service cannot be
  exercised; preserve it as an unverified gate unless evidence disproves the
  architecture.

## Output

Write the review result—and no other file—to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/reviews/fable-findings.md`

Use this structure:

```markdown
# V2 convergence findings — Claude Fable 5 architecture and domain audit

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
- **Evidence:** absolute/path:line — what current code, dependency, or canonical document actually does
- **Consequence:** concrete failure if implementation follows v2
- **Required plan change:** exact invariant, boundary, or step that must change

## V1 correction closure
| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P2-12 | yes/no | plan/ADR/code evidence; every `no` also appears as a finding |

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <which gate condition is or is not present>
- **Unverified:** <anything not verifiable and why>
```

Use `REPEAT_CONVERGENCE` if any blocker, unresolved owner decision,
architecture-changing finding, persistence/identity/lifecycle redesign, or
changed cross-repository interface remains. Use `READY_FOR_FOCUSED_AUDITS`
only when every v1 P0 is closed, no architecture decision remains, and all
remaining work is local correction or bounded verification.
