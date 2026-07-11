# V3 hybrid whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

**Execution precondition:** run this prompt only after the effort index and v3
plan status are changed from `draft` to `in-review` without changing plan
content. If the plan content or repository revisions move, refresh the baseline
before review rather than auditing a mutable target.

Review the frozen v3 Platform and Whetstone refactor plan adversarially. Find
where its architecture, domain model, ownership boundaries, state machines,
failure behavior, or Experiment contract is wrong, incomplete, or internally
contradictory. Prioritize defects that can create silent wrongness, ambiguous
authority, unaccounted paid work, lost provenance, unsafe recovery, regressed
published truth, or an Experiment that appears valid when it is not. Do not
manufacture findings for volume, and do not re-litigate an owner-resolved
choice unless concrete code or design evidence shows it cannot satisfy its
stated invariant.

This is one of two independent hybrid whole-system convergence reviews. Your
primary lens is architecture, domain coherence, ownership, and concrete
failure scenarios, but every finding must remain grounded in current
repositories and applicable dependency behavior. The review is hybrid:

1. first reconstruct and challenge v3 as a fresh system, without using the v2
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
stated invariant those choices make incoherent.

## Frozen review target

- Plan: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md`
- Effort index and review gates: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- V2 strict-inclusive unified feedback, for correction closure:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/reviews/unified-feedback.md`
- V2 source findings, for scenario, disagreement, and priority provenance:
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
If a repository moved, audit its current working tree and identify drift that
changes a plan assumption. The plan and canonical decision set remain frozen;
do not confuse approved documentation drift with implementation evidence.

## Required inspection surface

Read v3, the index, both glossaries, and all applicable ADRs, then perform the
fresh architecture reconstruction before using the v2 packet for the explicit
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
   DuckDB/MotherDuck, and Neon. Verify the seven named distinctions—Attempt
   authority versus eligibility, execution terminality versus Experiment
   acceptance, three lock scopes, execution identity versus reference
   identity, transaction pages versus Manifest identity, membership versus
   execution-recipe identity, and logical cancellation versus provider abort—
   remain true in every detailed section. Find shared, circular, or missing authority.
2. **Manifest-backed Registration.** Decide whether the caller-prepared
   Manifest is genuinely the immutable Operation membership authority rather
   than a digest-shaped restatement of a mutable source. Walk preparation,
   canonical identity, page boundaries, registrar ownership, hook atomicity,
   crash/resume, expiry, exact resubmission, empty submission, and the moment
   enqueue becomes legal. Challenge truncation, reordering, changed domain rows,
   and two callers with equal Operation keys but unequal sources. Challenge the
   scoped concrete/aggregate execution-recipe model, full domain equality, and
   whether cross-Operation dedup survives without stale-work reuse.
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
   exhausted request, cancellation followed by confirmed retry, a foreign
   cancellation, a synchronous paid call overlapping its replacement, and a
   shared ordinal already executed by another Operation. Require one deterministic
   owner and outcome at every step; reject prose that assumes a later adapter
   will decide unspecified semantics.
5. **Lifecycle and aggregate completeness.** Independently reconstruct all
   registration, enqueue, execution, retry, request, missing, cancellation,
   and terminal transitions. Test the total Operation-status precedence against
   mixtures that occur naturally in large paged Operations, including
   confirmed enqueue/`NOT_STARTED`, permanent enqueue failure, and abandoned
   partial Registration. Find states with
   no successor, multiple successors/owners, false terminality, or append-only
   history that cannot explain the current pointer and aggregate.
6. **Reference-aware cancellation topology.** Test whether top-level-only
   workflow topology is a real enforceable invariant rather than convention.
   Walk candidate discovery, workflow/reference locks, new references, shared
   work, partial DBOS cancellation failure, repeated requests, topology drift,
   foreign cancellation, late terminality, and explicit later authorization.
   Verify non-recursive cancellation preserves sticky local intent, while the
   accepted overlap contract stays visible and never masquerades as provider abort.
7. **Generation and scoring as one primitive.** Determine whether both roles
   genuinely use one reusable platform Operation lifecycle without erasing
   Whetstone's selection, domain result, provenance, profile/dataset identity,
   cost/accounting, or append-only semantics. Identify surviving parallel
   orchestration that conflicts with the target and legitimate domain logic
   the plan accidentally pushes into dr-platform. Prove immutable
   `selection_digest` permits successive Scoring Operations and acceptance can
   combine them without mutating Manifest membership.
8. **Experiment validity.** Treat strict completeness as a product/domain
   contract, not a reporting detail. Prove the expected Prediction set and
   required score cells are stable and enumerable; accepted domain outcomes
   are unambiguous; a model/task/profile-correlated failure remains partial;
   overrides cannot hide bias behind one ratio; operator confirmation and
   observed strata are durable; and re-evaluation does not rewrite history.
   Walk exact Manifest/domain/platform cuts, source-version invalidation,
   concurrent pointer promotion, and historical-versus-current semantics.
9. **Scheduling and pacing.** Verify urgency, shuffle, Manifest order, original
   result order, retry order, and accepted DBOS tie behavior are distinct and
   composable. Challenge page boundaries, concurrent Operations, sustained
   urgency, and multi-domain sleeps. The pre-experiment gate must prevent the
   historically harmful one-model-first failure rather than merely observe it.
   Accept final same-millisecond DBOS tie variance only if deterministic kernel
   mixing and requested/effective priority remain honest and sufficient.
10. **Three-lock export truth.** Trace the source Export Barrier, per-Operation
    mutation lock, and destination Publication Fence separately. Walk two
    exporters through H1/H2 inversion, Lease expiry and renewal, stale-stage
    cleanup, full rebuild, partial destination success, and crash before/during/
    after promotion. Verify atomic Analysis/Detail pointers, transactional
    kernel-table/cursor commit, bundle cleanup ownership, and explicit
    cross-family skew policy while operational Postgres remains durable truth.
11. **Two-plane reader contract and confidentiality.** Test root-cascade
    completeness, snapshot-built platform Attempts, intentionally sensitive
    Whetstone detail, excluded DBOS replay payloads, credential resolution,
    local/deployed adapter parity, remote-compute confirmation, Vercel runtime,
    and independent store failure. Find any reader that can observe a mixed
    snapshot, bypass a policy, or require a secret in an unsafe plane.
12. **Operator contract.** Ask whether an operator can explain, stop, retry,
    and validate a run using typed application state without raw DBOS mutation.
    Check previews, confirmation, idempotent request identity, partial failure,
    health output, abandoned Registration, requested/effective priority, typed
    lifecycle wait, and the difference between execution status, domain outcome,
    and Experiment acceptance.
13. **Cutover coherence.** Walk repository order, dependency pins, fresh
    schemas, durable names, secret/environment changes, deletion timing,
    rollback, and the first expensive Experiment. Find circular sequencing,
    especially deletion of analysis helpers before COPRO and zero-spend
    wait→export→pinned-read parity, or a clean-cut assumption contradicted by
    current consumers.
14. **Principles versus mechanisms.** Find any section that violates the
    one-happy-path, domain-agnostic-kernel, vocabulary, Pydantic-boundary,
    content-scoped-identity, append-only, or two-plane principles. Flag
    important behavior left as a slogan, callback convention, or acceptance
    wish rather than an implementable invariant.

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
closed because §4.9/§4.10 says it is; reconstruct the actual domain and failure
scenario. Report every open or contradictory item as a normal ranked finding
and in the closure matrices.

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
  from implementation work or a live gate v3 already specifies correctly.
- Classify every finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.
- Do not weaken an accepted invariant merely because a live service cannot be
  exercised; preserve it as an unverified gate unless evidence disproves the
  architecture.

## Output

Write the review result—and no other file—to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/reviews/fable-findings.md`

Use this structure:

```markdown
# V3 convergence findings — Claude Fable 5 architecture and domain audit

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
- **Consequence:** concrete failure if implementation follows v3
- **Required plan change:** exact invariant, boundary, or step that must change

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
