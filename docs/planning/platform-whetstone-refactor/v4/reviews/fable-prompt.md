# V4 hybrid whole-system convergence review — architecture and domain audit (Claude Fable 5, high)

**Execution precondition:** this prompt is prepared for a future review; v4 is
still `draft`, and review has not started. Run it only after the effort index
and `plan.md` status have been changed to `in-review`, every normative document
declared by `plan-manifest.json` has been frozen without changing its content,
and `reviews/review-baseline.json` has been captured. Immediately before review,
run the following command and abort on any failure or mismatch:

```bash
uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py \
  /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4 \
  --require-prompts --require-baseline
```

If the manifest, any declared document, either prompt, a repository revision or
dirty-path set, or the installed DBOS content has moved, do not review the
drifted target. Refresh the prompts when necessary, recapture the baseline, and
validate again.

Review the frozen v4 Platform and Whetstone refactor plan adversarially. Find
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

1. first reconstruct and challenge v4 as a fresh system, without using the v3
   findings as an answer key; then
2. explicitly prove or disprove closure of every v3 P0/P1/P2 item, every v2
   and v1 disposition, and all three v4 owner decisions.

The closure pass is a floor, not a limit. New defects count even when every v3
item is nominally addressed.

Because this single prompt performs both the fresh and closure phases in one
model context, it has weaker independence than two isolated workers. Preserve
that limitation in the findings baseline and do not let closure evidence alter
or erase findings stabilized during the fresh phase.

Preserve the three v4 owner decisions: (1) Whetstone/export cost totals may
undercount a discarded post-cancellation provider result, with provider
receipts remaining total-billing evidence and DBOS replay excluded; (2) the
highest successful platform Attempt ordinal is the accepted Generation Run;
and (3) acceptance currentness uses atomically checked Operation
`platform_cut_version` values. Also preserve the earlier accepted final DBOS
tie variance. Do not report these policies as findings by themselves. Do
report contradictions, broader hidden work than the accepted undercount,
required outcome cost loss, false provider-abort claims, selection/currentness
ambiguity, or failure to preserve deterministic kernel mixing.

## Frozen review target

- Authoritative packet manifest:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/plan-manifest.json`
- Normative documents, in the manifest's required read order:
  1. `plan.md` — system overview, invariants, lifecycle, phase gates, and review protocol;
  2. `contracts/platform.md` — dr-platform kernel contract;
  3. `contracts/whetstone.md` — Whetstone domain and platform-boundary contract;
  4. `contracts/publication.md` — export, publication, and two-plane reader contract; and
  5. `contracts/delivery.md` — migration, verification, cutover, rollback, and deferral contract.
- Traceability document, reserved for the closure phase:
  `traceability.md` — prior-review provenance, owner decisions, revision history, and source coverage.
- Effort index and review gates: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/README.md`
- Closure-only prior-review sources (do not open during the fresh phase):
- V3 strict-inclusive unified feedback, the primary closure source:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/reviews/unified-feedback.md`
- V3 underlying findings, for evidence and reviewer disagreement:
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/reviews/codex-findings.md`
  and
  `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/reviews/fable-findings.md`
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

V0 through v3, their review packets, and the frozen v4 target are immutable
during this review.

## Issued baseline

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/review-baseline.json`
is the exclusive issued baseline. Do not infer an issuance snapshot from this
prompt, conversation history, current Git state, or earlier packets. The
baseline must hash the manifest, every declared document in manifest order,
both prompts, each repository's branch/HEAD/material dirty paths, and the
selected DBOS dependency version, installed path, and content digest.

After the validator succeeds, independently record the SHA-256 digests of
`review-baseline.json` and `plan-manifest.json` in the findings. Confirm that
the baseline's reviewer/model/effort and exact output path match this prompt.
Any mismatch is an abort condition, including a repository or DBOS mismatch;
do not silently audit current drift.

## Required inspection surface

Perform two ordered phases. In the **fresh phase**, read the manifest, the five
normative documents in manifest order, the effort index, both glossaries, all
applicable ADRs, and live evidence. Do **not** read `traceability.md` or any v1,
v2, or v3 review prompt, findings, or unified feedback until fresh findings and
their severities have stabilized. Inspect current code, tests, dependencies,
configuration, migrations, and directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository proven directly affected by imports, durable data,
  workflow/schema names, export consumers, or deployment configuration.

Use repository graph tooling if it is present and current, but verify important
claims in raw code. Do not treat generated documentation or the plan's own
resolution tables as authoritative over implementable contracts.

Inspect the DBOS version and installed package path recorded by the validated
baseline. Ground dependency claims in that exact installed content rather than
memory or newer documentation.

Only after the fresh findings stabilize, begin the **closure phase**. Read
`traceability.md`, then v3, v2, and v1 review evidence in that order. Add missed
closure findings, but preserve which findings originated in the fresh phase.

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
   whether cross-Operation dedup survives without stale-work reuse. Walk a
   fresh-process restart through persisted target resolution and prove the
   kernel recipe envelope stays domain-opaque.
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
   accepted overlap contract stays visible and never masquerades as provider
   abort. Walk cancel-during-Claim and late enqueue through `NOT_ENQUEUED`,
   Claim invalidation, idempotent compensation, and terminal provenance.
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
   Walk missing Generation and Score cells, multiple successful Generation
   Runs, highest-ordinal selection, accepted Manifest relationship ownership,
   exact Manifest/domain/platform cuts, source-version invalidation, checked
   Operation-version cuts, concurrent pointer promotion, read-time validation,
   and historical-versus-current semantics.
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

## Explicit v3 strict-inclusive closure pass

After the fresh audit, compare v4 against the v3 synthesis in exact order:

1. P0-1 restart-safe execution target and opaque recipe resolution;
2. P0-2 representable, platform-current Experiment acceptance;
3. P0-3 owner-selected overlap accounting contract;
4. P0-4 accepted Generation Run selection;
5. P1-5 cancellation-safe claim/enqueue compensation;
6. P1-6 payload-safe DBOS step inspection;
7. P1-7 authoritative Analysis Bundle inventory; and
8. every preserved or extended P2 gate.

For each row, distinguish a selected architectural direction from complete
runtime enforcement. Every `no` or contradiction must also be a ranked
finding. Then continue through the v2 and v1 historical closure matrices.

## Explicit v2 and v1 closure pass

After the fresh audit, compare v4 against the v2 synthesis in exact order:

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

Also verify every row of the v1 disposition table and all three v4 owner
decisions across the normative packet, both glossaries, and ADRs 0001–0020. A
topic is not closed merely because traceability says it is; reconstruct the
actual domain and failure scenario. Report every open or contradictory item as
a normal ranked finding and in the closure matrices.

## Review rules

- Read-only review except for the required findings file: write only
  `reviews/fable-findings.md`; do not create scratch files in the repository or edit the
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
  from implementation work or a live gate v4 already specifies correctly.
- Classify every finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.
- Do not weaken an accepted invariant merely because a live service cannot be
  exercised; preserve it as an unverified gate unless evidence disproves the
  architecture.

## Output

Write the review result—and no other file—to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/fable-findings.md`

Use this structure:

```markdown
# V4 convergence findings — Claude Fable 5 architecture and domain audit

## Review baseline
- **Date:** YYYY-MM-DD
- **Packet validation:** exact successful `validate_review_packet.py --require-prompts --require-baseline` command
- **Review baseline SHA-256:** `<sha256 of reviews/review-baseline.json>`
- **Plan manifest SHA-256:** `<sha256 of plan-manifest.json>`
- **Independence limitation:** hybrid fresh plus closure phases shared one model context
- **dr-platform:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **whetstone-ai:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **unitbench:** <branch>, <HEAD>, <dirty/clean plus relevant drift>
- **DBOS:** <installed version and inspected package path>

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Class:** architecture-changing | owner-decision | local-correction | verification-gap
- **Plan contract:** <normative document path and heading> and/or ADR NNNN
- **Failure scenario:** concrete lifecycle or domain walk that violates the contract
- **Evidence:** absolute/path:line — what current code, dependency, or canonical document actually does
- **Affected repositories:** exact repository set and ownership boundary crossed
- **Required correction:** exact invariant, ownership, lifecycle, or persistence rule that must change
- **Closure impact:** v3/v2/v1 rows or owner decision this reopens, if any

## V3 strict-inclusive closure
| V3 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P1-7 and P2 | yes/no | plan/ADR/code evidence; every `no` also appears as a finding |

## V2 strict-inclusive closure
| V2 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P1-13 and P2 | yes/no | plan/ADR/code evidence; every `no` also appears as a finding |

## V1 disposition closure
| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 … P2-12 | yes/no | verify the retained/extended disposition rather than copying traceability |

## Verdict
- **Gate:** REPEAT_CONVERGENCE | READY_FOR_FOCUSED_AUDITS
- **Reason:** <which gate condition is or is not present>
- **Unverified:** <anything not verifiable and why>
```

Use `REPEAT_CONVERGENCE` if any blocker or previous P0 remains; an owner
decision is unresolved; a finding changes architecture, identity, ownership,
persistence, lifecycle, state, or a cross-repository interface; a fundamental
transaction, concurrency, publication, export, or cutover redesign remains; or
a contradiction makes a core invariant infeasible. Use
`READY_FOR_FOCUSED_AUDITS` only when every previous P0 is closed with evidence,
no owner or architecture decision remains, the core state and ownership model
is stable, and all remaining work is local correction or a bounded verification
gap.
