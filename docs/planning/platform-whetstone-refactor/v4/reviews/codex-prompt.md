# V4 hybrid whole-system convergence review — code and dependency audit (Codex 5.6, `sol`, high)

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

1. first reconstruct and audit v4 as a fresh design, without using the v3
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
lockfiles, configuration, migrations, and directly relevant documentation in:

- `/Users/daniellerothermel/drotherm/repos/dr-platform/`;
- `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`;
- `/Users/daniellerothermel/drotherm/repos/unitbench/`; and
- any sibling repository that imports/package metadata, workflow names,
  schemas, exports, or deployment configuration prove is directly affected.

Inspect the DBOS version and installed package path recorded by the validated
baseline. Verify signatures, status values, queue ordering, attribute behavior,
client load defaults, cancellation, and system-schema assumptions from that
exact installed content rather than memory or newer docs.

Only after the fresh findings stabilize, begin the **closure phase**. Read
`traceability.md`, then v3, v2, and v1 review evidence in that order. Add missed
closure findings, but preserve which findings originated in the fresh phase.

## Fresh audit questions

The list is a floor, not a checklist ceiling:

1. **Factual and blast-radius audit.** Verify every claimed caller, deletion,
   rename, dependency, pinned revision, workflow/queue name, schema/table name,
   current Whetstone generation/scoring behavior, rescore-selection behavior,
   Unitbench query surface, and Vercel/runtime assumption. Find consumers or
   durable contracts v4 missed.
2. **Manifest registration.** Prove the caller-prepared Manifest recipe is
   deterministic and implementable with current `dr-serialize`; exact equality
   is sufficient; page descriptors cannot be reordered/truncated; registrar
   Lease/token/cursor and completion CAS have complete predicates; hook and Item
   insertion share the promised transaction; crash/expiry recovery cannot
   duplicate domain rows; and enqueue is impossible before completion. Prove
   concrete Item execution recipes cover full canonical domain input and every
   behavior-affecting version, their ordered Operation aggregate is
   non-circular, cross-Operation dedup remains valid, and `ALREADY_PRESENT`
   cannot accept unequal domain content. Prove the persisted target ref,
   startup registry, conflict digest, opaque recipe envelope, and one resolver
   let a fresh process resume expired Claims and create automatic/requested
   Attempts without submit-time callables.
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
   safe `load_input=False`/`load_output=False` workflow inspection, the
   allowlisted system-schema step adapter with no payload selection or
   deserialization, workflow/step
   introspection, top-level-only topology, and non-recursive cancellation
   against the baseline-selected DBOS content. Flag any required API or atomicity DBOS does
   not expose.
5. **Reference-aware cancellation.** Trace candidate workflow discovery,
   workflow advisory locks, Operation/Item/Attempt lock order, the exclusivity
   predicate, unresolved DBOS-cancellation guards, new-reference races, partial
   DBOS failure, foreign cancellation, repeat requests, late terminal results,
   topology drift, and explicit later-Attempt authorization. Prove
   `cancel_children=True` is never needed or called. Walk cancel-during-Claim
   and late enqueue through Claim invalidation, `NOT_ENQUEUED`, lost-CAS
   compensation, and the append-only compensation ledger. Preserve accepted
   outcome-linked undercount without mistaking it for upstream abort or
   claiming provider receipts equal Whetstone totals.
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
   deterministic shuffle; service urgency remains independent; the
   baseline-selected DBOS same-Service-Class/same-timestamp behavior matches
   the explicitly accepted
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
   exact multi-Scoring-Operation membership, representable missing cells,
   highest-success selection, accepted Manifest relationship ownership,
   source-version invalidation, checked Operation-version cuts at promotion
   and read time, and current-pointer CAS cannot publish a stale evaluation.
   Check local DuckDB versus deployed
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
topic is not closed merely because traceability says it is; trace the actual
contract and evidence.
Report every open or contradictory item as a normal ranked finding and in the
closure matrices.

## Review rules

- Read-only review except for the required findings file: write only
  `reviews/codex-findings.md`; do not create scratch files in the repository or edit the
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
  distinguish a plan defect from implementation work or a live gate v4 already
  specifies correctly.
- Classify each finding as `architecture-changing`, `owner-decision`,
  `local-correction`, or `verification-gap`.
- Do not weaken an accepted invariant merely because live credentials are
  unavailable; preserve it as an unverified gate unless evidence disproves the
  design.

## Output

Write the review result—and no other file—to:

`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/reviews/codex-findings.md`

Use this structure:

```markdown
# V4 convergence findings — Codex 5.6 code and dependency audit

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
- **Failure scenario:** concrete sequence that violates the contract
- **Evidence:** absolute/path:line — what current code or API actually does
- **Affected repositories:** exact repository set and boundary crossed
- **Required correction:** exact invariant, schema, transaction, or step that must change
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
