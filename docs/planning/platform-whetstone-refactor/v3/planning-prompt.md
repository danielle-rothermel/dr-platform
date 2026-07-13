# Prompt: write the Platform and Whetstone refactor v3 plan

Work in `/Users/daniellerothermel/drotherm/repos/dr-platform` and create the
next version of the Platform and Whetstone refactor plan at:

`docs/planning/platform-whetstone-refactor/v3/plan.md`

This is planning work only. Do not implement the refactor. Do not create v3
review prompts or findings yet. Do not commit unless explicitly asked.

## Read first

Before drafting, read these instructions and artifacts in full:

1. `docs/agents/planning.md` and `docs/agents/domain.md`;
2. `docs/planning/platform-whetstone-refactor/README.md`;
3. the frozen v2 plan at
   `docs/planning/platform-whetstone-refactor/v2/plan.md`;
4. the strict-inclusive v2 synthesis at
   `docs/planning/platform-whetstone-refactor/v2/reviews/unified-feedback.md`;
5. both underlying v2 reviews:
   - `docs/planning/platform-whetstone-refactor/v2/reviews/codex-findings.md`;
   - `docs/planning/platform-whetstone-refactor/v2/reviews/fable-findings.md`;
6. the v1 synthesis at
   `docs/planning/platform-whetstone-refactor/v1/reviews/unified-feedback.md`;
7. the current `CONTEXT.md` files and canonical glossaries for `dr-platform`,
   `whetstone-ai`, and `unitbench`;
8. ADRs 0001 through 0018 in `docs/adr/`; and
9. the current code, tests, dependency pins, and runtime configuration in all
   three repositories, plus the installed DBOS implementation and any sibling
   repository patterns cited by the reviews.

Use the applicable domain-modeling, code-quality, and planning/grilling skills
if they are available. Treat current code and installed dependency behavior as
the authority for feasibility and drift; do not rely only on the frozen plan.

## Versioning rules

- V0, v1, and v2 are immutable. Never edit their plans, prompts, findings, or
  unified feedback.
- Copy v2's plan to `v3/plan.md` as the starting point, then revise the copy.
- V3 is the only mutable draft.
- Preserve useful v2 detail and accepted decisions. Remove or rewrite text
  superseded by v3 rather than layering contradictory addenda onto it.
- Add a v3 revision-history entry explaining that it incorporates the
  strict-inclusive v2 convergence synthesis.

## Required process

### 1. Re-audit the named baseline

Verify and record exact current revisions for `dr-platform`, `whetstone-ai`,
and `unitbench`, the installed DBOS version, and any material drift from the
v2 baselines. Re-run the narrow code/dependency inspections needed to validate
the review evidence. If current evidence changes a finding, document the
evidence and its consequence explicitly; do not silently discard the finding.

### 2. Resolve owner decisions one at a time

Before editing sections that depend on them, ask the owner one focused
question at a time, in the order below. For each question:

- explain the concrete failure scenario;
- state the Codex position, Fable position, and synthesis opinion where they
  differ;
- present the viable choices and consequences;
- give a recommendation; and
- wait for the answer before asking the next question or editing dependent
  plan/ADR/glossary text.

The synthesis opinion is advisory. The owner makes the decision.

1. **Execution-recipe identity.** Decide whether complete task/input content
   expands Whetstone's Prediction ID or is bound through a separate versioned
   `execution_recipe_digest`. Recommend the separate digest, covering the full
   domain input, workflow implementation/name/version, argument-recipe
   version, and relevant profile/parser/dataset versions. Persist it, include
   it in content-scoped workflow identity, and require exact canonical domain
   equality for `ALREADY_PRESENT`.
2. **Publication bundles and reader skew.** Decide the consumer-visible bundle
   boundaries and allowed cross-family `snapshot_seq` skew. Recommend atomic
   pointer promotion for mutually referential Analysis tables, an atomic root
   bundle for Detail data, and one transaction for kernel tables and cursor
   bookkeeping. Keep unrelated artifact families independent only with
   explicit reader tolerance/check rules.
3. **Paid-call cancellation.** Decide between genuinely abortable async
   provider adapters and logical cancellation plus a replacement-Attempt
   quiescence fence. Recommend logical cancellation plus quiescence unless
   every supported provider adapter can prove abort semantics.
4. **Experiment-acceptance persistence.** Decide the append-only acceptance
   record and atomic current-pointer model. Recommend tying each evaluation to
   exact generation/scoring Manifest digests, domain and Operation/Attempt
   cuts, policy version, observed matrix, required profiles, and override
   facts; later evaluations make earlier ones historical.
5. **Deterministic scheduling.** Decide between a pinned/patched DBOS dequeue
   contract with a stable third ordering key and another scheduler/queue
   representation. Prefer a feasible pinned or vendored DBOS revision ordered
   by `(priority, created_at, stable_tie_break)`; otherwise choose a different
   durable representation. Never weaken deterministic shuffle or starvation
   requirements to match DBOS 2.26.0.

Record every answer consistently in v3 and, when the repository's canonical
documentation rules require it, in the applicable ADR and glossary. Preserve
the reviewer disagreement in the v3 decision record even after the owner
selects a direction.

### 3. Incorporate every strict-inclusive finding

Resolve all findings from the v2 synthesis in this priority order.

#### P0 — architecture and owner decisions

1. Bind Manifest membership and exact domain equality to a complete,
   versioned execution recipe.
2. Replace the impossible DBOS 2.26.0 equal-time ordering assumption with an
   implementable deterministic scheduling contract.
3. Define provider-call quiescence or proved adapter abort so replacement
   Attempts cannot overlap paid work.
4. Publish promised referential table sets as atomic consumer-visible bundles,
   while defining permitted skew between intentionally independent families.
5. Specify the append-only Experiment-acceptance schema, source cut,
   invalidation/history semantics, and atomic current pointer.

#### P1 — correctness contracts

6. Give successive frozen scoring selections distinct Operation identities
   and allow acceptance to combine their domain rows.
7. Define local state and provenance when a new Operation links a shared
   execution cancelled by another Operation.
8. Make Operation status genuinely total, including confirmed enqueue with
   DBOS `NOT_STARTED` and permanent enqueue-error combinations.
9. Preserve a typed full-lifecycle wait and define COPRO's explicit Analysis
   Store export, pinned-snapshot refresh, and read loop.
10. Add an operator terminal transition for abandoned partial Registration
    after lease expiry.
11. Define whether a late DBOS `SUCCESS` or `ERROR` wins over local
    cancellation intent, preserving the observed terminal result and
    cancellation disposition separately.
12. Persist and expose requested versus effective priority when a reference
    joins an already-enqueued shared execution.
13. Define the next-Attempt request ledger's `max_attempts` relationship to
    the immutable RetryPolicy; prefer an optional tightening bound or remove
    the independent input and validate an exact policy echo.

#### P2 — implementation verification gates

Retain explicit gates for:

- live MotherDuck conditional Lease and fenced bundle promotion, including
  deployed DuckDB-SQL parity;
- live Neon transactional Lease/bundle behavior under pooling;
- local DuckDB OS-lock and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret wiring;
- OTLP initialization, degradation, and safe attributes;
- replacement Whetstone rescore-selection parity; and
- COPRO and zero-spend end-to-end behavior against the new wait/export/read
  contracts.

Do not leave deterministic same-millisecond DBOS ordering as a P2 discovery
gate. The installed-package evidence makes it a P0 design problem.

### 4. Preserve accepted v2 architecture

Do not reopen these decisions without new correctness evidence:

- dr-platform is the only Attempt-ordinal authority;
- Operation membership is caller-prepared and Manifest-backed, with no new
  durable input-spooling service;
- managed DBOS executions are top-level-only and cancellation is
  non-recursive;
- every destination/artifact has destination-local leasing and fencing,
  including the local DuckDB OS lock;
- Experiment completeness is strict by default and partial acceptance requires
  an explicit, persisted, stratified, operator-confirmed override;
- Operation-row serialization, execution-scoped DBOS attributes, secret-free
  workflow arguments/safe reads, and kernel-owned Export Barrier lock
  acquisition remain required; and
- operational Postgres remains durable truth, with the accepted two-plane
  Analysis/Detail direction, fresh-schema cut, append-only provenance,
  content-scoped identity, and destination-local state.

### 5. Make v3 implementation-ready

For each changed contract, define ownership, durable schema and keys, state
transitions, transaction/CAS boundary, idempotency and equality rules,
concurrency races, observability/operator controls, migration/cutover order,
cross-repository blast radius, tests, phase exit criteria, and rollback or
failure handling. Update closure/crosswalk tables so every v1 and v2 finding
has one unambiguous disposition.

Update canonical docs only after the dependent owner decision is made and only
where the repository's domain-documentation criteria require it. Keep the plan
and canonical docs mutually consistent.

### 6. Update the effort index only when v3 exists

After `v3/plan.md` is coherent:

- change v2 from `reviewed` to `superseded` in the effort index;
- add v3 as the current `draft` with a plan link;
- say that v3 feedback is not yet available and another whole-system
  convergence review is required; and
- leave all historical v0/v1/v2 packet links intact.

Do not create a v3 `reviews/` packet during this task.

## Completion checks and handoff

Before finishing:

- compare v3 against every P0, P1, P2, disagreement, and v1-disposition row in
  the v2 synthesis;
- search for stale v2 status labels, superseded contracts, old repository
  names, broken relative links, and internal contradictions;
- verify that v0, v1, and v2 artifacts did not change;
- verify canonical docs agree with recorded owner decisions;
- verify no implementation code changed; and
- run proportionate Markdown and diff checks.

Report:

1. all files created or changed;
2. the five owner decisions and their selected outcomes;
3. how every v2 synthesis finding was resolved;
4. canonical docs or ADRs updated and why;
5. current-code/dependency drift discovered;
6. verification performed and results;
7. anything explicitly deferred; and
8. any unresolved blocker that prevents v3 from becoming a coherent draft.
