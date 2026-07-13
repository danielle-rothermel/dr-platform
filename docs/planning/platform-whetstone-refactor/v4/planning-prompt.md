# Prompt: write the Platform and Whetstone refactor v4 plan

Work in `/Users/daniellerothermel/drotherm/repos/dr-platform` and create the
next version of the Platform and Whetstone refactor plan at:

`docs/planning/platform-whetstone-refactor/v4/plan.md`

This is planning work only. Do not implement the refactor. Do not create review
findings. Do not commit unless explicitly asked.

After the three owner decisions below have been answered, complete the v4 plan
and write the Codex and Claude review prompts in the same run without pausing
for another round of owner feedback.

## Read first

Before drafting, read these instructions and artifacts in full:

1. `docs/agents/planning.md` and `docs/agents/domain.md`;
2. `docs/planning/platform-whetstone-refactor/README.md`;
3. the frozen v3 plan at
   `docs/planning/platform-whetstone-refactor/v3/plan.md`;
4. the strict-inclusive v3 synthesis at
   `docs/planning/platform-whetstone-refactor/v3/reviews/unified-feedback.md`;
5. both underlying v3 reviews:
   - `docs/planning/platform-whetstone-refactor/v3/reviews/codex-findings.md`;
   - `docs/planning/platform-whetstone-refactor/v3/reviews/fable-findings.md`;
6. the v2 synthesis at
   `docs/planning/platform-whetstone-refactor/v2/reviews/unified-feedback.md`;
7. the current `CONTEXT.md` files and canonical glossaries for `dr-platform`,
   `whetstone-ai`, and `unitbench`;
8. ADRs 0001 through 0020 in `docs/adr/`; and
9. the current code, tests, dependency pins, and runtime configuration in all
   three repositories, plus the installed DBOS implementation and any sibling
   repository patterns cited by the reviews.

Use the applicable domain-modeling, code-quality, and planning/grilling skills
if they are available. Treat current code and installed dependency behavior as
the authority for feasibility and drift; do not rely only on the frozen plan.

## Versioning rules

- V0 through v3 are immutable. Never edit their plans, prompts, findings, or
  unified feedback.
- Copy v3's plan to `v4/plan.md` as the starting point, then revise the copy.
- V4 is the only mutable draft.
- Preserve useful v3 detail and every accepted owner decision. Remove or
  rewrite superseded text instead of layering contradictory addenda onto it.
- Add a v4 revision-history entry explaining that it incorporates the
  strict-inclusive v3 convergence synthesis.
- Keep v4 `draft` while planning. Do not mark it `in-review` until the plan and
  both issued review prompts are complete and the owner explicitly asks to
  start review.

## Required process

### 1. Re-audit the named baseline

Verify and record exact current revisions for `dr-platform`, `whetstone-ai`,
and `unitbench`, the installed DBOS version, and any material drift from the v3
baselines. Re-run the narrow code/dependency inspections needed to validate
the review evidence. If current evidence changes a finding, document the
evidence and consequence explicitly; do not silently discard the finding.

### 2. Resolve owner decisions one at a time

Before editing sections that depend on them, ask the owner one focused question
at a time, in the order below. For each question:

- explain the concrete failure scenario in plain language;
- state the Codex position, Claude position, and synthesis opinion where they
  differ;
- present the viable choices and their consequences;
- give a recommendation; and
- wait for the answer before asking the next question or editing dependent
  plan, ADR, or glossary text.

The synthesis opinion is advisory. The owner makes each decision. Do not ask
for feedback on technical corrections that the v3 synthesis already directs
unless fresh implementation evidence exposes a new material trade-off.

1. **Overlap cost truth.** Decide whether logical cancellation with accepted
   paid-call overlap must still durably account for every observable provider
   call, or whether cancellation may also cause spend undercounting. Recommend
   complete accounting through an idempotent in-step provider-call/Node-Attempt
   ledger. Require the decision to address provider success followed by
   database-commit failure and to keep DBOS replay payloads excluded from
   accounting truth.
2. **Accepted Generation Run.** Decide which successful Generation Run is
   accepted when one Prediction has multiple successful platform Attempt
   ordinals at the evaluation cut: all successes, the highest successful
   ordinal, or an explicitly persisted operator/domain selection. Recommend
   the highest successful platform Attempt ordinal, with earlier successes
   retained as superseded provenance rather than required score cells.
3. **Acceptance platform currentness.** Decide how the append-only acceptance
   pointer proves that its pinned platform cut is still current: a domain
   invalidation seam on every relevant platform mutation or an atomically
   checked platform-cut version at promotion and read time. Recommend the
   checked platform-cut version so `dr-platform` remains domain-agnostic.

Record every answer consistently in v4 and, when the repository's canonical
documentation rules require it, in the applicable ADR and glossary. Preserve
the reviewer disagreement in the v4 decision record even after the owner
selects a direction.

### 3. Incorporate every strict-inclusive v3 finding

Resolve all findings from the v3 synthesis in this priority order.

#### P0 — architecture and owner decisions

1. **Restart-safe execution target and recipe resolution.** Persist one
   immutable target key/version and define a startup registration/lookup
   contract containing the complete workflow, identity, argument, error, and
   recipe behavior. Every lifecycle entry point must use the same resolver.
   Missing or conflicting registration fails closed. Registration recomputes
   every concrete Item recipe digest and the ordered Operation aggregate.
   Keep the kernel envelope minimal and frozen; Whetstone owns and validates
   its opaque domain recipe payload.
2. **Representable, platform-current Experiment acceptance.** Key expected
   cells with identities that exist before an outcome. Represent missing
   Generation Runs and Score Attempts explicitly, with nullable selected
   outcome references or separate expected-generation and expected-scoring
   tables. Define ownership and transaction boundaries for accepted Manifest
   relationships. Apply the owner's selected platform-currentness mechanism
   to pointer promotion and reads.
3. **Durable accounting for accepted overlap.** Apply the owner's selected
   cost-truth contract. If accounting remains complete, specify the durable
   in-step ledger, idempotency key, crash gaps, reconciliation, export truth,
   and cost tests. If undercount is accepted, narrow the plan, gates, glossary,
   and ADR 0019 explicitly rather than leaving contradictory promises.
4. **Accepted Generation Run selection.** Apply the owner's deterministic rule
   when deriving expected scoring cells, freezing scoring selection, computing
   strict Experiment validity, and reporting superseded successful runs.

#### P1 — correctness contracts

5. **Prevent enqueue after logical cancellation.** Exclude cancellation intent
   from claim eligibility, invalidate outstanding Claims during cancellation,
   and require a claimant whose outcome CAS loses to cancellation to perform
   idempotent DBOS cancellation and record compensation. Distinguish
   `NOT_ENQUEUED` from a delivered DBOS cancellation.
6. **Make DBOS step inspection genuinely payload-safe.** Do not use
   `DBOSClient.list_workflow_steps` as though DBOS 2.26.0 exposes
   `load_output=False`. Use the version-specific allowlisted system-schema
   adapter unless fresh evidence justifies omitting timelines or pinning a
   patched DBOS release. Contract-test that input, output, and error payloads
   are neither loaded nor deserialized.
7. **Define one authoritative Whetstone Analysis Bundle inventory.** Make one
   section authoritative and reference it everywhere else. Start from
   `experiments`, `predictions`, `generation_runs`, `score_attempts`,
   `sweep_metrics`, and `failure_metrics`; keep node-attempt detail in the
   Detail Bundle; and name `score_attempts` as COPRO's candidate-level input.
   Change that inventory only when current reader queries provide concrete
   evidence for a different minimum set.

### 4. Preserve architecture that both v3 reviews accepted

Do not reopen these choices without new correctness evidence:

- deterministic kernel rank/claim/enqueue mixing is required, while exact
  final DBOS ordering among same-priority millisecond ties is not;
- Whetstone Analysis and Detail referential sets publish atomically, kernel
  tables and cursors commit together, and independent families use explicit
  reader skew policy;
- dr-platform remains the sole Attempt-ordinal authority;
- Operation membership is caller-prepared and Manifest-backed, managed DBOS
  workflows are top-level-only, cancellation is non-recursive, and every
  destination uses destination-local fencing;
- strict Experiment acceptance remains the default and partial acceptance
  requires explicit persisted operator confirmation;
- cancellation may overlap synchronous paid work; v4 decides accounting, not
  whether overlap itself is permitted;
- successive scoring selections are distinct Operations, foreign cancellation
  requires new local confirmation, Operation status is total, abandoned
  Registration has an operator transition, requested and effective priority
  remain visible, and next-Attempt bounds cannot loosen RetryPolicy;
- operational Postgres remains durable truth, with fresh schemas, append-only
  provenance, two-plane reads, and no compatibility migration; and
- workflow arguments and normal inspection remain secret-free, with DBOS
  replay payloads excluded from export and accounting truth.

### 5. Preserve and extend the verification gates

Retain explicit blocking gates for:

- live MotherDuck conditional Lease and fenced bundle promotion, including
  deployed DuckDB-SQL parity;
- live Neon transactional Lease/bundle behavior under pooling;
- local DuckDB OS-lock and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret wiring;
- OTLP initialization, degradation, and safe attributes;
- replacement Whetstone rescore-selection parity;
- COPRO and zero-spend end-to-end behavior against wait/export/pinned-read
  contracts; and
- same-millisecond multiple-dequeuer variance remaining inside the accepted
  kernel-mixing safety bound.

Add tests and gates for:

- restart in a fresh process after an expired Claim, followed by automatic and
  requested Attempt creation/enqueue;
- missing Generation Run, missing Score Attempt, next-Attempt reactivation,
  cancellation, reconciliation change, and a platform mutation racing
  acceptance-pointer promotion;
- shared late success plus cancel/retry producing multiple successful
  Generation Runs;
- cancel-during-claim and cancel-then-late-enqueue compensation;
- payload-safe DBOS step inspection; and
- the selected overlap-accounting guarantee using durable Whetstone/export
  totals rather than mock call counters or replay output.

Retain the Experiment-row-lock load test as a performance gate, not a current
correctness finding, unless new evidence changes that classification.

### 6. Make v4 implementation-ready

For every changed contract, define:

- repository and domain ownership;
- public types and caller responsibilities;
- durable schema, keys, constraints, and indexes;
- state transitions and total status mapping;
- transaction, lock, fencing, and CAS boundaries;
- idempotency and equality rules;
- crash windows, concurrency races, and recovery behavior;
- observability and operator controls;
- migration/cutover order and rollback or failure handling;
- cross-repository blast radius;
- exact tests and live verification;
- phase dependencies and exit criteria; and
- deletion of superseded paths in the hard cut.

Update the v1, v2, and v3 closure/crosswalk tables so every prior finding has
one unambiguous disposition. Distinguish a selected architectural direction
from complete runtime enforcement; do not mark a finding closed merely because
v4 repeats the intended invariant.

Update canonical docs only after the dependent owner decision is made and only
where the repository's domain-documentation criteria require it. Keep the
plan, ADRs, and glossaries mutually consistent.

### 7. Update the effort index when v4 is coherent

After `v4/plan.md` is coherent:

- change v3 from `reviewed` to `superseded` in the effort index;
- add v4 as the current `draft` with a plan link;
- say that v4 feedback is not yet available and another whole-system
  convergence review is required; and
- leave every historical v0-v3 plan, prompt, findings, and synthesis link
  intact.

### 8. Write the next-round review prompts without pausing

After completing the plan, canonical-doc updates, and effort-index update,
write both prompts directly:

- `docs/planning/platform-whetstone-refactor/v4/reviews/codex-prompt.md`; and
- `docs/planning/platform-whetstone-refactor/v4/reviews/fable-prompt.md`.

Use the v3 review prompts as structural templates, but update all paths,
baselines, closure questions, and source references for v4. Do not create
findings or placeholder findings files.

Both prompts must:

- enforce the execution precondition that `v4/plan.md` and the effort index
  are changed from `draft` to `in-review` without modifying contract content;
- freeze v4 once review begins and forbid the reviewer from editing the plan,
  canonical docs, implementation, or historical artifacts;
- require a fresh baseline and exact repository revisions;
- require explicit v3 P0/P1/P2 and v1/v2 closure auditing;
- preserve the three owner decisions and call out contradictions rather than
  silently choosing a different policy;
- require findings-first output with severity, concrete failure scenario,
  evidence, affected repositories, required correction, and closure impact;
- require independent gate verdicts of `REPEAT_CONVERGENCE` or
  `READY_FOR_FOCUSED_AUDITS`; and
- write only their named findings artifact when executed.

Keep the review lenses distinct:

- **Codex:** live code and installed dependency feasibility, DBOS behavior,
  schema/transaction/concurrency correctness, tests, and sibling-repository
  blast radius.
- **Claude/Fable:** architecture and domain coherence, ownership, lifecycle,
  failure walks, internal consistency, and implementation sufficiency.

The planner writes the prompts but does not execute either review.

## Completion checks and handoff

Before finishing:

- compare v4 against every P0, P1, P2, disagreement, v2/v1 disposition, and
  source-to-priority row in the v3 synthesis;
- search for stale v3 status labels, superseded contracts, contradictory bundle
  inventories, unsafe DBOS inspection claims, broken relative links, and
  internal inconsistencies;
- verify v0 through v3 artifacts did not change;
- verify canonical docs agree with the recorded owner decisions;
- verify no implementation code, tests, dependency files, or runtime
  configuration changed; and
- run proportionate Markdown and diff checks.

Report:

1. every file created or changed;
2. the three owner decisions and selected outcomes;
3. how every v3 synthesis finding was resolved;
4. canonical docs or ADRs updated and why;
5. current-code or dependency drift discovered;
6. verification performed and results;
7. both review prompts created;
8. anything explicitly deferred; and
9. any unresolved blocker that prevents v4 from becoming a coherent draft.
