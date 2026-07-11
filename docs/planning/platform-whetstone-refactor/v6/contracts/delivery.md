# Delivery, cutover, and verification contract

This normative document owns dependency and cutover order, phase boundaries,
transaction/concurrency/crash proof, pre-experiment gates, repository
verification, rollback, failure handling, and explicit deferrals.

## Part 4 — Cross-cutting

- **Dependencies:** dr-platform adds `duckdb` and `dbos[otel]` as direct
  dependencies, removes `dr-providers` and the `frames` extra, and declares
  `dbos[otel]>=2.26,<2.27`; the lockfile resolves exactly 2.26.0 and all DBOS
  contract fixtures run against that exact patch before any lock refresh.
  Existing `dr-serialize` canonical JSON is the single Manifest/request digest
  implementation. Whetstone keeps direct pandas and dr-providers
  dependencies, tracks the cut dr-platform revision, and refreshes its lock.
  Private-git CI authentication must precede `uv sync` in every affected repo.
- **DBOS contracts:** Whetstone's lockfile pins the exact resolved 2.26 patch.
  Tests cover normalized statuses, attributes/filtering, queue
  registration/retrieval and priority, enqueue identity/options, workflow/step
  inspection, recursive cancellation, and every allowlisted system-table
  field. A DBOS minor upgrade is a reviewed compatibility change.
- **Secrets:** MotherDuck token/DSN, Neon URL, DBOS system URL, and application
  database URLs stay in app-side environment/config. They never appear in
  platform defaults, DBOS workflow arguments, workflow attributes, OTLP
  attributes, logs, normal inspector reads, or standard export payloads.
- **Old data:** old `dr_dspy_*` and `published_*` tables remain readable by
  one-off old-code queries. No migration, adoption, stamp, backfill, dual
  write, compatibility view, or fallback read path is built. New code never
  writes them.

### 4.2 Migration and cutover order

Each phase must pass its exit gate before the next begins. Implementation
issues may split a phase, but may not reorder its dependency boundary.

1. **Contract preflight.** Pin DBOS 2.26.0, capture the exact public signatures
   and allowlisted schema in contract tests, prove queue priority inspection,
   record the accepted nondeterministic same-millisecond tie behavior, and
   prove live MotherDuck/Neon conditional lease, fenced promotion,
   and local-DuckDB/deployed query parity on a tiny fixture. Verify Vercel Node
   runtime and secret wiring and record fresh DB/application/queue/workflow
   names. If the exact DBOS or remote-store contracts cannot satisfy these
   tests, stop and revise the design before production code switches.
2. **Platform vocabulary and baseline.** Implement fixed naming, Pydantic
   records/options, enums, concrete Manifest recipe leaves, target refs,
   `platform_cut_version`, registration, next-Attempt, and enqueue-compensation
   ledgers plus the append-only enqueue-Claim ledger, the complete schema
   crosswalk, internally owned shared writer lock,
   Operation row locks, `change_seq` triggers, and fresh `0001`. Add pure state
   and aggregate tests before I/O flows.
3. **Platform lifecycle.** Implement caller-prepared Manifest validation,
   startup `TargetRegistry`/resolver, registrar Lease/cursor/completion CAS,
   bounded RegistrationHook pages with final-page context,
   deterministic shuffle, content-scoped enqueue, status normalization,
   append-only Attempts, automatic retry/missing reconciliation, idempotent
   caller-requested next Attempts, total status precedence, bounded
   `SubmitResult`, cancel-safe Claim eligibility/invalidation, late-enqueue
   compensation keyed to every durable Claim, reference-exclusivity rechecks,
   bounded `NO_WORKFLOW_FOUND` hazard resolution, and non-recursive reference-
   aware cancellation. A fresh
   process must resume an expired Claim and create/enqueue automatic and
   requested Attempts solely through the persisted target ref. Replace
   tests at the new external interface; delete old shallow-module tests only
   when coverage has moved.
4. **Whetstone generation cut.** Add local clock, new names/queues, generation
   target and failure mapping, secret-free workflow arguments, fresh Whetstone
   schema, and manifest-backed generation Operation adapter. Prove
   cross-Operation dedup, model-group shuffle, domain-failed regeneration, and
   absence of child workflows;
   only then remove queue_worker/fairness/stamp paths.
5. **Whetstone scoring cut.** Add scoring Item/Operation identity and target,
   freeze the populated-only candidate selection into a Manifest, migrate the experiment
   command, enforce one fixed accepted Generation relationship, assign
   monotonic ordinals to accepted Scoring relationships, add representable
   generation/scoring/candidate acceptance members, deterministic generation
   and run-pinned score selection with full `SUPERSEDED_GENERATION`
   provenance, terminal platform/DBOS promotion predicates, and the
   domain-source/platform-cut/current-pointer transaction. Prove harness-failed
   rescoring, the exact populated-only `PARTIAL` Generation Run selection
   predicate and narrowed legacy parity boundary, and its
   separation from strict and explicit partial Experiment acceptance. Prove
   stale-platform-cut detection, mutation-versus-promotion serialization, and
   narrowed parity with the retained rescore fixtures
   before deleting custom batching and raw orphan replay.
6. **Inspection and telemetry.** Land typed inspector/health models, Whetstone
   Typer commands, guarded cancel and next-Attempt controls, execution-scoped
   workflow attributes, payload-disabled workflow reads, the DBOS-2.26
   allowlisted step-metadata adapter, and optional OTLP.
   This phase is required before expensive experiments, not follow-up polish.
7. **Export and projections.** Implement kernel incremental export, DBOS and
   Whetstone staged bundle rebuilds, destination-local Leases/fencing and fault
   recovery, atomic Analysis and root-cascade Detail pointers (including
   snapshot-built platform Attempts), transactional kernel-table/cursor commit,
   explicit cross-family skew metadata, and full-rebuild equivalence checks.
   Populate a disposable local DuckDB, MotherDuck database, and Neon schema.
   Migrate COPRO and the zero-spend e2e smoke to
   `wait_operation`→explicit export→pinned Analysis Bundle reads; only after
   both pass may the old Whetstone analysis helpers be deleted.
8. **Unitbench swap.** Implement the local/remote Analysis adapters, remote
   compute policy, two-plane read routing, new allowlists/table configs, and
   parity tests plus Vercel server-only secret/runtime checks. Switch deployed environment only after every current page
   passes against the new stores; then retire `tools/unitbench_publish`.
9. **Final deletion/documentation.** Remove analysis/migration/legacy tests and
   exports named above, update READMEs/TESTING/composable/workbench docs,
   refresh dependency pins, search all three repos for old names, and run
   `graphify update .`.

### 4.3 Transaction, concurrency, and crash verification

The permanent test suite must cover:

- competing registrars with identical, reordered, truncated, extended, and
  hook-conflicting Manifests; crash/resume between every page; Lease expiry;
  stale owner CAS loss; exact resubmission; and enqueue prohibition before the
  completion CAS; confirmed abandonment of empty and partially committed
  Operations, plus completion-versus-abandonment serialization;
- crash before DBOS enqueue, after enqueue before outcome persistence, and
  after outcome before aggregate refresh;
- terminalization before late enqueue, Claim Lease expiry and replacement,
  several stale Claimants for one deterministic workflow, and claimant death
  after enqueue before outcome CAS, proving every Claim/enqueue-call identity
  survives and every compensation insert/reload uses its exact durable key;
- restart in a fresh process after an expired Claim, resolve the persisted
  target ref, resume the same Attempt, and create/enqueue later automatic and
  requested Attempts; missing/conflicting target registration fails closed;
- one shared failed execution observed by multiple Operations, with exactly
  one next ordinal per Operation and one content-scoped DBOS workflow;
- identical and distinct concurrent next-Attempt requests, source-advanced and
  maximum-exhausted dispositions, domain-failed generation, harness-failed
  scoring, and explicit retry after sticky cancellation;
- acceptance evaluation against the exact generation relationship and zero or
  more ordered scoring relationships; exact generation replay plus rejection
  of a second unequal generation relationship without pointer/source-version
  mutation; explicit missing Generation and missing Score cells; durable
  pre-scoring `PARTIAL`; accepted Manifest relationship finalization; next-Attempt reactivation,
  cancellation, and reconciliation changing the pinned platform cut;
  platform mutation racing pointer promotion; later Generation Run, Score
  Attempt, Manifest relation, and required-profile domain invalidation; exact
  evaluation replay; historical lookup; biased partial override provenance;
  and `SOURCE_ADVANCED`/`PLATFORM_CUT_ADVANCED` without stale pointer advance;
- scoring-cell selection across accepted Scoring relationships for
  success-success, failure-success, equal Attempt ordinal with different
  recipes, stale scored success followed by regeneration, populated
  `PARTIAL` plus `SUCCESS` coexistence at equal and unequal ordinals, and
  concurrent relationship/evaluation races. The fixtures prove each cell is
  pinned to its selected accepted Generation Run, other-run candidates persist
  as `SUPERSEDED_GENERATION`, and only the newest run-matched relationship with
  a success then its highest successful Attempt can win;
- populated-only rescore replacement parity using empty, space-only, tab-only,
  newline-only, and populated `PARTIAL` rows. The exact persisted predicate
  `terminal_submission_text IS NOT NULL AND terminal_submission_text ~
  '[^[:space:]]'` excludes the first four before
  selection identity and retains the populated row; selection, retry,
  outcome, and deletion fixtures assert the same boundary. `PARTIAL` remains
  scoring-eligible while failing strict Generation acceptance absent a
  separately persisted explicit policy;
- policy-gated retry, enqueue-try exhaustion, execution-attempt exhaustion,
  recovery exhaustion, sticky cancellation, and state-sensitive missing;
- reference-aware cancellation with exclusive, shared, newly racing,
  topology-violating child, partial DBOS cancellation failure, repeated
  request, already-terminal, and later-authorized workflows; recursive
  cancellation is asserted never called; a forced synchronous provider call
  continuing after DBOS `CANCELLED` proves that the confirmed replacement may
  overlap it, that the discarded call may be absent from Whetstone/export
  totals, and that DBOS replay payloads are not used to fill the gap;
- cancel during Claim and cancel followed by late enqueue, proving Claim
  invalidation, `NOT_ENQUEUED` versus delivered cancellation, idempotent DBOS
  compensation, and an append-only compensation record; kill the claimant
  after DBOS enqueue but before its losing outcome CAS and prove bounded
  reconciliation discovers the workflow and creates/replays compensation from
  the durable Claim. Cancel A, resolve any missing-row hazard, let B
  legitimately link/enqueue the same content, then run A's claimant and replay
  compensation; both must recheck exclusivity, persist `SKIPPED_SHARED`, issue
  no physical cancellation, and leave B's Attempt live. A separate bounded
  fixture proves repeated absence resolves `NO_WORKFLOW_FOUND` and unblocks
  new-reference creation;
- domain success persisted before workflow return, crash after persistence but
  before DBOS terminal success, recovery exhaustion, and promotion racing
  terminalization, proving no current acceptance promotes until all accepted-
  relationship Operations are terminal and every selected exact platform
  Attempt is terminal `SUCCEEDED` with DBOS `SUCCESS`;
- shared late success plus cancel/retry producing multiple successful
  Generation Runs, proving highest-ordinal selection and superseded provenance;
- payload-safe DBOS step inspection with serializer hooks that fail if input,
  output, or error payloads are selected or deserialized;
- cancel-then-resubmit through a new Operation with foreign cancellation
  provenance; success/error-before-cancel versus cancel-before-terminal races;
  requested/effective priority mismatch when URGENT links existing STANDARD
  work; and local terminal immutability after shared work later succeeds;
- production-isolation race for the last two Item completions, asserting the
  stored aggregate without a later repair, plus total status precedence;
- export writer/barrier ordering with an in-flight sequence allocation;
- crash/retry at every source, Lease, renewal, staging, promotion, MotherDuck,
  and Neon point; deterministic A(H1), B(H2), B-promotes, A-rejected for every
  bundle mode/destination, including `full_rebuild`; inject failure between
  every member build and prove no reader-visible pointer advances partially;
- kernel-table and cursor all-or-nothing commit, Analysis referential joins,
  Detail root closure, and Unitbench `TOLERATE_SKEW` versus
  `REQUIRE_COMPATIBLE_SNAPSHOT` reader behavior;
- full-rebuild versus incremental kernel equivalence and deterministic root
  sample completeness; and
- absent/misconfigured queues, app-version drift, missing DBOS rows, disabled
  OTLP, and unavailable telemetry exporters.

Tests control clocks, IDs, shuffle inputs, missing-observation counts, and
retry decisions; they do not sleep or depend on incidental queue timing.

Separately, load-test the Experiment row lock under concurrent Generation Run,
Score Attempt, accepted-Manifest, and acceptance-promotion writes. This is a
performance gate with recorded throughput/latency thresholds, not a current
correctness finding; failing it requires capacity or transaction-shape work
without weakening the serialization invariant.

### 4.4 Pre-experiment acceptance gates

No intensive experiment begins until all gates pass on the exact locked
revisions and a fresh disposable database:

1. **Manifest and shuffle safety (blocking):** competing/reordered/truncated
   submissions cannot alter Operation membership or enqueue early;
   deliberately model-grouped inputs are mixed
   in every 500-Item enqueue page; rerun produces identical ranks; original
   result order remains intact; no model block dominates a page beyond the
   declared fixture bound.
2. **Generation identity:** overlapping Operations for identical Predictions
   converge on the same Generation Run/workflow per attempt without duplicate
   provider calls. A fresh process resolves the persisted target and resumes an
   expired Claim plus automatic/requested later Attempts; missing or conflicting
   target registration fails closed. Golden identity fixtures prove
   `experiment_name` remains an explicit `prediction_id` input and that one
   Prediction invalidates exactly one Experiment in this cut.
3. **Generation/scoring lifecycle:** one experiment creates linked generation
   and successive selection-distinct scoring Operations, persists append-only
   domain outcomes, distinguishes
   platform success from domain outcome, regenerates after a domain failure,
   rescores after a harness failure, regenerates then scores late successful
   runs through a new selection digest, retries sticky cancellation only after
   confirmation, and resumes safely after injected process death. Strict
   completeness succeeds only with every required domain result; a deliberately
   model-biased failure remains `PARTIAL`, and an explicit stratified override
   persists its policy, counts, operator confirmation, exact source cuts, and
   immutable member matrix. Missing Generation and Score outcomes remain
   explicit cells. Before any scoring relationship exists, the command appends
   a durable `PARTIAL` evaluation with an empty canonical scoring-relationship
   set and explicit `MISSING_SCORE` members. The first accepted scoring
   relationship later produces a new evaluation without rewriting it. The
   first accepted Generation Operation/Manifest fixes membership; exact replay
   is idempotent, a second unequal relationship returns
   `GENERATION_MEMBERSHIP_CONFLICT`, and growth uses a new Experiment identity/
   version. Shared late success plus cancel/retry selects only the highest
   successful ordinal within that Generation lineage and preserves earlier
   successes as superseded provenance. For overlapping scoring cells, the
   cell is pinned to the selected accepted Generation Run; the newest accepted
   Scoring relationship containing a run-matched success wins, then its highest
   successful run-matched Attempt wins. Other-run candidates persist as
   `SUPERSEDED_GENERATION`; all candidates and supersession facts are bound
   into acceptance identity. Empty and whitespace-only `PARTIAL` rows are
   excluded before Manifest identity by the canonical persisted predicate;
   populated `PARTIAL` Generation Runs remain eligible for the deliberately
   narrowed replacement parity set but do not
   satisfy strict Generation acceptance without a separate explicit policy. A
   current strict evaluation cannot promote until every accepted-relationship
   Operation is terminal and every exact selected platform Attempt is terminal
   `SUCCEEDED` with DBOS `SUCCESS`. A
   later relevant domain outcome clears
   the pointer; a next Attempt, cancellation, or reconciliation version change
   makes the pinned platform cut stale at promotion/read time. Reevaluation
   appends and atomically promotes a new evaluation while preserving the former
   result as historical.
4. **Operator readiness:** list/show/items/attempts/workflow/queue/throttle and
   health JSON are accurate; reference-aware cancellation proves logical DBOS
   cancellation, shared-work retention, partial-failure reporting, no recursive
   cancellation, cancel-during-Claim invalidation, late-enqueue compensation,
   claimant-death compensation discovery from append-only Claims,
   `SKIPPED_SHARED` protection for a later legitimate reference, bounded
   `NO_WORKFLOW_FOUND` hazard resolution, `NOT_ENQUEUED` versus delivered
   cancellation, and a confirmed later-Attempt
   authorization. The gate injects
   cancellation during a synchronous paid call and proves that any overlap is
   labeled without claiming physical upstream stop or complete Whetstone cost
   accounting for a discarded outcome.
5. **Queue/pacing:** exact DBOS 2.26.0 status/API/schema contracts and priority
   config are verified; the allowlisted step adapter proves payloads are never
   selected or deserialized; deterministic kernel ranks and bounded pre-enqueue
   mixing are proven, while a same-millisecond/multiple-dequeuer fixture
   documents permitted final-order variance; runtime concurrency changes are
   visible, max sleep
   is enforced, and throttle pressure appears in health/traces.
6. **Export correctness:** incremental and full kernel outputs match; DBOS and
   domain rebuild bundles atomically promote one pointer; kernel tables and
   cursor bookkeeping commit together; destination-local Leases/fences reject
   older-after-newer promotion and survive every crash/partial-failure
   permutation in live MotherDuck and Neon; local DuckDB excludes a second OS
   writer; independent bundle skew is exposed and checked/tolerated by declared
   readers; no excluded DBOS payload/DSN appears.
7. **Unitbench parity:** every current aggregate, table, prediction-detail,
   and visualization query returns schema-valid results through local DuckDB
   and deployed MotherDuck/Neon adapters; remote compute policy blocks or
   confirms expensive pages as declared; Vercel preview proves Node runtime,
   server-only secret mapping, no native DuckDB bundle, and independent
   fail-closed behavior for missing Analysis versus Detail credentials.
8. **Cost/accounting:** Whetstone records remain the durable source for token
   and provider cost associated with persisted outcomes; trace and analytical
   totals reconcile to those records within exact fixture expectations. The
   gate explicitly permits provider receipts to exceed Whetstone/export totals
   when a post-cancellation result is discarded, and never uses DBOS replay
   payloads as accounting truth.
9. **Optimizer/e2e continuity:** COPRO and the zero-spend e2e smoke complete
   through typed lifecycle wait, explicit Whetstone Analysis Bundle export, and
   pinned-snapshot reads after the old operational analysis helpers are removed.

### 4.5 Repository verification

- **dr-platform:** `uv sync --group dev`; `uv run ruff check`; `uv run ty
  check`; `uv run pytest`, plus Postgres/DBOS integration and export fault
  suites explicitly documented in TESTING.md.
- **whetstone-ai:** `uv sync --group dev`; `./scripts/ci/lint.sh`;
  `./scripts/ci/unit.sh`; and the documented Postgres/DBOS integration command
  against a fresh schema.
- **unitbench:** use pnpm only; run its existing typecheck/build, `pnpm lint`,
  and `pnpm test`, plus opt-in live adapter parity tests for local DuckDB,
  MotherDuck, and Neon.
- **Cross-repo:** search code/tests/docs/config for `Batch`, `batch_submit`,
  `fair_order`, `order_key`, `dr_dspy`, `published_`, `dedup_enqueue`,
  `EnqueueOutcome`, `utc_now`, raw scoring replay, and direct DBOS system-table
  writes. Every remaining historical occurrence is either old data
  documentation or an explicit fixture. A retention check proves
  `enqueue_failure_from_whetstone_exception` remains the single named injected
  `classify_error` implementation and is not removed as a stale symbol.

### 4.6 Rollback and clean-cut assumptions

This is a code/config rollback, not a data rollback. Before cutover, preserve
the old database and deployment revision. New schemas, queue names, workflow
names, Operation keys, and stores are isolated from old ones. If a phase fails
before the final environment switch, deploy the old revision against old
names; discard the fresh new schema/stores. After new workflows execute, do
not point old code at new tables or attempt to translate durable state. Fix
forward or abandon the fresh run. There is no dual write/read period and no
automatic deletion of old or DBOS records.

### 4.7 Explicit post-experiment deferrals

V6 intentionally does not add export-aware DBOS retention, raw replay/resume/
fork controls, alert routing/threshold persistence, read-only MCP tools,
browser/Wasm analytics, generic permissions/tenancy, a web control plane,
distributed Conductor-style recovery, or direct DBOS system-table mutation.
Retention waits for measured growth and export-rebuild proofs; replay waits
for attempt/idempotency evidence; alerts wait for workload baselines; MCP must
be a thin adapter over the mature inspector.
The two typed next-Attempt reasons are not generic replay: they create a fresh
platform Attempt under the persisted bound and never resume/fork a DBOS row.
