# Whetstone contract

This normative document owns Whetstone identity, platform-boundary behavior,
generation and scoring Operations, Experiment acceptance, outcome and cost
truth, tests, and deletion/rename work. Publication schemas and reader
behavior are authoritative in the [publication contract](publication.md).

## Part 2 — whetstone-ai (lockstep overhaul)

### 2.1 Deletions

| What | Why |
|---|---|
| `analysis/` and `scripts/analysis/` (db, frames, inspect, report, plotting, sample_html and Q scripts) | Core analysis lives in Unitbench; one-offs use marimo/DuckDB against the Analysis Store. Not rebuilt. |
| `migration/` (`v0_encdec_backfill.py`, `v0_reshape.py`, ~1,400 loc) + `backfill-v0-encdec` CLI command + v0 test suites | One-time legacy backfill with no live callers; its target tables no longer exist in the fresh era. |
| `platform/queue_worker.py` | Collapses into startup `ExecutionTarget` registration; queue registration moves next to the workflow definitions. `enqueue_prediction_graph_workflows` (plural) already has zero production callers. |
| `fair_order_key` (records/hashing.py) + its column, indexes, JSONL field, and ORDER BY uses | Replaced by kernel-derived shuffle rank plus caller Service Class. |
| `db` `prediction_projection` table + its `io.py` helpers | Defined but never read or written; superseded by the export flow. |
| Entire `dr_dspy_*` Alembic history + `platform_db.py` stamp/adopt logic | Fresh single baseline with canonical names; plain `upgrade` only. |

### 2.2 Renames (frozen-string thaw)

The strings frozen during the dr_dspy→whetstone rename (to protect in-flight
durable state) thaw, because fresh tables mean no in-flight state to protect:

- Queue `dr-dspy-platform-generation-v1` → `whetstone-generation`; add
  `whetstone-scoring`. Both use `priority_enabled=True`.
- Workflow/step names `dr_dspy_platform_*_v1` → `whetstone_*`.
- `DBOS_APP_NAME "dr-dspy-platform-graph-v1"` → `whetstone`.
- Module `dspy_serialization.py` → a name reflecting its actual role.
- All `dr_dspy` table names in SQL, tests, and docs.

### 2.3 Identity

Stable content-addressed IDs stay (the concept is good), including
cross-Operation Generation Run identity. Whetstone supplies the execution
identity used by the kernel enqueue target, and each Generation Run and Score
Attempt maps one-to-one to the platform-owned Item Attempt ordinal. A
domain-outcome request never supplies an ordinal; it cites the terminal domain
row and lets dr-platform allocate the next one. The
legacy-byte-compatibility constraints and comments drop; golden digest
fixtures are re-pinned once. Digest recipes may simplify where the old bytes
forced awkward inputs.

Prediction ID remains the stable Whetstone domain identity selected by the
owner; it does not absorb workflow and argument-recipe evolution. For each
Prediction or scoring target, Whetstone instead builds the concrete
`ItemExecutionRecipe` from the exact canonical persisted domain model—including
the full task, graph, dimensions, and provider-configuration snapshots—plus
the generation or scoring workflow name/version, argument-recipe version,
application version, and applicable scoring/parser/dataset profile versions.
Generation Run, Score Attempt, platform execution, and DBOS workflow identity
include the resulting concrete `execution_recipe_digest`. Registration treats
an existing Prediction Spec as `ALREADY_PRESENT` only after exact canonical
model equality; `ON CONFLICT DO NOTHING` without comparison is forbidden.
For this cut, `experiment_name` remains an explicit input to `prediction_id`,
so one Prediction belongs to exactly one Experiment identity. Golden digest
fixtures pin that input. Cross-Experiment Prediction sharing would require a
relevance query and ascending multi-Experiment lock order and is outside this
cut.

The experiment-facing default Operation key is
`whetstone:{workflow_role}:{experiment_slug}:{operation_digest}`, where the
digest hashes the immutable group, role, and Operation spec. For scoring it
also hashes `selection_digest`, derived before registration from canonical JSON
of the complete ordered candidate Item recipes and required
scoring/parser/dataset axes. Each frozen selection therefore creates a distinct
Scoring Operation by construction; a changed selection can never collide with
an earlier Operation's immutable Manifest. One Experiment accepts exactly one
Generation Operation/Manifest, fixed by its first accepted generation
relationship, while it may link multiple Scoring Operations. Expanding or
otherwise changing generation membership requires a new Experiment identity
(normally a versioned `experiment_name`); it never appends a second Generation
Operation to the existing Experiment. Acceptance derives across that one
generation relationship and all pinned scoring domain rows and Manifest
digests. Generation Item key is `prediction_id`. Scoring Item
key hashes Generation Run ID plus scoring profile/parser/dataset axes without
the platform Attempt; adding that Attempt produces the content-scoped
`score_attempt_id`. Golden tests pin all recipes. Explicit caller Operation
keys remain supported and are checked against immutable group/role/spec and
selection digest on resubmit.

For generation ordinal `n`, `generation_run_id` hashes the Prediction ID,
concrete execution-recipe digest, and `n`; a stable domain request key hashes
`generation_run_id` plus the terminal
Generation Run outcome digest. For scoring ordinal `n`, `score_attempt_id`
hashes Generation Run ID, concrete scoring execution-recipe digest,
scoring/parser profile versions, dataset name/split, and `n`; a harness-failure
request key hashes that failed Score Attempt/harness
record plus its outcome digest. Cancellation retry uses
`cancel:{cancellation_request_id}:{item_id}`. Golden tests prove each request
creates exactly ordinal `n + 1` and that cross-Operation requests deduplicate
onto the same content-scoped execution for that ordinal.

### 2.4 Platform boundary simplification

- `submission.py` adapter shrinks: registers the versioned `ExecutionTarget`,
  prepares the immutable Manifest with recomputed recipe leaves, and calls kernel
  `submit`/`submit_jsonl`. In-memory generation already materializes
  `list(specs)`; JSONL performs a no-write manifest pass; scoring freezes the
  complete ordered candidate selection and its profile/dataset axes before
  submission rather than paging a changing query while registration commits.
  The `_enqueue_item`
  closure chain and `EnqueueOutcome` wrapping disappear.
  `enqueue_failure_from_whetstone_exception` remains as the injected
  `classify_error` implementation and maps `dr_providers.FailureClass` into
  the kernel-owned enum. The retention gate names that symbol explicitly.
- `platform_db.py` shrinks to schema upgrade with the default naming.
- `worker.py` (720-loc god-CLI) shrinks naturally after deletions; split only
  if it stays >400 loc.
- Whetstone registers its domain projections with the kernel export verb using
  the [authoritative six-member Analysis Bundle](publication.md#41-two-plane-table-inventory).
  Node Attempts remain in the Detail Bundle rather than becoming a seventh
  Analysis member.
- Whetstone defines two platform-facing `workflow_role` values:
  `generation` and `scoring`. The generation adapter submits Prediction Specs;
  the scoring adapter submits eligible Generation Runs plus the immutable
  scoring-profile/dataset axes needed to derive a content-scoped
  `score_attempt_id`. Platform Attempt ordinal maps one-to-one to the
  Whetstone generation or score attempt index for that role.
- Both targets declare `TOP_LEVEL_ONLY`; managed generation/scoring workflow
  bodies contain DBOS steps but no child-workflow start/enqueue. The existing
  external convenience starters are removed or kept strictly outside managed
  workflow bodies, and topology tests fail on any DBOS parent/child record.
- Platform workflow arguments contain only stable IDs and non-secret profile/
  dataset values. Generation/scoring steps resolve the application database
  URL from process configuration inside the execution boundary; no DSN, token,
  endpoint credential, or secret is durably serialized by DBOS.
- A Scoring Operation records its source Generation Operation key in its
  immutable caller-owned Operation spec. Whetstone, not dr-platform, waits for
  generation, selects eligible `SUCCESS` and populated `PARTIAL` Generation
  Runs, and explicitly submits the Scoring Operation. `PARTIAL` eligibility
  preserves current rescore candidate behavior but does not make that run a
  strict accepted Generation outcome; strict acceptance still requires
  `SUCCESS` unless a separately persisted explicit acceptance policy says
  otherwise. The kernel does not model a DAG or auto-start dependent
  Operations.
- `rescoring.py`'s custom chunk/in-flight accounting and
  `scoring_workflow_state.py`'s `__wrapped__` orphan replay are replaced by the
  shared bounded registration, content-scoped enqueue, reconciliation,
  attempt, retry, cancellation, and inspection contracts. Whetstone retains
  scoring eligibility, profile resolution, Score Attempt identity, and
  append-only result persistence.
- Domain-failed Generation Runs and harness-failed Score Attempts use
  `request_next_attempt(DOMAIN_OUTCOME)` after Whetstone verifies the cited
  append-only row. Cancelled work uses the same platform transition with
  `OPERATOR_CANCEL_RETRY` only through the confirmed operator command. Tests
  prove each path performs new work rather than linking to ordinal 0.
- The experiment-facing command reports both Operation statuses and one
  current append-only `ExperimentAcceptanceEvaluation`. Default `STRICT`
  acceptance requires every Manifest Prediction to have one accepted
  `GenerationRunStatus.SUCCESS` Generation Run and every required scoring
  profile for each accepted run to have a persisted
  `ScoreAttemptStatus.SUCCESS` row (not a `ScoreHarnessFailureRecord`). Any
  missing or rejected cell
  is `PARTIAL`, never complete, even when both Operations are `SUCCEEDED`.
  `PARTIAL_OVERRIDE` is allowed only through a frozen persisted policy naming
  expected-set digest, required profiles, stratum axes (at least model/task and
  scoring profile where applicable), per-stratum minimum counts and ratios,
  operator identity/confirmation/reason, and the observed count matrix/digest.
  A global ratio alone is invalid. Re-evaluation is append-only and never
  rewrites the domain outcomes or earlier evaluations it summarizes. See
  [ADR 0018](../../../../adr/0018-strict-experiment-acceptance.md) and
  [ADR 0020](../../../../adr/0020-append-only-experiment-acceptance.md).

  The command may evaluate immediately after the one Generation relationship
  is accepted and before any Scoring relationship exists. The empty canonical
  accepted-Scoring-relationship vector is valid identity input; the command
  persists a durable `PARTIAL` evaluation with explicit `MISSING_SCORE` members
  for every required cell. Accepting a later Scoring relationship increments
  the source version, clears the pointer, and produces a new append-only
  evaluation; it never rewrites the pre-scoring result.

  For each Prediction, both scoring-selection freeze and acceptance choose the
  `SUCCESS` Generation Run with the highest platform Attempt ordinal at the
  pinned source cut within the Experiment's single accepted Generation
  Operation/Item lineage. Ordinals are unique in that lineage, so no
  cross-Operation tie is possible.
  Earlier successful runs remain immutable superseded provenance, are reported
  as such, and do not create expected Score Attempt cells. The shared-late-
  success plus cancel/retry test proves the frozen scoring selection and strict
  evaluation derive the same accepted run.

  When several accepted Scoring relationships contain candidates for the same
  logical Prediction/profile/parser/dataset cell, acceptance first chooses the
  newest relationship, by its persisted monotonic accepted-Scoring-relationship
  ordinal, that has at least one successful candidate. Within that relationship
  it chooses the successful Score Attempt with the highest platform Attempt
  ordinal. Newer relationships with no successful candidate do not erase an
  older successful result. Every candidate and its selected, superseded, or
  rejected provenance is immutable; no reader recomputes a different winner.

#### Experiment-acceptance schema and transaction

Whetstone's fresh baseline adds owned Manifest relationships, five append-only
acceptance tables, and an Experiment pointer:

- `experiments` adds positive `acceptance_source_version` (starting at 1),
  nullable `current_acceptance_id`, and `acceptance_updated_at`. The pointer is
  current only when both its evaluation's domain source version and checked
  platform Operation cut still match.
- `experiment_operation_manifests` is Whetstone-owned and keyed by
  `(experiment_name, workflow_role, operation_key, manifest_digest)`, with
  selection digest for scoring, target ref, and accepted timestamp. A partial
  unique index on `experiment_name` for `workflow_role='generation'` permits
  exactly one accepted Generation Operation/Manifest per Experiment while
  retaining plural selection-distinct scoring relationships. The typed
  relationship result uses `ACCEPTED`, `ALREADY_ACCEPTED`, or
  `GENERATION_MEMBERSHIP_CONFLICT`; after the first generation relationship,
  exact replay returns `ALREADY_ACCEPTED` and any unequal generation
  Operation/Manifest returns `GENERATION_MEMBERSHIP_CONFLICT` without changing
  the Experiment source version or pointer. Membership expansion requires a
  new Experiment identity/version. The
  Whetstone RegistrationHook receives a typed final-page context and inserts or
  exact-reloads this relationship only in the same transaction that completes
  platform Registration. A partial/abandoned Registration never becomes an
  accepted Experiment input. Other unequal replay is a hard conflict. Inserting a new
  accepted relationship increments the domain source version and clears the
  pointer in that transaction. Each scoring relationship also receives a
  positive `accepted_scoring_ordinal`, allocated monotonically per Experiment
  while holding the Experiment row lock. It is unique with `experiment_name`,
  immutable, and never reused; exact replay returns the stored ordinal.
- `experiment_acceptance_evaluations` is keyed by deterministic
  `acceptance_id`, the full SHA-256 digest of canonical
  `{experiment_name, acceptance_source_version, generation_operation_key,
  generation_manifest_digest, scoring_relationships_digest,
  selected_scoring_candidates_digest, domain_cut_digest, platform_cut_digest,
  policy_digest, observed_matrix_digest}`. It persists status, the exact
  Generation Operation key and Manifest digest; the canonical scoring
  relationship vector ordered by `accepted_scoring_ordinal` and containing
  each ordinal, Operation key, Manifest digest, and selection digest; its
  digest (including the canonical empty-vector digest); the complete
  selected-scoring-input vector and digest (also valid when empty); domain and
  platform cut digests/payloads,
  required-profile set/digest, policy name/version/digest and frozen payload,
  observed matrix/digest, expected/accepted/missing/rejected counts, override
  mode, operator identity/confirmation/reason, evaluator application version,
  and creation timestamp. Rows are immutable.
- `experiment_acceptance_generation_members` is keyed by
  `(acceptance_id, prediction_id)`, identities from the single accepted
  Generation Manifest that exist before any outcome.
  It stores a required generation disposition, nullable selected
  `generation_run_id`, and nullable exact Generation Operation/Item/Attempt
  references. `MISSING`, `REJECTED`, and `SELECTED_SUCCESS` are representable;
  only the selected-success disposition permits the non-null selected run.
- `experiment_acceptance_scoring_members` is keyed by
  `(acceptance_id, prediction_id, scoring_profile_id,
  scoring_profile_version, parser_profile_id, parser_version, dataset_name,
  dataset_split)`. It stores nullable selected Generation Run and Score Attempt
  IDs, explicit `MISSING_GENERATION`, `MISSING_SCORE`, `REJECTED`, or
  `ACCEPTED` disposition, the nullable selected `accepted_scoring_ordinal`,
  exact nullable Scoring Operation/Item/Attempt references, and contributing
  relationship/Manifest identities. Thus every required cell exists even before either
  outcome.
- `experiment_acceptance_generation_candidates` is keyed by
  `(acceptance_id, prediction_id, generation_run_id)` and records
  `SELECTED`, `SUPERSEDED_SUCCESS`, or rejection provenance. It preserves all
  successful ordinals within the persisted Generation Operation/Item lineage
  without turning superseded runs into required scoring cells. All three
  generation member/candidate tables have immutable FKs. Transaction rollback
  removes uncommitted rows; persisted evaluation/member deletion is rejected
  by `ON DELETE RESTRICT` FKs and immutability triggers.
- `experiment_acceptance_scoring_candidates` is keyed by the acceptance ID,
  full logical scoring-cell key, `accepted_scoring_ordinal`, and
  `score_attempt_id`. It persists the contributing Scoring
  Operation/Manifest/selection identity, exact platform Item/Attempt ordinal,
  observed domain status, and one of `SELECTED`,
  `SUPERSEDED_RELATIONSHIP`, `SUPERSEDED_ATTEMPT`, or `REJECTED`. It records all
  candidates, including equal-ordinal candidates from different recipes, so
  the newest-successful-relationship then highest-successful-Attempt rule and
  every supersession remain reproducible.

Every transaction that inserts a relevant Generation Run, Score Attempt,
accepted generation/scoring Manifest relationship, or required-profile change
locks the Experiment, increments `acceptance_source_version`, and clears
`current_acceptance_id` before commit. This makes the prior evaluation
historical immediately; it does not delete or mutate it. The evaluator reads
one repeatable source cut and version, requires exactly one accepted Generation
relationship, loads every accepted Scoring relationship, and pins the canonically sorted
`PlatformOperationCut(operation_key, platform_cut_version)` vector for every
contributing Generation and Scoring Operation. It derives the full expected
generation/scoring matrix from that one Generation Manifest, accepted
highest-success Generation ordinals within each corresponding Operation/Item
lineage, and score winners by newest successful accepted-Scoring relationship
then highest successful Attempt ordinal inside that relationship. It persists
the complete scoring relationship order, selected-input digest, and all
candidate provenance; inserts or exact-reloads the immutable evaluation and members; then
locks the contributing platform Operation rows in ascending key order followed
by the Experiment row. Pointer promotion verifies every Operation version and
`acceptance_source_version=:read_version` before one atomic commit. Platform
mutation uses the same Operation locks, so a mutation racing promotion either
precedes the comparison and rejects it or follows the committed pointer and is
detected by the next read.

With no accepted Scoring relationship, the contributing platform cut contains
only the Generation Operation, the ordered relationship and selected-candidate
vectors are empty, and every required scoring member is `MISSING_SCORE`. The
evaluation status is durably `PARTIAL`. Later scoring acceptance follows the
ordinary source-version invalidation path and appends a distinct evaluation.

A domain CAS loss returns `SOURCE_ADVANCED`; an Operation-version loss returns
`PLATFORM_CUT_ADVANCED`. Both leave the evaluation historical and require
reevaluation. Exact replay does not duplicate members. `load_current_acceptance`
uses one repeatable-read transaction to load the pointer/evaluation and compare
the complete current Operation-version vector; mismatch returns typed
`STALE_PLATFORM_CUT`, never a current acceptance. Historical reads remain
available and never infer currentness from maximum timestamp. The pointer need
not be synchronously cleared by dr-platform, so the kernel stays domain-
agnostic; its checked version is the currentness authority.
- Before deleting `dr_platform.backoff.utc_now`, Whetstone introduces its own
  injected clock seam for `graph_workflow.py`'s three current call sites and
  updates the monkeypatched timing test. The generic `OperationProgress`
  import disappears with migration/rescore deletion or becomes `ProgressLog`
  only where a generic CLI heartbeat still exists.

### 2.5 Tests and docs

- Expected casualties: schema/migration DDL assertions (~200), queue_worker
  backoff/dedup tests (their subject moves into the kernel), analysis and v0
  suites. Preserved: import-isolation tests, records contracts (new goldens),
  e2e integration flow.
- Before deleting the old rescore path, fixtures pin its current selection:
  experiment and allowed Generation Run statuses; optional generation
  attempt; scoring/parser profile and dataset axes; exclusion of an existing
  Score Attempt at the requested base index; advancement after matching
  harness failures; stable fair-key/Prediction/Generation-Run ordering; limit
  behavior; orphan/in-flight classification; and multi-page selection. The new
  Manifest selection must match those candidate identities, including
  `SUCCESS` and populated `PARTIAL` Generation Runs, before the old SQL and
  batching flow are removed. A paired acceptance fixture proves that
  scoring-eligible `PARTIAL` does not satisfy strict Generation acceptance
  absent a separate explicit persisted policy.
- `optimization/copro.py` is repointed minimally to the typed lifecycle and
  Analysis Store contracts; no broader optimizer refactor. Each optimization
  iteration calls `wait_operation` for its Generation and Scoring Operations,
  explicitly exports the Whetstone Analysis Bundle, captures the committed
  bundle ID/`snapshot_seq`, and reads candidate results through a pinned
  local-DuckDB Analysis adapter backed by the bundle's `score_attempts` table;
  it refuses to drift to a newer bundle during the iteration. It never queries
  operational Postgres for analysis and export
  never happens implicitly. The zero-spend e2e smoke exercises the same
  wait→export→pinned-read loop. The old `analysis.frames` helpers are retained
  only until this replacement passes, then deleted in phase 7. Whetstone keeps
  its direct pandas dependency because COPRO still imports pandas;
  dr-platform's removed `frames` extra is not treated as a dependency source.
- Doc updates: README, `docs/composable/platform.md` (reconciled with this
  spec), `prompt.md`, `migration_log.md` (marked historical), the v0/v1
  migration docs (deleted or archived), TESTING.md.

---
