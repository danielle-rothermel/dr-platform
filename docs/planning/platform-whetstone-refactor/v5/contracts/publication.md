# Publication and two-plane reader contract

This normative document owns export and publication, bundle boundaries,
destination-local fencing, the Analysis and Detail inventories, Unitbench's
two-plane readers, confidentiality, compute policy, and destination failures.

## 1.6 Export flow (Analysis Store + Detail Store)

New module (`export.py`) with **one verb**:

- `export(...)` captures a stable source high-water mark and incrementally
  upserts rows after each destination's committed cursor and at or below that
  mark. `full_rebuild=True` recreates a destination from scratch (the escape
  hatch that keeps "rebuildable" true).
- **Standard tables (kernel-owned):** `<prefix>_operations`, `<prefix>_items`,
  `<prefix>_item_attempts`, `<prefix>_next_attempt_requests`,
  `<prefix>_enqueue_compensations`, throttle state,
  and an allowlisted DBOS telemetry
  projection. The DBOS shape includes workflow identity/status/name, queue,
  priority, safe platform attributes, application/executor identity,
  parent/fork links, recovery count, lifecycle timestamps, step identity and
  timing, child-workflow links, queue configuration, and application-version
  metadata. It explicitly excludes serialized inputs, outputs, errors,
  authenticated-role payloads, events, streams, notifications, and any column
  not on the reviewed allowlist. Current Whetstone inputs include
  `database_url`, so implicit/raw export is a credential leak. Safe typed
  platform/Whetstone records are authoritative; the inspector retrieves
  narrowly scoped DBOS detail on demand. See
  [ADR 0011](../../../../adr/0011-exclude-dbos-replay-payloads-from-export.md).
  The allowlisted DBOS telemetry projection is a full rebuild for v5: a
  DBOS-2.26-specific read adapter selects only reviewed columns in one stable
  source snapshot, builds destination staging tables, validates workflow/step
  keys and parent/child references, then atomically replaces the prior tables.
  It does not mix `workflow_status.updated_at` incrementality with
  `operation_outputs`, which has no reliable change cursor. Adapter schema
  drift fails closed; the previous projection remains readable.
- **Client augmentation:** apps register domain projections through a frozen
  Pydantic projection contract carrying build callables. Whetstone registers
  exactly the authoritative Analysis Bundle inventory in §4.1; node-attempt
  detail is not an Analysis member. A v5 client
  projection is a **full-rebuild Publication Bundle**, not a change-cursor
  artifact: each export builds uniquely named versioned staging tables for
  every bundle member from the captured source snapshot, validates all schemas,
  uniqueness, row counts, and declared cross-table referential checks, then
  atomically advances one bundle manifest/pointer. Ordinary readers resolve
  only that pointer. Failure leaves the complete previous bundle and cursor
  readable. This handles
  late-arriving joined rows correctly and matches the current rebuild
  semantics; incremental dependency tracking is deferred until measured scale
  justifies its affected-root and deletion complexity. Projections are
  designed for the analytical plane: storage-efficient, columnar-friendly,
  no raw blob dumps, but rich enough for mid-tier debugging.
  `detail_platform_attempts` is built here, inside the Whetstone application
  snapshot: it joins platform Attempts to Prediction roots and stamps the same
  domain snapshot ID. The root manifest and every root-cascaded Detail table
  form one atomic Detail Bundle. `detail_platform_attempts` is not populated
  from the independent incremental kernel bundle while claiming root-cascade
  completeness.
- **Sinks:** (1) MotherDuck sync of the DuckDB file/database — the Analysis
  Store; (2) Neon detail sink — selected tables and row/log-level rows for the
  Detail Store. Detail sampling is deterministic by declared root identity,
  never independent per row. For Whetstone the root is Prediction/Item
  identity: selecting a root cascades to all of its Generation Runs, Node
  Attempts, Score Attempts, failures, and logs so drill-through joins remain
  complete. The selection uses a versioned stable hash and threshold; v5
  starts at 100%, and changing the rate preserves membership monotonicity and
  repeatability. Root deletions/tombstones cascade through the same manifest.
  Both sinks are driven from the same verb; knobs are table selection,
  root-sample threshold, and per-sink enablement.
- No hidden triggers: export runs when the caller runs it (post-operation,
  cron, or ad hoc). Nothing in the submit/worker flows exports.
- Export commit state and publication authority are destination-local:
  DuckDB, MotherDuck, and Neon each persist one row per
  `(destination_id, bundle_key)` containing committed cursors, committed
  source `snapshot_seq`, Lease owner/expiry, monotonically increasing fencing
  token, promoted bundle ID, member checksums, and updated timestamp. Success in one destination
  never advances another. Operational Postgres supplies source change numbers
  and a new monotonically increasing `snapshot_seq` captured under the source
  barrier, but does not claim any destination committed it. See
  [ADR 0008](../../../../adr/0008-destination-local-export-state.md).
- Lease acquisition is a destination transaction that creates the state row
  or conditionally increments its fencing token only when the prior Lease is
  expired (database time) or already owned by the same run. It returns the
  token; contention returns `LEASE_HELD`, not a blind retry. A long build
  renews before one-third of its TTL remains with
  `WHERE owner=:run AND token=:token AND expires_at > destination_now()`; a
  lost renewal aborts before promotion. Only the current token holder may
  delete uniquely named bundle stages from older expired tokens. Stage names
  include destination, bundle, member, source snapshot, run ID, and fencing
  token. Cleanup owns all members by the bundle token and never deletes a
  version referenced by the current pointer.
- Promotion and all bundle cursor/snapshot commits are one destination
  transaction. Its
  CAS requires the current owner/token, an unexpired Lease, and candidate
  `snapshot_seq > committed_snapshot_seq` (or exact equality for idempotent
  replay with matching checksums). It validates every bundle member,
  swaps/upserts the complete kernel bundle or advances one reader-visible
  versioned-table pointer for a rebuild bundle, records all
  cursors/checksums/stages, and clears the Lease atomically.
  An older/equal-but-unequal candidate resolves `STALE_PROMOTION` and may not
  touch current tables. `full_rebuild=True` obeys the same monotonic test; it
  never resets ordering to zero.
- Local native DuckDB first takes a blocking exclusive OS lock with
  `fcntl.flock` on a sibling lockfile before opening the database read-write;
  the OS releases it on process death. This follows DuckDB's documented
  [single-writer-process boundary](https://duckdb.org/docs/current/connect/concurrency).
  The in-DuckDB state row and promotion
  transaction still allocate/check the token, so copied or resumed stale
  stages cannot promote. MotherDuck uses the same state table and conditional
  transaction in the attached MotherDuck database; live phase-1 tests must
  prove conditional update and atomic replacement on the exact deployed
  service before implementation proceeds. Neon uses a normal Postgres table,
  row lock/conditional update, and transaction; it never relies on
  session-level advisory locks, which are incompatible with transaction-mode
  [pooling](https://neon.com/docs/connect/connection-pooling) and
  [compute suspension](https://neon.com/docs/reference/compatibility).
- Sequence allocation is not commit ordering. Therefore every owning kernel
  write function for Operations, Items, Attempts, next-Attempt requests,
  enqueue compensations, and throttle state acquires the effort-specific
  shared Postgres advisory
  transaction lock internally before mutating exported rows; callers cannot
  opt in or bypass it through public APIs. Export acquires the matching
  exclusive **session** advisory lock first, waits for existing writers, then
  opens its repeatable-read transaction, captures the sequence high water, and
  extracts rows satisfying `previous_cursor < change_seq <= high_water` into
  destination staging. It commits the source transaction and releases the
  barrier immediately after bounded extraction; MotherDuck/Neon sync and
  destination promotion happen afterward. A crash before destination commit
  leaves its cursor unchanged and the idempotent delta is extracted again.
  Direct table mutation is private and a static search plus workflow-step
  throttle test enforce the ownership rule. See
  [ADR 0010](../../../../adr/0010-monotonic-change-sequence-with-export-barrier.md).
- `duckdb` becomes a core dependency; the `frames`/pandas extra is deleted.

Publication Bundle modes are deliberately different:

| Bundle | Source consistency | Destination write |
| --- | --- | --- |
| Kernel tables | `change_seq` delta under Export Barrier | all kernel upserts and per-table cursor bookkeeping commit in one destination transaction |
| Allowlisted DBOS telemetry | full DBOS-2.26 snapshot | independently timed validated staging build and atomic pointer/replacement |
| Whetstone Analysis projections | full application snapshot | all mutually referential Analysis members validate, then one atomic bundle pointer advances |
| Neon Detail root manifest/rows | same projection snapshot plus root sample manifest | all root-cascaded members validate, then one atomic Detail Bundle pointer advances |

The export run returns a frozen `ExportResult` with source snapshot IDs,
per-member row counts/checksums, committed bundle IDs and `snapshot_seq`
values, and one structured result per destination.
Partial success is a first-class result, never a generic exception after some
cursor already advanced.

| Failure point | Required recovery |
| --- | --- |
| Source extraction fails | Release barrier; no destination writes/cursors. |
| DuckDB staging/upsert fails | Roll back DuckDB transaction; its cursor stays unchanged. |
| DuckDB commits, MotherDuck fails | DuckDB cursor stays committed; MotherDuck cursor stays old and retries idempotently from its own state. |
| MotherDuck commits, Neon fails | MotherDuck remains valid; Neon retries from its own state. |
| Process dies before promotion | Lease expires; a newer token holder may delete the stale stage; current tables/cursor remain valid. |
| Process dies during promotion | Destination transaction rolls back both table and cursor, or commits both; never one without the other. |
| Process dies after promotion commit | Committed token/snapshot makes replay idempotent and rejects every older stage. |
| Lease expires during build | Renewal CAS fails; stale writer stops and cannot pass promotion CAS. |
| Older H1 promotes after newer H2 | H1 token/snapshot CAS fails; H2 remains visible. |
| Validation/checksum mismatch | Fail closed, retain prior destination version, report health failure. |

`full_rebuild=True` creates new staging/current tables and sets only the
selected destination's cursor after a fenced successful promotion. It does not
mutate another destination's state or reset monotonic snapshot ordering.
Export logs stable phase/artifact/destination
identifiers and never logs tokens, DSNs, or excluded payloads.

## Part 3 — unitbench (two-plane swap)

- **Retired:** `tools/unitbench_publish` CLI and the Neon `published_*`
  copy-step pipeline.
- **Analytical plane:** `read-layer.ts` becomes a typed intent interface with
  two real adapters. Local development selects `LocalDuckDbAnalysisAdapter`
  through `LOCAL_ANALYSIS_DATABASE_PATH` and runs against the exported file
  entirely on laptop compute. Deployed Vercel selects
  `MotherDuckPostgresAnalysisAdapter` through `ANALYSIS_DATABASE_URL`; no
  native DuckDB binary or persistent local filesystem is required in
  production. Both execute DuckDB SQL, validate rows at the existing boundary,
  and pass the same adapter contract/query-fixture suite.
- **Detail plane:** direct table/row viewers and the log-debugging pipeline
  read Neon detail tables fed by the kernel export's Neon sink. The detail
  schema is a **designed surface**: derived from the existing detail pages'
  query patterns, using the deterministic root-cascaded sampling contract at
  an initial 100% threshold.
- Each analytical read declares `RemoteComputePolicy` as `ALLOW`, `CONFIRM`,
  or `LOCAL_ONLY`. The local adapter ignores the remote-cost guard. The
  deployed adapter rejects `LOCAL_ONLY`; `CONFIRM` requires an explicit user
  confirmation flow before executing. This supports intensive local-only
  pages and cost warnings without changing query code or risking Vercel native
  runtime fragility.
- `DATABASE_URL` remains the Neon Detail Store connection;
  `ANALYSIS_DATABASE_URL` is MotherDuck's Postgres endpoint only in deployed
  environments. Server-only modules own both secrets. Browser/Wasm hybrid
  execution is deferred; it would require a separate client-data/auth design.
- Vercel configuration declares both variables only for server runtimes,
  verifies neither is prefixed `NEXT_PUBLIC_` nor reachable from client
  bundles, and exercises a deployed preview with the same Node runtime used by
  production. The local native DuckDB package/path is dev-only and must not be
  imported into the Vercel bundle; deployed analysis fails closed when
  `ANALYSIS_DATABASE_URL` is absent and detail pages fail independently when
  `DATABASE_URL` is absent. Secret rotation and preview/production environment
  mapping are explicit acceptance checks, not inferred from local `.env`.
- Visibility curation (which experiments show) becomes a flag/view in the
  stores, not a copy step.
- `docs/workbench/projections.md` (the old Postgres-projection consumer
  contract) is rewritten against the two-plane design.
- Frontend track = read-layer rewrite + page query changes; Python track =
  publish CLI retirement.

See [ADR 0014](../../../../adr/0014-dual-analysis-read-adapters.md).

---

## 4.1 Two-plane table inventory

This section is the authoritative Whetstone Analysis Bundle inventory. The
Analysis Store contains kernel tables, allowlisted DBOS telemetry, and exactly
these Whetstone full-rebuild projections: `experiments`, `predictions`,
`generation_runs`, `score_attempts`, `sweep_metrics`, and `failure_metrics`.
`score_attempts` is COPRO's candidate-level result input; `sweep_metrics` and
`failure_metrics` are aggregate inputs for Unitbench. Node Attempt payloads and
row-level detail remain exclusively in the Detail Bundle. These replace
aggregate reads from
`published_*` and retain the current Unitbench dimensions/measures: experiment
identity/kind, task/sample/model, generation/scoring/domain result states,
score, provider cost, latency, compression metrics, failure class/type, and
timestamps.

The Whetstone projection tables are one atomic Analysis Bundle because their
ordinary joins promise one source cut. Kernel tables plus their cursor
bookkeeping are a separate transactional bundle, and allowlisted DBOS telemetry
is another independently rebuilt bundle. Every table exposes its bundle ID and
committed `snapshot_seq`. A read contract that joins across these independent
families must declare one of two policies: `TOLERATE_SKEW`, with semantics that
remain valid across cuts, or `REQUIRE_COMPATIBLE_SNAPSHOT`, which checks bundle
metadata and fails closed when its declared maximum skew is exceeded. No reader
silently assumes universal snapshot equality.

Storage imposes no universal maximum skew between independent bundles; failure
of DBOS telemetry must not block Whetstone Analysis publication. A frozen
`SnapshotReadPolicy` makes the consumer rule explicit:
`TOLERATE_SKEW` has no numeric bound, while `REQUIRE_COMPATIBLE_SNAPSHOT`
requires non-negative `max_snapshot_seq_skew` (`0` means the same source cut).
All current Unitbench analytical queries read only the Whetstone Analysis
Bundle and all detail queries read only the Detail Bundle, so neither performs
a cross-family join. Kernel/DBOS diagnostic views remain separate. Any future
cross-family query must choose and test a policy before it can ship.

The Detail Store contains the deterministic root manifest plus
`detail_predictions`, `detail_prediction_payloads`,
`detail_generation_runs`, `detail_node_attempts`, `detail_score_attempts`,
`detail_score_harness_failures`, and `detail_platform_attempts`.
`detail_prediction_payloads` is the intentionally sensitive Whetstone-owned
surface for the current detail page's input/output/prompt/code/raw generation,
metrics, request, response, and validation fields; it is not sourced from raw
DBOS replay blobs. Every table carries the root Prediction ID and snapshot ID
so root-cascade completeness is testable. The manifest and all Detail tables
publish behind one atomic root-bundle pointer; a reader never selects members
directly by latest physical table name. There is no generic exported log
table in v5; workflow/step details are retrieved on demand through the typed
inspector.
