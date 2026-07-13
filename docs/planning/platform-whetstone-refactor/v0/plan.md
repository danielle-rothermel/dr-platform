# Platform Hard Cut — Joint Refactor Spec (v0)

**Status:** Reviewed v0 — adversarial review round 1 complete
**Date:** 2026-07-08
**Repos:** dr-platform (kernel), whetstone-ai (lockstep overhaul), unitbench (two-plane swap)
**Glossaries:** [dr-platform/CONTEXT.md](../../../../CONTEXT.md), [whetstone-ai/CONTEXT.md](../../../../../whetstone-ai/CONTEXT.md)

## Mode and goals

Both repos are in dev mode with no external users. This is a **hard cut to
the clean final state** before intensive experiments begin: breaking changes
are free, old data is abandoned in place (readable by old code, never
migrated), and every surface is hardened, simplified, and renamed to the
target domain — no compatibility shims, no deprecation paths.

### Design principles (enforced throughout)

1. **One happy path per verb.** Exactly one flow for submission, one for the
   worker lifecycle, one for export. Any second way to do a thing is deleted.
   Every flow step has exactly one named knob; unlabeled flow control is a bug.
2. **Domain-agnostic kernel.** dr-platform accepts callables and typed items,
   never step definitions; it knows nothing about LMs, prompts, or scoring.
3. **Vocabulary is law.** Operation/Item in the kernel; whetstone's domain
   nouns (prediction, generation run, score attempt, experiment) map to
   Operation/Item only at the platform boundary. Key-vs-id rule: `*_key` is
   caller-supplied identity, `*_id` is derived/generated.
4. **Model boundary rule.** Frozen Pydantic BaseModels only where data crosses
   a parse/validate/persist boundary (DB rows, JSONL, JSONB). Frozen slotted
   stdlib dataclasses for pure in-memory value/config/callable-carrying
   objects. All models frozen unless there is a stated reason.
5. **Two-plane data model.** Operational Postgres is the durable system of
   record only. The Analysis Store (DuckDB → MotherDuck) serves all aggregate
   analysis/exploration. The Detail Store (Neon) serves row/log-level viewers
   and deep debugging, fed by the same export flow with sampling knobs.

---

## Part 1 — dr-platform (kernel)

### 1.1 Deletions

| What | Why |
|---|---|
| `artifacts.py` + tests + 5 exports | Zero consumers anywhere. Restore from git if a real consumer appears. |
| `fairness.py` entirely (`fair_ordered`, `fair_ordered_windows`, `fair_ordered_item_windows`, `windows`, `Orderable`, `validate_window_size`) | Replaced by per-Item integer priority (§1.4). Enqueue-order fairness only ever interleaved within one submit call. |
| `naming.py` (`PlatformNaming`) and `ItemIdentity` (items.py) | Existed solely to preserve whetstone's frozen `dr_dspy_*` physical names, which are abandoned. Fixed canonical names; one `prefix` knob survives. |
| Old projections machinery (`projections.py` Postgres rebuild + pandas loader) and the `frames` extra | Replaced by the DuckDB export flow (§1.6). `duckdb` becomes a core dependency; pandas comes free via duckdb when needed. |
| Alembic migration `0002`, the stamp path in `db/migrate.py`, and the dual-lineage story | Stamp existed only for whetstone's frozen tables. New single `0001` baseline (§1.3). |
| Public `dedup_enqueue`, `EnqueueOutcome`, `EnqueueItem` callback type | Enqueue becomes library-executed (§1.5). Internal-only. |
| `InsertOutcome` enum + `insert_outcome_from_rowcount` | Duplicate of `ItemInsertStatus`; collapse to one enum. |
| `utc_now()` helper (backoff.py) | Inline `datetime.now(UTC)` everywhere; callers that need determinism keep passing `now=` explicitly. Whetstone defines its own helper if it wants one. |
| Dead/unused exports across `__init__` | `__init__` is rebuilt from scratch around the final API (§1.8); nothing is exported without a consumer or a documented purpose. |

### 1.2 Vocabulary and renames

"Batch" disappears as a meaningless prefix. The unit is the **Operation**
(caller-identified by `operation_key`) containing **Items**.

| Old | New |
|---|---|
| `batch_operations` / `batch_items` tables | `<prefix>_operations` / `<prefix>_items` |
| `BatchOperationRecord` / `BatchItemRecord` | `OperationRecord` / `ItemRecord` |
| `BatchOperationStatus` / `BatchItemEnqueueStatus` / `BatchItemInsertStatus` | `OperationStatus` / `ItemEnqueueStatus` / `ItemInsertStatus` |
| `BatchOperationCounts` / `BatchItemStatuses` | `OperationCounts` / `ItemStatuses` |
| `BatchSubmitResult` | `SubmitResult` |
| `submit_batch` / `submit_batch_jsonl` | `submit` / `submit_jsonl` |
| `batch_status.py` module | `status.py` |
| `submit_spec` parameter | `spec` (matches the column/record field) |
| Protocol method `item_id()` (caller-supplied) | `item_key()` (key-vs-id rule) |
| `batch_item_id()` digest | `item_id()` digest (derived: hashes `operation_key` + `item_key` + fixed recipe; no label inputs) |
| `claim_token()` | `claim_id()` (generated, `*_id` rule) |
| `order_key` column / protocol method | deleted; `priority` integer column (§1.4) |

`PlatformSchema(prefix="platform")` keeps exactly one constructor parameter:
`prefix`, defaulted. Two apps sharing a database pick different prefixes; the
Alembic version table stays prefix-parameterized.

### 1.3 Schema (new single baseline)

One new Alembic `0001` with canonical names. The old `0001`/`0002` and the
stamp/adopt machinery are deleted, not superseded.

- `<prefix>_operations`: `operation_key` (PK), `spec` JSONB, status, counts,
  timestamps. Check constraints as today (mirrored by Pydantic validators —
  keep the belt-and-suspenders design and its documentation).
- `<prefix>_items`: `item_id` (PK, derived digest), `operation_key` (FK),
  `item_key`, `group_key`, `priority` (int, NOT NULL), `spec` JSONB,
  insert/enqueue statuses, **lease columns**: `claim_id`, `claimed_at`,
  `workflow_id`, `attempt` (int, default 0), `enqueue_metadata` JSONB
  (**strictly caller-owned** — the library never reads or writes keys in it),
  `failure` JSONB, timestamps.
- `<prefix>_throttle_state`: unchanged shape (backoff + holds + tags in one
  table — deliberately composed, do not split).
- `<prefix>_export_state`: new; per-table export watermarks for §1.6.
- Enum check constraints in the migration are imported from `schema.py`
  (`enum_check`), never hand-typed.

Records model changes: `OperationRecord`, `ItemRecord`, `EnqueueFailure`,
`ThrottleBackoffState` stay Pydantic (persist boundary) and become **frozen**;
`update_batch_item_outcome`-style mutation goes through `model_copy(update=)`.

### 1.4 Priority (replaces fairness)

- Each Item carries `priority: int` (DBOS range: 1–2,147,483,647; lower runs
  first; same priority = FIFO).
- **Default: deterministically derived from the item's identity digest**,
  mapped into the priority range — a stable pseudo-random interleave, so
  concurrent operations mix and idempotent resubmission reproduces the same
  ordering. Callers may supply an explicit priority per item (optional
  protocol attribute) to control backlog.
- The library **always** sets priority at enqueue (`SetEnqueueOptions`) —
  never enqueue without one, because DBOS treats unprioritized work as
  highest-priority (jump-the-queue footgun).
- Contract: queues used with the platform must be registered with
  `priority_enabled=True`. Documented; verified at enqueue if DBOS exposes it.

### 1.5 Submission flow (the one way in)

One core pipeline: **insert records → claim → enqueue (dedup + priority)**,
with `submit(items, ...)` and `submit_jsonl(path, fields, ...)` as thin
item-source adapters over it. Windowing survives only as an internal memory
detail of the JSONL loader; it has zero ordering semantics.

**EnqueueTarget** (frozen dataclass; per-operation, passed to `submit`):

- `queue_name: str`
- `workflow: Callable` — the DBOS workflow to enqueue
- `args_for: Callable[[ItemRecord], tuple]` — item → workflow args

The library owns the entire enqueue moment: it mints `workflow_id`
deterministically from `(operation_key, item_id, attempt)`, sets priority,
and dedup-enqueues internally. Apps never touch DBOS enqueue APIs for
platform work.

**Dedup contract:** one normalized DBOS-status helper (in `dbos_config`) is
the single way any module reads a workflow's status (fixes the current
three-way modeling divergence between `dedup_enqueue`, `workflow_start_raced`,
and `observability`). Dedup skips only ACTIVE/ENQUEUED/SUCCESS workflows.
**Terminal-failed workflows do not block**: resubmitting an operation retries
its failures by incrementing `attempt`, which mints a fresh `workflow_id`.

**Empty submission** remains an ERROR-status operation (deliberate: a sweep
that produced zero items is a caller bug worth surfacing loudly). Documented
in the `submit` docstring.

Facade signatures: `EnqueueTarget` absorbs the enqueue-related parameters. If
`submit` still exceeds ~6 parameters after the reshape, introduce a single
`SubmitOptions` frozen dataclass rather than suppressing PLR0913 (currently
suppressed five times).

### 1.6 Export flow (Analysis Store + Detail Store)

New module (`export.py`) with **one verb**:

- `export(...)` incrementally upserts rows changed since the stored watermark
  (`<prefix>_export_state`) into a local DuckDB file; `full_rebuild=True`
  recreates from scratch (the escape hatch that keeps "rebuildable" true).
- **Standard tables (kernel-owned):** `<prefix>_operations`, `<prefix>_items`,
  and the DBOS system tables (workflow status, operation outputs). This is
  the platform's own telemetry — no domain knowledge involved.
- **Client augmentation:** apps register domain projections (reshaped
  `ProjectionSpec`, now a frozen dataclass carrying a build callable) that run
  in the same export pass — e.g. whetstone's predictions/generation-runs/
  score-attempts projections. Projections are *designed for* the analytical
  plane: storage-efficient, columnar-friendly, no raw blob dumps, but rich
  enough for mid-tier debugging.
- **Sinks:** (1) MotherDuck sync of the DuckDB file/database — the Analysis
  Store; (2) Neon detail sink — selected tables, optional sampling rate,
  row/log-level rows for the Detail Store. Both driven from the same verb;
  knobs: table selection, sample rate, sync on/off.
- No hidden triggers: export runs when the caller runs it (post-operation,
  cron, or ad hoc). Nothing in the submit/worker flows exports.
- `duckdb` becomes a core dependency; the `frames`/pandas extra is deleted.

### 1.7 Pacing (worker flow)

Adaptive backoff + operator holds remain **the single pacing mechanism**;
durable in-workflow `DBOS.sleep` remains the blocking primitive. No DBOS
static queue limiters (one mechanism only). The slot-starvation hazard is
neutralized by convention, stated in docs and enforced in review:
**one queue per throttle domain** — a sleeping workflow only ever blocks its
own domain's worker slots. Knobs: per-queue `worker_concurrency`, backoff
policy parameters, operator holds/tags.

### 1.8 Hygiene and structure

- `submission.py` (870 loc) decomposes: row↔model mapping moves to
  `records.py`; read helpers (`load_batch_operation` etc.) move out of the
  write module; `update_operation_summary` reuses `status.operation_counts`
  instead of re-implementing the five-way count.
- `__init__.py` is rebuilt from the final API outward (currently 94 exports;
  target is the intentional surface only). Whetstone tests that import
  non-exported internals (`prepare_submission_records`,
  `batch_item_insert_values`, …) lose that access — the coverage those tests
  provided moves into dr-platform's own suite.
- README rewritten (it currently claims the repo is an empty skeleton).
- `graphify update .` after the cut.

---

## Part 2 — whetstone-ai (lockstep overhaul)

### 2.1 Deletions

| What | Why |
|---|---|
| `analysis/` (db, frames, inspect, report, plotting, sample_html) | Core analysis lives in unitbench; one-offs happen manually in marimo against the Analysis Store. Not rebuilt. |
| `migration/` (`v0_encdec_backfill.py`, `v0_reshape.py`, ~1,400 loc) + `backfill-v0-encdec` CLI command + v0 test suites | One-time legacy backfill with no live callers; its target tables no longer exist in the fresh era. |
| `platform/queue_worker.py` | Collapses into `EnqueueTarget`; queue registration moves next to the workflow definitions. `enqueue_prediction_graph_workflows` (plural) already has zero production callers. |
| `fair_order_key` (records/hashing.py) + its column, indexes, JSONL field, and ORDER BY uses | Replaced by kernel priority. |
| `db` `prediction_projection` table + its `io.py` helpers | Defined but never read or written; superseded by the export flow. |
| Entire `dr_dspy_*` Alembic history + `platform_db.py` stamp/adopt logic | Fresh single baseline with canonical names; plain `upgrade` only. |

### 2.2 Renames (frozen-string thaw)

The strings frozen during the dr_dspy→whetstone rename (to protect in-flight
durable state) thaw, because fresh tables mean no in-flight state to protect:

- Queue `dr-dspy-platform-generation-v1` → canonical whetstone queue name.
- Workflow/step names `dr_dspy_platform_*_v1` → `whetstone_*`.
- `DBOS_APP_NAME "dr-dspy-platform-graph-v1"` → canonical.
- Module `dspy_serialization.py` → a name reflecting its actual role.
- All `dr_dspy` table names in SQL, tests, and docs.

### 2.3 Identity

Stable content-addressed IDs stay (the concept is good); the
legacy-byte-compatibility constraints and comments drop. Golden digest
fixtures are re-pinned once. Digest recipes may simplify where the old bytes
forced awkward inputs.

### 2.4 Platform boundary simplification

- `submission.py` adapter shrinks: builds an `EnqueueTarget` (queue, workflow,
  args_for) and calls kernel `submit`/`submit_jsonl`. The `_enqueue_item`
  closure chain and `EnqueueOutcome` wrapping disappear.
  `enqueue_failure_from_whetstone_exception` remains the injected
  `classify_error` — correct seam, keep the shape.
- `platform_db.py` shrinks to schema upgrade with the default naming.
- `worker.py` (720-loc god-CLI) shrinks naturally after deletions; split only
  if it stays >400 loc.
- Whetstone registers its domain projections with the kernel export verb
  (predictions, generation runs, node attempts, score attempts — designed for
  the analytical plane per §1.6).

### 2.5 Tests and docs

- Expected casualties: schema/migration DDL assertions (~200), queue_worker
  backoff/dedup tests (their subject moves into the kernel), analysis and v0
  suites. Preserved: import-isolation tests, records contracts (new goldens),
  e2e integration flow.
- `optimization/copro.py` is audited for references to deleted tables/modules
  and repointed minimally; no broader refactor.
- Doc updates: README, `docs/composable/platform.md` (reconciled with this
  spec), `prompt.md`, `migration_log.md` (marked historical), the v0/v1
  migration docs (deleted or archived), TESTING.md.

---

## Part 3 — unitbench (two-plane swap)

- **Retired:** `tools/unitbench_publish` CLI and the Neon `published_*`
  copy-step pipeline.
- **Analytical plane:** dashboards/aggregates read MotherDuck via
  `read-layer.ts` (it was built as exactly this seam).
- **Detail plane:** direct table/row viewers and the log-debugging pipeline
  read Neon detail tables fed by the kernel export's Neon sink. The detail
  schema is a **designed surface**: derived from the existing detail pages'
  query patterns, initially unsampled ("all initially, sample eventually").
- Visibility curation (which experiments show) becomes a flag/view in the
  stores, not a copy step.
- `docs/workbench/projections.md` (the old Postgres-projection consumer
  contract) is rewritten against the two-plane design.
- Frontend track = read-layer rewrite + page query changes; Python track =
  publish CLI retirement.

---

## Part 4 — Cross-cutting

- **Dependencies:** dr-platform adds `duckdb` (core). whetstone tracks
  dr-platform@main (uv git source); pins refresh at cut-over. CI's private
  git-dep PAT setup must be verified against any repo renames.
- **DBOS version:** verify the pinned dbos version supports
  `priority_enabled` queues + `SetEnqueueOptions(priority=...)`.
- **Secrets:** MotherDuck token + Neon URL live in app-side env, never in
  kernel config defaults.
- **Old data:** `dr_dspy_*` tables stay in place, readable by one-off
  queries; nothing migrates; nothing new writes to them.

### Open items to verify during implementation

1. DBOS priority API surface at the pinned version (registration + enqueue
   options + introspection of `priority_enabled`).
2. `copro.py` table/module reference audit.
3. Detail-plane table set: enumerate from unitbench's detail pages before
   designing the Neon sink schema.
4. MotherDuck access path for Next.js server-side reads (client library
   choice, connection pooling).
5. Whether whetstone's scoring flow needs an `EnqueueTarget` of its own
   (scoring workflows are scheduled/inline today, not queue-submitted).

### ADRs to write (after review rounds)

dr-platform: two-plane analysis architecture; adaptive backoff over DBOS
native limiters; priority replaces fairness; library-executed enqueue.
whetstone-ai: fresh tables, no data migration.

### Review protocol

Three adversarial rounds, two independent reviewers each (fable + codex),
findings incorporated between rounds; decision-changing findings return to
the owner before revision. Round 1: dr-platform changes + everything impacted
by them. Round 2: whetstone-ai + impacts. Round 3: the repo constellation as
a whole (including dr-providers/dr-serialize pinning, CI, secrets, and any
other on-disk consumers of whetstone's tables). Issues and orchestration
prompts are produced only after round 3.

### Revision log

- v0 (2026-07-08): initial spec from the grilling session; reviewed in round 1.
