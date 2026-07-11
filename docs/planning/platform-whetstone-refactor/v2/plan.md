# Platform Hard Cut — Joint Refactor Spec (v2)

**Status:** In review — frozen for whole-system convergence
**Date:** 2026-07-10
**Repos:** dr-platform (kernel), whetstone-ai (lockstep overhaul), unitbench (two-plane swap)
**Glossaries:** [dr-platform/CONTEXT.md](../../../../CONTEXT.md), [whetstone-ai/CONTEXT.md](../../../../../whetstone-ai/CONTEXT.md)
**Canonical decisions:** [Content-scoped execution identity](../../../adr/0001-content-scoped-execution-identity.md), [Platform-owned attempt lineage](../../../adr/0002-platform-owns-attempt-lineage.md), [Append-only attempt ledger](../../../adr/0003-append-only-attempt-ledger.md), [Kernel-owned failure taxonomy](../../../adr/0004-kernel-owned-failure-taxonomy.md), [Reference-aware cancellation](../../../adr/0005-reference-aware-cancellation.md), [Adaptive pacing and bounded slot occupancy](../../../adr/0006-accept-bounded-multi-domain-slot-occupancy.md), [Urgency versus shuffle order](../../../adr/0007-separate-urgency-from-shuffle-order.md), [Destination-local export state](../../../adr/0008-destination-local-export-state.md), [Manifest-backed transactional registration](../../../adr/0009-transactional-registration-hook.md), [Monotonic change sequence and export barrier](../../../adr/0010-monotonic-change-sequence-with-export-barrier.md), [DBOS export payload exclusion](../../../adr/0011-exclude-dbos-replay-payloads-from-export.md), [Scoring as a platform Operation](../../../adr/0012-scoring-as-platform-managed-operation.md), [Platform execution versus domain outcome](../../../adr/0013-separate-platform-execution-from-domain-outcome.md), [Dual analysis read adapters](../../../adr/0014-dual-analysis-read-adapters.md), [Two-plane stores](../../../adr/0015-two-plane-analysis-and-detail-stores.md), [Kernel-executed enqueue](../../../adr/0016-kernel-executes-platform-enqueue.md), [Fresh schemas without migration](../../../adr/0017-fresh-schemas-without-data-migration.md), [Strict Experiment acceptance](../../../adr/0018-strict-experiment-acceptance.md)

## Mode and goals

Both repos are in dev mode with no external users. This is a **hard cut to
the clean final state** before intensive experiments begin: breaking changes
are free, old data is abandoned in place (readable by old code, never
migrated), and every surface is hardened, simplified, and renamed to the
target domain — no compatibility shims, no deprecation paths.
See [ADR 0017](../../../adr/0017-fresh-schemas-without-data-migration.md).

### Design principles (enforced throughout)

1. **One happy path per verb.** Exactly one flow for submission, one for the
   worker lifecycle, one for export. Any second way to do a thing is deleted.
   Every flow step has exactly one named knob; unlabeled flow control is a bug.
2. **Domain-agnostic kernel.** dr-platform accepts callables and typed items,
   never step definitions; it knows nothing about LMs, prompts, or scoring.
   Its persisted `FailureClass` is a small neutral retry/pacing taxonomy;
   callers map domain-specific failures at the platform seam.
3. **Vocabulary is law.** Operation/Item in the kernel; whetstone's domain
   nouns (prediction, generation run, score attempt, experiment) map to
   Operation/Item only at the platform boundary. Key-vs-id rule: `*_key` is
   caller-supplied identity, `*_id` is derived/generated.
4. **Model boundary rule.** Structured records, options, callable-carrying
   targets, parsed input, persisted rows, and public results are frozen
   Pydantic `BaseModel`s with `extra="forbid"`. Protocols describe structural
   caller inputs. No dataclass exception is introduced; this follows the
   repository-wide Pydantic convention.
5. **Two-plane data model.** Operational Postgres is the durable system of
   record only. The Analysis Store (DuckDB → MotherDuck) serves all aggregate
   analysis/exploration. The Detail Store (Neon) serves row/log-level viewers
   and deep debugging, fed by the same export flow with sampling knobs. See
   [ADR 0015](../../../adr/0015-two-plane-analysis-and-detail-stores.md).

### Current-code re-audit (2026-07-10)

V2 was re-audited against these exact working trees:

- `dr-platform`: branch `07-08-refactor`,
  `7b9b340fd8f2717e44de36804396077b7beeb661`. Before v2 edits the tree had
  only the effort-index modification and the three untracked v1 review-result
  files (`codex-findings.md`, `fable-findings.md`, and
  `unified-feedback.md`); those review artifacts are inputs, not refactor
  implementation.
- `whetstone-ai`: branch `codex/versioned-planning-docs`,
  `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, clean before the canonical
  glossary edit required by the resolved owner decisions.
- `unitbench`: branch `codex/versioned-planning-docs`,
  `cafd493ab9e9c1940106037209b1b218097f847e`, clean.
- DBOS: installed `2.26.0` at
  `dr-platform/.venv/lib/python3.12/site-packages/dbos`; both Python lockfiles
  resolve 2.26.0 although both project declarations still say
  `dbos>=2.25.0`.

No application-code revision has moved since the v1 convergence review, so
all cited defects still reproduce. dr-platform still persists enqueue-only
rows, progressively discovers `requested_count`, lacks execution
reconciliation and export cursors, exposes 94 root names, and depends on
`dr-providers` plus the pandas `frames` extra. Whetstone still passes
`database_url` as a durable generation/scoring workflow argument, catches and
persists domain failures before returning DBOS success, registers queues
without `priority_enabled=True`, imports `dr_platform.backoff.utc_now`, and
owns caller-selected generation/scoring attempt indexes. Its rescore selector
filters by experiment, allowed Generation Run statuses, optional generation
attempt, scoring/parser profile and dataset; excludes an existing Score
Attempt at the requested base index; advances beyond matching harness-failure
indexes; and orders by fair-order key, Prediction ID, and Generation Run ID.
Unitbench still reads Neon `published_*` tables through `DATABASE_URL`, has no
`ANALYSIS_DATABASE_URL` adapter, and retains `tools/unitbench_publish`.
Whetstone's lock also still points at the obsolete dr-platform
`drprov-v02-migration` branch/revision. These are implementation drift items,
not changes to the accepted architecture.

The installed DBOS contract remains: persisted live statuses are `PENDING`,
`ENQUEUED`, and `DELAYED`; queue registration defaults
`priority_enabled=False`; `DBOSClient.list_workflows` defaults to loading
inputs and outputs; workflow attributes are one execution-scoped object;
`cancel_workflow` defaults `cancel_children=False` and recursive cancellation
does not accept an application reference predicate. DBOS 2.26 system-schema
and public-API assumptions remain exact-version contract-test obligations,
not compatibility claims for `>=2.25`.

### Unified invariants

These distinctions constrain every section below:

1. **Attempt authority is not eligibility.** dr-platform alone creates and
   numbers Attempts. Automatic retry policy, Whetstone domain policy, and an
   operator authorization are distinct reasons that may request creation.
2. **Execution terminality is not Experiment acceptance.** A DBOS workflow
   and platform Operation may succeed while Whetstone rejects the domain
   result or the Experiment remains incomplete.
3. **There are three independent lock scopes.** The source Export Barrier
   protects one extraction cut; the Operation row lock serializes
   registration, Item/Attempt mutation, and aggregate recomputation; the
   destination Publication Fence serializes one artifact's promotion.
4. **Execution identity is not reference identity.** DBOS owns one durable
   execution; dr-platform authoritatively stores every Operation/Attempt
   reference to that execution. DBOS attributes describe only the immutable
   execution.
5. **A transaction page is not an input set.** `page_size=500` bounds work in
   one transaction; only the caller-prepared immutable Manifest defines the
   complete Operation membership.

---

## Part 1 — dr-platform (kernel)

### 1.1 Deletions

| What | Why |
|---|---|
| `artifacts.py` + tests + 5 exports | Zero consumers anywhere. Restore from git if a real consumer appears. |
| `fairness.py` entirely (`fair_ordered`, `fair_ordered_windows`, `fair_ordered_item_windows`, `windows`, `Orderable`, `validate_window_size`) | Replaced by the explicit service-class and deterministic-shuffle contract (§1.4); batching remains an execution bound, not a fairness abstraction. |
| `naming.py` (`PlatformNaming`) and `ItemIdentity` (items.py) | Existed solely to preserve whetstone's frozen `dr_dspy_*` physical names, which are abandoned. Fixed canonical names; one `prefix` knob survives. |
| Old projections machinery (`projections.py` Postgres rebuild + pandas loader) and the `frames` extra | Replaced by the DuckDB export flow (§1.6). `duckdb` becomes a core dependency. Any retained pandas consumer declares pandas directly; Whetstone keeps its direct pandas dependency for COPRO. |
| Alembic migration `0002`, the stamp path in `db/migrate.py`, and the dual-lineage story | Stamp existed only for whetstone's frozen tables. New single `0001` baseline (§1.3). |
| Public `dedup_enqueue`, `EnqueueOutcome`, `EnqueueItem` callback type | Enqueue becomes library-executed (§1.5). Internal-only. |
| `InsertOutcome` enum + `insert_outcome_from_rowcount` | Duplicate of `ItemInsertStatus`; collapse to one enum. |
| `utc_now()` helper (backoff.py) | Inline `datetime.now(UTC)` in platform paths that already accept `now=`; Whetstone first adds its own injected clock for the three live graph-workflow call sites and deterministic tests. |
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
| `order_key` column / protocol method | replaced by kernel-derived `shuffle_rank`; callers select only `service_class` (§1.4) |

`PlatformSchema(prefix="platform")` keeps exactly one constructor parameter:
`prefix`, defaulted. Two apps sharing a database pick different prefixes; the
Alembic version table stays prefix-parameterized.

### 1.3 Schema (new single baseline)

One new Alembic `0001` with canonical names. The old `0001`/`0002` and the
stamp/adopt machinery are deleted, not superseded.

- `<prefix>_operations`: `operation_key` (PK), immutable `spec`, `metadata`,
  `retry_policy`, and Manifest identity; registration owner/Lease/cursor;
  full-lifecycle status; submission and execution counts; terminal reason;
  and timestamps. `status` covers registration through aggregate workflow
  execution; it never uses `COMPLETED` to mean only "enqueue finished."
  Check constraints are mirrored by Pydantic validators (keep the
  belt-and-suspenders design and its documentation).
- `<prefix>_items`: `item_id` (PK, derived digest), `operation_key` (FK),
  caller `item_key`, stable position/grouping fields retained by the schema
  crosswalk, scheduling policy, opaque `spec` JSONB, insert state,
  `current_attempt` (non-negative), and change-tracking timestamps. It does
  not overwrite prior workflow or failure history.
- `<prefix>_item_attempts`: append-only lineage keyed by
  `(item_id, attempt)`, with content-scoped `execution_key`, `workflow_id`,
  `workflow_role`, enqueue state, normalized execution outcome, lease fields
  (`claim_id`, `claimed_at`), source attempt/workflow, retry reason, source
  application version, failure/reconciliation facts, and creation, enqueue,
  terminal, and change-tracking timestamps. An attempt row may transition
  until terminal; a terminal row is immutable and a retry inserts the next
  ordinal instead of reusing or overwriting it. Multiple Operation Items may
  legitimately reference the same content-scoped DBOS workflow, so
  `workflow_id` is indexed but not globally unique in this table. See
  [ADR 0003](../../../adr/0003-append-only-attempt-ledger.md).
- `<prefix>_next_attempt_requests`: immutable idempotency ledger keyed by
  derived `request_id`, with `(item_id, request_key)` unique; source Attempt,
  typed reason, caller eligibility reference/digest, actor and confirmation
  facts, requested policy bound, persisted disposition, created Attempt when
  any, and timestamps. It records successful, exhausted, ineligible, and
  source-advanced outcomes so replaying one request returns the same result.
- Item/Attempt `enqueue_metadata` is deleted. Kernel correlation, lease,
  workflow, failure, and provenance facts use typed columns; Whetstone's
  `generation_run_id` is the typed content-scoped `execution_key`, and richer
  domain data stays in Whetstone tables. Operation-level `metadata` remains a
  caller-owned submission annotation, is validated on idempotent resubmission,
  and is never mutated by retry transitions.
- `<prefix>_throttle_state`: unchanged shape (backoff + holds + tags in one
  table — deliberately composed, do not split).
- No operational `<prefix>_export_state` table. Source tables expose monotonic
  change cursors; each destination owns its committed export state (§1.6).
- A shared Postgres sequence plus trigger assigns a unique `change_seq` on
  every insert/update of mutable exported kernel rows (Operations, Items,
  Item Attempts, next-Attempt requests, and throttle state). The baseline
  creates indexes beginning with `change_seq`; hard deletion of these rows is prohibited in the
  pre-experiment cut, so no kernel tombstone path exists yet.
- Enum check constraints in the migration are imported from `schema.py`
  (`enum_check`), never hand-typed.

Records model changes: `OperationRecord`, `ItemRecord`, `EnqueueFailure`,
`ThrottleBackoffState` stay Pydantic (persist boundary) and become **frozen**;
`update_batch_item_outcome`-style mutation goes through `model_copy(update=)`.

#### Schema and lifecycle crosswalk

The physical old names are `{prefix}_batch_submit_operations` and
`{prefix}_batch_submit_items`; v0's shorter names were factually wrong. The
fresh baseline implements this complete crosswalk.

| Current Operation column | V2 column | Action and invariant |
| --- | --- | --- |
| `operation_key` | `operation_key` | Retain PK; caller identity and idempotency key. |
| configured `group_key` label | `group_key` | Retain on Operation, not Item; Whetstone stores `experiment_name`. |
| none | `workflow_role` | New caller-owned stable role, consistent across all Items/Attempts. |
| `status` | `status` | Replace enqueue-only meaning with `REGISTERING`, `ENQUEUEING`, `RUNNING`, `CANCELLING`, `SUCCEEDED`, `PARTIAL`, `FAILED`, `CANCELLED`. |
| `requested_count` | `requested_count` | Retain; copied from the accepted Manifest before the first page write and immutable thereafter. |
| none | `manifest_version`, `manifest_digest`, `manifest_page_size`, `manifest_page_count` | New immutable caller-prepared input-set identity. |
| none | `registration_cursor`, `registration_lease_id`, `registration_lease_expires_at` | New durable registration authority; cursor is the next page index and advances only with the owning Lease CAS. |
| none | `retry_policy` | New immutable typed policy required by later automatic and caller-requested Attempt creation. |
| `inserted_count` | `inserted_count` | Retain RegistrationHook result count. |
| `already_present_count` | `already_present_count` | Retain RegistrationHook result count. |
| `enqueued_count` | `enqueued_count` | Retain as current-attempt enqueue count. |
| `already_scheduled_count` | `workflow_already_present_count` | Rename to match DBOS/workflow language. |
| `failed_count` | `enqueue_failed_count` | Rename and narrow to current-attempt enqueue failure. |
| none | `active_count`, `succeeded_count`, `terminal_failed_count`, `cancelled_count` | New current-attempt execution aggregates. |
| `spec` | `spec` | Retain immutable caller-owned Operation spec; linked scoring Operation stores its source generation key here. |
| `metadata` | `metadata` | Retain immutable caller annotations and exact resubmit equality check. |
| none | `terminal_reason` | New nullable stable reason such as `empty_submission`, `retry_exhausted`, or `cancelled`. |
| none | `cancel_requested_at` | New nullable operator-intent timestamp. |
| `created_at` | `created_at` | Retain. |
| none | `registration_completed_at`, `updated_at` | New lifecycle timestamps. |
| `completed_at` | `completed_at` | Retain name; now means aggregate terminal execution, not enqueue completion. |
| none | `change_seq` | New trigger-maintained export cursor. |

| Current Item column | V2 column/location | Action and invariant |
| --- | --- | --- |
| `batch_submit_item_id` | `item_id` | Rename PK; fixed digest of `operation_key` and caller `item_key`. |
| `operation_key` | `operation_key` | Retain FK. |
| `item_index` | `item_index` | Retain original caller order and `(operation_key, item_index)` uniqueness. |
| configured item label (`item_id`/`prediction_id`) | `item_key` | Clean-cut physical rename; `(operation_key, item_key)` remains unique. |
| configured `order_key`/`fair_order_key` | `shuffle_rank` | Delete caller ordering field; add kernel-derived stable rank. |
| none | `service_class`, `service_priority` | New checked scheduling pair. |
| none | `spec` | New opaque caller Item spec; domain tables remain authoritative for Whetstone workflow inputs. |
| `insert_status` | `insert_status` | Retain values `INSERTED`/`ALREADY_PRESENT`. |
| `enqueue_status` | Attempt `enqueue_state` | Move mutable enqueue lifecycle to the current Attempt. |
| `enqueue_metadata` | typed Attempt columns | Delete JSON; no compatibility field. |
| `failure` | Attempt `failure` | Move typed failure to the Attempt that observed it. |
| none | `current_attempt` | New non-negative pointer; starts at 0 and advances only in retry CAS. |
| `created_at` | `created_at` | Retain. |
| none | `updated_at`, `change_seq` | New mutation/export facts. |

`<prefix>_item_attempts` is new. Its columns are: `item_id`, `attempt`
(composite PK); `workflow_role`; `execution_key`; deterministic `workflow_id`;
`enqueue_state`; `enqueue_try`; `execution_state`; last normalized
`dbos_status`; `retry_disposition`; `claim_id`; `claimed_at`; `failure`;
`source_attempt`; `source_workflow_id`; `retry_reason`; nullable
`next_attempt_request_id`; `source_application_version`; missing-observation count/first/last timestamps;
DBOS cancellation request/result facts; and `created_at`, `enqueued_at`,
`terminal_at`, `updated_at`, `change_seq`. Attempt 0 has no source; later
Attempts require source provenance. `execution_key` and `workflow_id` are
indexed but not unique because content-scoped executions may be referenced by
multiple Operations.

`<prefix>_next_attempt_requests` is new. Its columns are `request_id` (PK),
`item_id`, `request_key`, `source_attempt`, `reason`, `eligibility_kind`,
`eligibility_record_id`, `eligibility_digest`, `requested_by`,
`operator_confirmed_at`, `max_attempts`, `disposition`, nullable
`created_attempt`, nullable `rejection_detail`, `created_at`, `resolved_at`,
and `change_seq`. The unique `(item_id, request_key)` constraint plus exact
payload-equality check makes request replay idempotent; `created_attempt` has a
composite FK to Item Attempts when present. Safe diagnostics only—no prompt,
output, credentials, or provider payload—may enter the eligibility fields.

Other table changes are explicit:

- `{prefix}_throttle_backoff` becomes `{prefix}_throttle_state`; all existing
  backoff/hold/tag columns remain, and `change_seq` is added.
- `{prefix}_projections` is deleted; destination-local export state replaces
  it.
- No source export-state table is created.
- Operation/Item enum, count, payload, and timestamp constraints are replaced
  by checks matching the new states. Attempt checks require claim fields only
  while claiming, a workflow ID before confirmed enqueue, terminal timestamps
  only for terminal execution states, failure payloads for error states, and
  non-negative attempt/enqueue-try values.
- Operation checks require Manifest count/page arithmetic, cursor within
  `[0, manifest_page_count]`, registration completion only at the final
  cursor, and registration Lease fields either all null or all present.
  Next-Attempt request checks enforce reason/source-state disposition shapes,
  operator confirmation for cancellation retry, and created-Attempt identity
  only for `CREATED`.
- Attempts reference Items normally; Item `(item_id, current_attempt)` has a
  composite `DEFERRABLE INITIALLY DEFERRED` FK to
  `(item_id, attempt)`, added after both tables exist. Registration can insert
  Item plus attempt 0 atomically, and no committed Item can point at a missing
  Attempt.
- Database triggers reject deletion of kernel lifecycle rows and reject any
  mutation of a terminal Attempt other than no-op equality. Retry always
  inserts a new row; retention remains deferred.
- Indexes cover Operation `(group_key)`, `(status, updated_at)`, registration
  Lease expiry, and
  `(change_seq)`; Item `(operation_key, item_index)`, `(operation_key,
  item_key)`, `(service_priority, shuffle_rank, item_id)`, and `(change_seq)`;
  Attempt `(workflow_id)`, `(execution_key, attempt)`, `(enqueue_state)`,
  `(execution_state)`, and `(change_seq)`; next-Attempt request
  `(item_id, source_attempt)`, `(disposition)`, and `(change_seq)`; throttle
  `(blocked_until)`, `(hold_until)`, and `(change_seq)`.

#### Public/model/file crosswalk

| Current surface | V2 surface | Action |
| --- | --- | --- |
| `SubmittableItem.item_id/order_key/group_key` | `SubmittableItem.item_key/spec/service_class` | `group_key` becomes a submit-level Operation value; shuffle is kernel-owned. |
| `ItemIdentity`, configurable digest labels | fixed `item_id()` recipe | Delete compatibility configuration. |
| `JsonlFieldNames(item_id, order_key, group_key)` | `JsonlFieldNames(item_key, group_key, service_class?, spec?)` | Index original position, validate one Operation group, derive shuffle rank. |
| `JsonlItemRef(item_id, order_key, byte_offset)` | `JsonlItemRef(item_key, item_index, byte_offset, service_class)` | Preserve file order separately from scheduling order. |
| unbounded `Iterable`/JSONL pages | `OperationManifest` plus `ManifestSource` | Caller freezes the complete ordered set and page descriptors before registration; platform does not spool. |
| `BatchSubmitResult.items` | bounded `SubmitResult.failure_previews` | Full detail moves to paginated inspector queries. |
| `SubmittedItem`, `EnqueueCandidate`, `EnqueueOutcome` | `ItemRecord`, `AttemptRecord`, `SubmitFailurePreview` | Delete callback-era transport shapes. |
| caller `attempt_index` controls | `NextAttemptRequest`, `NextAttemptResult`, `request_next_attempt` | Caller requests eligibility with a stable key; platform exclusively allocates the ordinal. |
| `OperationProgress` | `ProgressLog` | Rename generic CLI heartbeat helper so it is not confused with domain Operation status. |
| Postgres `ProjectionSpec`/frame loaders | export `ProjectionSpec` | New frozen Pydantic full-rebuild contract; no pandas-returning platform API. |

### 1.4 Scheduling: service class plus deterministic shuffle

Scheduling has two independent axes; neither is called fairness.

> **Pre-experiment requirement:** deterministic shuffling is mandatory, not a
> performance enhancement. Model-blocked execution order has brought down or
> invalidated multiple prior experiments. No experiment may start unless the
> acceptance gate proves that deliberately model-grouped input is mixed across
> every bounded claim/enqueue page while resubmission reproduces the same
> ranks and original result ordering remains intact.

1. `ServiceClass` is a kernel `StrEnum` with fixed DBOS priorities:
   `URGENT = 100`, `STANDARD = 1_000`, and `BACKFILL = 10_000`. Lower DBOS
   values run first. `STANDARD` is the default, and callers choose another
   class only for genuine urgency or deferrable work. The Item row persists
   both the semantic class and mapped integer for auditability; schema/model
   validation rejects a mismatched pair.
2. `shuffle_rank` is a positive 63-bit integer deterministically derived from
   the stable Item identity recipe, with `item_id` as the collision tie-break.
   Registration retains original `item_index` for caller/result order, but
   claim and enqueue pages order by
   `(service_priority, shuffle_rank, item_id)`. Thus model-grouped source input
   is stably mixed before equal-priority DBOS FIFO insertion, even if the
   generation process did not shuffle it.

The guarantee is stable approximate mixing within bounded enqueue pages, not
strict round-robin fairness. Concurrent submitters can interleave at the DBOS
queue, and sustained `URGENT` work may intentionally delay lower service
classes. Shuffle is never encoded as a wide random DBOS priority, so newly
arriving low-rank Items cannot indefinitely overtake an old Item within the
same class.

The library always sets `SetEnqueueOptions(priority=service_priority)`;
unprioritized DBOS work would otherwise jump ahead. Every platform queue is a
database-backed queue registered with `priority_enabled=True`. Before the
first claim/enqueue page, the kernel retrieves the persisted queue
configuration and fails closed if the queue is absent or priority is disabled;
the database-backed enqueue call itself does not validate this in DBOS 2.26.0.
Queue registration and startup conflict policy must not silently overwrite an
operator-adjusted runtime configuration. See
[ADR 0007](../../../adr/0007-separate-urgency-from-shuffle-order.md).

### 1.5 Submission flow (the one way in)

One core pipeline: **prepare immutable Manifest → bounded registration →
reconcile → claim → enqueue → aggregate**. `submit(manifest, source, ...)` and
`submit_jsonl(manifest, path, fields, ...)` differ only in how a caller reads
already-manifested Items. `SubmitOptions.page_size` defaults to 500 and is
captured in the Manifest; it is the one bound for registration transactions,
reconciliation/status reads, enqueue claims, and failure materialization.
Paging has no identity or fairness semantics by itself.

`OperationManifest` is a frozen Pydantic model with `format_version = 1`,
`operation_key`, `workflow_role`, `group_key`, `item_count`, `page_size`,
`items_digest`, ordered `ManifestPage` descriptors, and `manifest_digest`.
Each Item leaf is the lowercase SHA-256 hex digest of dr-serialize canonical
JSON for exactly `{"item_index", "item_key", "service_class", "spec"}`.
`items_digest` hashes canonical JSON of the ordered leaf-digest list. Each page
descriptor contains zero-based `page_index`, inclusive `start_index`,
exclusive `end_index`, and the digest of that page's ordered leaves; pages
must be contiguous, non-empty except when the whole Manifest is empty, cover
`[0, item_count)`, and use `page_size` except for the last page.
`manifest_digest` hashes canonical JSON of every preceding Manifest field
except itself. Strings are UTF-8, canonical JSON supplies key ordering and
number encoding, and the recipe is versioned; no filesystem path, byte offset,
timestamp, or caller iteration chunk enters identity. `ManifestSource` must
re-read each page and reproduce its leaf/page digests before the transaction;
for JSONL, a preflight pass writes no platform/domain state and the later pass
must match the same descriptors. The platform never durably spools caller
input.

The first submitter creates the Operation and claims registration with a
random `registration_lease_id` and database-time expiry under the Operation
row lock. A registrar may write only page `registration_cursor` and renews the
Lease in the same transaction. Before mutation, the caller derives all
Attempt-0 workflow identities for the validated page. Each page transaction
acquires, in order, the shared Export Barrier writer lock, sorted workflow
reference locks, and `SELECT ... FOR UPDATE` on the Operation;
requires exact `(manifest_digest, cursor, lease_id, lease_not_expired)`;
revalidates the page digest; invokes the typed `RegistrationHook`; inserts the
matching Items plus Attempt 0; recomputes aggregates; and advances the cursor
with one CAS. The hook may insert caller-owned domain rows but must be
idempotent and may not call DBOS or perform remote side effects. It returns a
frozen `RegistrationResult` keyed by caller `item_key`; missing, extra,
duplicate, or reordered accounting rolls back both domain and platform writes.

Only the transaction advancing the final page may set
`registration_completed_at`, clear the Lease, and make Items claimable. Every
claim, reconcile, next-Attempt request, and cancellation mutation requires
registration completion unless its explicit purpose is to terminate an
abandoned empty Operation. Enqueue is impossible while the marker is null.
After a crash, another registrar may claim only after database-time Lease
expiry and resumes at the persisted cursor; already committed pages are
re-read and digest-checked but never re-applied. A live competing registrar
returns `REGISTRATION_LEASE_HELD`. An expired holder loses every later cursor
CAS and must stop. Exact resubmission means equality of Manifest digest and
all immutable Operation fields (`group_key`, `workflow_role`, `spec`,
`metadata`, `retry_policy`, and target identity); a reordered, truncated,
extended, or otherwise changed source is a hard conflict before hooks or
enqueue. Empty Manifest invokes no hook and atomically completes registration
as `FAILED/empty_submission`. See
[ADR 0009](../../../adr/0009-transactional-registration-hook.md).

**EnqueueTarget** is a frozen Pydantic model, per Operation, with callable
fields excluded from serialization:

- `queue_name: str`
- `workflow_role: str` — caller-owned, stable, and searchable; the kernel does
  not enumerate domain roles
- `workflow: Callable` — the DBOS workflow to enqueue
- `topology: WorkflowTopology` — fixed to `TOP_LEVEL_ONLY` in this cut;
  registration rejects any other value
- `execution_for: Callable[[ItemRecord, int], ExecutionIdentity]` — Item plus
  platform attempt → content-scoped execution key and workflow ID
- `args_for: Callable[[ItemRecord, int], tuple]` — Item plus attempt →
  workflow args; returned values must pass the secret-free payload validator
- `classify_error: Callable[[BaseException], FailureSnapshot]`
- optional `registration_hook: RegistrationHook`

Managed workflow registration records the callable/name/topology tuple and
refuses duplicate names with different identities. Whetstone managed workflow
bodies may call DBOS steps but may not call `start_workflow`,
`enqueue_workflow`, or another workflow. Static checks and an integration
fixture prove managed executions have no DBOS parent/child rows. Cancellation
also fails closed without a physical DBOS call if inspection ever discovers a
descendant, treating that as topology drift rather than recursively cancelling
unknown references.

The library owns the entire enqueue moment, but not domain execution identity.
The caller supplies a stable, content-scoped execution identity through the
enqueue target; the kernel derives or receives the corresponding DBOS
`workflow_id`, sets queue options, and dedup-enqueues internally. Apps never
touch DBOS enqueue APIs for platform work. The same content submitted through
multiple Operations therefore converges on the same durable execution for a
given attempt, as recorded in
[ADR 0001](../../../adr/0001-content-scoped-execution-identity.md).
The interface ownership is recorded separately in
[ADR 0016](../../../adr/0016-kernel-executes-platform-enqueue.md).

The kernel alone owns the Item Attempt ordinal. Automatic reconciliation or
the caller-requested transition below may establish eligibility, but only the
kernel allocates under CAS before calling `execution_for` for the new ordinal.
Whetstone maps it one-to-one to Generation Run and Score Attempt indexes; it
does not maintain a second counter. When another Operation already created an
execution for that content and ordinal, enqueue deduplication links the local
Attempt reference to the existing workflow. See
[ADR 0002](../../../adr/0002-platform-owns-attempt-lineage.md).

**Dedup contract:** one normalized DBOS-status helper (in `dbos_config`) is
the single way any module reads a workflow's status (fixes the current
three-way modeling divergence between `dedup_enqueue`, `workflow_start_raced`,
and `observability`). The persisted DBOS statuses normalize without inventing
an `ACTIVE` status: live is `ENQUEUED`, `DELAYED`, or `PENDING`; success is
`SUCCESS`; retry-policy inputs remain distinct for `ERROR`,
`MAX_RECOVERY_ATTEMPTS_EXCEEDED`, `CANCELLED`, and a missing DBOS row. Live
and successful workflows block replacement.

Cancellation is sticky. `CANCELLED` is a distinct terminal platform outcome,
never part of automatic retry eligibility, and ordinary resubmission or
reconciliation must not replace it. Only an explicit, confirmed
`OPERATOR_CANCEL_RETRY` next-Attempt request may authorize later work. Raw DBOS
resume/fork remains outside the cut.

Operation cancellation is reference-aware and top-level-only. It first reads
the target's candidate workflow IDs without deciding, then acquires the shared
Export Barrier writer lock and transaction-scoped advisory reference locks for
those workflow IDs in lexical order. It next locks all target and currently
referencing Operation rows in ascending `operation_key`, Items in `item_id`
order, and current Attempts in `(item_id, attempt)` order; if the target's
current workflow set changed since the candidate read, it rolls back and
restarts the bounded page. Under those locks it records immutable
operator intent, changes Operation-local nonterminal Attempts to
`CANCEL_REQUESTED`, and snapshots their workflow IDs. A workflow is physically
eligible only when this locked predicate finds no other registered,
nonterminal current Attempt reference with the same `workflow_id` whose
Operation is not being cancelled in this transaction. Creating or linking a
new reference takes the same workflow advisory lock before its Operation row
lock and checks for any unresolved physical-cancel intent on an Attempt with
that workflow ID. A racer therefore either commits before the cancellation
lock and appears in the exclusivity predicate, or waits and then fails closed
on the committed cancellation guard; it cannot attach between the predicate
and the DBOS call.

After committing logical intent, the controller calls
`cancel_workflow(workflow_id, cancel_children=False)` only for the exclusive
top-level set. A final transaction under the same lock order records each
Attempt as physical cancel requested, skipped-shared, observed terminal, or
failed with typed diagnostics. Partial DBOS-call failure does not roll back
logical cancellation and leaves the Operation `CANCELLING` until every
physical result is resolved or explicitly acknowledged. Repeating the same
`cancellation_request_id` returns the stored plan/results and never repeats a
successful physical call; a different request against an already sticky
Attempt records `ALREADY_CANCELLED`. The old Attempt remains terminal even if
shared DBOS work later succeeds for another Operation. Any observed child
workflow is a topology violation: record it, do not physically cancel the
parent, and never invoke recursive DBOS cancellation. See
[ADR 0005](../../../adr/0005-reference-aware-cancellation.md).

**Retry policy:** `ERROR` is eligible for reconciliation only through a
frozen, typed `RetryPolicy` with a positive `max_attempts` (total Attempts,
including attempt 0) and an explicit set of retryable kernel failure classes.
The enqueue target supplies a pure error classifier at the domain seam;
classification and its safe diagnostic facts are persisted on the terminal
Attempt before the retry decision. The reconciliation transaction inserts the
next Attempt only when the classified failure is retryable and the bound is
not exhausted; it pre-derives the candidate identity and follows the same
workflow-reference-before-Operation lock order as caller-requested creation.
Missing, unclassifiable, validation, authentication, and other
permanent failures fail closed as terminal. The inspector reports retryable,
exhausted, and non-retryable failures separately. There is no unbounded or
background platform retry loop; advancement occurs during explicit bounded
reconciliation (including the reconcile phase of resubmission, inspection,
or export).

#### Caller-requested next Attempt

`request_next_attempt(request: NextAttemptRequest) -> NextAttemptResult` is
the one public transition for domain-outcome reattempts and explicit retries
after sticky cancellation. `NextAttemptRequest` is frozen and contains
`item_id`, expected `source_attempt`, caller-stable `request_key`,
`NextAttemptReason`, neutral `EligibilityReference(kind, record_id, digest)`,
`requested_by`, and optional operator confirmation. `request_id` is the fixed
SHA-256 digest of canonical JSON for `{"item_id", "request_key"}`; reusing
that pair with any unequal payload is a hard idempotency conflict.

The closed reason/source matrix is:

| Reason | Eligible current source | Additional requirement |
| --- | --- | --- |
| `DOMAIN_OUTCOME` | `SUCCEEDED` | Caller cites an append-only terminal Generation Run or Score Attempt outcome; no operator confirmation. |
| `OPERATOR_CANCEL_RETRY` | `CANCELLED` | Named operator plus non-null confirmation timestamp and cancellation request provenance. |

`ERROR` remains owned by automatic `RetryPolicy` reconciliation;
`RECOVERY_EXHAUSTED`, `MISSING`, permanent/exhausted `ERROR`, nonterminal
states, and enqueue-only failures are not eligible through this action in the
pre-experiment cut. dr-platform persists but does not interpret the caller's
domain eligibility reference. Whetstone constructs `DOMAIN_OUTCOME` only from
an append-only domain row in the same application database and pins its
digest; an audit can reproduce what it observed.

The caller derives the candidate execution/workflow identity for
`:source_attempt + 1` before mutation. The exact transaction acquires the
kernel's shared Export Barrier writer lock, the candidate workflow's advisory
reference lock, the Operation row `FOR UPDATE`, then the Item and current
Attempt, then inserts or reloads the request ledger. It also rejects any
unresolved cancellation guard for that workflow. Creation requires all predicates:
registration complete; `items.item_id = :item_id AND
items.current_attempt = :source_attempt`; current Attempt terminal and equal
to the reason's source state; request disposition unresolved; and
`:source_attempt + 1 < retry_policy.max_attempts`. It inserts Attempt
`:source_attempt + 1` with `PENDING/NOT_STARTED`, source Attempt/workflow,
reason, request ID, eligibility reference, application version, and the
caller-derived content execution identity; then updates the Item with
`WHERE item_id = :item_id AND current_attempt = :source_attempt`; resolves the
request `CREATED`; and recomputes the Operation aggregate before one commit.
Any failed predicate produces a persisted terminal request disposition and no
Attempt. `MAX_ATTEMPTS_EXHAUSTED` also supplies the Item/Operation terminal
reason where aggregation requires it.

Replaying the same request returns its persisted result. Concurrent identical
requests converge on one ledger row and one Attempt. Concurrent different
request keys naming the same source race under the Operation lock: the winner
creates exactly `source + 1`; every loser resolves `SOURCE_ADVANCED` after it
observes `current_attempt != source` and must never treat the winner as
authorization for `source + 2`. A created Attempt becomes the current
reference immediately, so Operation status leaves its prior terminal value
and becomes `ENQUEUEING`; prior cancellation timestamps and old terminal
Attempts remain immutable provenance. `OPERATOR_CANCEL_RETRY` authorizes only
the named Items—unrequested Items remain cancelled, allowing the Operation to
finish `PARTIAL`. The guarded CLI previews affected Items and maximum-attempt
exhaustion, requires confirmation, and assigns one stable request key per Item.

`FailureClass` moves into dr-platform. Whetstone maps
`dr_providers.FailureClass` in its existing classifier adapter; no
`dr_providers` type crosses or is persisted past the platform seam. After all
callers are migrated, dr-platform removes the `dr-providers` dependency. See
[ADR 0004](../../../adr/0004-kernel-owned-failure-taxonomy.md).

`MAX_RECOVERY_ATTEMPTS_EXCEEDED` normalizes to the distinct terminal outcome
`RECOVERY_EXHAUSTED`. Ordinary reconciliation never advances it, regardless
of the `RetryPolicy`; it requires future explicit operator intervention. The
inspector and health report surface recovery exhaustion separately from
retryable, retry-exhausted, cancelled, and permanent failures.

**Missing-workflow policy:** enqueue is necessarily split across the
application Postgres transaction and the DBOS system-database transaction.
The kernel first persists the Attempt, deterministic `workflow_id`, and Claim;
it then calls DBOS outside that transaction and finally records the enqueue
outcome with a CAS on `(item_id, attempt, claim_id, enqueue_state)`. A crash
before the outcome write is resolved from both the persisted enqueue state and
DBOS status:

- an unconfirmed Attempt with an expired Lease and no DBOS row is reclaimed
  and re-enqueued as the **same Attempt with the same workflow ID**;
- an unconfirmed Attempt whose workflow exists records that existing workflow
  as its enqueue outcome, without starting another;
- a confirmed enqueue is never treated as absent from one lookup. Only
  repeated absence across a configured grace period becomes terminal
  `MISSING`, preserving the first/last observation and lookup diagnostics;
- confirmed `MISSING` never creates a replacement automatically. It is an
  operator-visible integrity/configuration failure.

All transitions use source-state and `claim_id` predicates; losing a CAS means
another submitter won and the loser reloads rather than applying its stale
result.

#### Attempt state machines

Enqueue and execution are separate columns so a successful enqueue cannot be
mistaken for successful work.

| Enqueue source | Event | Enqueue target | Attempt behavior |
| --- | --- | --- | --- |
| `PENDING` | CAS claim wins | `CLAIMING` | Set fresh `claim_id`, `claimed_at`, increment `enqueue_try`. |
| `CLAIMING` | DBOS accepts workflow | `ENQUEUED` | Persist workflow/timestamp with matching claim CAS. |
| `CLAIMING` | same workflow already exists | `WORKFLOW_ALREADY_PRESENT` | Link existing workflow; never allocate a new Attempt. |
| `CLAIMING` | retryable enqueue error | `ENQUEUE_ERROR` | Persist typed failure; next reconciliation may return the same Attempt to `PENDING` while `enqueue_try < max_enqueue_tries`. |
| `CLAIMING` | permanent/exhausted enqueue error | `ENQUEUE_ERROR` | Terminal for this Item; no execution Attempt was started. |
| `CLAIMING` | Lease expires | state-sensitive recovery | Existing DBOS row confirms enqueue; absent row reclaims the same Attempt/ID. |

`RetryPolicy.max_enqueue_tries` defaults to 3 and counts separate enqueue
calls for one execution Attempt. There is no immediate sleep/retry loop: one
submit/reconcile invocation makes at most one enqueue call per claimed
Attempt. `RetryPolicy.max_attempts` defaults to 3 and counts execution Attempts
including attempt 0.

| Execution source | Normalized observation/operator event | Execution target | Replacement policy |
| --- | --- | --- | --- |
| `NOT_STARTED` | DBOS `ENQUEUED`/`DELAYED`/`PENDING` | `ACTIVE` | None. |
| `ACTIVE` | DBOS `SUCCESS` | `SUCCEEDED` | None automatically; `DOMAIN_OUTCOME` request may create the next Attempt. |
| `ACTIVE` | DBOS `ERROR` | `ERROR` | Classify and persist; insert attempt + 1 only when retryable and below bound. |
| `ACTIVE` | `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | `RECOVERY_EXHAUSTED` | Never automatic. |
| any nonterminal | reference-aware cancel | `CANCEL_REQUESTED` | Logical cancellation is immediate; physical DBOS cancellation only if exclusive. |
| `CANCEL_REQUESTED` | local cancellation finalized | `CANCELLED` | Sticky; only confirmed `OPERATOR_CANCEL_RETRY` may create the next Attempt. Shared DBOS work may still finish for another Operation. |
| confirmed enqueue | repeated absent DBOS row past grace | `MISSING` | Never automatic. |

An `ERROR` Attempt records `retry_disposition` as `RETRYABLE`, `PERMANENT`,
or `EXHAUSTED`. Creating the next Attempt and changing
`items.current_attempt` occur in one transaction guarded by
`WHERE current_attempt = :source_attempt`; the new row carries the source
workflow, application version, failure, and retry reason. The old row remains
terminal. Reconciliation may advance through already-existing shared failed
executions within one bounded pass, but stops at the first active, successful,
sticky, missing, exhausted, or newly enqueued execution.

#### Operation aggregation

Every kernel function that registers or changes Item/Attempt/request state
acquires the affected Operation row with `SELECT ... FOR UPDATE` before the
mutation and recomputes counts from current Attempts before committing. This
Operation serialization lock is independent of the shared Export Barrier
writer lock. A multi-Operation transaction takes Operation locks in ascending
`operation_key`; no code may reverse the order. Stored aggregates are never
left for a later inspector to repair.

The global application lock order is the shared Export Barrier writer lock;
for paths that create, link, or cancel references, transaction-scoped advisory
workflow locks sorted by workflow ID; Operation rows ascending by key; Items
ascending by ID; Attempts ascending by `(item_id, attempt)`; then
request/cancellation rows. Paths that do not touch workflow references omit
that lock tier but preserve the remaining order. DBOS and destination calls
occur only after the application transaction releases row locks.

The pure status function applies this total precedence, first match wins:

1. `REGISTERING` when `registration_completed_at IS NULL` (including a held or
   expired registration Lease).
2. `CANCELLING` when cancellation intent exists and any current Attempt has an
   unresolved physical-cancel disposition.
3. `ENQUEUEING` when any current Attempt is pending, claiming, or has a
   retryable enqueue error—including a newly caller-requested Attempt.
4. `RUNNING` when any current Attempt is active or an automatic execution
   retry is eligible.
5. Terminal derivation when every current Attempt is terminal:
   `SUCCEEDED` if all succeeded; `CANCELLED` if all are cancelled; `PARTIAL`
   if at least one succeeded and at least one is any non-success terminal
   state, or if explicitly retried Items finish while others remain cancelled;
   otherwise `FAILED`. Empty Manifest is `FAILED/empty_submission` and
   maximum-attempt exhaustion is preserved as a terminal reason.

Impossible mixtures fail validation rather than falling through. A
table-driven pure test covers every pairwise overlap plus registration,
cancellation, requested-Attempt, and all-terminal combinations.

Shared executions do not merge Operation status: each Operation-local Attempt
reference receives its own logical outcome while retaining the common
workflow ID for correlation.

#### DBOS call and correlation contract

Before claiming, the kernel retrieves the database-backed queue and validates
existence plus `priority_enabled=True`. For each claim it nests
`SetWorkflowID(execution.workflow_id)`,
`SetEnqueueOptions(priority=service_priority)`, and
`SetWorkflowAttributes(...)` around `DBOS.enqueue_workflow`. Attributes are
immutable execution-scoped facts only: `execution_key`, `workflow_role`,
content Attempt ordinal, and allowlisted safe content labels such as model or
dataset version. They never contain `operation_key`, platform `item_id`, a
mutable reference set, prompts, outputs, endpoints, credentials, database
URLs, or provider payloads. To find workflows for an Operation, inspectors
query authoritative Operation → Item → Attempt references, then call DBOS by
workflow ID. A later deduplicated reference never replaces the workflow's
attribute object.

Reconciliation batch-loads statuses through public DBOS/DBOSClient APIs using
workflow IDs. Application-row decisions and CAS writes occur in a separate
Postgres transaction; the plan never assumes a distributed transaction across
the application and DBOS databases and never mutates DBOS system tables.
Every normal DBOSClient workflow query passes `load_input=False` and
`load_output=False` explicitly. No standard inspector exposes a payload-read
option; any future payload debugger is a separately named, locally guarded,
redacted surface and is not part of this cut.

**Operation lifecycle:** the durable aggregate status covers submission and
execution. Registration/enqueue progress remains separately observable from
typed Item/Attempt states and counts. After each registration, enqueue,
reconciliation, or cancellation transaction, the same transaction recomputes
the Operation aggregate from authoritative platform rows. Between refreshes,
the stored aggregate may lag a DBOS transition; inspectors and export first
run bounded reconciliation before presenting it as current.

Platform terminal success means that required DBOS workflows completed
durably without a platform/infrastructure failure. It does not interpret
caller-domain outcomes: a Whetstone Generation Run may be partial/error and a
Score Attempt may be a harness failure even when the corresponding workflow
and Operation execution succeeded. Whetstone's experiment-facing result and
acceptance gates report linked Operation statuses beside the domain outcome
derived from its own append-only records. See
[ADR 0013](../../../adr/0013-separate-platform-execution-from-domain-outcome.md).
Experiment acceptance is the separate strict predicate in
[ADR 0018](../../../adr/0018-strict-experiment-acceptance.md).

**Empty submission** explicitly transitions the Operation to the failed
terminal state with an `empty_submission` reason (deliberate: an Operation
that produced zero Items is a caller bug worth surfacing loudly). It does not
fall into failure accidentally through `0 >= 0` count arithmetic.

Facade signatures: `EnqueueTarget` absorbs enqueue behavior,
`OperationManifest` fixes the page size, and frozen Pydantic `SubmitOptions`
carries registration and claim Lease durations, missing-workflow
grace/observation count, `RetryPolicy`, and `failure_preview_limit`. Defaults
are `page_size=500` while preparing a Manifest,
`registration_lease_seconds=60`, `claim_lease_seconds=60`, `missing_grace_seconds=60`,
`missing_required_observations=3`, and `failure_preview_limit=100`; every value
is positive and validated. The public facades do not suppress PLR0913.

`SubmitResult` is a bounded receipt, not a materialized Item collection and
not a workflow-result payload. It returns `operation_key`, the current
full-lifecycle status snapshot, registration/enqueue counts, total failure
count, up to `SubmitOptions.failure_preview_limit` frozen
`SubmitFailurePreview` records (default 100, each carrying stable Item/Attempt
identity, phase, and typed failure), and `failures_truncated`. It never returns
every Item. Complete Items and Attempts are available through stable,
paginated inspector queries ordered by `(item_index, item_id)`; no failure is
silently dropped because the total and truncation flag are mandatory.

### 1.6 Export flow (Analysis Store + Detail Store)

New module (`export.py`) with **one verb**:

- `export(...)` captures a stable source high-water mark and incrementally
  upserts rows after each destination's committed cursor and at or below that
  mark. `full_rebuild=True` recreates a destination from scratch (the escape
  hatch that keeps "rebuildable" true).
- **Standard tables (kernel-owned):** `<prefix>_operations`, `<prefix>_items`,
  `<prefix>_item_attempts`, `<prefix>_next_attempt_requests`, throttle state,
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
  [ADR 0011](../../../adr/0011-exclude-dbos-replay-payloads-from-export.md).
  The allowlisted DBOS telemetry projection is a full rebuild for v2: a
  DBOS-2.26-specific read adapter selects only reviewed columns in one stable
  source snapshot, builds destination staging tables, validates workflow/step
  keys and parent/child references, then atomically replaces the prior tables.
  It does not mix `workflow_status.updated_at` incrementality with
  `operation_outputs`, which has no reliable change cursor. Adapter schema
  drift fails closed; the previous projection remains readable.
- **Client augmentation:** apps register domain projections through a frozen
  Pydantic projection contract carrying a build callable — e.g. Whetstone's
  predictions/generation-runs/score-attempts projections. A v2 client
  projection is a **full rebuild**, not a change-cursor artifact: each export
  builds a uniquely named staging table from the captured source snapshot,
  validates its schema, uniqueness, row counts, and declared referential
  checks, then atomically replaces the destination table and records the
  snapshot. Failure leaves the previous table and cursor intact. This handles
  late-arriving joined rows correctly and matches the current rebuild
  semantics; incremental dependency tracking is deferred until measured scale
  justifies its affected-root and deletion complexity. Projections are
  designed for the analytical plane: storage-efficient, columnar-friendly,
  no raw blob dumps, but rich enough for mid-tier debugging.
  `detail_platform_attempts` is built here, inside the Whetstone application
  snapshot: it joins platform Attempts to Prediction roots and stamps the same
  domain snapshot ID. It is not populated from the independent incremental
  kernel artifact while claiming root-cascade completeness.
- **Sinks:** (1) MotherDuck sync of the DuckDB file/database — the Analysis
  Store; (2) Neon detail sink — selected tables and row/log-level rows for the
  Detail Store. Detail sampling is deterministic by declared root identity,
  never independent per row. For Whetstone the root is Prediction/Item
  identity: selecting a root cascades to all of its Generation Runs, Node
  Attempts, Score Attempts, failures, and logs so drill-through joins remain
  complete. The selection uses a versioned stable hash and threshold; v2
  starts at 100%, and changing the rate preserves membership monotonicity and
  repeatability. Root deletions/tombstones cascade through the same manifest.
  Both sinks are driven from the same verb; knobs are table selection,
  root-sample threshold, and per-sink enablement.
- No hidden triggers: export runs when the caller runs it (post-operation,
  cron, or ad hoc). Nothing in the submit/worker flows exports.
- Export commit state and publication authority are destination-local:
  DuckDB, MotherDuck, and Neon each persist one row per
  `(destination_id, artifact_key)` containing committed cursor, committed
  source `snapshot_seq`, Lease owner/expiry, monotonically increasing fencing
  token, promoted stage ID, and updated timestamp. Success in one destination
  never advances another. Operational Postgres supplies source change numbers
  and a new monotonically increasing `snapshot_seq` captured under the source
  barrier, but does not claim any destination committed it. See
  [ADR 0008](../../../adr/0008-destination-local-export-state.md).
- Lease acquisition is a destination transaction that creates the state row
  or conditionally increments its fencing token only when the prior Lease is
  expired (database time) or already owned by the same run. It returns the
  token; contention returns `LEASE_HELD`, not a blind retry. A long build
  renews before one-third of its TTL remains with
  `WHERE owner=:run AND token=:token AND expires_at > destination_now()`; a
  lost renewal aborts before promotion. Only the current token holder may
  delete uniquely named stages from older expired tokens. Stage names include
  destination, artifact, source snapshot, run ID, and fencing token.
- Promotion and cursor/snapshot commit are one destination transaction. Its
  CAS requires the current owner/token, an unexpired Lease, and candidate
  `snapshot_seq > committed_snapshot_seq` (or exact equality for idempotent
  replay with matching checksums). It validates the stage, swaps/upserts the
  artifact, records cursor/checksums/stage, and clears the Lease atomically.
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
  write function for Operations, Items, Attempts, next-Attempt requests, and
  throttle state acquires the effort-specific shared Postgres advisory
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
  [ADR 0010](../../../adr/0010-monotonic-change-sequence-with-export-barrier.md).
- `duckdb` becomes a core dependency; the `frames`/pandas extra is deleted.

Artifact modes are deliberately different:

| Artifact | Source consistency | Destination write |
| --- | --- | --- |
| Kernel tables | `change_seq` delta under Export Barrier | keyed upsert into staging/current tables; advance per-table cursor after commit |
| Allowlisted DBOS telemetry | full DBOS-2.26 snapshot | validated staging build and atomic replacement |
| Whetstone domain projections | full application snapshot | validated staging build and atomic replacement |
| Neon detail manifest/rows | same projection snapshot plus root sample manifest | transactional root-cascaded upsert/delete, then cursor/snapshot advance |

The export run returns a frozen `ExportResult` with source snapshot IDs,
per-artifact row counts/checksums, and one structured result per destination.
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

### 1.7 Pacing (worker flow)

Adaptive backoff + operator holds remain **the single pacing mechanism**;
durable in-workflow `DBOS.sleep` remains the blocking primitive. No DBOS
static queue limiters (one mechanism only). Whetstone keeps multi-domain graph
workflows: each node resolves its own `throttle_key`, so a sleeping workflow
may occupy a shared generation-queue slot while backing off one provider or
model. This residual slot-starvation risk is accepted, not described as
neutralized.

The pre-experiment bounds are explicit: the current 300-second maximum sleep per
preflight, sufficient `worker_concurrency` headroom, queued/active-age and
throttle-pressure health checks, runtime queue-concurrency inspection and
adjustment, and reference-aware guarded cancellation. The inspector must show
which active workflows are sleeping and their throttle domains when that
information is available from safe application state/traces. Per-Item queue
routing remains available to future single-domain clients but is not claimed
to isolate Whetstone's multi-domain graphs. Splitting graph nodes into child
workflows or moving throttling below the workflow-slot seam is explicitly out
of this hard cut. See
[ADR 0006](../../../adr/0006-accept-bounded-multi-domain-slot-occupancy.md).

### 1.8 Ownership, inspection, control, and telemetry

| Concern | dr-platform owns | DBOS owns | Whetstone owns |
| --- | --- | --- | --- |
| Identity | Operation/Item IDs, attempt ordinal, provenance | globally unique workflow ID execution | content execution key and domain record IDs |
| Lifecycle | manifest registration, enqueue, reconciliation, Attempt allocation/policy enforcement, logical cancellation, aggregates | durable workflow/step execution and raw statuses | generation/scoring domain eligibility, Experiment acceptance, and domain outcomes |
| Concurrency | Operation row serialization, application-row CAS, Claims/Leases, source Export Barrier, destination Publication Fences | queue dequeue and workflow idempotency | no parallel flow-control subsystem |
| Pacing | throttle state, holds/tags, policy | durable sleep and worker slots | throttle-key selection per node |
| Observability | typed joins, safe correlation, health derivation | workflow/step/queue facts and OTLP spans | experiment labels, provider/model/cost/domain result facts |
| Analysis | kernel export protocol and sink adapters | allowlisted telemetry source | domain projections and Unitbench-facing schemas |

The external seam is a small typed platform interface (`submit`, `reconcile`,
`request_next_attempt`, paginated `inspect_*`, `cancel_operation`, `export`).
DBOS and sink adapters are internal seams; callers do not reproduce
claim/Attempt/export mechanics.

The kernel provides frozen Pydantic inspection models for:

- Operation list/show and aggregate state;
- paginated Items and full append-only Attempt lineage;
- DBOS workflow and step timelines joined through safe attributes/IDs;
- queue configuration plus queued/active age;
- active throttle holds/backoff pressure; and
- a machine-readable health report.

Whetstone exposes these through a thin Typer CLI with human and `--json`
output. Reads use DBOSClient public APIs and application tables, not ad hoc
DBOS SQL. The health report includes oldest queued/active age, Operations with
no progress, normalized failure/missing/retry-exhaustion counts, holds and
backoff pressure, queue priority/configuration drift, application-version
mismatch, and incomplete cancellation. Thresholds are CLI inputs/config, not
persisted alert policy.

The initial controls are `cancel operation` and the guarded
`request-next-attempt` adapters for Whetstone domain outcomes or cancelled
Items. Both preview without writes until explicit confirmation where required,
write stable request identities and structured results, and preserve old
Attempts. Cancellation always uses `cancel_children=False` and fails closed on
topology drift. Generic retry, raw DBOS resume, and DBOS fork are not exposed.

Add `dbos[otel]>=2.26,<2.27` and optional config for `enable_otlp`, trace
endpoints, and `otel_attribute_format="semconv"`. No exporter configured means
normal operation. Safe platform correlation attributes plus Whetstone-owned
provider/model/token-count/provider-cost/throttle-delay facts enrich spans
where already available; prompts, outputs, credentials, database URLs, and raw
provider metadata are forbidden. Traces are diagnostic, never durable cost or
result storage. OTLP initialization/export failure degrades visibly without
failing an experiment.

### 1.9 Hygiene and structure

- `submission.py` becomes the deep manifest-registration/reconcile/enqueue
  module; `attempts.py` owns automatic and caller-requested Attempt creation;
  `records.py` owns row↔model mapping, `status.py` owns pure state/aggregate
  functions, `inspection.py` owns reads/health, `control.py` owns guarded
  cancellation, `export.py` owns the export interface, and DBOS/sink adapters
  remain private implementation modules. No new interface is introduced
  without a production and test adapter.
- `__init__.py` is rebuilt from the final API outward (currently 94 exports;
  target is the intentional surface only). Whetstone tests that import
  non-exported internals (`prepare_submission_records`,
  `batch_item_insert_values`, …) lose that access — the coverage those tests
  provided moves into dr-platform's own suite.
- The root export inventory is explicit: schema/config (`PlatformSchema`,
  `PlatformDbosConfig`, `build_dbos_config`, `build_platform_dbos_config`,
  `upgrade_platform_schema`); submission contracts (`SubmittableItem`,
  `OperationManifest`, `ManifestPage`, `ManifestSource`, `ExecutionIdentity`,
  `EnqueueTarget`, `WorkflowTopology`, `RegistrationHook`,
  `RegistrationResult`, `RetryPolicy`, `SubmitOptions`, `SubmitResult`,
  `submit`, `submit_jsonl`, `reconcile`); Attempt request contracts
  (`NextAttemptRequest`, `NextAttemptResult`, `NextAttemptReason`,
  `NextAttemptDisposition`, `EligibilityReference`, `request_next_attempt`);
  lifecycle records/enums
  (`OperationRecord`, `ItemRecord`, `AttemptRecord`, `OperationStatus`,
  `ItemInsertStatus`, `AttemptEnqueueState`, `AttemptExecutionState`,
  `RetryDisposition`, `FailureClass`, `FailureSnapshot`, `ServiceClass`);
  inspection/control (`OperationInspection`, `ItemInspection`,
  `AttemptInspection`, `HealthReport`, paginated `list_operations`,
  `inspect_operation`, `list_items`, `list_attempts`, `cancel_operation`);
  pacing (the retained throttle load/record/clear/hold/tag/delay functions and
  `ThrottleState`); export (`ProjectionSpec`, `ExportOptions`, `ExportResult`,
  `export`); and generic progress (`ProgressLog`). DBOS private exceptions,
  row mappers, status helpers, metadata keys, adapters, and CAS functions are
  module-private and absent from `__all__`.
- README rewritten (it currently claims the repo is an empty skeleton).
- `graphify update .` after the cut.

---

## Part 2 — whetstone-ai (lockstep overhaul)

### 2.1 Deletions

| What | Why |
|---|---|
| `analysis/` and `scripts/analysis/` (db, frames, inspect, report, plotting, sample_html and Q scripts) | Core analysis lives in Unitbench; one-offs use marimo/DuckDB against the Analysis Store. Not rebuilt. |
| `migration/` (`v0_encdec_backfill.py`, `v0_reshape.py`, ~1,400 loc) + `backfill-v0-encdec` CLI command + v0 test suites | One-time legacy backfill with no live callers; its target tables no longer exist in the fresh era. |
| `platform/queue_worker.py` | Collapses into `EnqueueTarget`; queue registration moves next to the workflow definitions. `enqueue_prediction_graph_workflows` (plural) already has zero production callers. |
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

The experiment-facing default Operation key is
`whetstone:{workflow_role}:{experiment_slug}:{operation_digest}`, where the
digest hashes the immutable group, role, and Operation spec. Generation Item
key is `prediction_id`. Scoring Item key hashes Generation Run ID plus scoring
profile/parser/dataset axes without the platform Attempt; adding that Attempt
produces the existing content-scoped `score_attempt_id`. Golden tests pin all
recipes. Explicit caller Operation keys remain supported and are checked
against immutable group/role/spec on resubmit.

For generation ordinal `n`, `generation_run_id` hashes the Prediction ID and
`n`; a stable domain request key hashes `generation_run_id` plus the terminal
Generation Run outcome digest. For scoring ordinal `n`, `score_attempt_id`
hashes Generation Run ID, scoring/parser profile versions, dataset name/split,
and `n`; a harness-failure request key hashes that failed Score Attempt/harness
record plus its outcome digest. Cancellation retry uses
`cancel:{cancellation_request_id}:{item_id}`. Golden tests prove each request
creates exactly ordinal `n + 1` and that cross-Operation requests deduplicate
onto the same content-scoped execution for that ordinal.

### 2.4 Platform boundary simplification

- `submission.py` adapter shrinks: prepares the immutable Manifest, builds an
  `EnqueueTarget` (queue, workflow, `args_for`), and calls kernel
  `submit`/`submit_jsonl`. In-memory generation already materializes
  `list(specs)`; JSONL performs a no-write manifest pass; scoring freezes the
  complete ordered candidate selection and its profile/dataset axes before
  submission rather than paging a changing query while registration commits.
  The `_enqueue_item`
  closure chain and `EnqueueOutcome` wrapping disappear.
  `enqueue_failure_from_whetstone_exception` remains the injected
  `classify_error` seam remains but maps `dr_providers.FailureClass` into the
  kernel-owned enum.
- `platform_db.py` shrinks to schema upgrade with the default naming.
- `worker.py` (720-loc god-CLI) shrinks naturally after deletions; split only
  if it stays >400 loc.
- Whetstone registers its domain projections with the kernel export verb
  (predictions, generation runs, node attempts, score attempts — designed for
  the analytical plane per §1.6).
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
  generation, selects eligible Generation Runs, and explicitly submits the
  Scoring Operation. The kernel does not model a DAG or auto-start dependent
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
- The experiment-facing command reports both Operation statuses and a
  separately persisted `ExperimentAcceptanceResult`. Default `STRICT`
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
  rewrites the domain outcomes it summarizes. See
  [ADR 0018](../../../adr/0018-strict-experiment-acceptance.md).
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
  Manifest selection must match those candidate identities before the old SQL
  and batching flow are removed.
- `optimization/copro.py` is audited for references to deleted tables/modules
  and repointed minimally; no broader refactor. Whetstone keeps its direct
  pandas dependency because COPRO still imports pandas; dr-platform's removed
  `frames` extra is not treated as a dependency source.
- Doc updates: README, `docs/composable/platform.md` (reconciled with this
  spec), `prompt.md`, `migration_log.md` (marked historical), the v0/v1
  migration docs (deleted or archived), TESTING.md.

---

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

See [ADR 0014](../../../adr/0014-dual-analysis-read-adapters.md).

---

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

### 4.1 Two-plane table inventory

The Analysis Store contains kernel tables, allowlisted DBOS telemetry, and the
Whetstone full-rebuild projections `experiments`, `predictions`,
`sweep_metrics`, and `failure_metrics`. These replace aggregate reads from
`published_*` and retain the current Unitbench dimensions/measures: experiment
identity/kind, task/sample/model, generation/scoring/domain result states,
score, provider cost, latency, compression metrics, failure class/type, and
timestamps.

The Detail Store contains the deterministic root manifest plus
`detail_predictions`, `detail_prediction_payloads`,
`detail_generation_runs`, `detail_node_attempts`, `detail_score_attempts`,
`detail_score_harness_failures`, and `detail_platform_attempts`.
`detail_prediction_payloads` is the intentionally sensitive Whetstone-owned
surface for the current detail page's input/output/prompt/code/raw generation,
metrics, request, response, and validation fields; it is not sourced from raw
DBOS replay blobs. Every table carries the root Prediction ID and snapshot ID
so root-cascade completeness is testable. There is no generic exported log
table in v2; workflow/step details are retrieved on demand through the typed
inspector.

### 4.2 Migration and cutover order

Each phase must pass its exit gate before the next begins. Implementation
issues may split a phase, but may not reorder its dependency boundary.

1. **Contract preflight.** Pin DBOS 2.26.0, capture the exact public signatures
   and allowlisted schema in contract tests, prove queue priority inspection
   and deterministic same-Service-Class dequeue behavior including equal-time
   inserts, and prove live MotherDuck/Neon conditional lease, fenced promotion,
   and local-DuckDB/deployed query parity on a tiny fixture. Verify Vercel Node
   runtime and secret wiring and record fresh DB/application/queue/workflow
   names. If the exact DBOS or remote-store contracts cannot satisfy these
   tests, stop and revise the design before production code switches.
2. **Platform vocabulary and baseline.** Implement fixed naming, Pydantic
   records/options, enums, Manifest/request digest recipes, registration and
   next-Attempt ledgers, the complete schema crosswalk, internally owned
   shared writer lock, Operation row locks, `change_seq` triggers, and fresh
   `0001`. Add pure state
   and aggregate tests before I/O flows.
3. **Platform lifecycle.** Implement caller-prepared Manifest validation,
   registrar Lease/cursor/completion CAS, bounded RegistrationHook pages,
   deterministic shuffle, content-scoped enqueue, status normalization,
   append-only Attempts, automatic retry/missing reconciliation, idempotent
   caller-requested next Attempts, total status precedence, bounded
   `SubmitResult`, and non-recursive reference-aware cancellation. Replace
   tests at the new external interface; delete old shallow-module tests only
   when coverage has moved.
4. **Whetstone generation cut.** Add local clock, new names/queues, generation
   target and failure mapping, secret-free workflow arguments, fresh Whetstone
   schema, and manifest-backed generation Operation adapter. Prove
   cross-Operation dedup, model-group shuffle, domain-failed regeneration, and
   absence of child workflows;
   only then remove queue_worker/fairness/stamp paths.
5. **Whetstone scoring cut.** Add scoring Item/Operation identity and target,
   freeze current candidate selection into a Manifest, migrate the experiment
   command, and prove harness-failed rescoring plus strict and explicit partial
   Experiment acceptance. Prove candidate parity with current rescore fixtures
   before deleting custom batching and raw orphan replay.
6. **Inspection and telemetry.** Land typed inspector/health models, Whetstone
   Typer commands, guarded cancel and next-Attempt controls, execution-scoped
   workflow attributes, safe DBOS reads, and optional OTLP.
   This phase is required before expensive experiments, not follow-up polish.
7. **Export and projections.** Implement kernel incremental export, DBOS and
   Whetstone staged rebuilds, destination-local Leases/fencing and fault
   recovery, root-cascade detail sink (including snapshot-built platform
   Attempts), and full-rebuild equivalence checks. Populate a
   disposable local DuckDB, MotherDuck database, and Neon schema.
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
  completion CAS;
- crash before DBOS enqueue, after enqueue before outcome persistence, and
  after outcome before aggregate refresh;
- one shared failed execution observed by multiple Operations, with exactly
  one next ordinal per Operation and one content-scoped DBOS workflow;
- identical and distinct concurrent next-Attempt requests, source-advanced and
  maximum-exhausted dispositions, domain-failed generation, harness-failed
  scoring, and explicit retry after sticky cancellation;
- policy-gated retry, enqueue-try exhaustion, execution-attempt exhaustion,
  recovery exhaustion, sticky cancellation, and state-sensitive missing;
- reference-aware cancellation with exclusive, shared, newly racing,
  topology-violating child, partial physical failure, repeated request,
  already-terminal, and later-authorized workflows; recursive cancellation is
  asserted never called;
- production-isolation race for the last two Item completions, asserting the
  stored aggregate without a later repair, plus total status precedence;
- export writer/barrier ordering with an in-flight sequence allocation;
- crash/retry at every source, Lease, renewal, staging, promotion, MotherDuck,
  and Neon point; deterministic A(H1), B(H2), B-promotes, A-rejected for every
  artifact mode/destination, including `full_rebuild`;
- full-rebuild versus incremental kernel equivalence and deterministic root
  sample completeness; and
- absent/misconfigured queues, app-version drift, missing DBOS rows, disabled
  OTLP, and unavailable telemetry exporters.

Tests control clocks, IDs, shuffle inputs, missing-observation counts, and
retry decisions; they do not sleep or depend on incidental queue timing.

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
   provider calls.
3. **Generation/scoring lifecycle:** one experiment creates linked generation
   and scoring Operations, persists append-only domain outcomes, distinguishes
   platform success from domain outcome, regenerates after a domain failure,
   rescores after a harness failure, retries sticky cancellation only after
   confirmation, and resumes safely after injected process death. Strict
   completeness succeeds only with every required domain result; a deliberately
   model-biased failure remains `PARTIAL`, and an explicit stratified override
   persists its policy, counts, and operator confirmation.
4. **Operator readiness:** list/show/items/attempts/workflow/queue/throttle and
   health JSON are accurate; reference-aware cancellation proves physical stop,
   shared-work retention, partial-failure reporting, no recursive cancellation,
   and a confirmed later-Attempt authorization.
5. **Queue/pacing:** exact DBOS 2.26.0 status/API/schema contracts and priority
   config are verified; deterministic same-Service-Class ordering—including
   same-instant insertion—is proven or blocks the cut; runtime concurrency
   changes are visible, max sleep
   is enforced, and throttle pressure appears in health/traces.
6. **Export correctness:** incremental and full kernel outputs match; DBOS and
   domain rebuilds atomically replace; destination-local Leases/fences reject
   older-after-newer promotion and survive every crash/partial-failure
   permutation in live MotherDuck and Neon; local DuckDB excludes a second OS
   writer; no excluded DBOS payload/DSN appears.
7. **Unitbench parity:** every current aggregate, table, prediction-detail,
   and visualization query returns schema-valid results through local DuckDB
   and deployed MotherDuck/Neon adapters; remote compute policy blocks or
   confirms expensive pages as declared; Vercel preview proves Node runtime,
   server-only secret mapping, no native DuckDB bundle, and independent
   fail-closed behavior for missing Analysis versus Detail credentials.
8. **Cost/accounting:** Whetstone records remain the durable source for token
   and provider cost; trace and analytical totals reconcile to them within
   exact fixture expectations.

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
  documentation or an explicit fixture.

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

V2 intentionally does not add export-aware DBOS retention, raw replay/resume/
fork controls, alert routing/threshold persistence, read-only MCP tools,
browser/Wasm analytics, generic permissions/tenancy, a web control plane,
distributed Conductor-style recovery, or direct DBOS system-table mutation.
Retention waits for measured growth and export-rebuild proofs; replay waits
for attempt/idempotency evidence; alerts wait for workload baselines; MCP must
be a thin adapter over the mature inspector.
The two typed next-Attempt reasons are not generic replay: they create a fresh
platform Attempt under the persisted bound and never resume/fork a DBOS row.

### 4.8 V0 unified-feedback incorporation (priority order preserved)

| Priority | Unified item | V2 retained resolution |
| --- | --- | --- |
| P0-1 | Workflow reconciliation | Separate enqueue/execution state machines, append-only Attempts, normalized statuses, CAS predicates, retry/cancel/missing policies, and full Operation aggregation. |
| P0-2 | Export consistency | `change_seq`, Export Barrier, stable snapshots, destination-local cursors, artifact-specific refresh modes, crash matrix, and root-cascade sampling. |
| P0-3 | Identity/dedup scope | Content-scoped caller execution identity plus platform-owned attempt ordinal and provenance. |
| P0-4 | Queue/throttle topology | Multi-domain workflows retained; residual slot occupancy explicitly bounded and observable. |
| P0-5 | Scheduling objective | Fixed Service Classes plus mandatory deterministic shuffle rank; DBOS 2.26 priority configuration verified before claim. |
| P1-6 | Searchable workflows | Immutable execution-scoped attributes plus authoritative Operation/Attempt reference lookup; no mutable reference set in DBOS. |
| P1-7 | Typed inspector/control | Frozen inspection models, Typer human/JSON adapters, health report, and reference-aware guarded cancellation only. |
| P1-8 | OTLP/health | Optional semconv OTLP, safe attributes, graceful degradation, and on-demand machine-readable health. |
| P1-9 | Seed/metadata ownership | Manifest-backed transactional `RegistrationHook`; typed Attempt columns; Item/Attempt metadata deleted; immutable Operation metadata retained. |
| P1-10 | Schema/lifecycle crosswalk | Complete column/constraint/index/model/protocol/JSONL/return crosswalk and explicit empty-submission branch. |
| P1-11 | Bounded registration/enqueue | Immutable complete Manifest plus one 500-row transaction-page contract and bounded `SubmitResult` failure previews. |
| P1-12 | Dependency/model rules | Kernel-owned failure enum, Whetstone mapping, dr-providers removal, and frozen Pydantic models throughout. |
| P2-13 | Mechanical blast radius | Clock, pandas, observability, scoring replay, names, tests, docs, dependencies, and cross-repo stale-symbol search enumerated. |
| P2-14 | Evidence-dependent operator features | Retention, replay, alerts, MCP, browser/Wasm, permissions, and generic control plane explicitly deferred. |

### 4.9 V1 unified-feedback incorporation (priority order preserved)

| Priority | Unified item | V2 resolution |
| --- | --- | --- |
| P0-1 | Caller-requested next Attempt | One `request_next_attempt` ledger/transition; closed reason/source matrix; exact Operation/Item CAS; stable request identity; concurrent-request rules; persisted exhaustion/provenance; aggregate reactivation; cancellation authorization; Whetstone identity mappings; deterministic generation/scoring/cancel tests. |
| P0-2 | Immutable registration Manifest | Caller-prepared canonical Manifest and page digests; one registrar Lease/token/cursor; page-level hook+Item transaction; final completion CAS; exact resubmission; crash/expiry recovery; enqueue gate; competing/reordered/truncated/resumed tests. |
| P0-3 | Destination fencing | Per-destination/artifact Lease and monotonic fencing token through promotion/cursor commit; OS lock for local DuckDB; transactional MotherDuck/Neon rows; renewal, stale-stage ownership, crash matrix, and deterministic H1-after-H2 rejection. |
| P0-4 | Complete cancellation topology | Top-level-only managed workflows, `cancel_children=False`, deterministic lock order/reference predicate, racing-reference serialization, partial physical-failure recording, idempotent repeated requests, sticky state, and explicit later-Attempt authorization. |
| P0-5 | Experiment acceptance | Strict completeness by default; platform terminality reported separately; partial acceptance only with persisted stratified thresholds/counts and operator confirmation; biased-failure gate. |
| P1-6 | Operation serialization | Every membership/Item/Attempt/request mutation locks the Operation row and recomputes aggregates in the same transaction; fixed multi-Operation lock order and last-two-completions race test. |
| P1-7 | Execution-scoped DBOS attributes | Attributes contain immutable execution facts only; Operation references remain authoritative platform rows and DBOS reads follow workflow IDs. |
| P1-8 | Total status precedence | `REGISTERING > CANCELLING > ENQUEUEING > RUNNING > terminal derivation`, including requested-Attempt and mixed-terminal cases, pinned by table-driven tests. |
| P1-9 | Detail Attempt snapshot | `detail_platform_attempts` joins platform Attempts to Prediction roots inside the Whetstone full snapshot and carries its snapshot ID. |
| P1-10 | Secret-free DBOS payloads | Whetstone resolves credentials from process config, workflow args carry no secrets, and normal DBOS reads explicitly disable input/output loading. |
| P1-11 | Writer-lock ownership | Every kernel function that owns a `change_seq` mutation acquires the shared barrier lock internally; workflow-step throttle and static direct-write tests enforce it. |
| P2-12 | Live verification boundaries | Opt-in live MotherDuck/Neon parity and fenced promotion, Vercel runtime/secret checks, exact DBOS 2.26.0 contracts and same-band ordering, and current Whetstone rescore-selection parity remain blocking implementation gates. |

### Owner decisions resolved for v2

1. One platform-owned caller-requested next-Attempt transition; no second
   Whetstone counter and no false platform failure.
2. Caller-prepared immutable Manifests; no platform durable spool.
3. No DBOS child workflows below managed executions; cancellation is always
   non-recursive.
4. Destination-local Lease/fencing for every artifact and destination,
   including an OS/process lock for local DuckDB.
5. Strict Experiment completeness by default; explicit persisted,
   stratified, operator-confirmed partial override only.

### Review protocol

V2 is frozen as `in-review`. Its independent Codex 5.6 (`sol`, high) and
Claude Fable 5 (high) prompts audit the complete dr-platform, Whetstone,
Unitbench, DBOS, export, and runtime constellation. Reviewers write separate
findings; synthesis happens only after both complete. No finding is patched
into v2. Decision-changing findings return to the owner and land only in a
successor draft.

### Revision log

- v0 (2026-07-08): initial spec from the grilling session; reviewed in round 1.
- v1 (2026-07-10): draft incorporating the v0 adversarial review packet and
  a re-audit of the current `dr-platform`, `whetstone-ai`, and affected sibling
  code, plus the owner-resolved identity, attempt, cancellation, scheduling,
  export, scoring, and Unitbench runtime decisions.
- v1 review freeze (2026-07-10): frozen for independent Codex 5.6 and Claude
  Fable 5 whole-system convergence reviews.
- v2 (2026-07-10): draft incorporating the complete v1 whole-system
  convergence review in preserved priority order and a current-code,
  dependency, configuration, sibling-repository, and installed-DBOS 2.26.0
  re-audit. Owner decisions resolve next-Attempt authority, caller-prepared
  Manifests, top-level-only cancellation, destination publication fencing,
  and strict Experiment acceptance.
- v2 review freeze (2026-07-10): owner-approved and frozen for independent
  Codex 5.6 and Claude Fable 5 hybrid whole-system convergence reviews.
