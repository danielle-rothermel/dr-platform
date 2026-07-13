# dr-platform kernel contract

This normative document owns the complete platform kernel contract. The
[packet entrypoint](../plan.md) owns system scope, lifecycle narrative, phase
ordering, and review protocol; the [delivery contract](delivery.md) owns
cutover and blocking verification.

## Part 1 — dr-platform (kernel)

### 1.1 Deletions

| What | Why |
|---|---|
| `artifacts.py` + tests + 5 exports | Zero consumers anywhere. Restore from git if a real consumer appears. |
| `fairness.py` entirely (`fair_ordered`, `fair_ordered_windows`, `fair_ordered_item_windows`, `windows`, `Orderable`, `validate_window_size`) | Replaced by the explicit service-class and deterministic-shuffle contract (§1.4); batching remains an execution bound, not a fairness abstraction. |
| `naming.py` (`PlatformNaming`) and `ItemIdentity` (items.py) | Existed solely to preserve whetstone's frozen `dr_dspy_*` physical names, which are abandoned. Fixed canonical names; one `prefix` knob survives. |
| Old projections machinery (`projections.py` Postgres rebuild + pandas loader) and the `frames` extra | Replaced by the [DuckDB export flow](publication.md#16-export-flow-analysis-store--detail-store). `duckdb` becomes a core dependency. Any retained pandas consumer declares pandas directly; Whetstone keeps its direct pandas dependency for COPRO. |
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
  `retry_policy`, Manifest identity, and
  `operation_execution_recipe_digest`;
  immutable execution-target reference; positive monotonic
  `platform_cut_version`;
  registration owner/Lease/cursor;
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
  `workflow_role`, `execution_recipe_digest`, enqueue state, normalized
  execution outcome, nullable current-Claim pointer, source attempt/workflow,
  retry reason, source
  application version, failure/reconciliation facts, and creation, enqueue,
  terminal, and change-tracking timestamps. An attempt row may transition
  until terminal; a terminal row is immutable and a retry inserts the next
  ordinal instead of reusing or overwriting it. Multiple Operation Items may
  legitimately reference the same content-scoped DBOS workflow, so
  `workflow_id` is indexed but not globally unique in this table. See
  [ADR 0003](../../../../adr/0003-append-only-attempt-ledger.md).
- `<prefix>_next_attempt_requests`: immutable idempotency ledger keyed by
  derived `request_id`, with `(item_id, request_key)` unique; source Attempt,
  typed reason, caller eligibility reference/digest, actor and confirmation
  facts, requested policy bound, persisted disposition, created Attempt when
  any, and timestamps. It records successful, exhausted, ineligible, and
  source-advanced outcomes so replaying one request returns the same result.
- `<prefix>_enqueue_claims`: append-only Claim/enqueue-call ledger keyed by
  `(item_id, attempt, claim_id)`. Each row records the deterministic workflow
  ID, Claim and Lease times, enqueue try, lifecycle disposition, invalidation
  or replacement facts, and nullable enqueue-call-started timestamp. A Claim
  may cross the DBOS call boundary at most once, and that transition is
  committed before the external call. Expiry, replacement, invalidation, and
  Attempt terminalization never delete or rewrite its identity.
- `<prefix>_enqueue_compensations`: append-only idempotency ledger keyed by
  `(item_id, attempt, claim_id)` with an immutable FK to the exact enqueue
  Claim. It records a claimant's DBOS enqueue side
  effect after its outcome CAS loses to cancellation: workflow ID, typed
  reason, cancellation result, safe diagnostics, and timestamps. This keeps
  terminal Attempts immutable while making late-enqueue compensation
  inspectable and replay-safe.
- Registration-abandonment and cancellation facts remain typed lifecycle
  columns: Operations carry confirmed abandonment facts; Attempts carry
  cancellation intent/disposition/origin and requested/effective priority.
  They are not folded into generic metadata.
- Item/Attempt `enqueue_metadata` is deleted. Kernel correlation, lease,
  workflow, failure, and provenance facts use typed columns; Whetstone's
  `generation_run_id` is the typed content-scoped `execution_key`, and richer
  domain data stays in Whetstone tables. Operation-level `metadata` remains a
  caller-owned submission annotation, is validated on idempotent resubmission,
  and is never mutated by retry transitions.
- `<prefix>_throttle_state`: unchanged shape (backoff + holds + tags in one
  table — deliberately composed, do not split).
- No operational `<prefix>_export_state` table. Source tables expose monotonic
  change cursors; each destination owns its committed export state in the
  [export contract](publication.md#16-export-flow-analysis-store--detail-store).
- A shared Postgres sequence plus trigger assigns a unique `change_seq` on
  every insert/update of mutable exported kernel rows (Operations, Items,
  Item Attempts, enqueue Claims, next-Attempt requests, enqueue
  compensations, and throttle state). The baseline
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

| Current Operation column | V6 column | Action and invariant |
| --- | --- | --- |
| `operation_key` | `operation_key` | Retain PK; caller identity and idempotency key. |
| configured `group_key` label | `group_key` | Retain on Operation, not Item; Whetstone stores `experiment_name`. |
| none | `workflow_role` | New caller-owned stable role, consistent across all Items/Attempts. |
| `status` | `status` | Replace enqueue-only meaning with `REGISTERING`, `ENQUEUEING`, `RUNNING`, `CANCELLING`, `SUCCEEDED`, `PARTIAL`, `FAILED`, `CANCELLED`. |
| `requested_count` | `requested_count` | Retain; copied from the accepted Manifest before the first page write and immutable thereafter. |
| none | `manifest_version`, `manifest_digest`, `manifest_page_size`, `manifest_page_count` | New immutable caller-prepared input-set identity. |
| none | `operation_execution_recipe_digest` | New immutable versioned aggregate over the target recipe and complete ordered Item recipe digests. |
| none | `target_key`, `target_version`, `target_contract_digest` | New immutable persisted reference resolved through the startup target registry by every lifecycle entry point. |
| none | `platform_cut_version` | New positive monotonic version, incremented once per transaction that changes acceptance-relevant lifecycle state for the Operation. |
| none | `registration_cursor`, `registration_lease_id`, `registration_lease_expires_at` | New durable registration authority; cursor is the next page index and advances only with the owning Lease CAS. |
| none | `registration_abandoned_at`, `registration_abandoned_by`, `registration_abandonment_reason` | New confirmed terminal operator transition after an expired Registration Lease; committed rows remain immutable provenance. |
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

| Current Item column | V6 column/location | Action and invariant |
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
`execution_recipe_digest`; `enqueue_state`; `enqueue_try`; `execution_state`; last normalized
`dbos_status`; `retry_disposition`; nullable `current_claim_id`; `failure`;
`source_attempt`; `source_workflow_id`; `retry_reason`; nullable
`next_attempt_request_id`; `source_application_version`; missing-observation count/first/last timestamps;
DBOS cancellation intent/result/origin facts; requested Service Class/priority,
effective DBOS priority, and priority source; and `created_at`, `enqueued_at`,
`terminal_at`, `updated_at`, `change_seq`. Attempt 0 has no source; later
Attempts require source provenance. `execution_key` and `workflow_id` are
indexed but not unique because content-scoped executions may be referenced by
multiple Operations.

`<prefix>_next_attempt_requests` is new. Its columns are `request_id` (PK),
`item_id`, `request_key`, `source_attempt`, `reason`, `eligibility_kind`,
`eligibility_record_id`, `eligibility_digest`, `requested_by`,
`operator_confirmed_at`, nullable tightening `max_attempts`, persisted
`effective_max_attempts`, `disposition`, nullable
`created_attempt`, nullable `rejection_detail`, `created_at`, `resolved_at`,
and `change_seq`. The unique `(item_id, request_key)` constraint plus exact
payload-equality check makes request replay idempotent; `created_attempt` has a
composite FK to Item Attempts when present. Safe diagnostics only—no prompt,
output, credentials, or provider payload—may enter the eligibility fields.

`<prefix>_enqueue_claims` is new. Its columns are `item_id`, `attempt`,
`claim_id` (composite primary key and FK to the Attempt), `workflow_id`,
positive `enqueue_try`, `claimed_at`, `lease_expires_at`, nullable
`enqueue_call_started_at`, `disposition`, nullable `invalidated_at`,
`invalidated_by`, `replacement_claim_id`, and `resolved_at`, plus `created_at`
and `change_seq`. The closed dispositions distinguish `CLAIMED`,
`CALL_STARTED`, `OUTCOME_RECORDED`, `EXPIRED`, `REPLACED`, and `INVALIDATED`;
identity and call-start facts are immutable. The Attempt's nullable
`current_claim_id` is only a checked current pointer and may be cleared during
terminalization; it is never Claim history. Starting a DBOS call atomically
sets `enqueue_call_started_at` while the Claim is current and valid. A claimant
that cannot win that CAS never calls DBOS.

`<prefix>_enqueue_compensations` is new. Its columns are `item_id`, `attempt`,
`claim_id` (composite primary key and Attempt/Claim provenance), `workflow_id`,
`reason`, `cancel_disposition`, nullable safe failure diagnostics,
`created_at`, `resolved_at`, and `change_seq`. Exact replay returns the
existing row; an unequal workflow or reason for the same key is an integrity
conflict. Its composite FK targets the immutable enqueue-Claim row. The closed
dispositions include unresolved `PENDING`/`FAILED` plus resolved `CANCELLED`,
`OBSERVED_TERMINAL`, `SKIPPED_SHARED`, and `NO_WORKFLOW_FOUND`. Indexes cover
`workflow_id`, unresolved disposition, and `change_seq`.

Other table changes are explicit:

- `{prefix}_throttle_backoff` becomes `{prefix}_throttle_state`; all existing
  backoff/hold/tag columns remain, and `change_seq` is added.
- `{prefix}_projections` is deleted; destination-local export state replaces
  it.
- No source export-state table is created.
- Operation/Item enum, count, payload, and timestamp constraints are replaced
  by checks matching the new states. Attempt checks permit a current-Claim
  pointer only while claiming, require a workflow ID before confirmed enqueue, terminal timestamps
  only for terminal execution states, failure payloads for error states, and
  non-negative attempt/enqueue-try values.
- Operation checks require Manifest count/page arithmetic, cursor within
  `[0, manifest_page_count]`, registration completion only at the final
  cursor, positive `platform_cut_version`, immutable target-ref equality, and
  registration Lease fields either all null or all present.
  Enqueue-Claim checks enforce one call-start transition per Claim, immutable
  workflow identity, and replacement links within the same Attempt.
  Next-Attempt request checks enforce reason/source-state disposition shapes,
  operator confirmation for cancellation retry, positive nullable tightening
  bounds, resolved effective bound no greater than the immutable RetryPolicy,
  and created-Attempt identity only for `CREATED`.
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
  `(execution_state)`, and `(change_seq)`; enqueue Claim
  `(workflow_id, disposition)`, `(lease_expires_at)`, and `(change_seq)`;
  next-Attempt request
  `(item_id, source_attempt)`, `(disposition)`, and `(change_seq)`;
  enqueue compensation `(workflow_id)`, unresolved disposition, and
  `(change_seq)`; throttle
  `(blocked_until)`, `(hold_until)`, and `(change_seq)`.

#### Public/model/file crosswalk

| Current surface | V6 surface | Action |
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
   is stably mixed before equal-priority DBOS insertion, even if the
   generation process did not shuffle it.

The guarantee is stable approximate mixing within bounded kernel claim/enqueue
pages, not strict round-robin fairness or identical final DBOS start order.
Installed DBOS 2.26.0 orders only by `(priority, created_at)` and may
nondeterministically reorder same-priority rows whose millisecond timestamps
tie, especially with multiple dequeuers. The owner accepts that tie-local
nondeterminism: reproducible `shuffle_rank`, original result ordering, and the
fixture's model-mixing bound are mandatory before enqueue; final dequeue order
is not. Concurrent submitters can also interleave at the DBOS queue, and
sustained `URGENT` work may intentionally delay lower service classes. Shuffle
is never encoded as a wide random DBOS priority, so newly arriving low-rank
Items cannot indefinitely overtake an old Item within the kernel claim order.

The library always sets `SetEnqueueOptions(priority=service_priority)`;
unprioritized DBOS work would otherwise jump ahead. Every platform queue is a
database-backed queue registered with `priority_enabled=True`. Before the
first claim/enqueue page, the kernel retrieves the persisted queue
configuration and fails closed if the queue is absent or priority is disabled;
the database-backed enqueue call itself does not validate this in DBOS 2.26.0.
Queue registration and startup conflict policy must not silently overwrite an
operator-adjusted runtime configuration. See
[ADR 0007](../../../../adr/0007-separate-urgency-from-shuffle-order.md).

### 1.5 Submission flow (the one way in)

One core pipeline: **prepare immutable Manifest → bounded registration →
reconcile → claim → enqueue → aggregate**. `submit(manifest, source, ...)` and
`submit_jsonl(manifest, path, fields, ...)` differ only in how a caller reads
already-manifested Items. `SubmitOptions.page_size` defaults to 500 and is
captured in the Manifest; it is the one bound for registration transactions,
reconciliation/status reads, enqueue claims, and failure materialization.
Paging has no identity or fairness semantics by itself.

`OperationManifest` is a frozen Pydantic model with `format_version = 3`,
`operation_key`, `workflow_role`, `group_key`, `target_ref`,
`operation_execution_recipe_digest`,
`item_count`, `page_size`, `items_digest`, ordered `ManifestPage` descriptors,
and `manifest_digest`.
Each Item leaf is the lowercase SHA-256 hex digest of dr-serialize canonical
JSON for exactly `{"item_index", "item_key", "service_class", "spec",
"execution_recipe_digest"}`. The digest is produced by the resolved target's
`recipe_for` before the Manifest is accepted; a caller cannot supply an
unverified aggregate without the concrete ordered leaves.
`items_digest` hashes canonical JSON of the ordered leaf-digest list. Each page
descriptor contains zero-based `page_index`, inclusive `start_index`,
exclusive `end_index`, and the digest of that page's ordered leaves; pages
must be contiguous, non-empty except when the whole Manifest is empty, cover
`[0, item_count)`, and use `page_size` except for the last page.
`manifest_digest` hashes canonical JSON of every preceding Manifest field
except itself, including `operation_execution_recipe_digest`. Strings are UTF-8,
canonical JSON supplies key ordering and
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
A hook may return `ALREADY_PRESENT` only after loading the existing domain row
and proving exact equality of its canonical domain model with the submitted
row; an identity collision with unequal content is a hard conflict that rolls
back the page.

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
`metadata`, `retry_policy`, and `operation_execution_recipe_digest`); a reordered, truncated,
extended, or otherwise changed source is a hard conflict before hooks or
enqueue. Empty Manifest invokes no hook and atomically completes registration
as `FAILED/empty_submission`. See
[ADR 0009](../../../../adr/0009-transactional-registration-hook.md).

A partially registered non-empty Operation is never left without an operator
successor. After its Registration Lease expires, `abandon_registration`
acquires the Export Barrier writer lock and Operation row, rechecks
`registration_completed_at IS NULL`, `registration_abandoned_at IS NULL`, and
database-time Lease expiry, then requires named operator confirmation and a
reason. One transaction clears the Lease, records the abandonment facts, and
sets sticky `FAILED/registration_abandoned`. Committed Items, Attempt-0 rows,
and domain hook rows remain immutable provenance; uncommitted Manifest pages
remain absent, nothing becomes claimable, and later resume/resubmit returns
`REGISTRATION_ABANDONED`. Concurrent completion wins by row lock/CAS and makes
abandonment ineligible. The command previews committed/remaining counts and has
no hard-delete mode.

**Execution target registration and resolution.** `ExecutionTargetRef` is a
frozen Pydantic model containing `target_key`, positive `target_version`, and
`target_contract_digest`. The ref is persisted immutably on the Operation and
enters its Manifest and exact-resubmission equality. `ExecutionTarget` is the
runtime-only frozen Pydantic model registered under that ref; its callable
fields are excluded from serialization:

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
- `recipe_for: Callable[[SubmittableItem], ExecutionRecipeEnvelope]` — pure;
  runs during Manifest preparation and page revalidation before Item insertion
- `classify_error: Callable[[BaseException], FailureSnapshot]`
- optional `registration_hook: RegistrationHook`

`target_contract_digest` hashes a frozen declaration of queue, workflow role,
managed workflow name/version, topology, argument-recipe version, recipe
envelope version, classifier version, and registration-hook name/version.
Callable objects are never identity. Reusing one key/version with a different
declaration is a startup conflict and fails closed.

`TargetRegistry` is the one concrete startup registry and implements the
public `TargetResolver` Protocol. Every process that can submit, reconcile,
wait, inspect with reconciliation, export with reconciliation, cancel, or
request a later Attempt registers the complete target set before serving work.
Every lifecycle facade receives the same resolver dependency and resolves the
persisted ref; a missing target, digest mismatch, or duplicate conflicting
registration returns typed `TARGET_UNAVAILABLE`/`TARGET_CONFLICT` and performs
no lifecycle mutation. No facade accepts an ad hoc replacement target.

The kernel owns only `ExecutionRecipeEnvelope`: format version, target ref,
managed workflow name/version/topology, argument-recipe version, and an opaque
canonical caller payload. Whetstone owns and validates
`WhetstoneExecutionRecipePayload`, including its complete domain input and all
profile, parser, dataset, graph, provider-configuration, and application
versions. dr-platform neither models nor interprets those nouns. It computes
the lowercase SHA-256 `execution_recipe_digest` over the envelope, persists it
on the Item's Attempts, and includes it in content-scoped execution identity.
Before Registration completes, the resolved target recomputes every concrete
Item envelope/digest from the source pages and the kernel recomputes the
ordered `operation_execution_recipe_digest`; every leaf, page, Manifest, and
Operation value must agree or the final CAS rolls back. Exact resubmission
repeats the same proof.

Managed workflow registration records the target ref plus
callable/name/topology tuple and
refuses duplicate names with different identities. Whetstone managed workflow
bodies may call DBOS steps but may not call `start_workflow`,
`enqueue_workflow`, or another workflow. Static checks and an integration
fixture prove managed executions have no DBOS parent/child rows. Cancellation
also fails closed without a physical DBOS call if inspection ever discovers a
descendant, treating that as topology drift rather than recursively cancelling
unknown references.

The library owns the entire enqueue moment, but not domain execution identity.
The caller supplies a stable, content-scoped execution identity through the
enqueue target, and that identity includes `execution_recipe_digest`; the
kernel derives or receives the corresponding DBOS
`workflow_id`, sets queue options, and dedup-enqueues internally. Apps never
touch DBOS enqueue APIs for platform work. The same content submitted through
multiple Operations therefore converges on the same durable execution for a
given attempt, as recorded in
[ADR 0001](../../../../adr/0001-content-scoped-execution-identity.md).
The interface ownership is recorded separately in
[ADR 0016](../../../../adr/0016-kernel-executes-platform-enqueue.md).

The kernel alone owns the Item Attempt ordinal. Automatic reconciliation or
the caller-requested transition below may establish eligibility, but only the
kernel allocates under CAS before calling `execution_for` for the new ordinal.
Whetstone maps it one-to-one to Generation Run and Score Attempt indexes; it
does not maintain a second counter. When another Operation already created an
execution for that content and ordinal, enqueue deduplication links the local
Attempt reference to the existing workflow. See
[ADR 0002](../../../../adr/0002-platform-owns-attempt-lineage.md).

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

When a new Operation links a content-scoped workflow already `CANCELLED` by
another Operation, reconciliation transitions the local nonterminal Attempt
directly to sticky `CANCELLED` with `cancellation_origin=FOREIGN_OPERATION`,
the originating `operation_key`, and the recorded foreign
`cancellation_request_id`. The confirmed `OPERATOR_CANCEL_RETRY` request may
cite that foreign request; ownership of the provenance and ownership of the
new local authorization are distinct. If no unique cancellation provenance can
be resolved from authoritative platform references, reconciliation fails
closed as an integrity error rather than inventing eligibility.

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
lock and checks for any unresolved DBOS-cancellation intent or invalidated
enqueue Claim whose call started but whose compensation remains unresolved for
that workflow ID. A racer therefore either commits before the cancellation
lock and appears in the exclusivity predicate, or waits and then fails closed
on the committed cancellation guard; it cannot attach between the predicate
and the DBOS call.

Claim eligibility separately requires no cancellation intent and a
nonterminal execution state. The cancellation-intent transaction invalidates
every outstanding enqueue Claim for affected Attempts without deleting its
append-only row. When no DBOS row exists, finalization records
`NOT_ENQUEUED`; it never describes DBOS's silent no-op as a delivered
cancellation. A Claim that never committed `enqueue_call_started_at` is
resolved by invalidation and cannot call DBOS. If a call-started claimant
enqueues after Claim invalidation, its outcome CAS necessarily loses. It
inserts or exact-reloads compensation using its durable
`(item_id, attempt, claim_id)` key, then acquires the Export Barrier writer
lock, workflow advisory lock, and referencing Operation/Item/Attempt rows in
the established order. Under those locks it re-evaluates the same reference-
exclusivity predicate used by operator cancellation. Another registered
nonterminal current-Attempt reference resolves compensation as
`SKIPPED_SHARED` without a DBOS call; only an exclusive top-level workflow may
receive idempotent `cancel_workflow(workflow_id, cancel_children=False)`.
Unresolved compensation keeps health degraded and prevents the cancellation
command from reporting fully resolved. This race is distinct from an already-
running synchronous provider call continuing after logical cancellation.

Claimant cooperation is not the only repair path. Bounded cancellation replay
and health reconciliation also inspect terminal `NOT_ENQUEUED` Attempts whose
append-only Claims were invalidated after `enqueue_call_started_at` committed.
Replay processes every such Claim independently by its exact durable key,
including expired or replaced claimants. For each unresolved Claim it inserts
or exact-reloads the compensation row and uses the same lock order and
reference-exclusivity predicate before any physical DBOS cancellation. If
another live reference exists it records `SKIPPED_SHARED`; if the workflow
exists exclusively it cancels or records the already observed terminal
result. If the workflow remains absent through the configured missing-workflow
grace period and observation count, replay records durable
`NO_WORKFLOW_FOUND`. `SKIPPED_SHARED`, `NO_WORKFLOW_FOUND`, `CANCELLED`, and
`OBSERVED_TERMINAL` are resolved hazards; only unresolved or failed
compensation blocks new-reference creation. This gives every invalidated call-
started Claim a bounded resolution event and repairs claimant death after
enqueue but before the losing outcome CAS without keeping the workflow
permanently unlinkable. An unresolved or unequal compensation keeps health
degraded and the Operation's cancellation incomplete; terminal Attempt fields
remain immutable.

Several expired or invalidated claimants may name the same deterministic
workflow. Each retains a distinct Claim and compensation key, but the workflow
advisory lock serializes repair. They converge independently to a resolved
disposition: at most the first exclusive repair needs a DBOS cancel; later
repairs observe terminal state or shared ownership and never invent a
different claimant identity.

After committing logical intent, the controller calls
`cancel_workflow(workflow_id, cancel_children=False)` only for the exclusive
top-level set. A final transaction under the same lock order records each
Attempt as `NOT_ENQUEUED`, DBOS cancel delivered, skipped-shared, observed terminal, or
failed with typed diagnostics. Partial DBOS-call failure does not roll back
logical cancellation and leaves the Operation `CANCELLING` until every
DBOS cancellation result is resolved or explicitly acknowledged. Repeating the same
`cancellation_request_id` returns the stored plan/results and never repeats a
successful DBOS call; a different request against an already sticky
Attempt records `ALREADY_CANCELLED`. The old Attempt remains terminal even if
shared DBOS work later succeeds for another Operation. Any observed child
workflow is a topology violation: record it, do not physically cancel the
parent, and never invoke recursive DBOS cancellation. See
[ADR 0005](../../../../adr/0005-reference-aware-cancellation.md).

For synchronous Whetstone provider steps, DBOS cancellation is explicitly
logical: it may update the workflow row while the in-flight upstream request
continues. Once DBOS reports `CANCELLED` and the local Attempt is finalized
`CANCELLED`, a confirmed `OPERATOR_CANCEL_RETRY` may create and enqueue the next
Attempt immediately. There is no provider-call quiescence fence and no claim
of upstream abort. The older call may overlap the replacement and incur
duplicate spend. If DBOS cancellation prevents the later Whetstone
outcome-persistence step, that discarded call and its price may be absent from
Whetstone and export totals; the provider's receipts remain the total-spend
record. No separate provider-call ledger is added, and DBOS replay payloads
remain excluded from accounting truth. Inspection distinguishes
`DBOS_CANCELLED` from `UPSTREAM_ABORT_PROVED` (the latter is not supported by
the pre-experiment adapters). See
[ADR 0019](../../../../adr/0019-accept-paid-call-overlap-after-cancellation.md).

Cancellation intent and observed execution outcome are separate columns. In
the final cancellation transaction, a DBOS `SUCCESS` or `ERROR` already
terminal before the DBOS cancel update/observation wins: the Attempt finalizes
`SUCCEEDED` or classified `ERROR`, while
`cancellation_disposition=OBSERVED_TERMINAL` preserves the operator intent and
race. `CANCELLED` is used when DBOS cancellation wins or an exclusive local
reference is logically finalized without an accepted prior terminal result.
After a local terminal row commits it is immutable: a later result from shared
work cannot rewrite local `CANCELLED`, and a synchronous provider call
continuing after DBOS `CANCELLED` does not become an accepted terminal workflow
result. Tests pin success-before-cancel, error-before-cancel,
cancel-before-step-return, shared-skip, and repeated reconciliation.

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
`requested_by`, optional operator confirmation, and optional positive
`max_attempts` tightening bound. `request_id` is the fixed
SHA-256 digest of canonical JSON for `{"item_id", "request_key"}`; reusing
that pair with any unequal payload is a hard idempotency conflict.

The closed reason/source matrix is:

| Reason | Eligible current source | Additional requirement |
| --- | --- | --- |
| `DOMAIN_OUTCOME` | `SUCCEEDED` | Caller cites an append-only terminal Generation Run or Score Attempt outcome; no operator confirmation. |
| `OPERATOR_CANCEL_RETRY` | `CANCELLED` | Named operator plus non-null confirmation timestamp and local or recorded foreign cancellation-request provenance. |

`ERROR` remains owned by automatic `RetryPolicy` reconciliation;
`RECOVERY_EXHAUSTED`, `MISSING`, permanent/exhausted `ERROR`, nonterminal
states, and enqueue-only failures are not eligible through this action in the
pre-experiment cut. dr-platform persists but does not interpret the caller's
domain eligibility reference. Whetstone constructs `DOMAIN_OUTCOME` only from
an append-only domain row in the same application database and pins its
digest; an audit can reproduce what it observed. A foreign-cancel retry cites
the originating request and Operation persisted on the local cancelled Attempt;
the new Operation's operator confirmation remains independently required.

The caller derives the candidate execution/workflow identity for
`:source_attempt + 1` before mutation. The exact transaction acquires the
kernel's shared Export Barrier writer lock, the candidate workflow's advisory
reference lock, the Operation row `FOR UPDATE`, then the Item and current
Attempt, then inserts or reloads the request ledger. It also rejects any
unresolved cancellation guard for that workflow. Creation requires all predicates:
registration complete; `items.item_id = :item_id AND
items.current_attempt = :source_attempt`; current Attempt terminal and equal
to the reason's source state; request disposition unresolved; and
`:source_attempt + 1 < min(retry_policy.max_attempts,
request.max_attempts)` when the request bound is present, otherwise
`:source_attempt + 1 < retry_policy.max_attempts`. The immutable Operation
policy is always the ceiling; a request may tighten it but never expand it.
The ledger persists both the nullable requested bound and resolved
`effective_max_attempts`; both participate in exact idempotency equality. It
inserts Attempt
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
[ADR 0004](../../../../adr/0004-kernel-owned-failure-taxonomy.md).

`MAX_RECOVERY_ATTEMPTS_EXCEEDED` normalizes to the distinct terminal outcome
`RECOVERY_EXHAUSTED`. Ordinary reconciliation never advances it, regardless
of the `RetryPolicy`; it requires future explicit operator intervention. The
inspector and health report surface recovery exhaustion separately from
retryable, retry-exhausted, cancelled, and permanent failures.

**Missing-workflow policy:** enqueue is necessarily split across the
application Postgres transaction and the DBOS system-database transaction.
The kernel first persists the Attempt, deterministic `workflow_id`, and an
append-only Claim row; before DBOS it commits the Claim's one-way
`enqueue_call_started_at` transition. It then calls DBOS outside that
transaction and finally records the enqueue outcome with a CAS on
`(item_id, attempt, current_claim_id, enqueue_state)` and resolves the Claim
row without erasing it. A crash
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

All transitions use source-state and current-Claim predicates; losing a CAS
means another submitter won and the loser reloads rather than applying its
stale result. An expired Lease inserts a new Claim row and changes only the
Attempt's current pointer; it never reuses or overwrites the expired Claim.

#### Attempt state machines

Enqueue and execution are separate columns so a successful enqueue cannot be
mistaken for successful work.

| Enqueue source | Event | Enqueue target | Attempt behavior |
| --- | --- | --- | --- |
| `PENDING` | CAS claim wins | `CLAIMING` | Insert a fresh append-only Claim, point `current_claim_id` to it, and increment `enqueue_try`. |
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
| `CANCEL_REQUESTED` | DBOS `CANCELLED` observed or shared-skip finalized | `CANCELLED` | Sticky; only confirmed `OPERATOR_CANCEL_RETRY` may create the next Attempt. A synchronous paid call may continue and overlap the replacement. |
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

The same owning transaction increments the Operation's positive
`platform_cut_version` exactly once whenever it changes Registration
completion/abandonment, Item `current_attempt`, Attempt enqueue/execution/
cancellation/reconciliation state, next-Attempt creation, or aggregate
terminality. Pure reads and exact idempotent no-op replays do not increment it.
The kernel exposes a typed, sorted `PlatformOperationCut` of
`(operation_key, platform_cut_version)` entries plus its canonical digest and
an atomic comparison helper. The vocabulary is domain-neutral: dr-platform
does not know which Experiments pin the cut.

The global application lock order is the shared Export Barrier writer lock;
for paths that create, link, or cancel references, transaction-scoped advisory
workflow locks sorted by workflow ID; Operation rows ascending by key; Items
ascending by ID; Attempts ascending by `(item_id, attempt)`; enqueue Claims by
`claim_id`; then request/compensation/cancellation rows. Paths that do not touch workflow references omit
that lock tier but preserve the remaining order. DBOS and destination calls
occur only after the application transaction releases row locks.

The pure status function applies this total precedence, first match wins:

1. `FAILED/registration_abandoned` when `registration_abandoned_at` is set.
2. `REGISTERING` when `registration_completed_at IS NULL` (including a held or
   expired Registration Lease that remains operator-resumable).
3. `CANCELLING` when cancellation intent exists and any current Attempt has an
   unresolved DBOS-cancellation disposition.
4. `ENQUEUEING` when any current Attempt is pending, claiming, or has a
   retryable enqueue error—including a newly caller-requested Attempt.
5. `RUNNING` when any current Attempt is confirmed `ENQUEUED` or
   `WORKFLOW_ALREADY_PRESENT` with execution `NOT_STARTED`, is `ACTIVE`, or has
   an automatic execution retry eligible. Confirmed enqueue transfers lifecycle
   authority to DBOS even before the first status observation.
6. Terminal derivation when every current Attempt has a terminal execution
   state or a permanent/exhausted `ENQUEUE_ERROR` that can never start:
   `SUCCEEDED` if all succeeded; `CANCELLED` if all are cancelled; `PARTIAL`
   if at least one succeeded and at least one is any non-success terminal
   state, or if explicitly retried Items finish while others remain cancelled;
   otherwise `FAILED`. Empty Manifest is `FAILED/empty_submission` and
   maximum-attempt, enqueue-try, and Registration exhaustion are preserved as
   terminal reasons.

Impossible mixtures fail validation rather than falling through. A
table-driven pure test covers every pairwise overlap plus abandoned/active
registration, both confirmed-enqueue/`NOT_STARTED` states, permanent enqueue
errors, cancellation, requested-Attempt, and all-terminal combinations.

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

The Attempt always persists both requested and effective scheduling facts. A
newly enqueued execution records `effective_service_priority` equal to the
requested mapped priority and `priority_source=ENQUEUED_HERE`. A
`WORKFLOW_ALREADY_PRESENT` reference reads the existing DBOS workflow priority,
persists it with `priority_source=LINKED_EXISTING`, and never mutates the shared
execution. The Item's requested Service Class remains immutable even when it
differs. Inspection and health expose requested/effective priority and flag the
mismatch; an `URGENT` reference therefore never implies that already-enqueued
STANDARD work was promoted. Live priority mutation is outside this cut.

Reconciliation batch-loads workflow statuses through DBOSClient APIs using
workflow IDs. Application-row decisions and CAS writes occur in a separate
Postgres transaction; the plan never assumes a distributed transaction across
the application and DBOS databases and never mutates DBOS system tables.
Every normal DBOSClient workflow query passes `load_input=False` and
`load_output=False` explicitly.

DBOS 2.26.0's public `DBOSClient.list_workflow_steps` cannot satisfy the same
boundary: it exposes no payload flag and deserializes output/error by default.
The standard step timeline therefore uses the pinned, version-specific
allowlisted system-schema read adapter already used by telemetry. It selects
only workflow ID, function ID/name, child-workflow ID, and lifecycle timing;
its SQL projection excludes input, output, error, and serialization columns.
Schema drift fails closed, and contract tests make the DBOS serializer raise if
any payload deserialization path is invoked. The adapter is read-only; direct
DBOS system-table writes remain forbidden. Any future payload debugger is a
separately named, locally guarded, redacted surface outside this cut.

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
[ADR 0013](../../../../adr/0013-separate-platform-execution-from-domain-outcome.md).
Experiment acceptance is the separate strict predicate in
[ADR 0018](../../../../adr/0018-strict-experiment-acceptance.md).

**Empty submission** explicitly transitions the Operation to the failed
terminal state with an `empty_submission` reason (deliberate: an Operation
that produced zero Items is a caller bug worth surfacing loudly). It does not
fall into failure accidentally through `0 >= 0` count arithmetic.

Facade signatures: `ExecutionTargetRef` plus the injected `TargetResolver`
resolve enqueue behavior,
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
[ADR 0006](../../../../adr/0006-accept-bounded-multi-domain-slot-occupancy.md).

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
`wait_operation`, `request_next_attempt`, paginated `inspect_*`,
`cancel_operation`, `abandon_registration`, `export`).
Lifecycle-driving calls accept the shared `TargetResolver`; DBOS and sink
adapters are internal seams, and callers do not reproduce
claim/Attempt/export mechanics.

The kernel provides frozen Pydantic inspection models for:

- Operation list/show and aggregate state;
- paginated Items and full append-only Attempt lineage;
- DBOS workflow and step timelines joined through safe attributes/IDs;
- queue configuration plus queued/active age;
- active throttle holds/backoff pressure; and
- a machine-readable health report.

`wait_operation(operation_key, options) -> OperationWaitResult` is the one
typed full-lifecycle wait. It repeatedly runs bounded reconciliation and reads
the authoritative Operation/Attempt inspection until the aggregate is
terminal, returning the terminal inspection plus elapsed/poll facts. Frozen
`OperationWaitOptions` carries positive poll interval, timeout, reconciliation
page bound, and an injected clock/sleeper for tests. Timeout raises typed
`OperationWaitTimeoutError` containing the last inspection; cancellation,
abandoned Registration, permanent enqueue failure, and partial terminality are
terminal results rather than generic exceptions. It never reads raw DBOS state
outside the normalized adapter and never implies domain acceptance.

Whetstone exposes these through a thin Typer CLI with human and `--json`
output. Workflow reads use payload-disabled DBOSClient APIs; step timelines
use only the reviewed DBOS-2.26 allowlisted read adapter above. The health
report includes oldest queued/active age, Operations with
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
  `ExecutionTargetRef`, `ExecutionTarget`, `ExecutionRecipeEnvelope`,
  `TargetResolver`, `TargetRegistry`, `WorkflowTopology`, `RegistrationHook`,
  `RegistrationResult`, `RetryPolicy`, `SubmitOptions`, `SubmitResult`,
  `submit`, `submit_jsonl`, `reconcile`, `OperationWaitOptions`,
  `OperationWaitResult`, `OperationWaitTimeoutError`, `wait_operation`);
  Attempt request contracts
  (`NextAttemptRequest`, `NextAttemptResult`, `NextAttemptReason`,
  `NextAttemptDisposition`, `EligibilityReference`, `request_next_attempt`);
  Claim and cancellation-compensation records (`EnqueueClaimRecord`,
  `EnqueueCompensationRecord`);
  lifecycle records/enums
  (`OperationRecord`, `ItemRecord`, `AttemptRecord`, `OperationStatus`,
  `ItemInsertStatus`, `AttemptEnqueueState`, `AttemptExecutionState`,
  `RetryDisposition`, `FailureClass`, `FailureSnapshot`, `ServiceClass`);
  platform-cut contract (`PlatformOperationCut`, atomic compare helper);
  inspection/control (`OperationInspection`, `ItemInspection`,
  `AttemptInspection`, `HealthReport`, paginated `list_operations`,
  `inspect_operation`, `list_items`, `list_attempts`, `cancel_operation`,
  `abandon_registration`);
  pacing (the retained throttle load/record/clear/hold/tag/delay functions and
  `ThrottleState`); export (`ProjectionSpec`, `ExportOptions`, `ExportResult`,
  `export`); and generic progress (`ProgressLog`). DBOS private exceptions,
  row mappers, status helpers, metadata keys, adapters, and CAS functions are
  module-private and absent from `__all__`.
- README rewritten (it currently claims the repo is an empty skeleton).
- `graphify update .` after the cut.

---
