# Platform Hard Cut — Joint Refactor Spec (v1)

**Status:** In review — frozen for whole-system convergence
**Date:** 2026-07-10
**Repos:** dr-platform (kernel), whetstone-ai (lockstep overhaul), unitbench (two-plane swap)
**Glossaries:** [dr-platform/CONTEXT.md](../../../../CONTEXT.md), [whetstone-ai/CONTEXT.md](../../../../../whetstone-ai/CONTEXT.md)
**Canonical decisions:** [Content-scoped execution identity](../../../adr/0001-content-scoped-execution-identity.md), [Platform-owned attempt lineage](../../../adr/0002-platform-owns-attempt-lineage.md), [Append-only attempt ledger](../../../adr/0003-append-only-attempt-ledger.md), [Kernel-owned failure taxonomy](../../../adr/0004-kernel-owned-failure-taxonomy.md), [Reference-aware cancellation](../../../adr/0005-reference-aware-cancellation.md), [Adaptive pacing and bounded slot occupancy](../../../adr/0006-accept-bounded-multi-domain-slot-occupancy.md), [Urgency versus shuffle order](../../../adr/0007-separate-urgency-from-shuffle-order.md), [Destination-local export state](../../../adr/0008-destination-local-export-state.md), [Transactional registration hook](../../../adr/0009-transactional-registration-hook.md), [Monotonic change sequence and export barrier](../../../adr/0010-monotonic-change-sequence-with-export-barrier.md), [DBOS export payload exclusion](../../../adr/0011-exclude-dbos-replay-payloads-from-export.md), [Scoring as a platform Operation](../../../adr/0012-scoring-as-platform-managed-operation.md), [Platform execution versus domain outcome](../../../adr/0013-separate-platform-execution-from-domain-outcome.md), [Dual analysis read adapters](../../../adr/0014-dual-analysis-read-adapters.md), [Two-plane stores](../../../adr/0015-two-plane-analysis-and-detail-stores.md), [Kernel-executed enqueue](../../../adr/0016-kernel-executes-platform-enqueue.md), [Fresh schemas without migration](../../../adr/0017-fresh-schemas-without-data-migration.md)

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

This draft was checked against `dr-platform` at `841c9e1` on
`07-08-refactor`, `whetstone-ai` at `3622b0a` after the July 9 phase-2 merge,
the current dirty `unitbench` working tree based on `32115f3`, and installed
DBOS 2.26.0. User-owned uncommitted files in those repositories are not part
of this plan change.

The v0 findings still reproduce: platform rows track enqueue rather than
execution, workflow failure has no path back to claim, Item mutation has no
export cursor, database-backed enqueue does not validate `priority_enabled`,
Whetstone still imports `dr_platform.backoff.utc_now`, and generation identity
remains content-scoped. The Whetstone re-audit adds one material fact: scoring
now has its own orphan detection/replay and rescore batching modules. V1
therefore moves scoring onto the same platform Operation lifecycle rather than
preserving that second control plane.

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

- `<prefix>_operations`: `operation_key` (PK), `spec` JSONB, full-lifecycle
  status, submission and execution counts, terminal reason where applicable,
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
  Item Attempts, and throttle state). The baseline creates indexes beginning
  with `change_seq`; hard deletion of these rows is prohibited in the
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

| Current Operation column | V1 column | Action and invariant |
| --- | --- | --- |
| `operation_key` | `operation_key` | Retain PK; caller identity and idempotency key. |
| configured `group_key` label | `group_key` | Retain on Operation, not Item; Whetstone stores `experiment_name`. |
| none | `workflow_role` | New caller-owned stable role, consistent across all Items/Attempts. |
| `status` | `status` | Replace enqueue-only meaning with `REGISTERING`, `ENQUEUEING`, `RUNNING`, `CANCELLING`, `SUCCEEDED`, `PARTIAL`, `FAILED`, `CANCELLED`. |
| `requested_count` | `requested_count` | Retain; immutable after initial registration contract is established. |
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

| Current Item column | V1 column/location | Action and invariant |
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
`source_attempt`; `source_workflow_id`; `retry_reason`;
`source_application_version`; missing-observation count/first/last timestamps;
DBOS cancellation request/result facts; and `created_at`, `enqueued_at`,
`terminal_at`, `updated_at`, `change_seq`. Attempt 0 has no source; later
Attempts require source provenance. `execution_key` and `workflow_id` are
indexed but not unique because content-scoped executions may be referenced by
multiple Operations.

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
- Attempts reference Items normally; Item `(item_id, current_attempt)` has a
  composite `DEFERRABLE INITIALLY DEFERRED` FK to
  `(item_id, attempt)`, added after both tables exist. Registration can insert
  Item plus attempt 0 atomically, and no committed Item can point at a missing
  Attempt.
- Database triggers reject deletion of kernel lifecycle rows and reject any
  mutation of a terminal Attempt other than no-op equality. Retry always
  inserts a new row; retention remains deferred.
- Indexes cover Operation `(group_key)`, `(status, updated_at)`, and
  `(change_seq)`; Item `(operation_key, item_index)`, `(operation_key,
  item_key)`, `(service_priority, shuffle_rank, item_id)`, and `(change_seq)`;
  Attempt `(workflow_id)`, `(execution_key, attempt)`, `(enqueue_state)`,
  `(execution_state)`, and `(change_seq)`; throttle `(blocked_until)`,
  `(hold_until)`, and `(change_seq)`.

#### Public/model/file crosswalk

| Current surface | V1 surface | Action |
| --- | --- | --- |
| `SubmittableItem.item_id/order_key/group_key` | `SubmittableItem.item_key/spec/service_class` | `group_key` becomes a submit-level Operation value; shuffle is kernel-owned. |
| `ItemIdentity`, configurable digest labels | fixed `item_id()` recipe | Delete compatibility configuration. |
| `JsonlFieldNames(item_id, order_key, group_key)` | `JsonlFieldNames(item_key, group_key, service_class?, spec?)` | Index original position, validate one Operation group, derive shuffle rank. |
| `JsonlItemRef(item_id, order_key, byte_offset)` | `JsonlItemRef(item_key, item_index, byte_offset, service_class)` | Preserve file order separately from scheduling order. |
| `BatchSubmitResult.items` | bounded `SubmitResult.failure_previews` | Full detail moves to paginated inspector queries. |
| `SubmittedItem`, `EnqueueCandidate`, `EnqueueOutcome` | `ItemRecord`, `AttemptRecord`, `SubmitFailurePreview` | Delete callback-era transport shapes. |
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

One core pipeline: **bounded registration → reconcile → claim → enqueue →
aggregate**, with `submit(items, ...)` and `submit_jsonl(path, fields, ...)` as
thin item-source adapters. `SubmitOptions.page_size` (default 500) is the one
bound for registration transactions, reconciliation/status reads, enqueue
claims, and failure materialization. Paging has no fairness semantics.

Before an Item page can become enqueue-eligible, the same application
transaction invokes a typed `RegistrationHook` for that page. The hook may
insert caller-owned domain rows but must be idempotent and may not call DBOS or
perform remote side effects. It returns a frozen `RegistrationResult` keyed by
caller `item_key`, distinguishing inserted and already-present inputs; the
kernel validates complete one-to-one accounting before writing the matching
Item insert states. Any hook error rolls back both caller rows and platform
rows for the page. Empty submission invokes no page hook and follows the
explicit failed-Operation transition. Whetstone uses this seam to register its
Experiment and Prediction Specs; generation workflows continue loading their
specs from Whetstone tables. See
[ADR 0009](../../../adr/0009-transactional-registration-hook.md).

**EnqueueTarget** is a frozen Pydantic model, per Operation, with callable
fields excluded from serialization:

- `queue_name: str`
- `workflow_role: str` — caller-owned, stable, and searchable; the kernel does
  not enumerate domain roles
- `workflow: Callable` — the DBOS workflow to enqueue
- `execution_for: Callable[[ItemRecord, int], ExecutionIdentity]` — Item plus
  platform attempt → content-scoped execution key and workflow ID
- `args_for: Callable[[ItemRecord, int], tuple]` — Item plus attempt →
  workflow args
- `classify_error: Callable[[BaseException], FailureSnapshot]`
- optional `registration_hook: RegistrationHook`

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

The kernel owns the Item attempt ordinal. Reconciliation advances it under a
compare-and-swap before asking the caller's execution-identity function for
the content-scoped execution at that ordinal. Whetstone maps it one-to-one to
`generation_run.attempt_index`; it does not maintain a second retry counter.
When another Operation has already created an execution for that content and
ordinal, enqueue deduplication links the platform attempt to that existing
workflow. See [ADR 0002](../../../adr/0002-platform-owns-attempt-lineage.md).

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
reconciliation must not create a replacement Attempt. A future explicit
operator retry action may do so, but raw DBOS resume/fork and a platform retry
command are outside the pre-experiment cut. Retryable execution failures
advance the platform-owned Attempt ordinal and obtain a fresh content-scoped
execution identity from the caller.

Operation cancellation is reference-aware. In one application transaction it
marks the Operation cancellation-requested and freezes the set of current
Attempt references. The control path marks those Operation-local Attempts
cancelled, but calls `cancel_workflow(..., cancel_children=True)` only for a
workflow with no other live Operation reference. Shared workflows remain live
for their other Operations. A final transaction records, per Attempt, whether
DBOS cancellation was requested, skipped because shared, observed terminal,
or failed; the inspector reports partial control failures and never claims
immediate cancellation because an executing step may finish first. Concurrent
new references and cancellation use row locks/CAS so neither can race past the
exclusivity check. See
[ADR 0005](../../../adr/0005-reference-aware-cancellation.md).

**Retry policy:** `ERROR` is eligible for reconciliation only through a
frozen, typed `RetryPolicy` with a positive `max_attempts` (total Attempts,
including attempt 0) and an explicit set of retryable kernel failure classes.
The enqueue target supplies a pure error classifier at the domain seam;
classification and its safe diagnostic facts are persisted on the terminal
Attempt before the retry decision. The reconciliation transaction inserts the
next Attempt only when the classified failure is retryable and the bound is
not exhausted. Missing, unclassifiable, validation, authentication, and other
permanent failures fail closed as terminal. The inspector reports retryable,
exhausted, and non-retryable failures separately. There is no unbounded or
background platform retry loop; advancement occurs during explicit bounded
reconciliation (including the reconcile phase of resubmission, inspection,
or export).

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
| `ACTIVE` | DBOS `SUCCESS` | `SUCCEEDED` | None; terminal. |
| `ACTIVE` | DBOS `ERROR` | `ERROR` | Classify and persist; insert attempt + 1 only when retryable and below bound. |
| `ACTIVE` | `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | `RECOVERY_EXHAUSTED` | Never automatic. |
| any nonterminal | reference-aware cancel | `CANCEL_REQUESTED` | Logical cancellation is immediate; physical DBOS cancellation only if exclusive. |
| `CANCEL_REQUESTED` | local cancellation finalized | `CANCELLED` | Sticky; never automatic. Shared DBOS work may still finish for another Operation. |
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

Every application transaction that changes Item/Attempt state recomputes the
affected Operation counts from current Attempts under the same writer lock.
The pure status function is the only status derivation:

- `REGISTERING` while fewer than `requested_count` Items are registered;
- `ENQUEUEING` while any current Attempt is pending/claiming/retryable enqueue
  error and cancellation was not requested;
- `RUNNING` while any current Attempt is active or an execution retry is
  eligible;
- `CANCELLING` while physical cancellation results remain unresolved;
- `SUCCEEDED` when every current Attempt succeeded;
- `CANCELLED` when every Item is logically cancelled;
- `PARTIAL` when at least one Item succeeded and at least one ended in a
  non-success terminal state; and
- `FAILED` when no Item succeeded and at least one ended in a non-cancellation
  terminal failure. Zero Items is `FAILED` with `empty_submission`.

Shared executions do not merge Operation status: each Operation-local Attempt
reference receives its own logical outcome while retaining the common
workflow ID for correlation.

#### DBOS call and correlation contract

Before claiming, the kernel retrieves the database-backed queue and validates
existence plus `priority_enabled=True`. For each claim it nests
`SetWorkflowID(execution.workflow_id)`,
`SetEnqueueOptions(priority=service_priority)`, and
`SetWorkflowAttributes(...)` around `DBOS.enqueue_workflow`. The kernel-owned
searchable attribute allowlist is `operation_key`, `item_id`, optional
`item_key`, `attempt`, and `workflow_role`; `group_key` is included only when
the caller declares it non-sensitive. Whetstone may add safe experiment/model
labels, but never prompts, outputs, endpoints, credentials, database URLs, or
provider payloads. Attributes mirror authoritative rows and are not identity
or accounting storage.

Reconciliation batch-loads statuses through public DBOS/DBOSClient APIs using
workflow IDs. Application-row decisions and CAS writes occur in a separate
Postgres transaction; the plan never assumes a distributed transaction across
the application and DBOS databases and never mutates DBOS system tables.

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

**Empty submission** explicitly transitions the Operation to the failed
terminal state with an `empty_submission` reason (deliberate: an Operation
that produced zero Items is a caller bug worth surfacing loudly). It does not
fall into failure accidentally through `0 >= 0` count arithmetic.

Facade signatures: `EnqueueTarget` absorbs enqueue behavior and frozen
Pydantic `SubmitOptions` carries `page_size`, claim Lease duration,
missing-workflow grace/observation count, `RetryPolicy`, and
`failure_preview_limit`. Defaults are `page_size=500`,
`claim_lease_seconds=60`, `missing_grace_seconds=60`,
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
  `<prefix>_item_attempts`, throttle state, and an allowlisted DBOS telemetry
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
  The allowlisted DBOS telemetry projection is a full rebuild for v1: a
  DBOS-2.26-specific read adapter selects only reviewed columns in one stable
  source snapshot, builds destination staging tables, validates workflow/step
  keys and parent/child references, then atomically replaces the prior tables.
  It does not mix `workflow_status.updated_at` incrementality with
  `operation_outputs`, which has no reliable change cursor. Adapter schema
  drift fails closed; the previous projection remains readable.
- **Client augmentation:** apps register domain projections through a frozen
  Pydantic projection contract carrying a build callable — e.g. Whetstone's
  predictions/generation-runs/score-attempts projections. A v1 client
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
- **Sinks:** (1) MotherDuck sync of the DuckDB file/database — the Analysis
  Store; (2) Neon detail sink — selected tables and row/log-level rows for the
  Detail Store. Detail sampling is deterministic by declared root identity,
  never independent per row. For Whetstone the root is Prediction/Item
  identity: selecting a root cascades to all of its Generation Runs, Node
  Attempts, Score Attempts, failures, and logs so drill-through joins remain
  complete. The selection uses a versioned stable hash and threshold; v1
  starts at 100%, and changing the rate preserves membership monotonicity and
  repeatability. Root deletions/tombstones cascade through the same manifest.
  Both sinks are driven from the same verb; knobs are table selection,
  root-sample threshold, and per-sink enablement.
- No hidden triggers: export runs when the caller runs it (post-operation,
  cron, or ad hoc). Nothing in the submit/worker flows exports.
- Export commit state is destination-local: DuckDB, MotherDuck, and Neon each
  persist their own per-artifact cursor/snapshot metadata and advance only
  after their own idempotent transaction commits. Success in one destination
  never advances another. Operational Postgres supplies source change numbers
  and high-water marks only. See
  [ADR 0008](../../../adr/0008-destination-local-export-state.md).
- Sequence allocation is not commit ordering. Therefore every platform write
  transaction acquires the effort-specific shared Postgres advisory
  transaction lock before mutating exported rows. Export acquires the matching
  exclusive **session** advisory lock first, waits for existing writers, then
  opens its repeatable-read transaction, captures the sequence high water, and
  extracts rows satisfying `previous_cursor < change_seq <= high_water` into
  destination staging. It commits the source transaction and releases the
  barrier immediately after bounded extraction; MotherDuck/Neon sync and
  destination promotion happen afterward. A crash before destination commit
  leaves its cursor unchanged and the idempotent delta is extracted again.
  Direct writes that bypass the shared writer lock are unsupported. See
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
| Process dies before atomic promotion | Drop/replace uniquely named stale staging on next run; current tables remain valid. |
| Validation/checksum mismatch | Fail closed, retain prior destination version, report health failure. |

`full_rebuild=True` creates new staging/current tables and resets only the
selected destination's cursor after successful promotion. It does not mutate
another destination's state. Export logs stable phase/artifact/destination
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
| Lifecycle | registration, enqueue, reconciliation, retry eligibility, logical cancellation, aggregates | durable workflow/step execution and raw statuses | generation/scoring eligibility and domain outcomes |
| Concurrency | application-row CAS, Claims/Leases, export writer lock | queue dequeue and workflow idempotency | no parallel flow-control subsystem |
| Pacing | throttle state, holds/tags, policy | durable sleep and worker slots | throttle-key selection per node |
| Observability | typed joins, safe correlation, health derivation | workflow/step/queue facts and OTLP spans | experiment labels, provider/model/cost/domain result facts |
| Analysis | kernel export protocol and sink adapters | allowlisted telemetry source | domain projections and Unitbench-facing schemas |

The external seam is a small typed platform interface (`submit`, `reconcile`,
paginated `inspect_*`, `cancel_operation`, `export`). DBOS and sink adapters
are internal seams; callers do not reproduce claim/retry/export mechanics.

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

The sole initial control is `cancel operation`. It is read-only until explicit
confirmation, shows the reference-aware cancellation plan, uses
`cancel_children=True` only for exclusive workflows, writes application intent
and results, and returns structured partial failures. Raw retry, DBOS resume,
and DBOS fork are not exposed.

Add `dbos[otel]>=2.26,<2.27` and optional config for `enable_otlp`, trace
endpoints, and `otel_attribute_format="semconv"`. No exporter configured means
normal operation. Safe platform correlation attributes plus Whetstone-owned
provider/model/token-count/provider-cost/throttle-delay facts enrich spans
where already available; prompts, outputs, credentials, database URLs, and raw
provider metadata are forbidden. Traces are diagnostic, never durable cost or
result storage. OTLP initialization/export failure degrades visibly without
failing an experiment.

### 1.9 Hygiene and structure

- `submission.py` becomes the deep registration/reconcile/enqueue module;
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
  `ExecutionIdentity`, `EnqueueTarget`, `RegistrationHook`,
  `RegistrationResult`, `RetryPolicy`, `SubmitOptions`, `SubmitResult`,
  `submit`, `submit_jsonl`, `reconcile`); lifecycle records/enums
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
identity used by the kernel enqueue target, and its generation attempt maps
one-to-one to the platform-owned Item attempt ordinal. The
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

### 2.4 Platform boundary simplification

- `submission.py` adapter shrinks: builds an `EnqueueTarget` (queue, workflow,
  args_for) and calls kernel `submit`/`submit_jsonl`. The `_enqueue_item`
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
- The experiment-facing command reports both Operation keys and does not call
  an Experiment complete until the required Generation and Scoring Operations
  reach their defined terminal acceptance states.
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
  `dbos>=2.26,<2.27`. Whetstone keeps direct pandas and dr-providers
  dependencies, tracks the cut dr-platform revision, and refreshes its lock.
  Private-git CI authentication must precede `uv sync` in every affected repo.
- **DBOS contracts:** Whetstone's lockfile pins the exact resolved 2.26 patch.
  Tests cover normalized statuses, attributes/filtering, queue
  registration/retrieval and priority, enqueue identity/options, workflow/step
  inspection, recursive cancellation, and every allowlisted system-table
  field. A DBOS minor upgrade is a reviewed compatibility change.
- **Secrets:** MotherDuck token/DSN, Neon URL, DBOS system URL, and application
  database URLs stay in app-side environment/config. They never appear in
  platform defaults, workflow attributes, OTLP attributes, logs, or standard
  export payloads.
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
table in v1; workflow/step details are retrieved on demand through the typed
inspector.

### 4.2 Migration and cutover order

Each phase must pass its exit gate before the next begins. Implementation
issues may split a phase, but may not reorder its dependency boundary.

1. **Contract preflight.** Pin DBOS 2.26, capture the exact public signatures
   and allowlisted schema in contract tests, prove queue priority inspection,
   prove MotherDuck Postgres-endpoint and local-DuckDB query parity on a tiny
   fixture, and record the fresh DB/application/queue/workflow names. No
   production code switches yet.
2. **Platform vocabulary and baseline.** Implement fixed naming, Pydantic
   records/options, enums, digest recipes, the complete schema crosswalk,
   shared writer lock, `change_seq` triggers, and fresh `0001`. Add pure state
   and aggregate tests before I/O flows.
3. **Platform lifecycle.** Implement bounded RegistrationHook pages,
   deterministic shuffle, content-scoped enqueue, status normalization,
   append-only Attempts, retry/missing/CAS reconciliation, bounded
   `SubmitResult`, and reference-aware cancellation. Replace tests at the new
   external interface; delete old shallow-module tests only when coverage has
   moved.
4. **Whetstone generation cut.** Add local clock, new names/queues, generation
   target and failure mapping, fresh Whetstone schema, and generation
   Operation adapter. Prove cross-Operation dedup and model-group shuffle;
   only then remove queue_worker/fairness/stamp paths.
5. **Whetstone scoring cut.** Add scoring Item/Operation identity and target,
   migrate the experiment command, and prove linked generation→scoring
   acceptance. Only then delete custom rescore batching and raw orphan replay.
6. **Inspection and telemetry.** Land typed inspector/health models, Whetstone
   Typer commands, guarded cancel, safe workflow attributes, and optional OTLP.
   This phase is required before expensive experiments, not follow-up polish.
7. **Export and projections.** Implement kernel incremental export, DBOS and
   Whetstone staged rebuilds, destination-local state, fault recovery,
   root-cascade detail sink, and full-rebuild equivalence checks. Populate a
   disposable local DuckDB, MotherDuck database, and Neon schema.
8. **Unitbench swap.** Implement the local/remote Analysis adapters, remote
   compute policy, two-plane read routing, new allowlists/table configs, and
   parity tests. Switch deployed environment only after every current page
   passes against the new stores; then retire `tools/unitbench_publish`.
9. **Final deletion/documentation.** Remove analysis/migration/legacy tests and
   exports named above, update READMEs/TESTING/composable/workbench docs,
   refresh dependency pins, search all three repos for old names, and run
   `graphify update .`.

### 4.3 Transaction, concurrency, and crash verification

The permanent test suite must cover:

- two submitters registering the same Operation and losing/winning every
  Item/current-attempt CAS;
- crash before DBOS enqueue, after enqueue before outcome persistence, and
  after outcome before aggregate refresh;
- one shared failed execution observed by multiple Operations, with exactly
  one next ordinal per Operation and one content-scoped DBOS workflow;
- policy-gated retry, enqueue-try exhaustion, execution-attempt exhaustion,
  recovery exhaustion, sticky cancellation, and state-sensitive missing;
- reference-aware cancellation with exclusive, shared, newly racing, child,
  and already-terminal workflows;
- stored aggregate recomputation after each state transition;
- export writer/barrier ordering with an in-flight sequence allocation;
- crash/retry at every source, staging, promotion, MotherDuck, and Neon point;
- full-rebuild versus incremental kernel equivalence and deterministic root
  sample completeness; and
- absent/misconfigured queues, app-version drift, missing DBOS rows, disabled
  OTLP, and unavailable telemetry exporters.

Tests control clocks, IDs, shuffle inputs, missing-observation counts, and
retry decisions; they do not sleep or depend on incidental queue timing.

### 4.4 Pre-experiment acceptance gates

No intensive experiment begins until all gates pass on the exact locked
revisions and a fresh disposable database:

1. **Shuffle safety (blocking):** deliberately model-grouped inputs are mixed
   in every 500-Item enqueue page; rerun produces identical ranks; original
   result order remains intact; no model block dominates a page beyond the
   declared fixture bound.
2. **Generation identity:** overlapping Operations for identical Predictions
   converge on the same Generation Run/workflow per attempt without duplicate
   provider calls.
3. **Generation/scoring lifecycle:** one experiment creates linked generation
   and scoring Operations, persists append-only domain outcomes, distinguishes
   platform success from domain outcome, and resumes safely after injected
   process death.
4. **Operator readiness:** list/show/items/attempts/workflow/queue/throttle and
   health JSON are accurate; reference-aware cancellation proves both physical
   stop and shared-work retention.
5. **Queue/pacing:** priority config is verified, STANDARD FIFO respects
   shuffled enqueue order, runtime concurrency changes are visible, max sleep
   is enforced, and throttle pressure appears in health/traces.
6. **Export correctness:** incremental and full kernel outputs match; DBOS and
   domain rebuilds atomically replace; destination-local cursors survive every
   partial-failure permutation; no excluded DBOS payload/DSN appears.
7. **Unitbench parity:** every current aggregate, table, prediction-detail,
   and visualization query returns schema-valid results through local DuckDB
   and deployed MotherDuck/Neon adapters; remote compute policy blocks or
   confirms expensive pages as declared.
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

V1 intentionally does not add export-aware DBOS retention, raw replay/resume/
fork controls, alert routing/threshold persistence, read-only MCP tools,
browser/Wasm analytics, generic permissions/tenancy, a web control plane,
distributed Conductor-style recovery, or direct DBOS system-table mutation.
Retention waits for measured growth and export-rebuild proofs; replay waits
for attempt/idempotency evidence; alerts wait for workload baselines; MCP must
be a thin adapter over the mature inspector.

### 4.8 V0 unified-feedback incorporation (priority order preserved)

| Priority | Unified item | V1 resolution |
| --- | --- | --- |
| P0-1 | Workflow reconciliation | Separate enqueue/execution state machines, append-only Attempts, normalized statuses, CAS predicates, retry/cancel/missing policies, and full Operation aggregation. |
| P0-2 | Export consistency | `change_seq`, Export Barrier, stable snapshots, destination-local cursors, artifact-specific refresh modes, crash matrix, and root-cascade sampling. |
| P0-3 | Identity/dedup scope | Content-scoped caller execution identity plus platform-owned attempt ordinal and provenance. |
| P0-4 | Queue/throttle topology | Multi-domain workflows retained; residual slot occupancy explicitly bounded and observable. |
| P0-5 | Scheduling objective | Fixed Service Classes plus mandatory deterministic shuffle rank; DBOS 2.26 priority configuration verified before claim. |
| P1-6 | Searchable workflows | Safe kernel/Whetstone workflow attributes with DBOSClient filtering; no identity/accounting duplication. |
| P1-7 | Typed inspector/control | Frozen inspection models, Typer human/JSON adapters, health report, and reference-aware guarded cancellation only. |
| P1-8 | OTLP/health | Optional semconv OTLP, safe attributes, graceful degradation, and on-demand machine-readable health. |
| P1-9 | Seed/metadata ownership | Transactional `RegistrationHook`; typed Attempt columns; Item/Attempt metadata deleted; immutable Operation metadata retained. |
| P1-10 | Schema/lifecycle crosswalk | Complete column/constraint/index/model/protocol/JSONL/return crosswalk and explicit empty-submission branch. |
| P1-11 | Bounded registration/enqueue | One 500-row page-size contract and bounded `SubmitResult` failure previews. |
| P1-12 | Dependency/model rules | Kernel-owned failure enum, Whetstone mapping, dr-providers removal, and frozen Pydantic models throughout. |
| P2-13 | Mechanical blast radius | Clock, pandas, observability, scoring replay, names, tests, docs, dependencies, and cross-repo stale-symbol search enumerated. |
| P2-14 | Evidence-dependent operator features | Retention, replay, alerts, MCP, browser/Wasm, permissions, and generic control plane explicitly deferred. |

### Review protocol

V1 is frozen as `in-review`. The two independent convergence prompts audit the
post-July-9 Whetstone scoring/generation cut and the full dr-platform,
Whetstone, Unitbench, DBOS, export, and runtime constellation. Reviewers write
separate findings; synthesis happens only after both complete. No finding is
patched into v1. Decision-changing findings return to the owner and land only
in a successor draft.

### Revision log

- v0 (2026-07-08): initial spec from the grilling session; reviewed in round 1.
- v1 (2026-07-10): draft incorporating the v0 adversarial review packet and
  a re-audit of the current `dr-platform`, `whetstone-ai`, and affected sibling
  code, plus the owner-resolved identity, attempt, cancellation, scheduling,
  export, scoring, and Unitbench runtime decisions.
- v1 review freeze (2026-07-10): frozen for independent Codex 5.6 and Claude
  Fable 5 whole-system convergence reviews.
