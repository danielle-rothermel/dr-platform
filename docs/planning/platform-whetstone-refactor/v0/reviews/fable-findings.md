# v0 review findings — fable (design coherence)

Reviewed: spec v0 (2026-07-08), dr-platform @ 07-08-refactor, whetstone-ai as
impact surface. Line references are to current code.

## F1. Retry-via-attempt has no detection step: a terminal-failed workflow's item can never re-enter the pipeline

- **Severity:** blocker
- **Spec section:** §1.5 (dedup contract), §1.3 (`attempt` column)
- **Evidence:** The spec's one submission pipeline is "insert records → claim
  → enqueue", and the claim step only sees `enqueue_status = PENDING` items
  (`src/dr_platform/submission.py:569-599`, `:602-634`). The only reconcile
  pass that returns items to PENDING is `prepare_enqueue_retries`
  (`submission.py:476-491`), which resets `FAILED` (enqueue-time failures)
  and stale `CLAIMING` rows — never `ENQUEUED`/`WORKFLOW_ALREADY_PRESENT`
  rows. An item whose *enqueue* succeeded but whose *workflow* later ended
  ERROR sits at `enqueue_status = ENQUEUED` forever. The spec's promise —
  "resubmitting an operation retries its failures by incrementing `attempt`"
  — names no flow step that reads DBOS workflow statuses during submission,
  and the dedup check ("skips only ACTIVE/ENQUEUED/SUCCESS") happens at the
  enqueue moment, which these items never reach.
- **Consequence:** The headline retry design is unimplementable as specified.
  "Its failures" silently conflates two distinct failure planes: enqueue
  failures (item row FAILED, already handled) and workflow terminal failures
  (item row ENQUEUED, DBOS ERROR — the case `attempt` exists for). Either
  the implementer invents the missing reconcile step ad hoc, or resubmission
  retries nothing and `attempt` stays 0 forever.
- **Suggested change:** Add an explicit reconcile step to §1.5's pipeline:
  on resubmit, for every item in a terminal enqueue status, resolve the DBOS
  status of `workflow_id(operation_key, item_id, attempt)` via the new
  normalized helper; if terminal-failed → `attempt += 1`, reset to PENDING,
  clear lease columns. Specify its cost model (one status lookup per
  enqueued item, or a batched query against the DBOS system tables) and
  state explicitly which item states each reconcile rule touches.

## F2. The incremental export watermark has nothing to watermark on: operations/items have no updated_at

- **Severity:** blocker
- **Spec section:** §1.6, §1.3
- **Evidence:** `export(...)` "incrementally upserts rows changed since the
  stored watermark". But `batch_operations` carries only
  `created_at`/`completed_at` and `batch_items` only `created_at`
  (`src/dr_platform/db/schema.py:83-84`, `:128`); §1.3's new column list
  says only "timestamps". Item rows mutate in place after insert — every
  enqueue-status transition, lease write, outcome, and (new) `attempt`
  increment (`submission.py:616-634`, `:660-684`). Only the throttle table
  has `updated_at` (`schema.py:168`).
- **Consequence:** With a created_at watermark, every post-insert mutation is
  invisible to incremental export: the Analysis Store shows items frozen in
  their first-seen state, permanently and silently wrong — the worst failure
  mode for a store that "serves all aggregate analysis". `full_rebuild=True`
  masks the bug on every manual inspection.
- **Suggested change:** §1.3 must add `updated_at NOT NULL` to both tables,
  state the discipline that maintains it (application-side on every UPDATE
  statement, or a trigger), and index it. §1.6 must name the watermark
  column per standard table — including which column serves for each DBOS
  system table (their schemas are not dr-platform's to change).

## F3. "One queue per throttle domain" is incompatible with whetstone's per-node throttle keys

- **Severity:** major
- **Spec section:** §1.7, and its collision with §1.5 (`EnqueueTarget` is
  per-operation with one `queue_name`)
- **Evidence:** A whetstone throttle domain is `(provider, endpoint, model)`
  — `throttle_key=f"{provider_kind}:{endpoint_kind}:{model}"`
  (`whetstone/platform/spec_builder.py:387`) — and it is resolved **per
  graph node**, not per workflow: each node runs its own throttle preflight
  and sleeps in-workflow (`whetstone/platform/graph_workflow.py:145-181`,
  `:308-340`; `node_execution.py:376`). One generation workflow whose graph
  mixes models spans multiple throttle domains; today there is exactly one
  generation queue (`queue_worker.py:16`). Meanwhile §1.5's `EnqueueTarget`
  carries a single `queue_name` per operation, and an experiment
  (= operation) sweeps multiple models across items.
- **Consequence:** The convention that "neutralizes" the slot-starvation
  hazard cannot be satisfied by the spec's own flagship client. Either
  workflows sleeping on model B's backoff occupy slots that model A's items
  need (starvation returns, now blessed by a doc that claims it's solved),
  or whetstone must shard queues by domain — which requires per-item queue
  routing that `EnqueueTarget` doesn't have, plus splitting multi-model
  graphs, which contradicts whetstone's whole graph design.
- **Suggested change:** Restate §1.7 honestly: the one-queue-per-domain rule
  bounds *item-level* domain mixing only; per-node multi-domain sleeps
  inside one workflow remain a residual hazard, mitigated by backoff caps
  (`DEFAULT_MAX_BACKOFF_SECONDS = 300`, `backoff.py:26`), not eliminated.
  If the rule is to be real, add `queue_name`-per-item routing to
  `EnqueueTarget` (e.g. `queue_for: Callable[[ItemRecord], str]`) and state
  what whetstone's queue set becomes. Decide in round 1 — it shapes the
  kernel API.

## F4. Minting workflow_id from (operation_key, item_id, attempt) silently destroys cross-operation dedup that whetstone relies on today

- **Severity:** major
- **Spec section:** §1.5 (library-owned enqueue moment)
- **Evidence:** Today the workflow id is derived from domain identity alone:
  `workflow_id = f(generation_run_id) = f(prediction_id, attempt_index)`
  (`whetstone/platform/queue_worker.py:100-106`,
  `graph_workflow.py:235-236`) — no operation in the recipe. Submitting the
  same content-addressed prediction spec under two different operation keys
  (re-running an experiment, overlapping copro iterations) dedups to one
  durable run via `dedup_enqueue`'s status check (`enqueue.py:43`). The new
  recipe is operation-scoped twice over: `item_id` already hashes
  `operation_key` (§1.2), and `workflow_id` hashes `operation_key` again.
- **Consequence:** Identical items in different operations mint distinct
  workflow ids and both run — duplicate LM spend, and duplicate/conflicting
  generation-run rows for whetstone's append-only stores, since
  `generation_run_id = f(prediction_id, attempt_index)` would now take its
  attempt from *per-operation* kernel counters (two operations at different
  `attempt` values mint diverging generation histories for the same
  prediction). Nothing in the spec acknowledges this behavior change.
- **Suggested change:** Make the scoping decision explicit in §1.5. If
  operations are intended to be isolated execution scopes, say so and state
  the duplicate-work consequence (and what it means for whetstone's
  content-addressed generation runs, §2.3). If cross-operation dedup should
  survive, mint `workflow_id` from `(item_key, attempt)`-level identity, or
  let the app inject the recipe through `EnqueueTarget`.

## F5. The seed hook is load-bearing and the spec never mentions it

- **Severity:** major
- **Spec section:** unaddressed by spec (§1.5 pipeline, §2.4 adapter)
- **Evidence:** Whetstone seeds experiment + prediction-spec rows inside the
  registration transaction (`whetstone/platform/submission.py:83`, `:119`,
  `:127-142`) — and the generation workflow *loads its spec from whetstone's
  domain tables by prediction_id*, not from any kernel row
  (`graph_workflow.py:134-136`, `:268-282`). The seed return set also
  defines `insert_status` accounting (`dr_platform/submission.py:307`,
  `:344-349`). §1.5's pipeline lists insert → claim → enqueue; §1.3 adds a
  new `spec` JSONB to items; §2.4 reshapes the adapter around
  `EnqueueTarget`. None of them says what happens to `seed`.
- **Consequence:** If seed is silently dropped, the enqueued workflow finds
  no prediction spec to load and the whole flow dies at the first step; if
  it survives, `SubmitOptions`/facade design (§1.5) is incomplete without
  it; if the intent is that items' new kernel-side `spec` JSONB replaces
  domain seeding, then whetstone's workflow-load path, FK structure, and
  `insert_status` semantics all change and none of that is specified.
- **Suggested change:** Add a "seed hook" decision to §1.5: keep it (state
  its place in the one pipeline and in `SubmitOptions`), and define what
  `insert_status` means now that items also carry a kernel `spec` payload.
  §2.4 should state that whetstone keeps loading specs from its own tables.

## F6. "enqueue_metadata is strictly caller-owned" contradicts the surviving flows — and the new design leaves the column with no writer at all

- **Severity:** major
- **Spec section:** §1.3 vs §1.5
- **Evidence:** Today the library is the *only* writer: it stores lease keys
  in it (`records.py:41-44`, `submission.py:626-629`), wipes it on every
  reset (`submission.py:449`, `:470`), enforces PENDING ⇒ empty in the
  record validator (`records.py:173-179`), and observability reads
  `workflow_id` out of it (`observability.py:104-112`). The caller's one
  write path is `EnqueueOutcome.metadata` merged at outcome time
  (`submission.py:772-777`) — whetstone uses it to store
  `generation_run_id` (`whetstone/platform/submission.py:156-162`). The
  spec deletes `EnqueueOutcome` and makes enqueue library-executed, and the
  lease columns absorb the library's keys — but no surviving API lets a
  caller put anything into "their" column, and nothing says whether the
  reconcile resets may still wipe it (wiping caller data violates the rule;
  not wiping leaves stale attempt-0 metadata on retried items).
- **Consequence:** A column whose stated contract ("the library never reads
  or writes keys in it") is violated by the reset flow on day one, or a
  dead column nothing can write. Observability and `submitted_item_from_row`
  (`submission.py:786-789`) also need explicit repointing to the lease
  columns, which the spec implies but never states.
- **Suggested change:** Either delete `enqueue_metadata`, or give the caller
  a real write path (optional `enqueue_metadata` attribute on the item
  protocol, captured at insert) and state reset semantics explicitly
  (resets touch lease columns only). List the observability/read-path
  repointing in §1.8.

## F7. §1.3's column lists silently change semantics vs the current schema, and the rename table is incomplete

- **Severity:** major
- **Spec section:** §1.2, §1.3
- **Evidence:**
  - `group_key` today lives on **operations** (`db/schema.py:73`) with a
    one-group-per-operation invariant enforced at submit
    (`submission.py:281-282`; `jsonl.py:51-60`). §1.3 lists `group_key` on
    **items** and not on operations — an unexplained semantic move.
  - Operations today carry a `metadata` JSONB distinct from `spec`
    (`schema.py:82`), compared on resubmit (`submission.py:411-414`). §1.3's
    operations list has no `metadata` and the deletions table doesn't
    delete it.
  - `item_index` (ordering + uniqueness, `schema.py:121`, `:146-149`; used
    for ordering in `observability.py:110`) vanishes from §1.3 with no
    deletion entry, and nothing states the replacement ordering for the two
    ORDER BYs that currently use `order_key` (`submission.py:585-588`,
    `:706-709`).
  - Models/fields carrying `order_key` are missing from the rename table:
    `SubmittedItem`, `EnqueueCandidate` (`submission.py:71-103`),
    `JsonlFieldNames`/`JsonlItemRef` (`jsonl.py:23-36`), and the
    `batch_submit_item_id` PK column (`schema.py:112`).
  - The rename table's "old" physical names are wrong: the tables are
    `{prefix}_batch_submit_operations`/`_items` (`naming.py:46-51`), not
    `batch_operations`/`batch_items`.
- **Consequence:** The spec's own principle is "vocabulary is law", but its
  law has gaps exactly where the implementer must guess — group semantics,
  operation metadata, candidate ordering. Guessing here changes observable
  behavior (result ordering, resubmit validation).
- **Suggested change:** Produce a column-by-column schema diff (old → new →
  rationale) for both tables and extend the rename table to every public
  model/field that changes, including JSONL field names.

## F8. Export leaves projection incrementality and sampling semantics undefined

- **Severity:** major
- **Spec section:** §1.6
- **Evidence:** Client projections "run in the same export pass" — but the
  pass is defined as watermark-incremental, while a projection is a join
  (predictions × generation runs × score attempts) whose already-exported
  rows change when late child rows arrive; the current machinery is
  delete-and-rebuild per version (`projections.py:120-166`), not
  incremental. Separately, the Neon detail sink has an "optional sampling
  rate" with no sampling unit; the Detail Store serves "row/log-level
  viewers" that join across tables.
- **Consequence:** If projections are naively watermarked, exported analysis
  rows are stale-joined and wrong; if they're full-rebuild, "same export
  pass" mixes two incompatible incrementality models under one verb without
  saying so. Independent per-row sampling breaks referential integrity in
  the detail plane (an item sampled in, its node attempts sampled out), so
  the drill-through viewers the plane exists for hit holes.
- **Suggested change:** State per artifact class: standard tables =
  watermark upsert; client projections = full rebuild per export (accepted
  cost) or a defined incremental contract. Define sampling as deterministic
  on a root key (e.g. `hash(item_key) < rate`) cascading to all child
  tables, so sampled entities are complete.

## F9. Watermarks in Postgres + a local DuckDB file are two sources of truth that desynchronize silently

- **Severity:** major
- **Spec section:** §1.6, and tension with principle 5 ("rebuildable")
- **Evidence:** `<prefix>_export_state` lives in operational Postgres; the
  export target is "a local DuckDB file" synced to MotherDuck. Nothing ties
  the watermark to the specific file it describes.
- **Consequence:** Delete or move the DuckDB file (or run export from a
  second machine/CI job against the same Postgres) and the shared watermark
  is already advanced: subsequent incremental exports silently skip
  everything before it — each writer gets a *different* hole, and the
  MotherDuck sync uploads whichever partial file ran last. "Rebuildable"
  holds only if a human remembers `full_rebuild=True`.
- **Suggested change:** Make the watermark live with the data it describes:
  store export state inside the DuckDB file (or derive it as
  `MAX(updated_at)` per table from the file at export start). If it must
  stay in Postgres, key `export_state` by a target-database identity and
  document the single-writer assumption.

## F10. The dedup contract's status vocabulary is imprecise, and it silently auto-retries CANCELLED workflows

- **Severity:** minor
- **Spec section:** §1.5 (dedup contract)
- **Evidence:** "Dedup skips only ACTIVE/ENQUEUED/SUCCESS" — but the code's
  normalized ACTIVE set already contains ENQUEUED plus DELAYED
  (`dbos_config.py:43-47`), which the spec's list omits; and CANCELLED sits
  in the failed set (`dbos_config.py:48-52`), so under "terminal-failed
  workflows do not block", resubmission increments `attempt` and re-runs
  work an operator deliberately cancelled.
- **Consequence:** The single normalized status helper is the spec's own fix
  for three-way modeling divergence, yet the spec doesn't define the
  normalized categories — and the CANCELLED policy resurrects killed
  operations on the next cron resubmit.
- **Suggested change:** Define the normalized categories in the spec and
  glossary (ACTIVE = {ENQUEUED, PENDING, DELAYED}, TERMINAL_FAILED,
  SUCCESS, MISSING) and make an explicit CANCELLED decision (suggest:
  CANCELLED blocks; retry requires an explicit operator gesture).

## F11. The kernel hard-depends on dr_providers.FailureClass — a quiet exception to "domain-agnostic kernel"

- **Severity:** minor
- **Spec section:** unaddressed by spec (principle 2; §1 has no mention of
  dr-providers)
- **Evidence:** `records.py:14` types `EnqueueFailure.failure_class` with
  `dr_providers.FailureClass`; `backoff.py:14`, `:30-35` builds the
  retryable-failure policy (`TRANSIENT`, `RATE_LIMITED`) from the same
  enum. dr-providers is the LM-provider library; Part 4 mentions it only as
  a round-3 pinning concern.
- **Consequence:** The kernel that "knows nothing about LMs" imports its
  failure taxonomy from the LM library, and the hard cut's own review
  rounds will flag it later at higher cost. Any non-LM adopter drags
  dr-providers in transitively.
- **Suggested change:** Decide in the spec: either bless FailureClass as a
  deliberately shared transport-failure taxonomy (document the exception to
  principle 2), or define a neutral kernel enum and map at the platform
  boundary alongside `classify_error`.

## F12. Vocabulary drift in surviving modules the plan didn't look at

- **Severity:** minor
- **Spec section:** §1.8 / unaddressed
- **Evidence:**
  - `OperationProgress(operation: str)` (`progress.py:20-27`) uses
    "operation" as a generic CLI-activity display label. After the rename,
    the kernel's flagship noun and this class collide: `OperationProgress`
    reads as "progress of an Operation" but tracks any heartbeat loop.
  - `OperationStatus.COMPLETED` means "enqueue finished", not "work done"
    (`batch_status.py:24-28`, `:106-124`) — while CONTEXT.md sells the
    Operation as "the unit whose lifecycle … the kernel tracks". With
    retry-via-attempt, an operation whose workflows all terminally failed
    still reads COMPLETED; the two status planes (item enqueue status vs
    DBOS workflow status) are nowhere named in the glossary.
  - The glossary owns Priority/Throttle Domain but not claim, lease,
    attempt, or watermark — all spec-load-bearing terms (review-prompt §3's
    exact worry).
- **Consequence:** The rename makes the collision worse than today, and
  "COMPLETED" operations with zero successful workflows will mislead every
  new reader of the Analysis Store.
- **Suggested change:** Rename the progress class to something
  operation-neutral (`Heartbeat`/`ProgressLog`); either rename operation
  statuses to enqueue-scoped values (e.g. `ENQUEUE_COMPLETE`) or add
  glossary entries distinguishing the enqueue plane from the work plane;
  add claim/lease/attempt/watermark to CONTEXT.md.

## F13. Killing windowing for in-memory submit removes the transaction-size bound, not just ordering

- **Severity:** minor
- **Spec section:** §1.5 ("Windowing survives only as an internal memory
  detail of the JSONL loader")
- **Evidence:** Today `chunk_size` does three jobs: fairness interleave
  (deleted, fine), per-window registration transaction size
  (`submission.py:217-235`), and the enqueue page size
  (`submission.py:256-263`). The spec frames windowing purely as an
  ordering concern.
- **Consequence:** A 100k-item in-memory `submit` becomes one registration
  transaction and one unbounded result payload (`SubmitResult.items`
  materializes every row, `submission.py:702-714`) — regressing exactly the
  "large sweeps" the kernel exists for.
- **Suggested change:** State that insert chunking and enqueue paging
  survive as unnamed-but-real internal batching (or as one named knob in
  `SubmitOptions`), with zero ordering semantics; consider whether
  `SubmitResult` should return counts + failures only.

## F14. Empty-submission ERROR is an accident of the count arithmetic, not a designed state

- **Severity:** minor
- **Spec section:** §1.5 ("Empty submission remains an ERROR-status
  operation (deliberate)")
- **Evidence:** `operation_status_from_counts` reaches ERROR for zero items
  only because `failed_count(0) >= requested_count(0)`
  (`batch_status.py:118-121`) — the same branch that means "everything
  failed". No code distinguishes "sweep produced nothing" from "all items
  failed to enqueue".
- **Consequence:** Fine today, but the spec now *documents* the behavior as
  deliberate while the state machine produces it by coincidence; any future
  edit to the ERROR threshold silently changes the documented empty-submit
  contract, and operators can't tell the two very different situations
  apart in the operations table.
- **Suggested change:** Make the zero-item case an explicit branch (and, if
  worth it, a distinct status or a `requested_count = 0` note in the status
  docs) so the documented contract has a named home in code.

---

## Verdict

The three findings most likely to change the plan: **F1** (the
retry-via-attempt contract names no step that can ever detect a
terminal-failed workflow — the spec's flagship retry story is currently
unimplementable as written), **F3** (the slot-starvation "neutralization"
assumes workflow ↔ throttle-domain is 1:1, but whetstone throttles per graph
node and mixes domains inside one workflow and one operation, so §1.7's
convention and §1.5's single-queue `EnqueueTarget` cannot both stand), and
**F2/F4 jointly** (the export watermark has no change column to watch, and
the new workflow-id recipe silently drops cross-operation dedup whetstone's
content-addressed identity currently provides — both are silent-wrongness
bugs rather than loud failures). Not verified: actual DBOS priority /
`priority_enabled` API surface at the installed version (the constraint is a
floor, `dbos>=2.25.0` in `pyproject.toml:22`, not a pin — spec Part 4 calls
it "pinned"; static review only, and the review constraints barred running
code); unitbench's detail-page query patterns (repo not in this round's
inputs — F8's sampling concern is argued from the spec's own description of
the detail plane); and MotherDuck/Neon sink mechanics (no client code exists
yet to read). I confirmed the spec's "zero consumers" claim for
`artifacts.py` (whetstone's `artifact` hits are its own copro file outputs;
its only dr-platform imports are `await_operation`-family), and the README
does still claim the repo is an empty skeleton, so §1.8's rewrite item
stands.
