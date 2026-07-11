# V1 convergence findings — Claude Fable 5 architecture and domain audit

## Review baseline
- **Date:** 2026-07-10
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, clean
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, clean
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean

Drift from the frozen prompt revisions is documentation-only and changes no
plan assumption. `dr-platform` moved from `841c9e1` by exactly one commit
(`7b9b340`, the v1 freeze commit adding the plan and review prompts).
`whetstone-ai` moved from `6ff95c7` by one commit (`ccd9818`), a 12-line
`CONTEXT.md` edit that aligns the glossary with the plan's Operation
lifecycle vocabulary. `unitbench` is at exactly the prompt revision; the
dirty working tree the plan's re-audit described is now committed as
`cafd493`. All non-doc code cited below is identical to the audited
revisions. DBOS 2.26.0 is installed in both Python repos
(`whetstone-ai/.venv/.../dbos-2.26.0`).

The plan's own current-code re-audit claims were re-verified and hold:
`priority_enabled` is not set at queue registration
(`/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/queue_worker.py:41-49`),
the DB-backed enqueue path only warns on a priority/config mismatch
(`.venv/.../dbos/_queue.py:388-390`), dequeue orders by
`priority ASC, created_at ASC` (`dbos/_sys_db.py:3784-3787`),
`SetWorkflowAttributes`, `DBOSClient.list_queues`,
`list_workflows(workflow_ids=..., attributes=...)`, and
`cancel_workflow(..., cancel_children=True)` all exist as the plan assumes
(`dbos/__init__.py:6-8,54-57`, `dbos/_client.py:469-478,841-870`,
`dbos/_dbos.py:838-848,1899-1925`), `dr_platform/__init__.py` exports exactly
94 names, `utc_now` is imported at
`whetstone-ai/src/whetstone/platform/graph_workflow.py:17` with call sites at
`:336`, `:416`, `:434`, and generation workflow inputs carry `database_url`
(`graph_workflow.py:130`). MotherDuck's Postgres wire-protocol endpoint
exists as a vendor product, so ADR 0014's deployed adapter is feasible in
principle (parity itself remains gate 7 / phase 1 work).

## F1. The plan erases every retry path for domain-outcome failures: harness-failed scores and domain-failed generations become permanently unretryable
- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.5 (Retry policy: "Retryable execution failures advance the platform-owned Attempt ordinal"; ordinal advances only when the classified DBOS `ERROR` is retryable), §2.3/§2.4 ("Platform Attempt ordinal maps one-to-one to the Whetstone generation or score attempt index"; rescore batching and orphan replay "are replaced by the shared … attempt, retry … contracts"), ADR 0002 ("it does not maintain a second retry counter"), ADR 0012, ADR 0013.
- **Evidence:**
  - `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:145-181` — every node failure (including provider failures after DBOS step-retry exhaustion, `:343-349`) is caught and converted to an error result record; `:183-203` then persists the domain outcome and returns `generation_run_id`. A domain-failed Generation Run therefore terminates as DBOS `SUCCESS`.
  - `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/scoring_workflow.py:76-137` — the scoring workflow persists its result (`persist_score_result_step`, `:463-489`, harness-failure branch at `:471-474`) and returns normally. A harness failure is DBOS `SUCCESS`, exactly as ADR 0013 states.
  - `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/rescoring.py:212-233` — today's recovery for those outcomes is a caller-chosen `score_attempt_index` (default 0, explicitly settable), and `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/submission.py:67` — generation resubmission takes a caller-supplied `attempt_index`. Both mechanisms are deleted by §2.1/§2.4.
  - `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:128-152` — `score_attempt_id` embeds `attempt_index`; a new Score Attempt requires a new ordinal.
- **Consequence:** Under v1, the only mechanism that can mint a new content-scoped execution is kernel reconciliation classifying a DBOS `ERROR` as retryable (§1.5). But the flagship domain's failures are, by ADR 0013 and by current workflow code, *platform successes*. Sequence: score attempt at ordinal 0 → harness failure persisted → workflow `SUCCESS` → platform Attempt `SUCCEEDED`, terminal. Resubmitting the same eligible Generation Run recreates the same Item (`item_key` hashes run + profile axes without the Attempt, §2.3), whose attempt 0 dedups onto the existing successful workflow (§1.5 dedup contract: success blocks replacement) — no new scoring ever occurs. The identical trap holds for generation: transient provider failures that exhaust node step retries persist as domain-failed runs under workflow `SUCCESS`, and the deleted caller `attempt_index` was the only regeneration path. The plan's retry machinery (RetryPolicy, ordinal CAS, retry provenance columns) is effectively dead code for Whetstone, and experiments will systematically lose samples in a provider/model-correlated way — precisely the "experiment that appears valid when it is not" this review is instructed to prioritize, and unmitigated by any §4.4 gate (gate 3 proves crash resume, not domain-failure recovery). This is an internal contradiction between ADR 0002 (platform exclusively owns the ordinal, advanced only by platform-failure policy) and ADR 0012/0013 (Whetstone owns eligibility and domain outcomes, which the platform must not interpret).
- **Required plan change:** Define an explicit domain-outcome retry seam before implementation. Owner must choose one: (a) a caller-requested ordinal-advance API — Whetstone eligibility selects a terminal current Attempt plus proof of a terminal domain failure and asks the kernel to CAS the next ordinal under the existing provenance rules (`retry_reason = domain_outcome`); (b) Whetstone workflows re-raise a typed exception after persisting terminal domain failures so kernel classification covers them — this narrows ADR 0013 and must be recorded as such; or (c) retain a scoped rescore/regenerate submission path as a named exception to "one happy path." Whichever is chosen must state its identity, provenance, cancellation-interaction, and budget (`max_attempts`) semantics, and gate 3 must prove a harness-failed score and a domain-failed generation are actually re-executed.

## F2. Sticky cancellation plus content-scoped identity, with no operator retry in the cut, permanently freezes an experiment's content after one cancel
- **Severity:** major
- **Class:** owner-decision
- **Plan contract:** §1.5 ("Cancellation is sticky … ordinary resubmission or reconciliation must not create a replacement Attempt. A future explicit operator retry action may do so, but … a platform retry command are outside the pre-experiment cut"), §1.8 ("The sole initial control is `cancel operation`"), §4.7 (replay/retry deferred), ADR 0001, ADR 0005.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:29-62` — `prediction_id` embeds `experiment_name`, so any resubmission of the same Experiment reproduces the same Item content and the same content-scoped execution identities. Plan §1.5: reconciliation "stops at the first active, successful, sticky, missing, exhausted, or newly enqueued execution," and the enqueue table links an existing workflow via `WORKFLOW_ALREADY_PRESENT` without allocating a new Attempt.
- **Consequence:** Cancel a Generation Operation (the one exposed control, and the tool an operator will reach for during the first intensive experiments), then resubmit the experiment: every Item's attempt 0 links the existing `CANCELLED` execution, reconciliation stops at sticky, and the new Operation terminates `CANCELLED` without doing work. There is no operator action inside the cut that can ever run that content again; the only recourse is renaming the experiment, which mints new identities and orphans all prior provenance. The system can stop a run but can never resume one — a one-way door for exactly the phase (expensive, exploratory experiments) this cut is preparing for. The plan defers "raw replay/resume/fork" with evidence-based reasoning (§4.7), but a minimal cancellation-reversal is a different, smaller action than raw DBOS replay, and the plan never acknowledges the freeze consequence it creates.
- **Required plan change:** An explicit owner decision, recorded in the plan: either (a) include one guarded `retry cancelled` operator action in the cut — insert-next-ordinal under the same reconciliation CAS and provenance rules (`retry_reason = operator_cancel_retry`), read-only until confirmed, consistent with ADR 0003/0005 — or (b) explicitly accept the freeze, document it in the operator contract and CONTEXT.md cancellation entry, and add a §4.4 gate-4 assertion that operators understand cancel is irreversible for that experiment name within the cut. Note that if F1 chooses option (a), the same API closes most of this gap.

## F3. Operation aggregate recomputation names no serialization primitive that actually serializes concurrent writers
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5 Operation aggregation ("Every application transaction that changes Item/Attempt state recomputes the affected Operation counts from current Attempts under the same writer lock"), §4.3 ("stored aggregate recomputation after each state transition").
- **Evidence:** The only writer lock the plan defines is the effort-wide *shared* advisory transaction lock of §1.6/ADR 0010, which platform writers hold concurrently — shared advisory locks do not conflict with each other, only with export's exclusive lock. No Operation-scoped lock is specified anywhere in §1.3 or §1.5.
- **Consequence:** Two transactions finalizing the last two Items of an Operation concurrently each recompute counts from a snapshot that cannot see the other's uncommitted transition; both commit; the stored aggregate says `RUNNING` (or wrong counts) even though every Attempt is terminal. Because aggregate refresh only happens inside subsequent state-changing or reconcile transactions, an Operation whose last two transitions raced can durably retain a false non-terminal status and a never-set `completed_at` until some later actor happens to reconcile it — silent wrongness in the exact row export, inspection, and the experiment command read. The mitigating sentence ("inspectors and export first run bounded reconciliation") assumes the defect is transient lag, not a permanently stale terminal state.
- **Required plan change:** Specify the serialization invariant: every transaction that mutates Item/Attempt state for an Operation acquires the Operation row lock (`SELECT … FOR UPDATE` on `<prefix>_operations`) before recomputing the aggregate, or recomputation is expressed as a set-based `UPDATE … FROM` that re-derives counts atomically under that row lock. Add the two-writers-finish-last race to the §4.3 test list explicitly.

## F4. The pure status function has no stated clause precedence, and several states legitimately overlap
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.5 Operation aggregation ("The pure status function is the only status derivation").
- **Evidence:** The eight derivation bullets in §1.5 are individually predicated but not ordered. With `page_size=500`, a large Operation routinely has some current Attempts `PENDING`/`CLAIMING` while others are already `ACTIVE` — satisfying both the `ENQUEUEING` and `RUNNING` clauses; `CANCELLING` can overlap both while physical results are unresolved.
- **Consequence:** Two conforming implementations can return different statuses for the same row set; inspector output, export, and the experiment command would disagree across revisions without any bug being locatable. Behavior lives only in prose (plan principle 10).
- **Required plan change:** State the total precedence order (e.g., `REGISTERING > CANCELLING > ENQUEUEING > RUNNING > terminal derivation`) as part of the status-function contract, and pin it with a table-driven pure-function test enumerating overlapping combinations.

## F5. `detail_platform_attempts` has no defined artifact mode, so its root-cascade and snapshot-alignment guarantees are unimplementable as written
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.6 (artifact-mode table: kernel tables are `change_seq` deltas; Neon detail rows come from "same projection snapshot plus root sample manifest"), §4.1 ("Every table carries the root Prediction ID and snapshot ID so root-cascade completeness is testable").
- **Evidence:** `detail_platform_attempts` (§4.1) is kernel Attempt data, but kernel tables are exported under per-destination `change_seq` cursors while every other detail table is built from the Whetstone full-application snapshot. Kernel Attempt rows carry `operation_key`/`item_id`, not a root Prediction ID or a domain snapshot ID; §1.6 and §4.1 never say which artifact produces `detail_platform_attempts` or how its rows join the manifest.
- **Consequence:** If implemented from the kernel incremental artifact, a root Prediction selected in the manifest can have Generation Runs from snapshot *S* but platform attempts from an unrelated cursor position — drill-through joins that §1.6 promises are complete can silently miss or over-include attempts, and the §4.1 testability claim (root ID + snapshot ID on every table) is false for this table.
- **Required plan change:** State that the Detail Store's platform-attempt rows are built inside the same Whetstone projection snapshot (joining kernel attempt rows to Prediction roots at snapshot time, stamped with the snapshot ID), not from the incremental kernel artifact; or drop the table from the root-cascade completeness claim and document it as cursor-consistent instead.

## F6. "Defined terminal acceptance states" for Experiment completion are never defined
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §2.4 ("does not call an Experiment complete until the required Generation and Scoring Operations reach their defined terminal acceptance states"), §4.4 gate 3, ADR 0013.
- **Evidence:** No section, glossary entry (`whetstone-ai/CONTEXT.md` Experiment), or ADR states which Operation statuses are acceptable (`SUCCEEDED` only? `PARTIAL` with what bound?) or what domain-outcome completeness an Experiment requires before it may be called complete.
- **Consequence:** Especially combined with F1 — where `PARTIAL` generation with model-correlated missing samples is the expected steady state — an implementer can truthfully satisfy §2.4 while declaring complete an Experiment missing an arbitrary, biased fraction of its Predictions. This is exactly a prose-only invariant with no acceptance proof (plan principle 10), on the single surface that decides experiment validity.
- **Required plan change:** Define the acceptance predicate: the Operation statuses accepted per role, the required ratio of domain-successful Generation Runs and Score Attempts per Experiment (or an explicit operator-confirmed override), and add its verification to gate 3.

## F7. Secrets remain inside DBOS replay payloads; the plan guards only the export boundary while making inspector reads routine
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.6/ADR 0011 (export excludes serialized inputs/outputs; "Current Whetstone inputs include `database_url`, so implicit/raw export is a credential leak"), §1.8 (inspector "retrieves narrowly scoped DBOS detail on demand"; attributes/OTLP forbid database URLs), Part 4 Secrets.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:130` and `scoring_workflow.py:78` — `database_url` is the first workflow argument today, and nothing in §2.2–§2.4 changes the new `args_for` contract to stop passing it; `.venv/.../dbos/_client.py:862-863` — `list_workflows(load_input=True, load_output=True)` are the defaults, so any inspector read that does not explicitly disable them fetches credential-bearing payloads.
- **Consequence:** The credential the plan itself flags as the leak rationale stays durably persisted in every workflow row of the DBOS system database, one default-parameter mistake away from appearing in inspector JSON, logs, or a future MCP adapter. The plan treats a data-at-rest problem as an export-filter problem.
- **Required plan change:** Since v1 already rewrites workflow names and argument construction, require that platform-enqueued workflow args carry no secrets — Whetstone workflows resolve their database URL from process configuration inside the step — and additionally require the kernel's DBOS read adapter to always pass `load_input=False, load_output=False` except in an explicitly named debug call.

## F8. The shared-writer-lock discipline for exported rows is asserted but its acquisition point is unowned
- **Severity:** minor
- **Class:** verification-gap
- **Plan contract:** §1.6/ADR 0010 ("every platform write transaction acquires the effort-specific shared Postgres advisory transaction lock before mutating exported rows. … Direct writes that bypass the shared writer lock are unsupported.").
- **Evidence:** Throttle state carries `change_seq` and is written from inside running Whetstone workflow steps through independently created engines — `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:397-420` (`record_throttle_failure_state`) and `:423-438` (`clear_throttle_backoff_state`) — i.e., by domain code paths far from the kernel submission/reconcile flows. The plan never states which layer acquires the advisory lock.
- **Consequence:** If lock acquisition is left to callers, the highest-frequency writers of an exported table (per-node throttle updates during live generation) are the likeliest to bypass the barrier, re-creating the skipped-sequence export bug ADR 0010 exists to prevent — silently, since nothing detects an unlocked write.
- **Required plan change:** One sentence of ownership: the kernel write functions for every `change_seq`-bearing table acquire the shared advisory transaction lock internally (callers cannot forget it), and the §4.3 barrier test includes a throttle write issued from inside a workflow step.

## V0 coverage gaps
- None. All fourteen §4.8 items were checked against the v1 body, both
  glossaries, and the ADRs; each is resolved substantively rather than
  nominally. Two items are resolved by defensible substitution rather than
  literally: v0 §2's DBOS `updated_at` incremental contract became a
  validated full-rebuild snapshot (justified by `operation_outputs` having no
  reliable cursor), and v0 §5's "workload simulation" for random priorities
  became the fixed-band service-class design that removes the starvation
  mechanism entirely. v0 §2's deletion/tombstone requirement is answered by
  prohibiting kernel hard deletion plus root-cascaded detail tombstones.
  Note that F1 above shows the v0 §1 reconciliation state machine, while
  internally complete, does not *cover the flagship domain's actual failure
  mode*; that is a new v1 finding, not a v0 item left open.

## Verdict
- **Gate:** REPEAT_CONVERGENCE
- **Reason:** F1 is a blocker and architecture-changing: the retry/attempt
  contract cannot express the dominant real failure mode of the only client
  domain, and fixing it changes the kernel/caller ordinal-ownership boundary
  (ADR 0002/0012/0013 interaction) or the workflow failure-propagation
  contract. F2 is an unresolved owner decision on the cancellation/identity
  boundary. Both trip the convergence gate conditions independently;
  F3–F8 alone would not have.
- **Unverified:** MotherDuck Postgres-endpoint *query parity* with local
  DuckDB (endpoint existence confirmed from vendor documentation; parity is
  phase-1/gate-7 work and cannot be verified from the repositories).
  DBOS `created_at` tie-breaking granularity for same-instant enqueues within
  one priority band (ordering read from `_sys_db.py:3784-3787`; sub-second
  collision behavior not exercised). Whetstone rescore-candidate SQL
  semantics for harness-failure rows at the same attempt index were not
  fully traced (`rescoring.py` selection joins); F1 does not depend on them —
  it rests on the deleted caller-chosen attempt indexes and the
  SUCCESS-on-domain-failure workflow behavior, both verified directly.
