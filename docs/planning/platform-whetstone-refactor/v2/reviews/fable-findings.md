# V2 convergence findings — Claude Fable 5 architecture and domain audit

## Review baseline
- **Date:** 2026-07-10
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, dirty exactly as issued: modified `CONTEXT.md`, ADRs 0002/0003/0005/0008/0009/0010/0011/0012/0013, and the effort index; untracked ADR 0018, the three v1 review-result files, and `docs/planning/platform-whetstone-refactor/v2/` (plan plus the two prompts — expected review-packet drift). No application code is modified.
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, dirty only in the approved canonical `CONTEXT.md` edit.
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean.
- **DBOS:** 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos` (`dbos-2.26.0.dist-info/METADATA:4`).

No repository moved from the issued baseline, so every v1-cited code fact was
re-checkable at the same revisions. The plan's current-code re-audit paragraph
was spot-verified and holds: `dr_platform/__init__.py` exports exactly 94
names; `whetstone-ai/src/whetstone/platform/graph_workflow.py:130` still takes
`database_url` as the first durable workflow argument; queue registration
(`whetstone-ai/src/whetstone/platform/queue_worker.py:41-49`) still omits
`priority_enabled`; whetstone's lock still pins dr-platform to the obsolete
`drprov-v02-migration` branch (`whetstone-ai/uv.lock:523`) while both
pyprojects still declare `dbos>=2.25.0` (`dr-platform/pyproject.toml:22`,
`whetstone-ai/pyproject.toml:12`); `dr-serialize` provides `canonical_json`
(`dr-serialize/src/dr_serialize/canonical.py:26`).

The v2 DBOS 2.26.0 contract claims were verified in the installed package and
all hold: persisted statuses include `DELAYED` and
`MAX_RECOVERY_ATTEMPTS_EXCEEDED` (`dbos/_sys_db.py:102-111`);
`priority_enabled` defaults `False` and database-backed enqueue skips priority
validation (`dbos/_queue.py:74`, `:385-390`); database-backed queue
configuration is persisted and retrievable (`dbos/_schemas/system_database.py:300-320`,
`dbos/_client.py:469-478`); `DBOSClient.list_workflows` defaults
`load_input=True, load_output=True` (`dbos/_client.py:862-863`);
`cancel_workflow` defaults `cancel_children=False` and the recursive cascade
has no application reference predicate (`dbos/_dbos.py:1899-1901`,
`dbos/_sys_db.py:845-887`); workflow-ID conflict on enqueue updates only
`recovery_attempts`/`updated_at`/`executor_id` — it never revives a terminal
status and never replaces `attributes` (`dbos/_sys_db.py:648-668`, `:700-704`),
which supports both the dedup contract and the execution-scoped attribute
promise; `operation_outputs` carries step identity and timing
(`dbos/_schemas/system_database.py:154-171`); the `otel` extra exists
(`dbos-2.26.0.dist-info/METADATA:31-34`).

The fresh reconstruction found no violated authority boundary among
dr-platform, DBOS, Whetstone, Unitbench, Postgres, DuckDB/MotherDuck, and
Neon: the five named distinctions are applied consistently in every detailed
section I traced, the Manifest is a genuine membership authority (identity is
caller-content-derived, spool-free, and conflict-checked before hooks or
enqueue), the three lock scopes are acquired in one stated global order with
no order inversion, and the export crash matrix is coherent per destination.
The defects below are completeness holes in otherwise sound contracts, found
by walking the prompt's concrete scenarios; none reopens an owner decision.

## F1. A new Operation that links a foreign-cancelled content-scoped execution has no defined execution state and no eligible recovery reason
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5 dedup contract and sticky cancellation (v2/plan.md:478-491), execution state machine (v2/plan.md:656-664), reason/source matrix (v2/plan.md:555-560), §2.3 cancel request key (v2/plan.md:1097-1098); ADR 0001, ADR 0005; unified invariant 1.
- **Evidence:** Walk the prompt's shared-ordinal scenario: Operation A exclusively cancels execution E at ordinal 0; the experiment is later resubmitted as Operation B with the same content (content-scoped identity is deliberate, ADR 0001; `whetstone-ai/src/whetstone/records/hashing.py:29-51` hashes `experiment_name` into `prediction_id`, so resubmission reproduces E's identity). B's registration new-reference guard passes once A's cancellation intent is resolved (v2/plan.md:505-508 guards only *unresolved* physical-cancel intent). B's attempt 0 then links E as `WORKFLOW_ALREADY_PRESENT` (v2/plan.md:645), because DBOS enqueue on an existing workflow ID leaves the row `CANCELLED` — the conflict update touches only `recovery_attempts`/`updated_at` (`dbos/_sys_db.py:648-668,700-704`) and DBOS cancel had already cleared its queue fields (`dbos/_sys_db.py:858-874`). B now holds a nonterminal Attempt whose observed DBOS status is `CANCELLED` with **no local cancel intent**. The execution-state table (v2/plan.md:656-664) has no transition for that observation: `CANCELLED` is reachable only via local `CANCEL_REQUESTED`, and the dedup contract lists DBOS `CANCELLED` merely as a "retry-policy input" while §1.5's retry policy makes only `ERROR` reconcilable (v2/plan.md:528-539). If an implementer maps it to a terminal fail-closed state that is not `CANCELLED`, neither reason in the closed matrix applies (`DOMAIN_OUTCOME` needs `SUCCEEDED`; `OPERATOR_CANCEL_RETRY` needs `CANCELLED`); if they map it to `CANCELLED`, `OPERATOR_CANCEL_RETRY` demands "cancellation request provenance" (v2/plan.md:560) and a request key `cancel:{cancellation_request_id}:{item_id}` (v2/plan.md:1097-1098) that names a cancellation request owned by Operation A, which B never issued — the plan nowhere authorizes cross-Operation provenance citation.
- **Consequence:** The exact freeze v1 F2/P0-1 was resolved for the *cancelling* Operation reappears one step later on the mainline recovery path (cancel, then resubmit the experiment): Operation B either fails validation, silently terminalizes without an authorized successor, or two conforming implementations diverge on whether B's Item is recoverable at all. The prompt's requirement of "one deterministic owner and outcome at every step" is unmet for this scenario.
- **Required plan change:** Add the missing observation transition — a nonterminal Attempt whose linked workflow is observed DBOS `CANCELLED` without local cancel intent becomes local `CANCELLED` (sticky, foreign-cancel provenance recorded) — and extend `OPERATOR_CANCEL_RETRY` to state explicitly that the cited cancellation request may belong to another Operation (the recorded foreign `cancellation_request_id`), keeping operator confirmation mandatory. Add the cancel-then-resubmit-experiment walk to §4.3 and gate 3/4.

## F2. The default scoring Operation key cannot express a second scoring pass, so strict acceptance is unreachable after any late regeneration without abandoning the documented default
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §2.3 default Operation key recipe (v2/plan.md:1083-1090), §1.5 exact-resubmission hard conflict (v2/plan.md:425-429), §2.4 scoring adapter and acceptance (v2/plan.md:1135-1165); ADR 0009, ADR 0012, ADR 0018.
- **Evidence:** The default key is `whetstone:{workflow_role}:{experiment_slug}:{operation_digest}` where the digest hashes only "the immutable group, role, and Operation spec" (v2/plan.md:1083-1085); a Scoring Operation's spec stores its source generation key (v2/plan.md:232, 1135-1136). None of those inputs changes when the eligible-run selection changes. Manifests are immutable and per-key: "a reordered, truncated, extended, or otherwise changed source is a hard conflict" (v2/plan.md:428-429), and `request_next_attempt` operates only on existing Items — a Manifest cannot gain members. Now run the plan's own flagship sequence (§4.4 gate 3): generation completes; scoring Operation S1 freezes the eligible-run selection and registers; a domain-failed Generation Run is regenerated via `DOMAIN_OUTCOME`, producing a **new** `generation_run_id` (ordinal is hashed in, v2/plan.md:1092-1093) and therefore a new scoring `item_key` (v2/plan.md:1086-1088) in no existing Operation. Scoring that run requires a second Scoring Operation — the routine today: `whetstone-ai/src/whetstone/platform/rescoring.py:212-233` runs repeated selection passes as a matter of course. Under the default recipe, S2 derives the *same* `operation_key` as S1 with a different Manifest → deterministic `REGISTRATION`/manifest hard conflict.
- **Consequence:** ADR 0018 requires every accepted run's required profiles to have a `SUCCESS` Score Attempt before acceptance; the plan simultaneously makes the scoring pass that could satisfy it unsubmittable under the documented default key. The failure is loud, not silent, and explicit caller keys (v2/plan.md:1089-1090) are an escape hatch — but the experiment-facing default, the one the acceptance gates will exercise, contradicts the acceptance contract on the expected iterate loop (generate → score → regenerate failures → score late runs).
- **Required plan change:** Make successive scoring selections distinct Operations by construction: include the frozen candidate-selection digest (or an explicit selection sequence) in the scoring `operation_digest`, or define the experiment-facing default as one Operation per frozen selection with a stated key recipe. State explicitly that an Experiment may comprise multiple Scoring Operations and that acceptance is derived from domain rows across all of them. Add regenerate-then-score-late-runs to gate 3.

## F3. The total Operation-status precedence has no clause for the routine enqueued-but-not-yet-observed Attempt, so the "total" function fails validation on the happy path
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5 Operation aggregation precedence and "Impossible mixtures fail validation" (v2/plan.md:693-712); v1 P1-8 closure claim (v2/plan.md:1495).
- **Evidence:** Enqueue and execution are separate columns (v2/plan.md:637-641). Immediately after a successful enqueue — the single most common state any large Operation passes through — the current Attempt is `enqueue_state=ENQUEUED` (or `WORKFLOW_ALREADY_PRESENT`, v2/plan.md:644-645) with `execution_state=NOT_STARTED`, because `NOT_STARTED → ACTIVE` requires a reconciliation *observation* of a live DBOS status (v2/plan.md:658). Walk the precedence: rule 3 matches only "pending, claiming, or … retryable enqueue error" (v2/plan.md:699-701); rule 4 matches only "active or an automatic execution retry is eligible" (v2/plan.md:702-703); rule 5 requires every current Attempt terminal (v2/plan.md:704). An Operation whose Attempts are all `ENQUEUED/NOT_STARTED` matches no clause, and v2/plan.md:710 directs that fall-through to *fail validation*.
- **Consequence:** A literal implementation raises on every ordinary Operation between enqueue and first reconciliation; a practical implementer silently patches the hole, and two conforming implementations can patch it differently (`ENQUEUEING` vs `RUNNING`) — exactly the divergence P1-8 exists to prevent, now on the most frequent mixture rather than a rare overlap. The mandated table-driven test cannot be written from the current clause set.
- **Required plan change:** Add the missing clause and pick its tier explicitly (recommended: `RUNNING` once every current Attempt is at least confirmed-enqueued, since the work is with DBOS; or extend `ENQUEUEING` — either is fine, but exactly one must be stated). Cover `ENQUEUED/NOT_STARTED` and `WORKFLOW_ALREADY_PRESENT/NOT_STARTED` in the table-driven test enumeration, and state which enqueue states count as "terminal" for rule 5 (permanent `ENQUEUE_ERROR` with `NOT_STARTED` execution, v2/plan.md:647).

## F4. An abandoned, partially registered non-empty Operation has no terminal transition
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.5 registration completion requirement (v2/plan.md:416-424); prompt lifecycle-completeness requirement (states with no successor).
- **Evidence:** "Every claim, reconcile, next-Attempt request, and cancellation mutation requires registration completion unless its explicit purpose is to terminate an abandoned **empty** Operation" (v2/plan.md:417-420). Registration resumption requires the caller to re-present a Manifest source that reproduces the digests (v2/plan.md:396-399, 424-426). If a registrar crashes mid-Manifest and the caller never resumes — or the caller's source can no longer reproduce the digest (a changed JSONL) — the Operation holds committed Items plus Attempt-0 rows, `registration_completed_at` null, an expired Lease, and status `REGISTERING` (precedence rule 1, v2/plan.md:695-696). Cancellation is barred by the registration-completion predicate; hard deletion is trigger-prohibited (v2/plan.md:200-202, 303-305).
- **Consequence:** A permanent `REGISTERING` zombie: not cancellable, not completable, not deletable, forever surfaced by the health report's no-progress check with no operator action that can resolve it. Its committed domain-hook rows (Experiment/Prediction Spec rows) also remain attached to an Operation that can never run.
- **Required plan change:** Extend the abandoned-Operation exception beyond empty submissions: after registration-Lease expiry, permit an explicit operator cancellation (or `FAILED/registration_abandoned` terminal transition) of a partially registered Operation, with the same guarded confirmation used elsewhere. Add it to the §4.3 registrar crash matrix.

## F5. The local outcome of a cancel-requested Attempt whose workflow succeeds before the physical call is ambiguous
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.5 cancellation final transaction (v2/plan.md:513-526) versus the execution-state row `CANCEL_REQUESTED → CANCELLED` (v2/plan.md:663).
- **Evidence:** DBOS cancel refuses to override `SUCCESS`/`ERROR` (`dbos/_sys_db.py:858-866`), so late success is a real, handled DBOS outcome, and the final transaction records an Attempt disposition of "observed terminal" (v2/plan.md:513-517). But the execution-state table's only exit from `CANCEL_REQUESTED` is `CANCELLED` ("local cancellation finalized"), and no text states whether an observed-terminal-success Attempt finishes `SUCCEEDED` (honoring the paid result) or `CANCELLED` (honoring sticky operator intent).
- **Consequence:** The two readings yield different Operation terminal derivations (`SUCCEEDED`/`PARTIAL` vs `CANCELLED`, v2/plan.md:704-708) and different downstream eligibility (a `SUCCEEDED` current Attempt is `DOMAIN_OUTCOME`-eligible; a `CANCELLED` one requires operator confirmation). Inspector, export, and the experiment command can disagree across implementations for the same history.
- **Required plan change:** One sentence in the state machine: an Attempt in `CANCEL_REQUESTED` whose workflow is observed `SUCCESS` (or `ERROR`) before the physical call finalizes in that observed terminal state with the cancel disposition recorded as `observed terminal`; only Attempts whose workflow was actually cancelled (or skipped-shared, per the stated rule) finalize `CANCELLED`. Pin it in the §4.3 late-success test.

## F6. A later urgent reference to an already-enqueued shared execution silently runs at the original priority
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.4 Service Class (v2/plan.md:341-353, 362-366); §1.5 dedup link (v2/plan.md:474-476, 645); ADR 0001, ADR 0007; prompt scheduling-composability question.
- **Evidence:** Priority is fixed at enqueue: the first Operation to create content-scoped workflow W sets `SetEnqueueOptions(priority=service_priority)`; a later Operation whose Item declares `URGENT` for the same content links W via `WORKFLOW_ALREADY_PRESENT` and never issues an enqueue call. DBOS keeps the stored priority — the conflict path updates nothing relevant (`dbos/_sys_db.py:648-668`) and dequeue orders by the persisted value. The Item-row class/priority consistency check (v2/plan.md:349-351) is Item-local and cannot see the divergence.
- **Consequence:** An operator who resubmits stalled content as `URGENT` gets standard-priority execution with an `URGENT` label on their Item — invisible in the inspector as specified, and misleading during exactly the incidents urgency exists for.
- **Required plan change:** State the rule (recommended: a linked reference inherits the execution's enqueue-time priority; the Item records both its requested class and the effective execution priority) and surface requested-versus-effective mismatch in Attempt inspection and the health report. Raising a live workflow's priority stays out of the cut.

## F7. The kernel-table export artifact's commit unit is unspecified, so cross-table consistency inside the Analysis Store is undefined
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.6 artifact-mode table "advance per-table cursor after commit" (v2/plan.md:906) versus the single-transaction failure row "Roll back DuckDB transaction; its cursor stays unchanged" (v2/plan.md:919); ADR 0008; prompt two-plane reader-contract question.
- **Evidence:** One export pass extracts all kernel tables under one barrier snapshot (v2/plan.md:889-895), but the destination side speaks of *per-table* cursors while the failure matrix and Publication Fence speak of one artifact transaction. If each kernel table is its own artifact/cursor, a crash between table commits leaves Operations at high-water H while `item_attempts` remains at an older cursor — aggregates referencing Attempt rows the destination does not yet have. Separately, kernel tables (incremental) and Whetstone projections (full snapshot rebuild) commit under independent fences by design, so an Analysis Store reader joining across the two artifact families can always observe mixed source moments; §4.1 documents the two families (v2/plan.md:1268-1273) but no reader guidance exists, unlike the Detail Store's explicit root-cascade/snapshot-ID contract (v2/plan.md:1283-1286).
- **Consequence:** Not durable-truth corruption (operational Postgres is unaffected, and the next successful pass converges), but a window in which Analysis Store cross-table or cross-artifact reads are silently inconsistent with no way for a reader to detect it — mid-tier debugging queries drawing wrong conclusions is the plausible harm.
- **Required plan change:** State the atomicity unit: all kernel tables extracted in one pass commit with their cursors in one destination transaction per destination (making "per-table cursor" a bookkeeping detail inside one fence), and add one sentence to §4.1/Unitbench guidance that kernel artifacts and projection artifacts are independently timed — cross-family joins must tolerate or check `snapshot_seq` skew.

## F8. The next-Attempt request ledger's `max_attempts` column has no stated relationship to the governing `RetryPolicy` bound
- **Severity:** minor
- **Class:** local-correction
- **Plan contract:** §1.3 request columns (v2/plan.md:270-279, "requested policy bound" at :185) versus the creation predicate `:source_attempt + 1 < retry_policy.max_attempts` (v2/plan.md:578-579).
- **Evidence:** The ledger persists a per-request `max_attempts`, but the only bound the specified transaction evaluates is the Operation's immutable `retry_policy.max_attempts`. Nothing states whether the request value must equal the policy, may tighten it (min of the two), or is advisory provenance only — yet idempotent replay compares exact payload equality (v2/plan.md:277-279), so the ambiguity is durable.
- **Consequence:** Implementations can disagree on whether a request with a lower bound is rejected, honored, or ignored; the persisted "requested policy bound" can then contradict the disposition actually applied, confusing the audit trail the ledger exists to provide.
- **Required plan change:** One sentence: define `max_attempts` on the request as (recommended) an optional tightening bound — creation requires `source_attempt + 1 < min(retry_policy.max_attempts, request.max_attempts)` — or as a pure echo of the policy validated for equality. Reflect the choice in the request check constraints (v2/plan.md:294-297).

## V1 correction closure

Each item was first re-derived from the fresh reconstruction, then checked
against the v1 unified feedback's exact requirements.

| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 caller-requested next Attempt | yes | One `request_next_attempt` transition with closed reason/source matrix, exact CAS predicates, idempotent ledger, concurrency (`SOURCE_ADVANCED`), exhaustion, aggregate reactivation, and Whetstone identity recipes (v2/plan.md:544-606, 270-279, 1092-1100; ADR 0002/0003; CONTEXT.md Attempt). Gate 3 proves regeneration, rescoring, and cancel retry (v2/plan.md:1396-1397). Every v1 sub-requirement (states/reasons, CAS, idempotency, Operation status, cancellation interaction, identity recipes, gates) is present. F1 and F2 are adjacent *new* scenario gaps, not reopenings. |
| P0-2 immutable registration Manifest | yes | Caller-prepared canonical Manifest with count/digests/pages, one registrar Lease + cursor CAS, page-transaction hook atomicity, completion CAS gating enqueue, exact-resubmission equality, crash/expiry resume, empty-submission branch (v2/plan.md:372-431; ADR 0009; CONTEXT.md Manifest/Registration; §4.3 tests v2/plan.md:1347-1351). |
| P0-3 destination fencing | yes | Per-`(destination_id, artifact_key)` Lease + monotonic fencing token held through promotion/cursor commit, renewal, stale-stage ownership, `STALE_PROMOTION`, `flock` for local DuckDB, transactional MotherDuck/Neon rows, H1/H2 crash matrix (v2/plan.md:845-931; ADR 0008; CONTEXT.md Publication Fence; gate 6). |
| P0-4 complete cancellation topology | yes | `TOP_LEVEL_ONLY` enforced at registration, static/integration topology proofs, `cancel_children=False` (verified default, `dbos/_dbos.py:1899-1901`), deterministic lock order, candidate re-read restart, racer serialization via advisory reference locks + committed-guard fail-closed, partial physical failure recording, idempotent repeat, sticky + explicit later authorization (v2/plan.md:450-456, 487-526; ADR 0005). Residual: F1 (foreign-cancelled observation) and F5 (late-success outcome) are completion gaps in adjacent state semantics, not in the cancellation protocol itself. |
| P0-5 Experiment acceptance | yes | Strict completeness default over the Manifest-defined expected set; `PARTIAL` on any missing/rejected cell; `PARTIAL_OVERRIDE` only via persisted stratified policy with counts and operator confirmation; global ratio invalid; append-only re-evaluation (v2/plan.md:1151-1165; ADR 0018; whetstone CONTEXT.md Experiment/Experiment Acceptance; gate 3 biased-failure proof v2/plan.md:1398-1400). |
| P1-6 Operation mutation/aggregate serialization | yes | Every registering/mutating function takes `SELECT … FOR UPDATE` on the Operation before recomputing in the same transaction; ascending multi-Operation lock order; explicitly independent of the Export Barrier; last-two-completions race test (v2/plan.md:677-691, 1365-1366). |
| P1-7 execution-scoped DBOS attributes | yes | Attributes limited to immutable execution facts; `operation_key`/`item_id` forbidden; lookup via authoritative platform rows then DBOS by ID; dedup never replaces the attribute object (v2/plan.md:722-732) — consistent with verified DBOS conflict behavior (`dbos/_sys_db.py:648-668`). |
| P1-8 total Operation-status precedence | no | The five-tier precedence and table-driven test mandate exist (v2/plan.md:693-712), but the clause set is not total: the routine `ENQUEUED/NOT_STARTED` state matches no tier and is directed to fail validation — see F3. |
| P1-9 detail platform Attempts inside the Whetstone snapshot | yes | `detail_platform_attempts` is built inside the Whetstone application snapshot, joining Attempts to Prediction roots and stamping the same snapshot ID; explicitly not populated from the incremental kernel artifact (v2/plan.md:828-831, 1278-1286). |
| P1-10 secret-free DBOS payloads and explicit safe reads | yes | Workflow args carry only stable IDs/non-secret values; credentials resolved from process config inside execution; every normal DBOSClient query passes `load_input=False, load_output=False` explicitly (v2/plan.md:738-741, 1131-1134; ADR 0011) — necessary given verified `True` defaults (`dbos/_client.py:862-863`). |
| P1-11 kernel-owned shared writer-lock acquisition | yes | Every owning kernel write function for all five `change_seq` families acquires the shared advisory transaction lock internally; callers cannot bypass via public APIs; workflow-step throttle test plus static direct-write search (v2/plan.md:885-899; ADR 0010). |
| P2-12 live integration/version/order/rescore verification boundaries | yes | Phase-1 contract preflight blocks production code on live MotherDuck/Neon lease/promotion, exact DBOS 2.26.0 contracts, same-instant same-class ordering, and Vercel runtime/secrets (v2/plan.md:1293-1300); gates 5-7 retain them as blocking; rescore-selection parity fixtures precede deletion (v2/plan.md:1178-1185). Preserved as named gates, correctly not claimed as statically closed. |

**Owner decisions.** All five are expressed consistently across v2, both
glossaries, and the ADRs, with no artifact narrowing or broadening another:
(1) single platform-owned next-Attempt transition — v2/plan.md:544-606, ADR
0002/0003, dr-platform CONTEXT.md Attempt; (2) caller-prepared immutable
Manifests, no platform spool — v2/plan.md:383-399, ADR 0009, CONTEXT.md
Manifest; (3) no child workflows, always non-recursive cancellation —
v2/plan.md:450-456/493-526, ADR 0005, CONTEXT.md Cancellation; (4)
destination-local Lease/fencing including the DuckDB OS lock —
v2/plan.md:845-884, ADR 0008, CONTEXT.md Publication Fence; (5) strict
Experiment acceptance with stratified override only — v2/plan.md:1151-1165,
ADR 0018, whetstone CONTEXT.md. §4.9's closure table was treated as a claim
list, not evidence; each row was verified against the body sections cited
above.

## Verdict
- **Gate:** READY_FOR_FOCUSED_AUDITS
- **Reason:** Every v1 P0 is closed in substance, all five owner decisions are
  resolved and consistently canonicalized, and no finding in this review is a
  blocker, an unresolved owner decision, or architecture-changing: F1-F3 are
  major but bounded spec completions (one state-machine transition plus a
  provenance rule; one key-recipe input; one precedence clause) that move no
  ownership boundary, persistence shape, or cross-repository interface, and
  F4-F8 are local corrections. P1-8 carries the one residual `no` (F3), which
  is a clause-coverage hole inside an otherwise specified and test-mandated
  contract, not a redesign. The remaining work is exactly "local correction or
  bounded verification"; F1-F3 should nonetheless be folded into the plan (or
  its successor draft) before the affected phases (3, 5) begin, since each
  sits on a mainline flow the acceptance gates will exercise.
- **Unverified:** Live MotherDuck conditional-lease/fenced-promotion behavior
  and DuckDB-SQL query parity over its Postgres endpoint; live Neon
  transactional lease behavior under pooling; Vercel runtime/secret wiring;
  DBOS same-instant `created_at` tie ordering within one priority band
  (dequeue order read from code, sub-millisecond collisions not exercised);
  MotherDuck "sync of the DuckDB file/database" mechanics for the Analysis
  Store sink; and the full semantics of the current rescore-candidate SQL
  (pinned instead by the §2.5 parity fixtures). All are already named
  phase-1/gate obligations in v2; none weakens an accepted invariant, and
  per the review rules they remain preserved gates rather than findings.
