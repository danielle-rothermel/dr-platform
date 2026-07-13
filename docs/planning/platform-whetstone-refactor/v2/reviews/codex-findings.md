# V2 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** 2026-07-10
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, dirty only in the issued planning/canonical-doc packet plus the v1 review results and v2 prompts; no application-code drift
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, dirty only in the approved `CONTEXT.md` update; no application-code drift
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean
- **DBOS:** 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`

## F1. The Manifest and workflow identity are not bound to the execution recipe they deduplicate
- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.5, §2.3, ADR 0001, and ADR 0009
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:382` defines the Manifest from role/group and Item leaves, while `:425` requires equality of an undefined “target identity” and `:433` makes the queue, workflow, `execution_for`, and `args_for` callables non-serialized target fields; no target/recipe digest appears in the Operation schema at `:159`. `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:29` derives `prediction_id` from `task_id` and selected axes but not the persisted task snapshot/input content, and `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/submission.py:202` silently accepts an existing `prediction_id` with `ON CONFLICT DO NOTHING` rather than proving exact record equality.
- **Consequence:** An exact Manifest can be resumed with a different queue/workflow/argument recipe, and a different Operation can submit changed task content under the same current Prediction ID. The platform then derives the same Generation Run/workflow ID and links to stale DBOS work or the old domain row, silently executing or publishing the wrong content instead of rejecting the conflict.
- **Required plan change:** Define one versioned `execution_recipe_digest` that covers the complete domain execution input and stable target identity (including the workflow/profile/dataset versions that affect behavior), persist it with the Operation/Attempt, include it in Manifest equality and the content-scoped execution/workflow identity, and require Whetstone's RegistrationHook to compare every existing domain row for exact equality before returning `ALREADY_PRESENT`.

## F2. Per-artifact promotion permits a destination to expose a mixed cross-table snapshot
- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.6, §4.1, ADR 0008, and ADR 0015
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:815` builds and replaces each client projection table separately, `:845` stores publication authority per `(destination_id, artifact_key)`, `:863` promotes one artifact under its own CAS, and `:911` makes partial destination/artifact success first-class. No plane-level committed-snapshot pointer or reader selection rule is defined. Current Unitbench already issues cross-table reads, for example `/Users/daniellerothermel/drotherm/repos/unitbench/src/lib/read-layer.ts:70` joins predictions and prediction details in one query.
- **Consequence:** `predictions` can expose H2 while `sweep_metrics` or another Analysis table remains at H1, and a Neon root manifest/detail table can advance without all of its root-cascaded rows. Per-table `snapshot_id` values make the mismatch detectable after the fact but do not stop ordinary readers from observing missing joins, stale metrics, or incomplete detail roots.
- **Required plan change:** Make each consumer-visible plane a fenced publication bundle: build and validate all mutually referential staging tables for one source snapshot, then atomically advance one bundle manifest/pointer that readers resolve. If physical table replacement cannot be one transaction on a destination, retain versioned tables and switch a single transactional view/manifest only after every member is ready; define retry and garbage-collection ownership at the bundle token.

## F3. DBOS cancellation cannot provide the physical provider-call stop required by the gate
- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.5 cancellation, §1.8 controls, gate 4, and ADR 0005
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:513` treats `cancel_workflow(..., cancel_children=False)` as the physical-cancel action and `:1401` requires the operator gate to prove physical stop. Installed DBOS only updates the workflow row to `CANCELLED` in `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:845`; its only step preemption path polls and cancels an async task in `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_core.py:1637`. Whetstone's paid provider boundary is a synchronous `@DBOS.step` at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:343` and performs the provider call at `:365`.
- **Consequence:** Cancelling an active Generation workflow can mark DBOS and the platform cancelled while the provider request continues and incurs cost. A subsequently authorized Attempt can overlap or duplicate that paid call, so the claimed physical-stop and cost-control invariant cannot pass on the named runtime.
- **Required plan change:** Specify a cancellation/quiescence protocol for the provider boundary. Either make the provider step genuinely cancellable and async/preemptible with an adapter-level abort contract, or treat DBOS cancellation as logical only and prohibit a replacement Attempt until the prior paid call has reached a durable, observed quiescent outcome. The acceptance gate must inject cancellation during the provider call and prove no overlapping paid request, not merely observe DBOS `CANCELLED`.

## F4. Strict Experiment acceptance has no persisted schema, snapshot cut, or stale-result rule
- **Severity:** major
- **Class:** architecture-changing
- **Plan contract:** §2.4, gate 3, and ADR 0018
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:1151` names a persisted `ExperimentAcceptanceResult` and lists partial-policy fields, but the schema/crosswalk and table inventories at `:154`, `:1046`, and `:1266` define no acceptance-result/policy table, keys, foreign keys, append-only identity, or transaction. The live Whetstone domain schema begins its Experiment surface at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/db/schema.py:133` and has no acceptance record to extend; the live models likewise proceed from `ExperimentRecord` directly to Prediction records at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/models.py:264`.
- **Consequence:** Implementations can disagree about which Generation Run is accepted, which scoring-profile set is authoritative, and whether a result remains valid after a next Attempt reactivates an Operation. A previously persisted “complete” result can therefore remain visible after its expected set or current outcomes change, or a partial override can be recorded without a reproducible database snapshot.
- **Required plan change:** Add an append-only Whetstone acceptance schema and transaction contract keyed to the exact generation/scoring Manifest digests and a domain snapshot/version. Persist selected Generation Run and Score Attempt IDs, required-profile set, observed matrix, policy version, operator facts, and source Operation/Attempt cuts; define how later Attempts make earlier evaluations historical rather than current and how one current acceptance pointer is advanced atomically.

## F5. DBOS 2.26.0 has no deterministic tie-break for same-millisecond, same-Service-Class dequeues
- **Severity:** major
- **Class:** architecture-changing
- **Plan contract:** §1.4, phase 1, gate 5, and ADR 0007
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:347` relies on kernel shuffle enqueue order followed by equal-priority DBOS FIFO, and `:1293` plus `:1405` require deterministic equal-time ordering. Installed DBOS stores `workflow_status.created_at` as a millisecond integer with a database default at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_migration.py:238`, then dequeues using only `priority ASC, created_at ASC` at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:3784`; `workflow_uuid` or another stable tie-break is absent.
- **Consequence:** Multiple sequential enqueues in one millisecond have unspecified relative order, and concurrent workers using `SKIP LOCKED` can further vary the selected tied rows. The mandatory shuffle safety gate is already disproved against the issued runtime rather than merely awaiting a test, so model-grouped work can re-cluster unpredictably inside a page.
- **Required plan change:** Do not proceed with the claimed ordering contract on unmodified DBOS 2.26.0. Obtain and pin a DBOS dequeue contract with a stable third ordering key, or choose a different queue/scheduling representation that durably carries `shuffle_rank` without the starvation behavior the plan rejected. Update the contract test to force identical millisecond timestamps and multiple dequeuers.

## F6. The retained COPRO and e2e flows lose both their lifecycle wait and their analysis source
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.8, §2.1, §2.5, and §4.2
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v2/plan.md:968` omits a wait primitive from the new public seam, `:1052` deletes Whetstone's entire analysis package, `:843` says export never runs automatically, and `:1186` only says COPRO is “repointed minimally” without naming either replacement. Live COPRO imports `AwaitOperationTimeoutError`, `await_operation`, and `whetstone.analysis.frames` at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/optimization/copro.py:15`; the required smoke path also calls `await_operation` at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/scripts/e2e/fixture_smoke.py:142`.
- **Consequence:** The retained optimizer cannot know when full generation execution has settled and cannot compute candidate scores after the analysis deletion. Repointing it to operational Postgres would violate the two-plane invariant, while reading DuckDB without an explicit export leaves each optimization iteration stale.
- **Required plan change:** Name the replacement contract before deletion: retain or add a typed full-lifecycle wait built on reconcile/inspection, and make COPRO explicitly export the required Whetstone projection and read a pinned Analysis Store snapshot between candidate iterations (or define another owner-approved domain-result read that does not violate the two-plane rule). Add COPRO and the zero-spend e2e smoke to the phase exit gates and enumerate the removed runtime/config helper migrations.

## V1 correction closure
| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 caller-requested next Attempt | yes | §1.5 defines the ledger, reason/source matrix, exact current-Attempt CAS, concurrency dispositions, maximum bound, aggregate reactivation, and Whetstone ordinal mapping. |
| P0-2 immutable registration Manifest | no | Membership/Lease/cursor/completion mechanics are defined, but the Manifest and persisted Operation do not bind the target/execution recipe and existing Whetstone domain rows need not compare equal; see F1. |
| P0-3 destination fencing | yes | §1.6 supplies a per-artifact Lease, monotonic token, renewal, promotion CAS, stale-stage ownership, local OS lock, and older-after-newer rejection. F2 is the additional cross-artifact publication defect, not a failure of the original per-artifact H1/H2 fence. |
| P0-4 complete cancellation topology | no | Top-level-only registration and `cancel_children=False` close the descendant-reference defect, but the required physical stop is infeasible for the live synchronous provider step and later paid work lacks a quiescence fence; see F3. |
| P0-5 Experiment acceptance | no | Strict/stratified policy text is present, but no persisted acceptance schema, source cut, selection identity, or invalidation/current-pointer rule exists; see F4. |
| P1-6 Operation mutation/aggregate serialization | yes | §1.5 requires the Operation row lock before Item/Attempt/request mutation, same-transaction recomputation, sorted multi-Operation locking, and the last-two-completions race test. |
| P1-7 execution-scoped DBOS attributes | yes | §1.5 limits attributes to immutable execution facts and makes Operation→Item→Attempt rows the authoritative many-reference lookup. |
| P1-8 total Operation-status precedence | yes | §1.5 gives a first-match total order and explicit mixed-terminal derivation plus table-driven overlap tests. |
| P1-9 detail platform Attempts inside the Whetstone snapshot | no | §1.6 correctly builds `detail_platform_attempts` in the Whetstone source snapshot, but independent artifact promotion can expose it beside other detail tables from another snapshot; see F2. |
| P1-10 secret-free DBOS payloads and explicit safe reads | yes | §1.5/§2.4 remove credentials from workflow args and require every normal list query to pass `load_input=False, load_output=False`; the installed signatures support both flags. |
| P1-11 kernel-owned shared writer-lock acquisition | yes | §1.6 puts acquisition inside every owning kernel mutation and names static plus workflow-step throttle enforcement. |
| P2-12 live integration/version/order/rescore verification boundaries | no | Live MotherDuck/Neon/Vercel and new-adapter checks remain proper gates, but installed DBOS already lacks the required same-time dequeue tie-break; see F5. Current rescore semantics are preserved as a deletion gate but cannot be compared until the replacement exists. |

## Owner-decision consistency
| Owner decision | Encoded consistently? | Evidence or qualification |
| --- | --- | --- |
| One platform-owned caller-requested next Attempt | yes | Platform glossary Attempt, Whetstone Generation Run/Score Attempt entries, ADR 0002/0003/0012/0013, and §1.5/§2.3 agree. |
| Caller-prepared immutable Manifest | yes | Platform glossary Manifest/Registration, ADR 0009, and §1.5 agree on caller preparation and no durable platform spool; F1 is a missing execution-recipe binding, not a return to spooling. |
| No managed DBOS child workflows; non-recursive cancellation | yes | Platform glossary Cancellation, ADR 0005, and §1.5/§2.4 agree; F3 concerns the meaning of physical cancellation at the synchronous paid step. |
| Destination-local Lease/fencing plus local DuckDB OS lock | yes | Platform glossary Publication Fence, ADR 0008, and §1.6 agree; F2 requires a bundle-level publication unit above the per-artifact fence. |
| Strict completeness with explicit stratified confirmed override | yes | Whetstone glossary Experiment/Acceptance, ADR 0018, and §2.4 agree on policy; F4 is the missing durable transaction/schema needed to enforce it. |

## Verdict
- **Gate:** REPEAT_CONVERGENCE
- **Reason:** Three blocker findings and five architecture-changing findings remain. They require an execution-recipe identity contract, bundle-level publication, a cancellation/quiescence design, an Experiment-acceptance persistence model, and a scheduling contract DBOS 2.26.0 can satisfy; focused audits would otherwise review an infeasible core.
- **Unverified:** Live MotherDuck and Neon lease/promotion transactions, local-versus-deployed query parity, Vercel Node bundling and server-only secret wiring, OTLP behavior, and replacement rescore parity could not be exercised without the not-yet-written adapters, schemas, deployed preview, and credentials. These remain gates and do not weaken their invariants.
