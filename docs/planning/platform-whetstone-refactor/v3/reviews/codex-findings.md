# V3 convergence findings — Codex 5.6 code and dependency audit

## Review baseline

- **Date:** 2026-07-10
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, dirty only in the frozen planning/canonical-doc packet, immutable v1/v2 review results, and issued v3 prompts; no application-code drift before this required findings file
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, dirty only in the approved canonical `CONTEXT.md` update; no application-code drift
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean
- **DBOS:** 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`

## F1. The post-submit lifecycle cannot recover the execution target or verify the recipe it claims to persist

- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.5, §1.8, ADR 0001, ADR 0002, and ADR 0009
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md:484` defines `EnqueueTarget` as the only holder of the workflow, `execution_for`, and `args_for` callables, but its complete field list through `:498` has no recipe-producing callable and `:500` merely asserts that the target “also supplies” `ItemExecutionRecipe`s. The Manifest leaf at `:418` hashes exactly `item_index`, `item_key`, `service_class`, and `spec`, so it carries no ordered Item recipe digest that the kernel can compare with the aggregate supplied at `:415`. More importantly, the only public next-Attempt API at `:646` accepts `NextAttemptRequest` alone even though `:673` requires the caller-derived next execution identity, while `reconcile` and `wait_operation` are exposed without an `EnqueueTarget` at `:1103` and `wait_operation` must invoke reconciliation at `:1118`. Managed registration records only callable/name/topology at `:514`, not the complete target or its identity/argument/recipe functions. Current dr-platform avoids this problem by requiring the enqueue callback at the drive boundary (`/Users/daniellerothermel/drotherm/repos/dr-platform/src/dr_platform/submission.py:494`), but v3 deletes that callback without specifying a restart-safe replacement registry or parameter.
- **Consequence:** After the original `submit` process exits, inspection/export/wait reconciliation cannot derive args or identities for an expired enqueue Claim, an automatic retry, or a caller-requested Attempt. An implementer must either leave valid work permanently `PENDING`, recreate domain logic ad hoc in each caller, or trust an unverified caller-supplied aggregate/identity that can link changed content to stale paid work. P0-1's complete-recipe proof and P1-9's lifecycle wait are therefore not implementable from the stated public contracts.
- **Required plan change:** Define one explicit target-registration and lookup contract keyed by immutable persisted target identity. It must register the complete workflow, `args_for`, `execution_for`, and `recipe_for` behavior in every process that may reconcile; reject missing or conflicting registrations; pass or resolve that target in `reconcile`, `wait_operation`, `request_next_attempt`, inspection-triggered reconciliation, and export-triggered reconciliation; and recompute/compare every concrete Item recipe digest plus the ordered Operation aggregate before Registration completion. Add restart tests where a new process resumes an expired Claim and creates/enqueues both automatic and requested Attempts using the registered target.

## F2. Experiment acceptance cannot represent missing Generation cells and is not invalidated by changes to its pinned platform cut

- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §2.4, ADR 0018, ADR 0020, and owner decision 4
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md:1358` makes `generation_run_id` part of the `experiment_acceptance_members` primary key, although strict acceptance at `:1323` must materialize every missing/rejected Prediction and `:1361` makes only `score_attempt_id` nullable. PostgreSQL primary-key columns cannot be null, so an expected Prediction with no Generation Run has no legal membership row and cannot be reproduced as an exact missing matrix cell. Separately, evaluations pin `platform_cut_digest` and exact Operation/Item/Attempt references at `:1347`–`:1365`, and ADR 0018 says platform terminal success remains necessary (`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0018-strict-experiment-acceptance.md:5`), but the invalidation list at `plan.md:1367` covers only domain rows, accepted Manifest relationships, and profile changes. It does not bump `acceptance_source_version` when `request_next_attempt` reactivates an Operation (`plan.md:703`), cancellation changes an Attempt/Operation, or reconciliation changes the pinned platform outcome. Readers use only the pointer at `:1379`, so they do not revalidate the platform cut.
- **Consequence:** A biased or wholly missing generation stratum cannot be represented by the supposedly immutable member matrix, and an Experiment can remain visibly `STRICT`-accepted after its required Operation is reactivated or cancelled. That is silent stale publication at the exact boundary ADR 0020 was added to close.
- **Required plan change:** Key expected membership by identities that exist before outcomes (Prediction plus required profile/parser/dataset axes), with nullable selected Generation Run and Score Attempt foreign keys and explicit missing/rejected dispositions; alternatively use separate expected-generation and expected-scoring member tables. Define the accepted Manifest relationship storage and final-registration transaction. Then make every relevant platform mutation invalidate all affected Experiments in the same application transaction, or replace pointer-only currentness with an atomic checked platform-cut version that readers verify. Add tests for no Generation Run, no Score Attempt, next-Attempt reactivation, cancellation after acceptance, and reconciliation changes between evaluation insertion and pointer promotion.

## F3. The accepted cancellation overlap still loses the older call's cost at the current Whetstone/DBOS step boundary

- **Severity:** major
- **Class:** architecture-changing
- **Plan contract:** §1.5, §4.3, ADR 0019, and owner decision 3
- **Evidence:** V3 promises that overlapping calls are accounted at `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md:604`–`:613` and requires the forced-overlap test to retain both observable costs at `:1619`–`:1624`. In current Whetstone, the synchronous paid call returns inside `execute_lm_node_step` (`/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:343`–`:394`), but durable `NodeAttempt`/cost persistence is a later DBOS step invoked at `:190` and implemented at `:466`. Installed DBOS checks the workflow status before starting the next operation and raises immediately for `CANCELLED` (`/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:2499`–`:2506`). Thus a provider response that returns after logical cancellation may exist only in DBOS's serialized step output; the Whetstone persistence step never runs, while v3 excludes raw DBOS outputs from normal inspection/export.
- **Consequence:** The owner-accepted replacement can overlap and charge twice, but the older completed call can be absent from Whetstone's durable token/provider-cost source and every standard export. Cost reconciliation can undercount exactly the duplicate spend the owner required the system to label and account.
- **Required plan change:** Add a durable provider-call/Node-Attempt accounting boundary that commits the observable response usage/cost before the synchronous DBOS step returns, independently of later workflow continuation, and define idempotency plus crash behavior for provider-success-before-ledger-commit. If that remaining external-call/database gap cannot be closed for a provider, narrow the invariant and cost gate explicitly rather than claiming both observable calls are retained. The forced-cancellation test must assert the durable Whetstone ledger and exported totals, not a mock call counter or DBOS replay output.

## F4. DBOS 2.26 public step inspection cannot satisfy the plan's no-payload-load promise

- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5, §1.8, ADR 0011, and v1 P1-10
- **Evidence:** V3 promises safe workflow and step timelines at `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v3/plan.md:1109`–`:1116`, forbids normal payload loading at `:859`–`:866`, and requires the CLI to use DBOSClient public APIs rather than DBOS SQL at `:1129`. Installed `DBOSClient.list_workflow_steps` exposes only `limit` and `offset` and forwards directly to the system database (`/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_client.py:1068`–`:1075`). The underlying method defaults `load_output=True`, selects serialized output/error columns, and deserializes them (`/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:1836`–`:1874`). Unlike `list_workflows`, the public client method does not expose `load_output=False`.
- **Consequence:** A conforming inspector cannot provide the promised step timeline through public DBOSClient APIs without loading provider outputs/errors into process memory. Discarding them after deserialization does not satisfy the security boundary and can expose large or sensitive payloads to error handling, tracing, or memory diagnostics.
- **Required plan change:** For DBOS 2.26, either omit step timelines from the standard inspector, pin a DBOS patch that exposes `load_output=False`, or explicitly reuse the reviewed version-specific allowlisted system-schema adapter already permitted for telemetry. Whichever choice is made, contract-test that no input/output/error deserialization occurs; do not leave this as a live gate because the installed signature already disproves the public-API path.

## V2 strict-inclusive closure

| V2 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 complete execution recipe and exact domain equality | no | Exact canonical domain equality is specified, but no `recipe_for`/ordered-leaf verification or restart-safe complete target registry exists; see F1. |
| P0-2 implementable scheduling contract | yes | §1.4 and owner decision 5 consistently narrow the invariant to deterministic kernel rank/claim/page mixing and explicitly permit DBOS same-millisecond tie variance; installed 2.26 queue priority/configuration APIs support the stated pre-enqueue contract. |
| P0-3 cancellation semantics under accepted paid overlap | no | Logical cancellation and reference topology are coherent, but the current paid-step/persistence boundary drops the older observable cost after DBOS cancellation; see F3. |
| P0-4 consumer-visible Publication Bundles and cross-family skew | yes | §1.6/§4.1 define atomic Analysis and Detail pointers, one kernel destination transaction, bundle-token cleanup, and explicit `TOLERATE_SKEW`/`REQUIRE_COMPATIBLE_SNAPSHOT` policies. Live store behavior remains a P2 gate. |
| P0-5 append-only Experiment acceptance and current pointer | no | The member key cannot represent missing Generation cells and platform-cut mutations do not invalidate the current pointer; see F2. |
| P1-6 successive scoring selections | yes | `selection_digest` enters the default Scoring Operation key and acceptance spans a sorted non-empty set of scoring Manifest digests. |
| P1-7 foreign-cancelled shared execution | yes | Reconciliation records sticky `CANCELLED` with foreign Operation/request provenance and requires a new local operator confirmation. |
| P1-8 total Operation status | yes | §1.5 explicitly places confirmed enqueue plus `NOT_STARTED` in `RUNNING`, includes permanent enqueue failure terminality, and specifies a first-match precedence. |
| P1-9 lifecycle wait and COPRO export/read loop | no | The wait/export/pinned-read sequence is specified, but `wait_operation` cannot perform retry/enqueue reconciliation after restart without the missing target registry in F1. |
| P1-10 abandoned partial Registration | yes | `abandon_registration` has Lease-expiry, completion-race, confirmation, provenance-retention, and no-enqueue predicates. |
| P1-11 late DBOS terminal result after cancellation intent | yes | Prior DBOS `SUCCESS`/`ERROR` wins, cancellation disposition remains separate, and committed local terminality is immutable. |
| P1-12 requested versus effective priority | yes | Attempt persistence and inspection distinguish requested Service Class from linked execution priority/source. |
| P1-13 request-ledger maximum bound | yes | The request bound only tightens immutable RetryPolicy, both requested/effective values persist, and creation uses their minimum. |
| P2 retained live verification boundaries | no | MotherDuck, Neon, DuckDB, Vercel, OTLP, rescore, and COPRO live gates are retained, but DBOS step inspection is a known API mismatch rather than an unverified gate; see F4. |

## V1 disposition closure

| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 caller-requested next Attempt | no | The reason/source matrix, CAS, ledger, and bounds are sound, but the public request/reconcile lifecycle cannot resolve the complete target needed to derive/enqueue the new Attempt after restart; see F1. |
| P0-2 immutable registration Manifest | no | Membership/Lease/cursor/completion rules are closed, but the Manifest and target API do not let the kernel verify every concrete execution recipe against the ordered aggregate; see F1. |
| P0-3 destination fencing | yes | Destination-local Lease/token renewal, stale-promotion CAS, stage ownership, local OS lock, and H1/H2 rejection are specified for every sink; live behavior remains gated. |
| P0-4 complete cancellation topology | yes | Managed workflows are top-level-only, reference creation/cancellation share advisory locks, child discovery fails closed, and `cancel_children=False` is mandatory. F3 concerns cost accounting under the separately accepted overlap decision, not descendant safety. |
| P0-5 Experiment acceptance | no | Strict/stratified policy exists, but the durable membership/currentness model still permits unrepresentable missing cells and stale platform cuts; see F2. |
| P1-6 Operation serialization | yes | Every relevant mutation takes the Operation lock and recomputes aggregates in the same transaction with a global multi-Operation order. |
| P1-7 execution-scoped DBOS attributes | yes | Attributes contain immutable execution facts while authoritative platform rows retain the many-Operation reference set. |
| P1-8 total status precedence | yes | The precedence now covers registration, cancellation, confirmed enqueue, retry eligibility, and all terminal mixtures. |
| P1-9 Detail Attempt snapshot | yes | `detail_platform_attempts` is built in the Whetstone source snapshot and promoted with the complete root-cascaded Detail Bundle. |
| P1-10 secret-free DBOS payloads and safe reads | no | Workflow arguments are made secret-free and workflow listing can disable payloads, but the required public step-timeline API always deserializes outputs/errors; see F4. |
| P1-11 writer-lock ownership | yes | Kernel-owned mutation functions acquire the shared Export Barrier lock internally and the plan names static plus workflow-step enforcement. |
| P2-12 live verification boundaries | yes | The exact remote-store, Vercel, OTLP, rescore, and COPRO gates remain explicit; owner decision 5 consistently removes exact DBOS final tie ordering while preserving deterministic kernel mixing. |

## Owner-decision consistency

| V3 owner decision | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| 1. Separate complete execution-recipe identity | no | The chosen identity direction is consistent across glossary/ADR/plan, but the public target has neither a recipe producer/verifier nor a restart-safe complete target lookup; see F1. |
| 2. Atomic promised Publication Bundles with explicit independent-family skew | yes | ADR 0008/0015, §1.6, §4.1, and the Analysis/Detail reader rules agree on bundle boundaries and skew policy. |
| 3. Logical cancellation with accepted, accounted paid overlap | no | The logical/overlap choice is consistent, but the current DBOS/Whetstone step boundary prevents the older returned cost from reaching durable Whetstone records; see F3. |
| 4. Append-only acceptance with source-version current pointer | no | Append-only evaluation intent is consistent, but missing Generation membership and platform-cut invalidation make the pointer incomplete; see F2. |
| 5. Deterministic kernel mixing without deterministic final DBOS ties | yes | Plan, glossary, ADR 0007, installed DBOS ordering, and gates consistently preserve pre-enqueue mixing while permitting same-priority/same-millisecond final variance. |

## Verdict

- **Gate:** REPEAT_CONVERGENCE
- **Reason:** Two blocker findings and three open v2 P0/owner decisions remain. The plan needs a restart-safe execution-target/recipe contract, a representable and platform-invalidated acceptance model, and a durable accounting boundary for accepted cancellation overlap. The DBOS step-inspection mismatch is bounded but must also be corrected before implementation.
- **Unverified:** Live MotherDuck conditional Lease/bundle promotion and DuckDB-SQL parity, Neon transaction/pooling behavior, local DuckDB OS-lock crash behavior, Vercel Node/native exclusion and server-only secrets, OTLP degradation/safety, replacement rescore parity, and COPRO/zero-spend end-to-end behavior could not be exercised because the adapters/schemas are not implemented and live credentials/deployments were not part of this review. These remain blocking gates without weakening their stated invariants.
