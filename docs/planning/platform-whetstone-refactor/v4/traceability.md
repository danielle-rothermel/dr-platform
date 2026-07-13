# V4 traceability and review provenance

**Role:** non-normative traceability. Fresh reviewers read the normative
documents declared in `plan-manifest.json` first. This document is used only
for closure and provenance. Where a disposition below describes runtime
behavior, the corresponding normative requirement lives in `plan.md` or one
of the four contracts.

## 4.8 V0 unified-feedback incorporation (priority order preserved)

| Priority | Unified item | V4 retained resolution |
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

## 4.9 V1 unified-feedback incorporation (priority order preserved)

| Priority | Unified item | V4 disposition |
| --- | --- | --- |
| P0-1 | Caller-requested next Attempt | The ledger/reason/CAS/bound contract is retained; runtime enforcement is completed by persisted target refs and the mandatory resolver used after restart by request/reconcile/wait. |
| P0-2 | Immutable registration Manifest | Membership mechanics remain; each leaf now includes the resolver-recomputed recipe digest and final Registration recomputes the ordered aggregate before completion. |
| P0-3 | Destination fencing | Retained destination Lease/token/OS-lock/H1-H2 fencing; promoted promised referential sets to atomic Publication Bundles with explicit cross-family skew policy. |
| P0-4 | Complete cancellation topology | Retained top-level-only reference locking and non-recursive DBOS cancellation; added foreign provenance, cancel-safe Claim invalidation, `NOT_ENQUEUED`, and late-enqueue compensation. Owner accepts overlap and possible accounting undercount. |
| P0-5 | Experiment acceptance | Retained strict/stratified policy; expected cells are representable before outcomes, accepted Manifest relationships have Whetstone ownership, and domain source plus checked platform cut jointly govern the pointer. |
| P1-6 | Operation serialization | Every membership/Item/Attempt/request mutation locks the Operation row and recomputes aggregates in the same transaction; fixed multi-Operation lock order and last-two-completions race test. |
| P1-7 | Execution-scoped DBOS attributes | Attributes contain immutable execution facts only; Operation references remain authoritative platform rows and DBOS reads follow workflow IDs. |
| P1-8 | Total status precedence | Added terminal abandoned Registration, confirmed-enqueue/`NOT_STARTED` RUNNING, permanent enqueue failure terminality, and exhaustive table tests. |
| P1-9 | Detail Attempt snapshot | Retained snapshot-built `detail_platform_attempts`; the full root-cascaded Detail set now promotes through one atomic pointer. |
| P1-10 | Secret-free DBOS payloads | Whetstone resolves credentials from process config, workflow reads disable payloads, and the standard step timeline uses an allowlisted DBOS-2.26 adapter that never selects or deserializes payload columns. |
| P1-11 | Writer-lock ownership | Every kernel function that owns a `change_seq` mutation acquires the shared barrier lock internally; workflow-step throttle and static direct-write tests enforce it. |
| P2-12 | Live verification boundaries | Live MotherDuck/Neon/DuckDB/Vercel/OTLP/rescore/COPRO gates remain blocking. Exact DBOS final tie order is intentionally not required; a fixture documents accepted variance while proving deterministic kernel mixing. |

## 4.10 V2 strict-inclusive convergence incorporation

| Priority | V2 synthesis finding | V4 disposition |
| --- | --- | --- |
| P0-1 | Complete execution recipe and exact domain equality | Selected direction remains; v4 completes runtime enforcement with opaque caller payload ownership, persisted target refs, registry resolution, concrete recipe leaves, and recomputed aggregate verification. |
| P0-2 | Implementable deterministic scheduling | Owner narrowed the contract to deterministic kernel rank/claim/enqueue mixing and accepted same-millisecond DBOS tie nondeterminism. DBOS 2.26.0 remains pinned; no fork or alternate scheduler. |
| P0-3 | Paid-call cancellation/quiescence | Owner rejected quiescence and accepts duplicate spend. Logical DBOS cancellation permits confirmed replacement; overlap is labeled and tested. V4 additionally accepts undercount for a discarded post-cancellation outcome and relies on provider receipts for total billing rather than adding a provider-call ledger. |
| P0-4 | Consumer-visible publication bundles | Analysis referential tables and the Detail root closure each use one atomic pointer; kernel tables/cursors use one transaction; unrelated families expose checked/tolerated `snapshot_seq` skew. |
| P0-5 | Durable Experiment acceptance | Append-only direction remains; v4 fixes missing-cell representation, Manifest relationship ownership, highest-success selection, and checked Operation-version currentness at promotion/read time. |
| P1-6 | Successive scoring selections | `selection_digest` enters the default Scoring Operation key; an Experiment may combine domain rows from multiple immutable Scoring Operations. |
| P1-7 | Foreign-cancelled shared execution | Local Attempt becomes sticky `CANCELLED` with foreign Operation/request provenance; a new local operator confirmation may cite it. |
| P1-8 | Total Operation status | Confirmed enqueue with `NOT_STARTED`, permanent enqueue errors, abandoned Registration, overlaps, and terminal mixtures have one explicit precedence. |
| P1-9 | Lifecycle wait and COPRO read loop | Added typed `wait_operation`; COPRO and zero-spend e2e explicitly wait, export, and read one pinned Analysis Bundle between iterations. |
| P1-10 | Abandoned partial Registration | Added confirmed `abandon_registration` after Lease expiry; committed platform/domain rows remain provenance and never enqueue. |
| P1-11 | Late DBOS terminal after cancel | Observed `SUCCESS`/`ERROR` wins if terminal before cancellation finalization; cancellation disposition and intent remain separate. Committed local terminality is immutable. |
| P1-12 | Requested versus effective priority | Attempts persist both values and their source; linked references inherit DBOS enqueue-time priority and health reports mismatches. |
| P1-13 | Request `max_attempts` | Optional request value may tighten but never expand immutable RetryPolicy; ledger persists requested/effective bounds and uses their minimum. |
| P2 | Live verification boundaries | Retained live MotherDuck conditional Lease/bundle promotion and SQL parity, Neon transactional behavior under pooling, DuckDB OS-lock/crash behavior, Vercel Node/native exclusion/secrets, OTLP degradation/safety, rescore parity, and COPRO/zero-spend end to end. |

## 4.11 V3 strict-inclusive convergence incorporation

| Priority | V3 synthesis finding | V4 disposition |
| --- | --- | --- |
| P0-1 | Restart-safe execution target and recipe resolution | **Direction selected and specified; runtime enforcement remains an implementation gate.** Persist immutable target ref/digest; require startup `TargetRegistry`; route every lifecycle driver through one resolver; keep the kernel recipe envelope minimal and caller payload opaque; recompute every leaf and ordered aggregate; prove fresh-process expired-Claim plus automatic/requested Attempt recovery. Codex called the lifecycle open; Claude called identity closed with a recipe-ownership ambiguity; v4 merges both corrections. |
| P0-2 | Representable, platform-current Experiment acceptance | **Direction selected and specified; runtime enforcement remains an implementation gate.** Separate generation/scoring/candidate member tables represent missing outcomes; Whetstone owns accepted Manifest relationships; sorted Operation-version cuts are checked at promotion and every current read. Codex called this a blocker; Claude called persistence closed; v4 follows the synthesis that the append-only direction is closed but enforcement was not. |
| P0-3 | Durable accounting for accepted overlap | **Resolved by owner policy, intentionally without complete accounting.** A discarded post-cancellation result may be absent from Whetstone/export totals; provider receipts remain total-billing evidence; no provider-call ledger or DBOS replay accounting is added. Codex required complete accounting, Claude treated overlap accounting as closed, and the synthesis recommended the ledger; the owner selected accepted undercount to avoid complexity for unused outcomes. |
| P0-4 | Accepted Generation Run selection | **Resolved by owner policy.** Highest successful platform Attempt ordinal at the pinned cut is selected; earlier successes are superseded provenance and do not require score cells. Codex did not report it; Claude called it bounded; the synthesis made it owner-visible. |
| P1-5 | Prevent enqueue after logical cancellation | Claim eligibility excludes cancellation; intent invalidates Claims; no-row finalization is `NOT_ENQUEUED`; a claimant losing its outcome CAS performs idempotent DBOS cancellation and records append-only compensation. |
| P1-6 | Payload-safe DBOS step inspection | DBOSClient workflow reads disable payloads; standard step timelines use the version-pinned allowlisted system-schema adapter and contract-test that payload columns are never selected or deserialized. |
| P1-7 | One Analysis Bundle inventory | §4.1 is authoritative: `experiments`, `predictions`, `generation_runs`, `score_attempts`, `sweep_metrics`, and `failure_metrics`; COPRO reads `score_attempts`; Node Attempts remain Detail-only. |
| P2 | Preserved and extended verification gates | All v3 live gates remain blocking; added fresh-process target recovery, missing acceptance cells, platform-cut races, multiple successful runs, cancellation compensation, payload-safe inspection, and the selected outcome-linked accounting contract. Experiment-row-lock load remains a performance gate, not a correctness finding. |

Reviewer disagreements remain visible in the v3 row dispositions and owner
decisions below. The overall gate remains another independent whole-system
convergence review; no table marks runtime behavior closed merely because v4
selects and specifies an architecture.

## Owner decisions resolved for v3

1. Keep Prediction ID as Whetstone domain identity and bind execution through
   a separate versioned concrete `execution_recipe_digest` covering the full
   Item domain input and every immutable execution-affecting version. Persist
   concrete digests on Attempts and an ordered aggregate digest on the
   Operation, include the concrete digest in content-scoped workflow identity,
   and require exact canonical domain equality for `ALREADY_PRESENT`.
2. Publish the mutually referential Whetstone Analysis tables through one
   atomic pointer, the Detail root manifest and all root-cascaded tables through
   one atomic root-bundle pointer, and all kernel tables plus cursor bookkeeping
   in one destination transaction. Keep unrelated kernel, DBOS telemetry,
   Whetstone projection, and Detail families independently timed only when
   readers explicitly tolerate or check their `snapshot_seq` skew.
3. Treat DBOS cancellation of synchronous paid steps as logical and allow a
   confirmed replacement Attempt immediately after DBOS/local `CANCELLED`,
   without an upstream-provider quiescence fence. The owner explicitly accepts
   overlapping provider calls and duplicate spend. Codex considered
   no-overlap cost control an architecture blocker; Fable considered the
   reference topology closed and the remaining states local; the synthesis
   recommended logical cancellation plus quiescence. The owner rejected that
   recommendation in favor of a smaller lifecycle contract that labels
   overlap but does not prevent it. V4 further narrows the accounting promise
   as recorded below.
4. Persist Experiment acceptance as append-only evaluations plus immutable
   expected/observed membership rows. Pin exact generation and all contributing
   scoring Manifest digests, domain and Operation/Attempt cuts, policy version,
   required profiles, matrix, and override facts. Relevant later outcomes bump
   the Experiment's source version and clear its pointer; evaluation advances
   one current pointer only by source-version CAS, leaving every older result
   reproducible and historical.
5. Require deterministic kernel `shuffle_rank`, claim order, enqueue order,
   and bounded model mixing, but not identical final DBOS dequeue/start order.
   Same-priority workflows sharing a millisecond `created_at` may reorder
   nondeterministically under DBOS 2.26.0 and multiple dequeuers. Codex treated
   the absent third key as an architecture failure; Fable left it as an
   unverified gate; the synthesis recommended a patched DBOS or another
   scheduler. The owner narrowed the product requirement and accepted tie-local
   nondeterminism, so v3 retains pinned DBOS 2.26.0 without a fork.

## Owner decisions resolved for v4

1. Accept that paid-call overlap after logical cancellation may undercount
   Whetstone/export spend when the older provider result is discarded. Cost
   accounting exists to associate price with persisted outcomes; no separate
   in-step provider-call ledger is added solely for an unused outcome.
   Provider-side receipts remain the external billing record, and DBOS replay
   payloads remain excluded from accounting truth. Codex required a durable
   ledger for complete accounting; Claude treated overlap accounting as
   closed; the v3 synthesis recommended the ledger. The owner instead selected
   the lower-complexity undercount contract for this cut.
2. When one Prediction has multiple successful Generation Runs at the pinned
   evaluation cut, accept the run with the highest platform Attempt ordinal.
   Earlier successes remain superseded provenance and do not require scoring.
   Codex did not report this ambiguity; Claude treated it as a bounded rule;
   the v3 synthesis made it owner-visible because it controls strict
   completeness and paid scoring scope. Both Claude and the synthesis
   recommended the selected rule.
3. Prove acceptance platform currentness with checked Operation
   `platform_cut_version` values. Each evaluation pins a sorted Operation/
   version vector; pointer promotion and every current read compare it
   atomically, and a mismatch fails closed as historical. Codex treated stale
   platform cuts as a blocker; Claude considered the pointer architecture
   closed; the v3 synthesis found Codex's mutation scenario decisive and
   recommended this checked-version mechanism over domain invalidation. The
   selected rule keeps dr-platform domain-agnostic.

## Prior owner decisions retained from v2

1. One platform-owned caller-requested next-Attempt transition; no second
   Whetstone counter and no false platform failure.
2. Caller-prepared immutable Manifests; no platform durable spool.
3. No DBOS child workflows below managed executions; cancellation is always
   non-recursive.
4. Destination-local Lease/fencing for every artifact and destination,
   including an OS/process lock for local DuckDB.
5. Strict Experiment completeness by default; explicit persisted,
   stratified, operator-confirmed partial override only.

## Revision log

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
- v3 (2026-07-10): draft incorporating the strict-inclusive v2 convergence
  synthesis. Owner decision 1 keeps Prediction ID as domain identity and adds
  separate complete, versioned execution-recipe digests with exact domain-row
  equality. Owner decision 2 makes promised referential sets atomic
  Publication Bundles while retaining explicit checked/tolerated skew between
  independent families. Owner decision 3 accepts paid-call overlap after
  logical DBOS cancellation and removes physical-abort/no-overlap claims from
  the gate. Owner decision 4 adds append-only acceptance evaluations with
  exact cuts, source-version invalidation, and one atomic current pointer.
  Owner decision 5 keeps deterministic kernel mixing but accepts
  same-millisecond DBOS dequeue nondeterminism, removing the patch/fork from
  scope. All five v3 owner decisions are resolved.
- v3 review preparation (2026-07-10): Codex and Claude whole-system
  convergence prompts prepared; no findings or unified feedback exist, and
  lifecycle status remains `draft` until the prompts are issued.
- v3 review freeze (2026-07-10): plan and effort index changed to `in-review`
  without altering the reviewed contract; Codex and Claude prompts are issued.
- v4 (2026-07-11): draft incorporating the strict-inclusive v3 convergence
  synthesis. The owner accepts the complete v3 packet as historical truth
  despite its legacy review provenance and absent strict review baseline;
  no retrospective baseline is fabricated. Owner decision 1 accepts undercount
  for discarded post-cancellation provider outcomes, keeps provider receipts as
  the external billing record, and declines a separate provider-call ledger.
  Owner decision 2 accepts the successful Generation Run with the highest
  platform Attempt ordinal and retains earlier successes only as superseded
  provenance. Owner decision 3 pins and atomically checks Operation
  `platform_cut_version` values at acceptance promotion and read time.

## Normalization record

The authoritative pre-normalization source is the read-only snapshot
`/private/tmp/platform-whetstone-v4-monolith-before-normalization.md`, verified
before editing at SHA-256
`bfbf13016cca60cd29241072f27aa746e15db8776d87c0e84574d4be7dcc6a59`.
Normalization moved complete detailed sections to one authoritative contract
and replaced the monolithic entrypoint with a bounded lifecycle/navigation
document. It did not change the effort lifecycle or issue review artifacts.

No source heading was removed as a duplicate. Repeated high-level context in
the new entrypoint is deliberate navigation and does not replace the detailed
contract. No contradictory normative source requirements were identified;
there are therefore no preservation ambiguities recorded for later owner
resolution.

## Source coverage appendix

Every heading in the preserved monolith has exactly one primary destination.
`moved` means its complete contract or provenance moved to that destination;
`merged` would mean its content was combined into a broader destination;
`removed-duplicate` would mean an exact duplicate was deleted in favor of a
named authoritative copy. This normalization uses only `moved`.

| Source heading | Destination heading | Disposition | Reason |
| --- | --- | --- | --- |
| `# Platform Hard Cut — Joint Refactor Spec (v4)` | `plan.md` → `# Platform Hard Cut — Joint Refactor Spec (v4)` | moved | The title and packet identity remain at the normative entrypoint. |
| `## Mode and goals` | `plan.md` → `## Mode and goals` | moved | Status, hard-cut scope, and goals belong at the entrypoint. |
| `### Design principles (enforced throughout)` | `plan.md` → same heading | moved | Cross-packet principles constrain every contract. |
| `### Current-code re-audit (2026-07-11)` | `plan.md` → same heading | moved | The reviewed factual baseline is packet-wide context. |
| `### Unified invariants` | `plan.md` → same heading | moved | Cross-domain distinctions remain entrypoint invariants. |
| `## Part 1 — dr-platform (kernel)` | `contracts/platform.md` → same heading | moved | Platform owns the kernel contract. |
| `### 1.1 Deletions` | `contracts/platform.md` → same heading | moved | Platform deletion work stays with the kernel. |
| `### 1.2 Vocabulary and renames` | `contracts/platform.md` → same heading | moved | Kernel vocabulary is platform-owned. |
| `### 1.3 Schema (new single baseline)` | `contracts/platform.md` → same heading | moved | Kernel persistence belongs to platform. |
| `#### Schema and lifecycle crosswalk` | `contracts/platform.md` → same heading | moved | The complete field/lifecycle mapping stays beside the schema. |
| `#### Public/model/file crosswalk` | `contracts/platform.md` → same heading | moved | Public surface changes stay beside platform vocabulary. |
| `### 1.4 Scheduling: service class plus deterministic shuffle` | `contracts/platform.md` → same heading | moved | Scheduling is a kernel responsibility. |
| `### 1.5 Submission flow (the one way in)` | `contracts/platform.md` → same heading | moved | Registration, enqueue, retry, and cancellation are kernel lifecycle. |
| `#### Caller-requested next Attempt` | `contracts/platform.md` → same heading | moved | dr-platform owns Attempt allocation and its request ledger. |
| `#### Attempt state machines` | `contracts/platform.md` → same heading | moved | Attempt transitions are authoritative platform behavior. |
| `#### Operation aggregation` | `contracts/platform.md` → same heading | moved | Aggregate state and lock order are platform behavior. |
| `#### DBOS call and correlation contract` | `contracts/platform.md` → same heading | moved | DBOS interaction is the platform's internal execution seam. |
| `### 1.6 Export flow (Analysis Store + Detail Store)` | `contracts/publication.md` → `## 1.6 Export flow (Analysis Store + Detail Store)` | moved | Export and destination failure behavior have one publication home. |
| `### 1.7 Pacing (worker flow)` | `contracts/platform.md` → same heading | moved | Throttle state and durable pacing are kernel-owned. |
| `### 1.8 Ownership, inspection, control, and telemetry` | `contracts/platform.md` → same heading | moved | The platform external seam and safe operational surfaces stay together. |
| `### 1.9 Hygiene and structure` | `contracts/platform.md` → same heading | moved | Platform module/API deletion work stays with its contract. |
| `## Part 2 — whetstone-ai (lockstep overhaul)` | `contracts/whetstone.md` → same heading | moved | Whetstone domain behavior has one normative home. |
| `### 2.1 Deletions` | `contracts/whetstone.md` → same heading | moved | Whetstone deletion work stays with its owner. |
| `### 2.2 Renames (frozen-string thaw)` | `contracts/whetstone.md` → same heading | moved | Durable Whetstone names are domain-owned. |
| `### 2.3 Identity` | `contracts/whetstone.md` → same heading | moved | Prediction/run/score identity is Whetstone behavior. |
| `### 2.4 Platform boundary simplification` | `contracts/whetstone.md` → same heading | moved | Generation/scoring adapters and acceptance behavior meet here. |
| `#### Experiment-acceptance schema and transaction` | `contracts/whetstone.md` → same heading | moved | Whetstone owns acceptance rows, cuts, and pointer transactions. |
| `### 2.5 Tests and docs` | `contracts/whetstone.md` → same heading | moved | Domain parity, COPRO, deletion, and documentation gates stay with Whetstone. |
| `## Part 3 — unitbench (two-plane swap)` | `contracts/publication.md` → same heading | moved | Unitbench is the consumer boundary of the two-plane publication contract. |
| `## Part 4 — Cross-cutting` | `contracts/delivery.md` → same heading | moved | Dependencies, secrets, old-data policy, and phase ordering govern delivery. |
| `### 4.1 Two-plane table inventory` | `contracts/publication.md` → `## 4.1 Two-plane table inventory` | moved | One authoritative publication inventory eliminates competing lists. |
| `### 4.2 Migration and cutover order` | `contracts/delivery.md` → same heading | moved | Ordered phase dependencies are delivery-owned. |
| `### 4.3 Transaction, concurrency, and crash verification` | `contracts/delivery.md` → same heading | moved | Cross-contract proof belongs to the delivery gate. |
| `### 4.4 Pre-experiment acceptance gates` | `contracts/delivery.md` → same heading | moved | Blocking live and integration gates govern cutover. |
| `### 4.5 Repository verification` | `contracts/delivery.md` → same heading | moved | Repository commands and stale-symbol searches are delivery proof. |
| `### 4.6 Rollback and clean-cut assumptions` | `contracts/delivery.md` → same heading | moved | Rollback behavior is part of cutover. |
| `### 4.7 Explicit post-experiment deferrals` | `contracts/delivery.md` → same heading | moved | Scope exclusions constrain delivery. |
| `### 4.8 V0 unified-feedback incorporation (priority order preserved)` | `traceability.md` → `## 4.8 V0 unified-feedback incorporation (priority order preserved)` | moved | Historical disposition is non-normative provenance. |
| `### 4.9 V1 unified-feedback incorporation (priority order preserved)` | `traceability.md` → `## 4.9 V1 unified-feedback incorporation (priority order preserved)` | moved | Historical disposition is non-normative provenance. |
| `### 4.10 V2 strict-inclusive convergence incorporation` | `traceability.md` → `## 4.10 V2 strict-inclusive convergence incorporation` | moved | Reviewer closure and disagreement history is traceability. |
| `### 4.11 V3 strict-inclusive convergence incorporation` | `traceability.md` → `## 4.11 V3 strict-inclusive convergence incorporation` | moved | Reviewer closure and disagreement history is traceability. |
| `### Owner decisions resolved for v3` | `traceability.md` → `## Owner decisions resolved for v3` | moved | Why the policies were selected is provenance; the selected policies remain normative elsewhere. |
| `### Owner decisions resolved for v4` | `traceability.md` → `## Owner decisions resolved for v4` | moved | Reviewer positions and owner rationale are provenance. |
| `### Prior owner decisions retained from v2` | `traceability.md` → `## Prior owner decisions retained from v2` | moved | Historical decision lineage is traceability. |
| `### Review protocol` | `plan.md` → `## Review protocol` | moved | Lifecycle and fresh-review read order belong at the entrypoint. |
| `### Revision log` | `traceability.md` → `## Revision log` | moved | Version history is non-normative provenance. |
