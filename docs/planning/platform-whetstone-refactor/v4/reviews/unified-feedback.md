# V4 convergence review — unified feedback

## Review basis and synthesis method

- **Frozen packet baseline:** review baseline SHA-256 `92a3c74c67550275bb87baf16d1fab9bf093a86a0b9f4e4a92b9d778549f470f`; plan manifest SHA-256 `9a35e26c089aa93ebc23c9955c98051bfea209c7f16d823fea28fe48d507989f`.
- **Sources:** `v4/reviews/codex-findings.md`, `v4/reviews/fable-findings.md`, and `v3/reviews/unified-feedback.md` for prior closure and provenance.
- **Independence limitation:** each reviewer performed its fresh and closure phases in one model context. Fable stabilized its fresh findings before opening traceability or prior reviews; Codex records only the shared-context limitation.
- **Method:** strict-inclusive synthesis. Every source finding has exactly one disposition below. True duplicates retain source attribution; disagreements are not resolved by editorial merging.

## Source-finding dispositions

| Source finding | Disposition | Unified target or evidence |
| --- | --- | --- |
| Codex F1 — multiple accepted Generation Operations make highest Attempt ordinal non-total and contradict the singular generation Manifest cut | **accepted** | **A1**. The frozen contract permits several accepted generation relationships, persists one generation Manifest digest, and defines Attempt ordinal only within an Operation-scoped Item. |
| Codex F2 — no deterministic Score Attempt selection across required Scoring Operations | **accepted** | **OD2**. Multiple scoring Operations may contribute the same logical cell, but one selected Score Attempt is persisted without a total selection or rejection rule. |
| Codex F3 — current rescore parity includes `PARTIAL` while the target selects only `SUCCESS` | **accepted** | **OD3**. The deletion gate and target selection contract cannot both hold for the frozen current-behavior fixture. |
| Fable F1 — one generation Manifest per evaluation versus several accepted generation relationships | **duplicate — A1** | A1 includes the same singular/plural persistence defect and additionally retains Codex's cross-Operation ordinal evidence. Fable's Experiment-growth scenario remains part of A1. |
| Fable F2 — claimant death after enqueue and before losing outcome CAS leaves a live workflow behind `NOT_ENQUEUED` | **accepted** | **L1**. The surviving-claimant compensation path does not cover this crash window, and no specified health or replay detector repairs it. |
| Fable F3 — acceptance invalidation assumes experiment-scoped Prediction identity without pinning it | **accepted** | **L2**. The current identity supplies one owning Experiment, but the normative simplification allowance does not preserve that input explicitly. |
| Fable F4 — non-empty scoring-digest set prevents a pre-scoring evaluation | **accepted** | **OD4**. Missing-score rows are representable, but the evaluation itself cannot exist before any scoring relationship. |
| Fable F5 — garbled `classify_error` sentence leaves the retained seam ambiguous | **accepted** | **L3**. The sentence and stale-symbol gate do not determine whether the named function survives. |

No source finding is rejected or left unverified. The separate verification gaps in this document are implementation gates, not unsupported review findings.

## Architecture-changing finding

### A1. Define one coherent generation-membership and accepted-run contract

The relationship schema accepts every `(experiment_name, workflow_role, operation_key, manifest_digest)` tuple, so an Experiment may legally accumulate Generation Operations and Manifests. The evaluation schema and `acceptance_id`, however, persist one `generation_manifest_digest`. At the same time, the selected Generation Run is the highest successful platform Attempt ordinal, but platform ordinals are total only within an Item, and Item identity includes the Operation key. Two accepted Generation Operations can therefore each produce ordinal `0` for the same Prediction while carrying different recipe digests.

This fails in two concrete scenarios retained from the source reviews:

1. extending an Experiment requires G2/M2 because exact resubmission of G1/M1 with changed membership is a hard conflict; the evaluator cannot represent the full accepted generation relationship set; and
2. G1 and G2 each succeed for the same Prediction at equal Item-local ordinals; the stated highest-ordinal rule cannot select one run.

**Required owner decision OD1:** choose one coherent invariant:

- allow exactly one accepted Generation Operation/Manifest per Experiment, enforce uniqueness and reject a second unequal relationship with a typed disposition; or
- support plural generation Manifests, pin their sorted Operation/Manifest set in evaluation identity, define the expected Prediction set, and define a deterministic accepted-run order across Operations/Items, including equal ordinals and differing recipe digests.

Either choice changes persistence and the Experiment contract. Candidate/member rows must persist the selected Operation, Item, Attempt, and supersession provenance unambiguously. The gate suite must cover Experiment growth and two accepted Generation Operations producing successes for one Prediction.

**Sources:** Codex F1; Fable F1 (duplicate).

## Owner decisions

### OD1. Generation membership and accepted-run ordering

Resolve A1 before revising dependent acceptance identity, schema, ADR, glossary, and gate text. The one-generation choice fixes membership at first registration; the plural choice supports growth but requires set-valued identity and a cross-Operation ordering rule.

### OD2. Deterministic Score Attempt selection across Scoring Operations

Selection-distinct Scoring Operations can legally produce multiple successful Score Attempts for the same Prediction/profile/parser/dataset cell. The acceptance member key collapses those candidates into one row and persists one `selected_score_attempt_id`, but no rule chooses by ordinal, recipe, Manifest precedence, or conflict.

Choose either:

- a deterministic total policy over successful candidates, with candidate/supersession provenance and all selection inputs included in `acceptance_id`; or
- a prohibition on overlapping logical cells across accepted Scoring Manifests, enforced by database constraint and evaluation-time conflict.

Gate success-success, failure-success, equal-ordinal/different-recipe, and concurrent-selection cases.

**Source:** Codex F2.

### OD3. Whether scoring selection retains `PARTIAL` Generation Runs

Current default rescore selection includes `SUCCESS` and populated `PARTIAL` Generation Runs, while v4 freezes scoring selection to `SUCCESS`. The replacement therefore cannot match the current candidate identities required by the deletion gate.

Choose either:

- retain `PARTIAL` for scoring selection while separately defining whether it can satisfy strict Generation acceptance; or
- intentionally remove `PARTIAL` support and narrow the parity gate to the retained `SUCCESS` subset, recording the behavior break explicitly.

**Source:** Codex F3.

### OD4. Evaluation semantics before the first Scoring Operation

The member schema can represent `MISSING_SCORE`, but the evaluation identity requires a non-empty scoring Manifest set. Choose either:

- allow an empty canonical scoring-digest set and persist a durable `PARTIAL` evaluation; or
- define evaluation only after the first accepted scoring relationship and specify a distinct experiment-command result before then.

**Source:** Fable F4.

## Local corrections

### L1. Detect and compensate claimant death after enqueue but before outcome CAS

After cancellation records terminal `NOT_ENQUEUED`, an invalidated claimant can enqueue outside the application transaction and then die before its losing CAS writes compensation. The terminal Attempt is immutable, the missing-workflow path applies to nonterminal Attempts, and no compensation row exists for health checks to find.

Add a bounded reconciliation rule: cancellation replay and/or health reconciliation must check DBOS existence for `NOT_ENQUEUED`-finalized Attempts, insert or replay the compensation ledger row when a workflow appears, and issue idempotent cancellation under the same ledger key. Add claimant-death-after-enqueue-before-CAS to the crash matrix.

**Source:** Fable F2.

### L2. Pin experiment-scoped Prediction identity for this cut

State that `experiment_name` remains an input to `prediction_id`, and include that invariant in the golden-digest gate, so every Generation Run and Score Attempt invalidates exactly one Experiment. Cross-Experiment sharing would instead require a relevance query plus ascending multi-Experiment lock order and is an architecture change outside this local correction.

**Source:** Fable F3.

### L3. Repair the normative `classify_error` sentence

Name the retained `enqueue_failure_from_whetstone_exception` function, or its explicit successor, as the injected `classify_error` implementation that maps `dr_providers.FailureClass` into the kernel-owned enum. Add the chosen symbol to the relevant retention or stale-symbol gate.

**Source:** Fable F5.

## Synthesis opinions

### Synthesis opinion — source classifications versus the owner-question policy

Codex classifies the `PARTIAL` scoring mismatch as a local correction, and Fable classifies pre-scoring evaluation semantics as a local correction. Each offers alternatives that change externally visible scoring or Experiment-evaluation behavior. Under the question policy, those choices are owner decisions; therefore they are surfaced as OD3 and OD4 rather than silently selected during editing.

### Synthesis opinion — generation-selection closure

Fable says v4 owner decision 2 is closed because highest successful ordinal is consistently stated and unique per Item. Codex says it is reopened because the schema permits several generation Operations, hence several Items with equal ordinals. Codex's counterexample governs the general schema-permitted case: wording consistency within one Item does not make the selection total across permitted Items. The selected highest-ordinal direction remains recorded, but it is not implementable until OD1 constrains membership or supplies a cross-Operation order.

## Reviewer disagreements

| Topic | Codex position | Fable position | Preserved synthesis treatment |
| --- | --- | --- | --- |
| Generation membership and accepted-run selection | Blocker; singular generation digest contradicts plural accepted relationships, and Item-local ordinals tie across Operations. | Major owner decision for singular versus plural Manifests; says prior highest-ordinal decision itself remains closed. | A1 accepts the shared persistence defect and preserves the disagreement about closure. OD1 must resolve both membership and, if plural, total ordering. |
| V3 P0-2 acceptance representability | Not closed because A1 makes the accepted relationship set unrepresentable. | Previously repaired missing-cell/currentness design is closed, with F1 and F4 described as new adjacent defects. | The v3 missing-cell and platform-cut corrections are preserved, but the broader representability objective is reopened by A1 and OD4. |
| V3 P0-4 accepted Generation Run | Not closed because highest ordinal is only Item-total. | Closed, subject only to pinning experiment-scoped Prediction identity. | Reopened for schema-permitted multiple Generation Operations; L2 remains independently required. |
| V2 P0-5 / P1-6 and v1 P0-5 | Reopened by nondeterministic generation and score candidate selection. | P0-5 closed with new F1/F4 defects; P1-6 closed because scoring selections are distinct Operations. | Acceptance is reopened: distinct scoring Operations do not determine which successful Score Attempt populates a collapsed logical cell. |
| Rescore parity gate | Contradictory and impossible as written because current behavior includes `PARTIAL`. | Marks the retained rescore parity boundary correctly specified but unverified; does not report the status mismatch. | OD3 retains Codex's direct live-code/contract contradiction; the eventual corrected parity run remains a verification gate. |
| Overall gate | `REPEAT_CONVERGENCE`. | `REPEAT_CONVERGENCE`. | Agreement on the gate, although Codex cites a blocker plus two additional findings and Fable cites F1's unresolved persistence/owner choice. |

## Prior closure and provenance

### V3 strict-inclusive closure

| V3 item | Unified v4 status | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 restart-safe execution target and opaque recipe resolution | closed in the frozen design | Both reviewers find the persisted target ref, resolver, opaque envelope, concrete leaf recomputation, fresh-process tests, and fail-closed behavior explicit. Runtime proof remains a gate. |
| P0-2 representable, platform-current Experiment acceptance | reopened in part | Missing cells and atomically checked platform-cut currentness are preserved, but A1 cannot represent the permitted generation relationship set and OD4 cannot persist a pre-scoring evaluation. |
| P0-3 owner-selected overlap accounting | closed in the frozen design | Both reviewers preserve the owner-selected discarded-result undercount, provider receipts as external billing truth, and replay exclusion. |
| P0-4 accepted Generation Run selection | reopened | The stated highest Item-local ordinal is not total across several accepted Generation Operations; see A1 and the preserved disagreement. |
| P1-5 cancellation-safe claim/enqueue compensation | reopened only for a residual crash window | The normal losing-claimant compensation path is retained; L1 covers claimant death after enqueue and before the outcome CAS. |
| P1-6 payload-safe DBOS step inspection | closed in the frozen design | Both reviewers confirm the version-pinned payload-excluding system-schema adapter is the specified conforming path for DBOS 2.26.0. Runtime/schema-drift tests remain gates. |
| P1-7 authoritative Analysis Bundle inventory | closed | Both reviewers accept the six-member authoritative inventory and `score_attempts` COPRO source. |
| Preserved and extended P2 gates | open as verification gates | Fable finds the declared gates preserved; Codex identifies OD3's contradiction in the rescore parity gate, which must be corrected before that gate can pass. |

### Earlier closure carried forward

- V2 P0-1 through P0-4 and P1-7 through P1-13 remain closed in the frozen design, with their implementation proofs retained as gates.
- V2 P0-5 and P1-6 are reopened by A1 and OD2: multiple accepted generation/scoring Operations are representable as relationships but not deterministically reducible into the singular selected acceptance rows.
- V1 P0-1 through P0-4 and P1-6 through P1-11 remain closed in the frozen design except that L1 extends the cancellation crash matrix.
- V1 P0-5 is reopened by A1, OD2, and OD4.
- The retained v1/v2 P2 live-verification boundary remains open, and OD3 must first make its rescore parity sub-gate internally consistent.
- All three v4 owner decisions recorded before review remain textually applied: discarded-result cost undercount and platform-cut currentness remain closed; highest successful ordinal remains the selected direction but is reopened in enforceability by A1.

## Verification gaps retained as blocking implementation gates

These are plausible, explicitly planned boundaries that the read-only convergence reviews did not execute:

- live MotherDuck conditional Lease and fenced bundle promotion, including DuckDB-SQL parity;
- live Neon transaction and pooling behavior, including compute suspension;
- local DuckDB `fcntl.flock` and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret mapping;
- OTLP initialization, degradation, and safe attributes;
- not-yet-implemented export, acceptance, compensation, and payload-safe adapter schemas and transactions;
- same-millisecond multiple-dequeuer variance staying inside the accepted mixing bound;
- Experiment-row-lock load threshold;
- corrected Whetstone rescore-selection parity against frozen fixtures; and
- COPRO and zero-spend wait, export, and pinned-read execution.

The exact verification needed is the named live, crash, contract, concurrency, parity, load, or end-to-end test in the delivery gates. These gaps do not weaken A1 or OD2–OD4, which are contract contradictions rather than missing runtime evidence.

## Verdict

- **Gate:** REPEAT_CONVERGENCE

**Proposed gate: `REPEAT_CONVERGENCE`.**

This follows mechanically from the gate policy because:

- A1 is a blocker and architecture-changing finding;
- OD1–OD4 are unresolved owner decisions;
- A1 and OD2 change persistence and Experiment domain semantics across `dr-platform` and `whetstone-ai`; and
- the singular generation cut, cross-Operation ordinal rule, score selection, and rescore deletion gate contain contradictions that make core acceptance/cutover invariants infeasible as written.

`READY_FOR_FOCUSED_AUDITS` is unavailable until a successor resolves the owner decisions, applies L1–L3, proves every previous P0 closed with evidence, and passes another independent whole-system convergence review. Implementation remains blocked; the verification gaps stay as blocking gates after convergence.
