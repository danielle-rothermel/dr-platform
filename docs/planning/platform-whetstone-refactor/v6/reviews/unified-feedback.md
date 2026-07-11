# V6 convergence review — unified feedback

## Review basis and synthesis method

- **Frozen packet baseline:** review baseline SHA-256 `7ebdf0db177da1d91b184c24f7892329065cadd9c2a83d633a5efb98f1cf7deb`; plan manifest SHA-256 `8abc0504f0383e6b53a497c327778af992c1bcb3de12c1ee408309a4a39e4631`.
- **Sources:** `v6/reviews/codex-findings.md`, `v6/reviews/fable-findings.md`, and `v5/reviews/unified-feedback.md` for prior closure and provenance.
- **Independence limitation:** each reviewer performed its fresh and closure phases in one model context. Fable reports that F1-F3 were stabilized before its closure read and that F4 was added during closure; Codex records only the shared-context limitation.
- **Method:** strict-inclusive synthesis under the question-and-gate policy. Each of the seven source findings has exactly one disposition below. Reviewer gate fields are treated as inputs rather than authority.

## Source-finding dispositions

| Source finding | Disposition | Unified target or evidence |
| --- | --- | --- |
| Codex F1 — explicit-policy `PARTIAL` acceptance is not representable by the generation-member schema | **accepted** | **A1**. The settled strict-plus-explicit-partial policy permits a selected `PARTIAL` run, while the schema permits a selected run only under `SELECTED_SUCCESS`. The contract cannot persist the permitted outcome truthfully. |
| Codex F2 — `NO_WORKFLOW_FOUND` can resolve while an independently committed enqueue remains in flight | **accepted** | **A2**. Repeated absence is not proof of enqueue-call quiescence. A late DBOS commit can appear after the hazard is final, with no required successor observation or compensation path. |
| Codex F3 — application `snapshot_seq` equality cannot prove compatible application and DBOS source cuts | **accepted** | **A3 / V6-OD1**. Independently timed source captures can carry the same application-derived sequence while representing different moments. Destination fencing and per-bundle atomicity do not establish a shared cross-database cut. |
| Fable F1 — durable `NO_WORKFLOW_FOUND` can be falsified by a delayed enqueue commit | **duplicate** of **A2** | Fable supplies the same delayed-commit counterexample, claimant-success-after-resolution subcase, and missing-successor defect as Codex F2. Its evidence and local-correction classification remain attributed below; no evidence is discarded. |
| Fable F2 — the populated-only `PARTIAL` predicate has ctype-dependent SQL semantics and a different Python whitespace rule | **accepted** | **L1**. The packet promises one exact end-to-end boundary but does not pin SQL character-class semantics or align the retained Python guard. The exact live Postgres classification remains a verification item, but the specification mismatch is established. |
| Fable F3 — pinned Analysis Bundle reads lack a retention or typed failure contract | **accepted** | **V1**. Once a newer bundle is promoted, the stated cleanup protection no longer covers the older bundle pinned by COPRO. The required retention-or-failure rule and promotion/cleanup race proof are bounded. |
| Fable F4 — scoring-freeze wording implies one-run Manifest membership while parity requires plural-run membership | **accepted** | **L2**. This is a normative wording contradiction with different identity and paid-work consequences, even though the run-pinning gate prevents silent acceptance wrongness. |

Disposition count: six `accepted`, one `duplicate`, zero `rejected`, and zero `unverified` source findings. The source-level `unverified` notes are retained as verification gaps below rather than used to erase an established defect.

## Architecture changes

### A1. Represent policy-accepted `PARTIAL` generation membership truthfully

The explicit stratified-partial override is already a settled policy. Its accepted Generation Run must therefore be representable without calling a domain-`PARTIAL` run `SELECTED_SUCCESS`, omitting its selected run, or rejecting an outcome the policy permits.

Add a truthful selected-partial-policy state, or an equivalent status-bearing selected disposition, and bind it to the immutable policy authorization, observed `PARTIAL` status, selected Generation Run, exact Operation/Item/Attempt proof, platform `SUCCEEDED` and DBOS `SUCCESS`, cell pins, provenance, matrix, and `acceptance_id`. Gate both strict rejection and authorized partial promotion/current-read paths.

This changes the acceptance state schema and reopens the representability portion of v3 P0-2, v2 P0-5/P1-6, v1 P0-5, and the retained strict/stratified policy. It does not reopen terminal execution closure, run-pinned score reduction, or populated-only scoring eligibility.

**Source:** Codex F1. Fable reported the acceptance boundary converged and did not identify this schema contradiction.

### A2. Make call-started enqueue hazard resolution safe against a later DBOS commit

An enqueue that crossed `enqueue_call_started_at` executes in an independent DBOS transaction. A finite sequence of missing-row observations cannot prove that transaction is incapable of committing. Treating `NO_WORKFLOW_FOUND` as final can therefore clear the reference guard before paid work appears, after which claimant death or exact reload leaves no actor required to re-observe and compensate it.

The existing cancellation invariant mechanically requires a safe protocol. The successor must either establish enqueue-call quiescence before final resolution, install a durable DBOS-side guard, or retain/re-observe a durable unresolved or append-compatible successor state until a late commit is cancelled, marked `SKIPPED_SHARED`, or observed terminal under the established exclusivity locks. It must specify claimant behavior when enqueue success becomes known after `NO_WORKFLOW_FOUND` and gate a DBOS insert that commits after the current grace/count.

This changes compensation lifecycle semantics and reopens the safe-terminal-resolution portion of v4 L1, v3 P1-5, and v1 P0-4. Durable Claim identity and reference exclusivity remain closed.

**Sources:** Codex F2; Fable F1 is a duplicate of this target.

### A3. Give cross-source compatibility truthful cut semantics

The application bundle and DBOS telemetry are captured independently. Applying the application Postgres `snapshot_seq` to both does not make equality a same-source-cut proof; a DBOS transition between the two captures is sufficient to falsify that claim.

The correction must give each source a truthful cut coordinate. V6-OD1 must choose whether to preserve strong same-cut semantics through a real causal coordination/barrier or to weaken the reader contract so equality is not a same-cut proof and compatibility instead uses explicit source coordinates plus a declared temporal or causal tolerance. Gate a DBOS state transition between application-cut capture and telemetry capture.

This reopens v2 P0-4 and retained v3 owner decision 2 only for cross-family compatibility. Atomic promotion of each bundle and destination-local fencing remain closed.

**Source:** Codex F3. Fable reported publication and two-plane readers stable; that closure position is defeated by the independently timed-cut counterexample.

## Genuine owner decisions

### V6-OD1. Select the cross-source compatibility contract

The owner must choose between:

1. **Preserve same-cut semantics.** Add causal coordination that proves the application and DBOS captures belong to one compatible cut. This retains the strongest reader promise at higher implementation and operational cost.
2. **Declare bounded compatibility.** Persist truthful coordinates for both captures, prohibit `snapshot_seq` equality from meaning “same source cut,” and define the allowed temporal or causal tolerance. This weakens the consumer promise but matches independently timed capture.

This is a genuine architecture and consumer-contract choice. Existing invariants do not determine which compatibility strength the product needs.

No other source finding creates a domain owner question. A1 follows from the already accepted explicit-partial override; A2 follows from operator-confirmed cancellation and reference safety; L1 and L2 reconcile existing exactness and membership rules; V1 requires either bounded retention or a typed failure/restart contract, but does not change domain meaning.

Separately, the convergence assessment creates **PD1**, a process-level decision: automatic successor generation must remain stopped until the owner decides whether to redesign/bound the review method for the repeatedly reopening boundaries or explicitly authorize one manually bounded successor with exit criteria. PD1 is not a hidden product-policy choice.

## Mechanical and local corrections

### L1. Make populated-only `PARTIAL` selection one exact end-to-end predicate

Pin the fresh-database character semantics or replace the POSIX class with an explicit ctype-independent rule, then align or remove the retained Python `str.strip()` guard. Add at least one non-ASCII-whitespace-only fixture, such as U+00A0, across selection, identity, retry, outcome, and deletion parity.

This is a local execution correction to resolved V5-OD1, not a reopening of populated-only selection or a new preference question.

**Source:** Fable F2.

### L2. Separate plural scoring membership from accepted-run derivation

Rewrite the scoring-freeze sentence so the Manifest contains the populated-only narrowed candidate set, including plural eligible runs per Prediction, while freeze and evaluation separately derive the accepted run used for cell pinning. Preserve `SUPERSEDED_GENERATION` behavior when the derived runs differ across cuts.

This is a local normative correction. Core run-pinned totality remains closed.

**Source:** Fable F4.

## Verification gaps

### V1. Define and prove pinned-bundle survival or typed loss

Specify either a retention window/count that protects promoted non-current bundles pinned by readers, or typed `PINNED_BUNDLE_GONE` behavior with a defined restart-at-new-pin path. Gate promotion and cleanup during an active pinned COPRO iteration.

**Source:** Fable F3.

The following retained gaps are not additional source findings and do not substitute for A1-A3 or L1-L2:

- A1: persist an authorized `PARTIAL` run through promotion and current read, with strict-versus-override schema fixtures.
- A2: block DBOS `init_workflow`, resolve the current grace/count, commit afterward, then kill the claimant before return/outcome CAS; also exercise the claimant-success-after-resolution path.
- A3: transition DBOS state between application-cut capture and telemetry capture and prove the chosen compatibility rule.
- L1: run the exact predicate against the provisioned Postgres ctype and retained Python boundary, including non-ASCII whitespace.
- V1: race promotion and cleanup against a pinned iteration.
- The live MotherDuck, Neon, local DuckDB, Vercel, OTLP, DBOS allowlisted-schema, same-millisecond mixing, row-lock load, rescore-parity, COPRO, and zero-spend end-to-end gates remain unexecuted and must run on locked revisions.
- The deployed probability and duration of a stalled enqueue commit remain unmeasured, but that uncertainty does not invalidate A2's correctness counterexample.

## Reviewer disagreements

| Topic | Codex position | Fable position | Synthesis opinion |
| --- | --- | --- | --- |
| Explicit-policy `PARTIAL` acceptance | The schema cannot represent the expressly permitted selected `PARTIAL` state; architecture and prior acceptance closure reopen. | Acceptance is converged; its residuals are predicate exactness and wording only. | **Synthesis opinion:** Codex F1 is accepted. The direct schema/policy contradiction defeats the claimed full closure. |
| Delayed enqueue after `NO_WORKFLOW_FOUND` | Blocker, architecture-changing, and a reopening of cancellation-safe resolution. | Major local correction; v5 L3 remains closed and this is a new adjacent residual. | **Synthesis opinion:** the failure mechanism is shared and merged as A2. Because the disposition lifecycle and correctness of terminal hazard resolution change, the gate policy treats it as lifecycle/architecture-changing even though append-only Claims make the edit bounded. |
| Cross-family source cuts | Equal application-derived `snapshot_seq` values cannot prove a same cut across independently captured stores; a previously settled publication claim reopens. | Publication bundles and two-plane readers remain stable; its only publication finding is pinned-version retention. | **Synthesis opinion:** Codex F3 is accepted and distinct from Fable F3. Per-bundle atomicity remains closed, but the cross-family compatibility claim does not. |
| V5 cancellation closure | Fable v5 F2 remains open because absence cannot prove that a started enqueue will not later commit. | V5 F2 closes as written; the delayed commit is a new residual after that closure. | **Synthesis opinion:** provenance is best stated narrowly: durable Claim identity and shared-reference exclusivity close, while safe final resolution of a call-started hazard remains open. |
| Gate rationale | Three architecture findings require repeat convergence and show possible non-convergence. | One bounded lifecycle residual requires repeat convergence; all other findings are local or verification gaps. | **Synthesis opinion:** both reviewers propose `REPEAT_CONVERGENCE`, but Codex's accepted A1-A3 evidence changes the process conclusion from “one final bounded successor” to a stop for process-level review. |

## Prior closure and provenance

- V5 Codex F1 durable Claim identity is closed: both reviewers agree the append-only Claim ledger preserves exact claimant identity across replacement and terminalization. A2 concerns when a call-started Claim may be declared safely resolved.
- V5 Codex F2 terminal execution before promotion is closed. A1 concerns truthful representation of an authorized domain-`PARTIAL` selection, not terminal platform/DBOS proof.
- V5 Codex F3 and V5-OD1 populated-only selection are closed as product decisions. L1 is an execution-exactness correction to the chosen boundary.
- V5 Fable F1 run-pinned total scoring selection is closed in its core reduction. L2 preserves the accepted reduction and corrects only contradictory freeze/membership wording.
- V5 Fable F2 closes durable Claim discovery and shared-reference exclusivity, but not safe final resolution after call start. A2 retains that narrower open portion.
- Restart-safe targets, deterministic scheduling, request authority, singular Generation membership, Experiment-scoped Prediction identity, pre-scoring durable `PARTIAL` evaluation, payload-safe inspection, Analysis inventory, destination fencing, per-bundle atomic promotion, cutover order, and retained classifier ownership are not reopened by the seven source findings.
- Cross-family compatible-snapshot semantics are reopened by A3 despite the stability of destination fencing and individual bundle publication.

## Synthesis opinions

### Synthesis opinion — convergence versus thrashing

V5 was correctly classified as localized non-convergence and v6 was explicitly the bounded closure pass. Under that checkpoint, v6 does not qualify for outcome (a), `READY_FOR_FOCUSED_AUDITS`: A1 and A3 are not wording or runtime-gate residue, and A2 changes terminal compensation lifecycle semantics.

It also exceeds outcome (b), another automatically generated bounded final correction. The cancellation boundary has reopened again after successive detector, durable-identity, exclusivity, and bounded-resolution repairs. Acceptance, which Fable considered converged, contains a new state-representation contradiction. Most importantly, A3 introduces a new architecture defect in cross-family publication semantics, a subsystem previously carried as settled.

The accepted evidence therefore selects outcome (c): the current planning/review method is non-convergent at this checkpoint. This does not mean the entire architecture is unstable; most subsystems remain closed. It means automatic successor generation is no longer justified by “one last bounded pass.” A process-level decision and tighter exit method are required before any v7 is authorized.

### Synthesis opinion — owner-question boundary

Do not manufacture owner questions for A1, A2, L1, L2, or V1. Their answers follow from existing acceptance, cancellation, exact-predicate, membership, and pinned-reader invariants. V6-OD1 is the only product/architecture decision because the frozen invariants do not select between strong coordinated same-cut semantics and explicitly weaker bounded compatibility.

PD1 must be resolved before V6-OD1 is asked or any successor is created. If the owner authorizes continuation, stabilize the remaining queue as `V6-OD1`, record that decision, and require a deliberately bounded successor plus a changed review method and explicit stop/exit criteria.

## Verdict

- **Gate:** REPEAT_CONVERGENCE

The gate follows mechanically from A1-A3, the reopened acceptance and cancellation state semantics, the reopened cross-repository publication contract, and unresolved V6-OD1. However, `REPEAT_CONVERGENCE` is not authorization to create v7 automatically.

**Exact next action:** stop automatic successor generation and surface PD1 as the sole next process-level question: whether to stop/redesign the convergence method or explicitly authorize one manually bounded successor with named exit criteria. Only if continuation is authorized should the owner then answer V6-OD1; no plan successor should be drafted before both decisions are durably recorded.
