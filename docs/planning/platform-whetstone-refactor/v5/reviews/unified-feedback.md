# V5 convergence review — unified feedback

## Review basis and synthesis method

- **Frozen packet baseline:** review baseline SHA-256 `aef6d4b8326ab5aa9fd1ca2a68a156ee713873c7a63df726684347011a60d64b`; plan manifest SHA-256 `49b8f3542e775796bba7ca5b2f7b9626862db313f46da7a621798fc254cf773f`.
- **Sources:** `v5/reviews/codex-findings.md`, `v5/reviews/fable-findings.md`, and `v4/reviews/unified-feedback.md` for prior closure and provenance.
- **Independence limitation:** each reviewer performed its fresh and closure phases in one model context. Fable stabilized its fresh findings before reading traceability or prior reviews; Codex records the shared-context limitation without a stronger phase-isolation claim.
- **Method:** strict-inclusive synthesis under the question-and-gate policy. Each of the five source findings has exactly one disposition below. No source finding is erased by classification, disagreement, or prior-closure language.

## Source-finding dispositions

| Source finding | Disposition | Unified target or evidence |
| --- | --- | --- |
| Codex F1 — terminalization or Claim replacement destroys the Claim identity needed for claimant-death compensation | **accepted** | **A1**. The mutable Attempt fields cannot durably represent every enqueue Claim while compensation and exact replay require `(item_id, attempt, claim_id)`. The reconciliation key is therefore unreconstructible in the stated terminalization and Lease-reuse scenarios. |
| Codex F2 — strict acceptance can publish domain success before the linked platform execution is terminal | **accepted** | **L1**. ADR 0018 requires DBOS and platform terminal success, but the evaluator checks domain winners and cut stability without a terminal-state predicate. This is a direct contract omission, not a new policy choice. |
| Codex F3 — populated-`PARTIAL` eligibility cannot equal the current status-only rescore candidate set | **accepted** | **OD1**. Live selection includes status-matching `PARTIAL` rows before the later populated-output rejection, while v5 promises both populated-only Manifest eligibility and current candidate-identity parity. Those observable behaviors cannot both be preserved. |
| Fable F1 — scoring-cell selection is not pinned to the accepted Generation Run and is not total over run-distinct candidates | **accepted** | **L2**. The strict predicate already requires a score of the accepted run. The stale-run and equal-ordinal counterexamples prove that the winner rule fails that existing invariant. Run pinning is mechanically required; only the separate `PARTIAL` candidate-set policy remains OD1. |
| Fable F2 — compensation can cancel a content-shared workflow referenced by another live Operation | **accepted** | **L3**. The compensation paths omit ADR 0005's reference-exclusivity predicate. Under either reading of unresolved cancellation intent, the contract permits shared-work cancellation or permanent fail-closed linking. |

No source finding is duplicate, rejected, or unverified. The five defects interact in two concentrated areas—claim/cancellation repair and strict Experiment acceptance—but each has a distinct failure mechanism and required correction.

## Architecture change

### A1. Preserve every enqueue Claim identity across replacement and terminalization

The compensation schema and replay contract use `(item_id, attempt, claim_id)` as the exact identity, while the Attempt schema permits only the current Claim fields and requires those fields to be cleared outside `CLAIMING`. Cancellation can therefore finalize `NOT_ENQUEUED` before a slow claimant enqueues, and Lease reuse can replace the claimant identity that later reconciliation must name.

The accepted v5 exact-replay invariant mechanically determines the correction: persist append-only Claim/enqueue-call identity records, including every expired or invalidated claimant, and reference those durable records from compensation. A successor could instead redefine compensation identity, but that would revise an accepted v5 invariant and would require an explicit new owner decision; synthesis does not silently choose that broader alternative.

Specify convergence when several expired claimants target the same deterministic workflow. Gate terminalization-before-late-enqueue, claimant death after enqueue before outcome CAS, Lease expiry and claimant replacement, several stale claimants, compensation insert/reload, and replay against the durable identity.

**Source:** Codex F1.

**Closure impact:** reopens the claimant-death portion of v4 L1, v3 P1-5, and the cancellation-safe portion of v1 P0-4. Fable's statement that v4 L1 is closed describes the detector added in v5, but does not answer Codex's schema-level proof that the detector lacks its required key.

## Owner decision

### OD1. Define the observable `PARTIAL` rescore-parity boundary

The current selector admits `SUCCESS` and `PARTIAL` by status, then the scoring path later rejects empty or whitespace-only `PARTIAL` output. V5 instead places only populated `PARTIAL` runs in the frozen Manifest while requiring candidate-identity parity with that selector. The deletion gate cannot satisfy both claims.

Choose one behavior:

1. **Preserve the current status-selected candidate set.** Put every status-selected `PARTIAL` in the Manifest and retain the later typed rejection or harness-failure outcome for empty and whitespace-only output.
2. **Adopt populated-only selection.** Define one exact persisted populated-output predicate and explicitly narrow deletion parity away from the old status-only candidate identities.

Both choices must pin empty, whitespace-only, and populated `PARTIAL` fixtures and align selection identity, retry behavior, scoring outcomes, and deletion-gate language.

**Source:** Codex F3.

This is the only unresolved owner decision. It changes externally visible scoring-selection behavior and cannot be settled editorially. It must be answered before dependent scoring-freeze and winner-rule text is revised.

## Local corrections

### L1. Require terminal platform execution before strict-acceptance promotion

Under the existing ascending Operation locks, require each selected domain candidate's exact platform Attempt to have the compatible terminal execution state and require every contributing Operation to satisfy the terminal state required by the strict or persisted override policy. Until then, return a typed non-current or partial result and do not promote the pointer.

Gate persistence before workflow return, crash after persistence before DBOS terminal success, recovery exhaustion, and a promotion race with terminalization.

This is mechanical because ADR 0018 already makes DBOS and platform terminal success necessary; no product or ownership choice remains.

**Source:** Codex F2.

**Closure impact:** reopens the platform-terminal portion of v3 P0-2, v2 P0-5, and v1 P0-5 without reopening the separate platform-cut currentness correction.

### L2. Pin each scoring cell to its accepted Generation Run

Before selecting a scoring winner, restrict eligible candidates to Score Attempts whose `generation_run_id` equals the evaluation's selected accepted Generation Run, or a run explicitly accepted by the persisted partial/override policy. Persist candidates for other runs with an immutable superseded-generation disposition; they cannot satisfy or win the cell. Then apply relationship precedence and highest Attempt ordinal only within the run-matched Item lineage.

Gate a stale scored success followed by regeneration, and coexistence of `PARTIAL` and `SUCCESS` runs with equal and unequal Attempt ordinals. The fixtures must prove that only a score of the accepted run can satisfy strict acceptance.

Fable classifies this as an owner decision, but the accepted strict predicate already answers the domain question: a strict cell requires a score of the accepted run. The remaining `PARTIAL` selection-policy choice is preserved separately as OD1 rather than hidden inside this correction.

**Source:** Fable F1.

**Closure impact:** reopens v4 Codex F2/OD2's claimed closure and its v2 P0-5/P1-6 and v1 P0-5 provenance, but only for run binding and total candidate reduction. Singular Generation membership, pre-scoring evaluation, and platform-cut currentness remain closed.

### L3. Apply reference exclusivity to compensation and define hazard resolution

Both the surviving-claimant and bounded replay/health compensation issuers must take the established advisory-workflow-lock then Operation-row lock order and re-evaluate ADR 0005's reference-exclusivity predicate before physical DBOS cancellation. If another registered nonterminal current Attempt references the workflow, record a replayable `SKIPPED_SHARED`-style disposition and issue no DBOS cancellation.

Define a bounded resolution event for invalidated Claims behind terminal `NOT_ENQUEUED`, including a durable no-workflow-found disposition after the missing-workflow grace window, so new-reference creation has a decidable guard rather than permanent ambiguity.

Gate the race in which A is cancelled, B legitimately links or enqueues the same content, and A's claimant plus replay compensation subsequently run; B's workflow and Attempt must remain live.

This is mechanical because reference-aware physical cancellation is already the governing ADR invariant. The ambiguous wording must be made decidable, but neither defective interpretation is a viable owner policy.

**Source:** Fable F2.

**Closure impact:** a new adjacent defect in the v5 claimant-death repair and ADR 0005 compensation carve-out; it does not duplicate A1's missing durable Claim identity.

## Reviewer disagreements

| Topic | Codex position | Fable position | Preserved synthesis treatment |
| --- | --- | --- | --- |
| Claimant-death closure | The detector cannot reconstruct its Claim-keyed ledger identity after terminalization or Claim replacement; v4 L1 and earlier cancellation closure reopen. | V4 L1 is closed by bounded discovery/replay; Fable F2 is only an adjacent exclusivity defect. | A1 accepts Codex's schema counterexample. L3 separately accepts Fable's exclusivity race. Both must be corrected. |
| Populated `PARTIAL` parity | The live status-only selector and populated-only Manifest rule contradict candidate-identity parity; OD3 remains open. | OD3 is closed because populated `PARTIAL` eligibility is normatively retained and distinct from strict acceptance. | OD1 accepts Codex's direct live-code/contract mismatch. Fable's strict-acceptance distinction remains valid but does not make the candidate identities equal. |
| Scoring run binding | Codex does not report the stale-run winner defect; its F2 instead requires platform terminality. | Blocker and owner decision; OD2 is not total or pinned to the accepted run. | L2 accepts the defect but treats run matching as mechanically required by the existing strict predicate. The genuine candidate-set choice remains OD1. |
| Strict-acceptance closure | Codex reopens the platform-terminal portion because domain rows can precede DBOS/platform terminal success. | Fable reports platform-cut currentness and most acceptance structure coherent, while reopening scoring reduction through F1. | Both narrow reopenings are retained: L1 covers execution terminality and L2 covers run-matched score selection. Neither erases the closed cut-currentness work. |
| Cancellation repair classification | Codex F1 requires a persistence change and classifies it architecture-changing. | Fable F2 classifies reference exclusivity and hazard resolution as a local correction. | A1 is the persistence change; L3 is the local lifecycle correction. The shared subsystem does not make them duplicates. |
| Overall gate | `REPEAT_CONVERGENCE`. | `REPEAT_CONVERGENCE`. | Agreement. The policy independently yields the same gate because A1 changes persistence and OD1 is unresolved. |

## Prior closure and provenance

### V4 source-finding closure

| V4 item | V5 synthesis status | Evidence or remaining gap |
| --- | --- | --- |
| Codex F1 / Fable F1 — Generation membership | closed | V5 enforces one accepted Generation relationship per Experiment with typed exact-replay/conflict behavior and new Experiment identity for growth. No v5 source finding defeats that singular-membership correction. |
| Codex F2 / OD2 — deterministic Score Attempt selection | reopened in part | Relationship and Attempt precedence are present, but Fable F1 proves the candidate space is not pinned to the accepted Generation Run and is non-total across run-distinct Items. See L2. |
| Codex F3 / OD3 — `PARTIAL` rescore parity | open | Codex F3 proves that populated-only eligibility still cannot equal the live status-only candidate identities. See OD1. |
| Fable F2 / L1 — claimant-death compensation | reopened | V5 adds discovery/replay, but Codex F1 proves its exact Claim-keyed identity is not durable. Fable F2 adds the distinct reference-exclusivity race. See A1 and L3. |
| Fable F3 / L2 — Experiment-scoped Prediction identity | closed | Both v5 reviewers report `experiment_name` retained in `prediction_id` and protected by the golden gate. |
| Fable F4 / OD4 — durable pre-scoring `PARTIAL` evaluation | closed | Empty scoring-relationship vectors and explicit `MISSING_SCORE` members are now specified. |
| Fable F5 / L3 — retained classifier seam | closed | The named `enqueue_failure_from_whetstone_exception` seam and retention check are present. |

### Earlier closure carried forward

- V3 P0-1, P0-3, P0-4, P1-6, P1-7, and their retained v1/v2 foundations remain closed in the frozen design, subject to their declared implementation gates.
- V3 P0-2 is reopened only for platform terminality (L1) and run-matched score reduction (L2); its missing-cell representation and atomic platform-cut currentness remain closed.
- V3 P1-5 and the cancellation-safe portion of v1 P0-4 reopen because A1 makes the required compensation identity unreconstructible; L3 adds the shared-reference race.
- V2 P0-5/P1-6 and v1 P0-5 reopen only for terminal platform execution and deterministic run-matched scoring selection.
- V4 OD1 singular Generation membership and OD4 pre-scoring evaluation remain closed. V4 OD2 is reopened by L2. V4 OD3 remains unresolved as OD1 here.
- Accepted undercount, Experiment-scoped Prediction identity, restart-safe target resolution, publication fencing, two-plane reader boundaries, payload-safe DBOS inspection design, and retained classifier ownership remain closed as reported by the v5 reviewers.

## Verification gaps

No source finding is dispositioned `unverified`; the following are retained implementation gates rather than substitutes for the accepted defects:

- live MotherDuck conditional Lease and fenced bundle promotion, including DuckDB-SQL parity;
- live Neon transactions, pooling, and compute suspension;
- local DuckDB `fcntl.flock` and bundle-promotion crash behavior;
- Vercel Node runtime, native-DuckDB exclusion, and server-only secret mapping;
- OTLP initialization, degradation, and safe attributes;
- full DBOS allowlisted-schema and payload-exclusion contract tests;
- same-millisecond multi-dequeuer mixing bounds and Experiment-row-lock load thresholds;
- corrected rescore parity with empty, whitespace-only, and populated `PARTIAL` fixtures; and
- COPRO and zero-spend wait, export, and pinned-read continuity.

The exact verification required is the corresponding live-store, crash, contract, concurrency, parity, load, or end-to-end test on locked revisions. A1 additionally requires the Claim-replacement and multiple-stale-claimant crash matrix; L1 requires persistence-before-terminal and promotion-race fixtures; L2 requires stale-run and equal-ordinal run-binding fixtures; L3 requires the shared-reference compensation race.

## Synthesis opinions

### Synthesis opinion — convergence versus thrashing

The v5 findings are predominantly narrower refinements, not evidence that the whole design is thrashing. V1-v4 closure remains intact across target identity, scheduling, publication, reader ownership, export inventory, payload safety, singular Generation membership, pre-scoring evaluation, and most cutover structure. Four of the five v5 findings sharpen two already known boundaries with concrete crash or ordering counterexamples; the fifth is a new adjacent race in the same cancellation mechanism.

Convergence is nevertheless incomplete in those concentrated boundaries. Claim/cancellation repair has reopened across successive rounds and now requires a persistence correction, while Experiment acceptance still has one unresolved observable-behavior decision and two missing predicates. This is localized non-convergence rather than broad architectural churn: the successor should be bounded, but another whole-system pass is required because A1 changes persistence and the reopened acceptance/cancellation invariants are core.

### Synthesis opinion — owner-question boundary

Only OD1 belongs in the owner queue. A1's durable Claim record follows from the already accepted Claim-keyed exact-replay identity; L1 follows from ADR 0018 terminal-success semantics; L2 follows from the strict score-of-accepted-run predicate; and L3 follows from ADR 0005 reference exclusivity. Treating those as preference questions would re-litigate accepted invariants rather than resolve factual contract omissions.

## Verdict

- **Gate:** REPEAT_CONVERGENCE

The gate follows mechanically because A1 is architecture-changing and changes persistence, OD1 is unresolved, and the accepted findings reopen core lifecycle and strict-acceptance invariants. `READY_FOR_FOCUSED_AUDITS` is unavailable until a successor records OD1, applies A1 and L1-L3, closes the reopened prior items with evidence, and passes another independent whole-system convergence review. The bounded verification gaps remain implementation gates after convergence.
