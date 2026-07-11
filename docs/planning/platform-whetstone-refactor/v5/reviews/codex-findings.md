# V5 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** 2026-07-11
- **Packet validation:** `uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5 --require-prompts --require-baseline` — `Valid: true`
- **Review baseline SHA-256:** `aef6d4b8326ab5aa9fd1ca2a68a156ee713873c7a63df726684347011a60d64b`
- **Plan manifest SHA-256:** `49b8f3542e775796bba7ca5b2f7b9626862db313f46da7a621798fc254cf773f`
- **Independence limitation:** hybrid fresh and closure phases shared one model context
- **dr-platform:** branch `07-08-refactor`, HEAD `7b9b340fd8f2717e44de36804396077b7beeb661`; the issued planning/canonical-document dirty set matched the baseline before this required findings file
- **whetstone-ai:** branch `codex/versioned-planning-docs`, HEAD `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`; dirty only in the baseline-recorded `CONTEXT.md`
- **unitbench:** branch `codex/versioned-planning-docs`, HEAD `cafd493ab9e9c1940106037209b1b218097f847e`; clean
- **DBOS:** installed version 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`

## F1. Claimant-death reconciliation cannot reconstruct the compensation key after cancellation terminalizes the Attempt
- **Severity:** major
- **Class:** architecture-changing
- **Plan contract:** `contracts/platform.md` §1.3 “Schema (new single baseline)” and §1.5 cancellation; ADR 0005; `contracts/delivery.md` §§4.3–4.4
- **Failure scenario:** Claim A is persisted on an Attempt, cancellation invalidates it, observes no DBOS row, and finalizes the Attempt as terminal `NOT_ENQUEUED`. The schema requires `claim_id`/`claimed_at` only while `enqueue_state=CLAIMING`, so terminalization must clear the only persisted Claim identity. A then completes its outside-transaction DBOS enqueue and dies before its losing outcome CAS. Reconciliation can discover the deterministic workflow, but it cannot insert or exact-reload the required `(item_id, attempt, claim_id)` compensation because the terminal Attempt no longer contains `claim_id`. Reuse after Lease expiry is worse: the mutable Attempt can retain only the newest Claim, not the stale claimant that actually created the workflow. The planned detector therefore cannot satisfy its own idempotency/provenance key and may leave paid work live behind `NOT_ENQUEUED` or invent the wrong claimant provenance.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/contracts/platform.md:173-183` defines the single mutable Claim fields; `:197-203` makes `claim_id` part of the compensation primary key and exact-replay identity; `:212-216` permits Claim fields only while claiming; `:517-539` terminalizes `NOT_ENQUEUED` and later requires reconciliation to recreate the same Claim-keyed compensation while keeping terminal Attempt fields immutable. Installed DBOS confirms enqueue commits independently of the application CAS at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:669-703`.
- **Affected repositories:** `dr-platform`; Whetstone generation/scoring paid-work safety depends on this boundary
- **Required correction:** Persist every enqueue Claim identity durably across replacement and terminalization, for example in an append-only Claim/enqueue-call ledger referenced by compensation, or redefine the compensation identity around a durable cancellation/Attempt key that reconciliation can reconstruct. Specify how multiple expired claimants for one deterministic workflow converge, and gate terminalization, claimant death, Lease reuse, and replay against that exact schema.
- **Closure impact:** Fable F2 and L1 remain open; v3 P1-5 and the cancellation-safe portion of v1 P0-4 are reopened

## F2. Strict acceptance can promote persisted domain successes before their platform executions become terminal
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** ADR 0018; `contracts/whetstone.md` §2.4 and “Experiment-acceptance schema and transaction”; `contracts/platform.md` §1.5 “Operation aggregation” and “Operation lifecycle”
- **Failure scenario:** A generation or scoring workflow executes its final persistence step, committing a successful Generation Run or Score Attempt, and pauses or crashes before its workflow function returns. Whetstone's evaluator can now see every required domain-success row while the linked DBOS workflow and platform Attempt are still `PENDING`/`ACTIVE` and the Operation is `RUNNING`. The v5 evaluator derives winners and checks only that `platform_cut_version` has not changed; it never requires the referenced Attempts or contributing Operations to be terminal. It can therefore promote a current strict acceptance during this window even though ADR 0018 makes DBOS and platform terminal success necessary. A later reconciliation changes the cut and makes the pointer historical, but that does not undo the interval in which a nonterminal or ultimately failed execution was published as current acceptance.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0018-strict-experiment-acceptance.md:3-6` requires DBOS and dr-platform terminal success. The evaluator contract at `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/contracts/whetstone.md:273-293` derives domain winners and compares Operation versions without a terminal-state predicate. Live Whetstone commits generation output before returning at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:190-203` and scoring output before returning at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/scoring_workflow.py:119-136`; installed DBOS marks `SUCCESS` only after the function returns at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_core.py:570-586`.
- **Affected repositories:** `whetstone-ai` acceptance and `dr-platform` Attempt/Operation cut boundary; published Analysis/Unitbench readers can observe the premature pointer
- **Required correction:** Under the existing ascending Operation locks, require every selected domain candidate's exact platform Attempt to have a compatible terminal execution state and require the contributing Operation terminal states demanded by the strict/override policy before pointer promotion. Return a typed non-current/partial result otherwise, and add persistence-before-workflow-return, post-persistence crash, recovery-exhaustion, and promotion-race fixtures.
- **Closure impact:** reopens the platform-terminal portion of v1 P0-5, v2 P0-5, and v3 P0-2; otherwise none

## F3. The populated-PARTIAL rule still cannot preserve the current rescore candidate set it claims to match
- **Severity:** major
- **Class:** owner-decision
- **Plan contract:** `contracts/whetstone.md` §2.4 and §2.5; `contracts/delivery.md` phase 5, §4.3, and gate 3; OD3
- **Failure scenario:** The old selector is run with its default allowed statuses. Its SQL selects every `PARTIAL` Generation Run without testing whether `terminal_submission_text` is populated; only later, inside scoring, does the live validator reject an empty/whitespace `PARTIAL`. V5 instead defines only populated `PARTIAL` runs as scoring-eligible while requiring the replacement Manifest to match the old selector's candidate identities. An empty-output `PARTIAL` is therefore a current candidate but not a v5 eligible Manifest member. The parity fixture must either disagree with the frozen selector or violate the selected populated-only policy, so the old rescore path still has no passable deletion gate.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/rescoring.py:47-50` defaults to both `SUCCESS` and `PARTIAL`; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/db/io.py:565-655` filters only by status and does not test terminal submission content; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/scoring.py:262-269` applies the populated check later. The v5 contract claims both populated-only eligibility and current candidate-identity parity at `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v5/contracts/whetstone.md:129-136` and `:322-332`.
- **Affected repositories:** `whetstone-ai` scoring selection and deletion gate; `dr-platform` Scoring Operation Manifest boundary
- **Required correction:** Decide and state which observable behavior is preserved: either include every current status-selected `PARTIAL` in the Manifest and retain the later typed rejection/harness-failure behavior, or define populated eligibility with an exact persisted predicate and explicitly narrow parity away from the old candidate set. Pin empty, whitespace, and populated `PARTIAL` fixtures and align selection, identity, retry, and deletion text.
- **Closure impact:** Codex F3 and OD3 remain open; the retained rescore-parity row is reopened

## V4 source-finding closure
| V4 source finding | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| Codex F1 | yes | `contracts/whetstone.md:202-217` enforces one accepted Generation relationship with a partial unique index, exact replay, and typed unequal conflict; generation selection is now Item-lineage-local. |
| Codex F2 | yes | `contracts/whetstone.md:184-191,219-220,263-271,277-287` assigns immutable relationship ordinals, selects newest relationship containing success then highest successful Attempt, and persists candidate/supersession inputs. |
| Codex F3 | no | “Populated” eligibility does not equal the live status-only candidate set; see F3. |
| Fable F1 | yes | Same singular membership proof as Codex F1; membership growth requires a new Experiment identity. |
| Fable F2 | no | The detector cannot reconstruct its Claim-keyed compensation after terminalization or Claim replacement; see F1. |
| Fable F3 | yes | `contracts/whetstone.md` §2.3 explicitly retains `experiment_name` in `prediction_id`, and delivery gate 2 pins it. |
| Fable F4 | yes | `contracts/whetstone.md:166-172,295-299` makes the empty scoring-relationship vector valid and persists `PARTIAL` plus `MISSING_SCORE`. |
| Fable F5 | yes | `contracts/whetstone.md:103-107` retains `enqueue_failure_from_whetstone_exception`; delivery §4.5 names it in the retention check. |

## V5 owner-decision and local-correction closure
| Item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| OD1 — singular Generation membership | yes | Partial uniqueness, exact replay, typed unequal conflict, singular evaluation identity, and lineage-local selection are coherent and gated. |
| OD2 — deterministic scoring selection | yes | Monotonic accepted relationship ordinals, newest-successful-relationship precedence, highest successful Attempt, and complete candidate provenance are coherent and gated. |
| OD3 — populated-PARTIAL scoring eligibility | no | Populated-only eligibility cannot equal the live status-only candidate identities; see F3. |
| OD4 — durable pre-scoring PARTIAL evaluation | yes | Empty relationship/candidate vectors, explicit `MISSING_SCORE` members, append-only later replacement, and currentness checks are specified. |
| L1 — claimant-death compensation | no | The required reconciliation key is not durably reconstructible; see F1. |
| L2 — Experiment-scoped Prediction identity | yes | `experiment_name` remains an explicit identity input with a golden gate. |
| L3 — retained classifier seam | yes | The named live function is retained as the injected mapping seam and protected by a retention search. |

## Earlier closure regression audit
| Prior item | Still closed? | Evidence or remaining gap |
| --- | --- | --- |
| every v4 prior-closure row and retained owner policy | no | Singular generation membership closes the earlier representability/ordering rows, and the selected overlap, tie-variance, publication, recipe, target-resolution, and payload-safe policies remain coherent. F1 reopens cancellation-safe Claim compensation (v3 P1-5/v1 P0-4), F2 reopens the platform-terminal part of strict/current acceptance (v3 P0-2/v2 P0-5/v1 P0-5), and F3 leaves the retained rescore-parity gate open. |

## Verdict
- **Gate:** REPEAT_CONVERGENCE
- **Reason:** OD3 is unresolved; L1's claimed correction requires a different durable Claim/compensation persistence contract; and F1 changes persistence and cancellation lifecycle. The mechanical `READY_FOR_FOCUSED_AUDITS` conditions therefore are not met even though OD1, OD2, OD4, L2, L3, and most earlier architecture close.
- **Unverified:** Live MotherDuck conditional Lease/fenced promotion and DuckDB-SQL parity; Neon transaction/pooling/suspension behavior; local DuckDB `fcntl.flock` crash behavior; Vercel Node/native-package exclusion and server-only secret mapping; OTLP initialization/degradation; exact same-millisecond multiple-dequeuer mixing bounds; Experiment-row-lock load thresholds; full DBOS allowlisted-schema contract tests; and COPRO/zero-spend wait→export→pinned-read execution. Each remains a correctly placed blocking phase gate; none supplies evidence against F1–F3.
