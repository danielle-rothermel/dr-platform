# V4 convergence findings — Codex 5.6 code and dependency audit

## Review baseline
- **Date:** 2026-07-11
- **Packet validation:** `uv run --script /Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/scripts/validate_review_packet.py /Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4 --require-prompts --require-baseline` — `Valid: true`
- **Review baseline SHA-256:** `92a3c74c67550275bb87baf16d1fab9bf093a86a0b9f4e4a92b9d778549f470f`
- **Plan manifest SHA-256:** `9a35e26c089aa93ebc23c9955c98051bfea209c7f16d823fea28fe48d507989f`
- **Independence limitation:** hybrid fresh plus closure phases shared one model context
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, issued planning/canonical-doc dirty set matched the baseline before this required findings file
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, dirty only in issued `CONTEXT.md`
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean
- **DBOS:** 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`

## F1. Multiple accepted Generation Operations make the owner-selected highest Attempt ordinal non-total and contradict the singular generation Manifest cut
- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** `contracts/whetstone.md` §2.3, §2.4, and “Experiment-acceptance schema and transaction”; ADR 0018; ADR 0020; v4 owner decision 2
- **Failure scenario:** An Experiment first accepts Generation Operation G1 for Prediction P. A later deployment changes an execution-affecting application/workflow recipe version without changing P's canonical domain model, and an explicit caller Operation key submits P through G2. Both registrations are legal and both accepted relationships are retained. G1 Attempt 0 and G2 Attempt 0 can then produce two successful Generation Runs with distinct recipe digests and the same ordinal. The rule “highest platform Attempt ordinal” cannot choose between them, and there is no globally unique Item ordinal because Item identity is Operation-scoped. Independently, the evaluator says it loads the complete accepted Manifest relationship set while its identity and row shape carry only one `generation_manifest_digest`, so it cannot reproduce which generation membership set governed the decision.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/contracts/whetstone.md:69` — explicit caller Operation keys remain supported; `:152`–`:154` claim ordinals are unique per Item and therefore cannot tie; `:169`–`:177` allow an accepted relationship for every `(experiment_name, workflow_role, operation_key, manifest_digest)` with no generation-role uniqueness constraint; `:178`–`:188` persist exactly one generation Manifest digest despite `:211`–`:220` loading the complete relationship set. `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/contracts/platform.md:39`–`:40` make `item_id` a digest of `operation_key + item_key`, so ordinals from G1 and G2 belong to different Items. Current Whetstone confirms Prediction identity is independent of Operation key and application/workflow version at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:29`–`:60`.
- **Affected repositories:** `dr-platform` and `whetstone-ai`; platform Operation-scoped lineage crosses into Whetstone Experiment acceptance
- **Required correction:** Choose and enforce one generation-membership contract. Either constrain each Experiment to exactly one accepted Generation Operation/Manifest and reject a second unequal accepted generation relationship, or make evaluations pin a sorted set of generation Operation/Manifest digests and define a deterministic accepted-run order across Items (including equal ordinals and differing recipe digests). The generation member/candidate rows must persist the selected Operation/Item/Attempt unambiguously, and the concurrency/gate suite must cover two accepted Generation Operations for one Prediction.
- **Closure impact:** reopens v3 P0-2/P0-4, v2 P0-5, v1 P0-5, and v4 owner decision 2

## F2. Acceptance has no deterministic Score Attempt selection across the multiple Scoring Operations it requires
- **Severity:** major
- **Class:** owner-decision
- **Plan contract:** `contracts/whetstone.md` §2.3, §2.4, and “Experiment-acceptance schema and transaction”; ADR 0012; ADR 0018; ADR 0020
- **Failure scenario:** Scoring Operation S1 produces a successful score for a Generation Run/profile cell. A later selection-distinct S2 legally includes the same logical cell after an application/workflow recipe change or after harness-failure recovery and produces another successful Score Attempt. The Score Attempt IDs differ because recipe digest and ordinal are identity inputs, but the acceptance member key has only Prediction/profile/parser/dataset axes. The contract requires one nullable selected Score Attempt ID and exact platform reference without saying whether to choose highest ordinal, newest recipe, a Manifest-precedence order, or reject duplicate successes. Two conforming evaluators can persist different scores and platform references for the same `acceptance_id` input set.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/contracts/whetstone.md:57`–`:66` require selection-distinct Scoring Operations and acceptance across all of them; `:76`–`:83` make `score_attempt_id` depend on concrete recipe digest and ordinal; `:195`–`:202` collapse all contributing Scoring Operations into one logical member row with a single selected Score Attempt but no selection rule; `:216`–`:221` say the evaluator loads all accepted Manifests and derives the matrix without resolving duplicate successful score candidates. The current rescore query demonstrates that successive score ordinals are normal domain behavior by advancing past matching harness failures at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/db/io.py:581`–`:605`, and current Score Attempt identity is already ordinal-bearing at `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:128`–`:152`.
- **Affected repositories:** `whetstone-ai` and `dr-platform`; platform-owned scoring lineage crosses into Whetstone acceptance and downstream Analysis/COPRO rows
- **Required correction:** Select an owner-approved deterministic policy for duplicate successful scoring candidates (for example, highest successful platform Attempt ordinal within an explicitly ordered recipe/Manifest cut), or prohibit overlapping logical cells across accepted Scoring Manifests with a database constraint and evaluation-time conflict. Persist candidate/supersession provenance analogous to Generation candidates, include the selection inputs in `acceptance_id`, and gate success-success, failure-success, equal-ordinal/different-recipe, and concurrent-selection cases.
- **Closure impact:** reopens v2 P0-5/P1-6 and v1 P0-5; this is a new owner decision not covered by the three v4 owner decisions

## F3. The deletion gate simultaneously requires current rescore parity and excludes a currently scoreable default status
- **Severity:** major
- **Class:** local-correction
- **Plan contract:** `contracts/whetstone.md` §2.4 and §2.5; `contracts/delivery.md` phases 5 and 9 plus the retained rescore-parity P2 gate
- **Failure scenario:** The parity fixture uses the current default rescore request. Current selection includes both `SUCCESS` and `PARTIAL` Generation Runs, and the scorer explicitly accepts a `PARTIAL` run with terminal submission text. V4 instead says scoring-selection freeze chooses the highest `SUCCESS` run. The new Manifest therefore omits current `PARTIAL` candidates, so it cannot match the pinned candidate identities and the old rescore path can never pass its deletion gate. If implementation silently keeps `PARTIAL`, it violates the stated selection rule and different components can disagree on the scoring set.
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/rescoring.py:47`–`:50` define current default statuses as `SUCCESS` plus `PARTIAL`, and `:212`–`:223` use that default; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/scoring.py:262`–`:269` explicitly treats a populated `PARTIAL` run as scoreable. `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v4/contracts/whetstone.md:152`–`:158` restrict scoring selection to a `SUCCESS` run, while `:250`–`:257` requires the replacement Manifest to match the current allowed-status candidate identities before deletion.
- **Affected repositories:** `whetstone-ai` and `dr-platform`; the Whetstone scoring cutover gate controls deletion of the old flow planned from dr-platform
- **Required correction:** State whether scoring (separate from strict acceptance) continues to support `PARTIAL` Generation Runs. If yes, preserve them in the frozen selection and define that they cannot satisfy strict Generation acceptance unless policy says otherwise. If no, record the intentional behavior break and change the parity gate to compare only the retained `SUCCESS` subset rather than claiming full current candidate parity.
- **Closure impact:** reopens the v3/v2/v1 retained P2 rescore-parity gate

## V3 strict-inclusive closure
| V3 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 restart-safe execution target and opaque recipe resolution | yes | Persisted target ref, startup registry, opaque envelope, resolver use across lifecycle facades, concrete leaf recomputation, and fresh-process gates are explicit in `contracts/platform.md` §1.5 and `contracts/delivery.md` §§4.2–4.4. |
| P0-2 representable, platform-current Experiment acceptance | no | Missing cells and atomic platform-cut checks are now representable, but the accepted generation relationship set and singular generation Manifest/effective selection contradict; see F1. |
| P0-3 owner-selected overlap accounting contract | yes | Plan, ADR 0019, glossary, crash tests, and cost gate consistently permit discarded-result undercount, preserve provider receipts as total-billing evidence, and exclude DBOS replay. |
| P0-4 accepted Generation Run selection | no | Highest successful ordinal is stated, but it is only total within one Item while the accepted-relationship schema allows multiple Generation Operations/Items; see F1. |
| P1-5 cancellation-safe claim/enqueue compensation | yes | Claim eligibility/invalidation, `NOT_ENQUEUED`, lost-CAS cancellation, and append-only compensation are explicit and race-tested. |
| P1-6 payload-safe DBOS step inspection | yes | The pinned allowlisted system-schema adapter excludes payload columns and has serializer-failure contract tests; installed public API still lacks a payload flag as correctly documented. |
| P1-7 authoritative Analysis Bundle inventory | yes | `contracts/publication.md` §4.1 names exactly six Analysis members and COPRO's `score_attempts` source; other documents reference it. |
| Preserved/extended P2 gates | no | Most gates remain blocking, but the current-rescore parity gate contradicts the `SUCCESS`-only target selection; see F3. |

## V2 strict-inclusive closure
| V2 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 complete execution recipe and exact domain equality | yes | Concrete opaque recipe leaves, exact canonical domain equality, persisted target ref, and final aggregate recomputation are specified and gated. |
| P0-2 implementable scheduling contract | yes | Kernel rank/claim/enqueue mixing is deterministic; installed DBOS 2.26.0 tie-local variance is explicitly accepted and tested rather than overclaimed. |
| P0-3 cancellation semantics under accepted paid overlap | yes | Reference topology, logical cancellation, compensation, overlap labeling, and the owner-selected outcome-linked undercount are consistent. |
| P0-4 consumer-visible Publication Bundles and cross-family skew | yes | Analysis and Detail referential sets have atomic pointers, kernel tables/cursors share one destination transaction, and independent families require explicit skew policy. |
| P0-5 append-only Experiment acceptance and current pointer | no | Currentness mechanics close the prior cut race, but accepted generation and scoring candidate selection is not deterministic over the relationship sets the schema permits; see F1 and F2. |
| P1-6 successive scoring selections | no | Selection-distinct Operations now exist, but acceptance does not deterministically select among duplicate successful logical cells across them; see F2. |
| P1-7 foreign-cancelled shared execution | yes | Foreign Operation/request provenance and new local confirmation are explicit and fail closed when provenance is unresolved. |
| P1-8 total Operation status | yes | Confirmed enqueue/`NOT_STARTED`, cancellation, permanent enqueue failure, abandoned registration, and terminal mixtures have an explicit first-match order. |
| P1-9 lifecycle wait and COPRO loop | yes | Typed wait, explicit export, committed bundle pin, and old-helper deletion ordering are specified. |
| P1-10 abandoned partial Registration | yes | Lease-expired confirmed abandonment is terminal, provenance-preserving, non-enqueueing, and serialized against completion. |
| P1-11 late DBOS terminal after cancellation | yes | Prior `SUCCESS`/`ERROR` wins with separate cancellation disposition; committed local terminality is immutable. |
| P1-12 requested versus effective priority | yes | Both priorities and source are persisted; linked work is not silently promoted and mismatch is inspectable. |
| P1-13 request-ledger maximum bound | yes | The request bound only tightens immutable policy; requested/effective values and exact replay semantics are persisted. |
| Retained P2 live verification boundaries | no | Live-store/runtime gates remain correctly blocking, but rescore parity cannot pass under the conflicting status contract; see F3. |

## V1 disposition closure
| V1 item | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| P0-1 caller-requested next Attempt | yes | Closed reason/source matrix, idempotent request ledger, exact Item CAS, platform-only ordinal allocation, bounds, foreign cancellation, and fresh-process resolution are specified. |
| P0-2 immutable registration Manifest | yes | Complete caller-prepared membership, page digests, Lease/cursor/completion CAS, transactional hook, exact resubmit, recipe recomputation, and abandonment are explicit. |
| P0-3 destination fencing | yes | Destination-local Lease/token renewal, stage ownership, monotonic promotion, DuckDB OS lock, and H1/H2 crash rejection are explicit for every sink. |
| P0-4 complete cancellation topology | yes | Managed workflows are top-level-only, reference-aware, lock-serialized, non-recursive, compensation-safe, and require explicit later authorization. |
| P0-5 Experiment acceptance | no | Strict/override policy and current pointer exist, but generation and scoring accepted-candidate selection is still ambiguous for permitted multi-Operation histories; see F1 and F2. |
| P1-6 Operation serialization | yes | Operation row locking, ascending multi-Operation order, same-transaction aggregate recomputation, and last-two-finish race proof are retained. |
| P1-7 execution-scoped attributes | yes | Immutable execution facts remain in DBOS; authoritative many-Operation references stay in platform rows. |
| P1-8 total status precedence | yes | Registration, cancellation, enqueue, running/retry, and all terminal mixtures are total and tested. |
| P1-9 Detail Attempt snapshot | yes | `detail_platform_attempts` is built in the Whetstone snapshot and promoted with the atomic root-cascaded Detail Bundle. |
| P1-10 secret-free payloads and safe reads | yes | Secrets leave workflow args; workflow reads disable payloads; step inspection uses the pinned payload-excluding adapter. |
| P1-11 writer-lock ownership | yes | Owning kernel writes acquire the shared Export Barrier lock internally; static and workflow-step tests enforce the boundary. |
| P2-12 live verification | no | The named live gates remain, but the rescore parity sub-gate is internally contradictory; see F3. |

## V4 owner-decision closure
| Owner decision | Closed? | Evidence or remaining gap |
| --- | --- | --- |
| Outcome-linked cost may undercount a discarded post-cancellation result | yes | Plan, ADR 0019, both glossaries, forced-overlap test, and cost gate consistently preserve the selected undercount and provider-receipt boundary. |
| Highest successful platform Attempt ordinal is the accepted Generation Run | no | The rule is stated consistently but is not total across multiple accepted Generation Operations whose Item-scoped ordinals can tie; see F1. |
| Acceptance currentness uses atomically checked Operation `platform_cut_version` values | yes | Operation versions increment with acceptance-relevant platform mutation; promotion and every current read compare the complete sorted vector under Operation locks and fail closed on mismatch. |
| Earlier accepted final DBOS tie variance | yes | Kernel mixing remains deterministic while same-priority/same-millisecond final DBOS order is explicitly permitted and verified as variance, not equality. |

## Verdict
- **Gate:** REPEAT_CONVERGENCE
- **Reason:** F1 is a blocker and architecture-changing acceptance defect, v4 owner decision 2 is not enforceable for all schema-permitted histories, and F2 introduces an unresolved score-selection owner decision across the deliberately multi-Operation scoring model. F3 also leaves a required deletion/live gate impossible as written.
- **Unverified:** Live MotherDuck conditional Lease/promotion and deployed DuckDB-SQL parity; live Neon transaction/pooling behavior; local DuckDB `flock` crash behavior; Vercel Node/native exclusion and server-only secret mapping; OTLP initialization/degradation; the not-yet-implemented export, acceptance, and adapter schemas; and COPRO/zero-spend end-to-end execution. These remain blocking gates without weakening their invariants. The exact DBOS 2.26.0 status, queue-ordering, priority, payload-loading, step-inspection, cancellation, and system-schema claims were statically verified from the baseline-selected installed package.
