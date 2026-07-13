# V1 convergence findings — Codex 5.6 code and dependency audit

## Review baseline

- **Date:** 2026-07-10
- **dr-platform:** `07-08-refactor`, `7b9b340fd8f2717e44de36804396077b7beeb661`, clean
- **whetstone-ai:** `codex/versioned-planning-docs`, `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, clean
- **unitbench:** `codex/versioned-planning-docs`, `cafd493ab9e9c1940106037209b1b218097f847e`, clean
- **DBOS:** 2.26.0 at `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos`

## F1. Platform-owned execution retries provide no way to create valid later Whetstone domain attempts

- **Severity:** blocker
- **Class:** owner-decision
- **Plan contract:** §1.5, §2.3–2.4, ADR 0002, ADR 0012, and ADR 0013
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:335` — only reconciliation of a failed DBOS execution advances the platform-owned ordinal, successful workflows block replacement, and the explicit retry command is deferred at lines 343–358; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:510` — a Generation Run may be `partial`/`error` and a Score Attempt may be a harness failure while DBOS and the Operation succeed; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/graph_workflow.py:153` — live generation catches node/provider exceptions, converts them to domain error results, persists the terminal Generation Run, and returns normally at line 203, so DBOS records `SUCCESS`; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/platform/worker.py:243` — the live rescore surface deliberately accepts `--score-attempt-index` at lines 266–269; `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/records/hashing.py:128` — that index is part of `score_attempt_id` today.
- **Consequence:** following v1 removes the only mechanism that requests generation attempt 1 or scoring attempt 1 after a DBOS-successful workflow produced an unsuccessful domain outcome. Ordinary resubmission resolves to the successful Attempt/Operation, while platform reconciliation is forbidden from advancing it. Valid regeneration and rescoring are silently suppressed unless Whetstone falsely turns domain outcomes into platform execution failures, which would violate ADR 0013.
- **Required plan change:** decide and specify a pre-experiment domain-reattempt/rescore contract. Either add an explicit platform action that allocates the next ordinal after a terminal successful execution for a caller-approved domain reason, including CAS/provenance/Operation-status rules, or separate the platform execution-retry ordinal from the Whetstone generation/score attempt ordinal and restore a Whetstone-owned content-attempt axis. Update ADRs 0002, 0012, and 0013 and both identity recipes together.

## F2. Concurrent exporters can promote an older snapshot after a newer one and regress destination state

- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.6, ADR 0008, ADR 0010, and pre-experiment gate 6
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:594` — export may be invoked post-operation, by cron, or ad hoc; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:602` — the only cross-run lock described is the source Export Barrier, and it is released before destination sync/promotion at lines 608–610; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:570` — full-rebuild projections atomically replace the current table and record their snapshot but define no destination writer lease, fencing token, or monotonic promotion CAS; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/adr/0008-destination-local-export-state.md:3` — the ADR claims destination-local state handles a second writer without specifying how. DuckDB's official concurrency contract states that ordinary file-backed read/write supports writers within one process, not uncoordinated writer processes: https://duckdb.org/docs/current/connect/concurrency.
- **Consequence:** exporter A can capture H1, exporter B can capture H2, B can promote H2 first, and A can then replace tables and/or cursor metadata with H1. Kernel deltas can be replayed from a regressed cursor, and full DBOS/Whetstone rebuilds can silently replace newer analysis/detail data with an older snapshot. A second process can also fail merely opening the local DuckDB writer.
- **Required plan change:** add a per-destination/per-artifact single-writer protocol held through promotion and cursor commit. Specify acquisition, expiry, fencing, monotonic `WHERE committed_snapshot < candidate_snapshot` promotion, stale-stage cleanup ownership, and `full_rebuild` ordering for DuckDB, MotherDuck, and Neon. The source barrier remains short, but an older run must be unable to promote after a newer run.

## F3. Bounded registration has no Operation-level ownership or immutable input-set contract

- **Severity:** blocker
- **Class:** architecture-changing
- **Plan contract:** §1.3, §1.5, §4.3, and V0 items P1-9 through P1-11
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:291` — `submit(items, ...)` accepts a paged item source, but the plan defines only per-page hook accounting and no expected item count/digest, registration lease, page cursor, or completion CAS; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:162` — `requested_count` becomes immutable only after an undefined “initial registration contract,” while line 466 derives `REGISTERING` by comparing against that count; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:976` — the required two-submitter test names Item/current-attempt CAS but no registrar winner predicate. The live predecessor demonstrates the unresolved shape: `/Users/daniellerothermel/drotherm/repos/dr-platform/src/dr_platform/submission.py:217` registers separate transactions page by page, updates `requested_count` progressively at lines 335–340, and uses per-Item `ON CONFLICT DO NOTHING` at lines 364–372 without an Operation registration owner.
- **Consequence:** two callers using the same immutable Operation spec but different ordering, truncation, or item sets can interleave pages. The committed Operation may become a union of both submissions, item-index conflicts can silently discard one caller's rows after its RegistrationHook already inserted domain records, and one caller can mark registration complete and begin enqueue before the other source is exhausted. A crash has no durable cursor proving which exact source is being resumed.
- **Required plan change:** define one registration authority and immutable manifest. For example, pre-index/spool to a manifest containing total count plus ordered item-key/spec digest, then claim registration with an Operation-row lock/lease and advance a durable page cursor under CAS; or explicitly materialize the source before registration. Enqueue/reconcile must require a committed `registration_completed_at` tied to that manifest, and resubmission must prove exact manifest equality.

## F4. The shared Export Barrier lock does not serialize Operation aggregate recomputation

- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5 Operation aggregation, ADR 0010, and §4.3 aggregate verification
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:462` — every Item/Attempt writer recomputes and stores aggregate counts “under the same writer lock”; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:602` — that lock is explicitly a shared advisory transaction lock, which only conflicts with export's exclusive lock. PostgreSQL documents that shared advisory locks do not conflict with other shared locks: https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS. The live predecessor has the same lost-summary shape at `/Users/daniellerothermel/drotherm/repos/dr-platform/src/dr_platform/submission.py:694`: it reads all Items and then overwrites every aggregate column at lines 745–757 without locking the Operation row.
- **Consequence:** concurrent transitions for two Items in one Operation can each compute a snapshot that excludes the other's uncommitted change; the last aggregate write can leave counts and a terminal status permanently stale even though both Item/Attempt CAS transitions committed. The export lock prevents source extraction during either writer but provides no writer-versus-writer ordering.
- **Required plan change:** require an exclusive per-Operation row/advisory lock before every Item/Attempt transition plus aggregate recomputation, with a fixed lock order for multi-Operation transactions. Alternatively stop persisting aggregates and derive them transactionally. Add the two-writer test at the isolation level used in production and assert the stored terminal status without relying on a later inspector repair.

## F5. One shared DBOS workflow cannot retain searchable attributes for every referencing Operation

- **Severity:** major
- **Class:** local-correction
- **Plan contract:** §1.5 DBOS call/correlation contract, ADR 0001, and V0 P1-6
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:328` — multiple Operations deliberately converge on one DBOS workflow; `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:485` — every enqueue sets scalar `operation_key`, `item_id`, and `attempt` attributes and promises they are searchable. In installed DBOS 2.26, `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_context.py:559` states `SetWorkflowAttributes` records attributes at workflow creation; `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:653` shows workflow-ID conflict updates omit `attributes`; and `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:961` shows the later public update replaces the whole attribute object rather than append-merging it.
- **Consequence:** the first Operation to create a shared workflow owns its DBOS attributes. A later deduplicated Operation cannot be found with `DBOSClient.list_workflows(attributes={"operation_key": later_key})`; overwriting attributes would instead make the first Operation disappear and races between references would lose values. Operator search and health checks therefore disagree with the authoritative many-to-one Attempt table.
- **Required plan change:** make DBOS attributes execution-scoped only (for example execution key, role, and safe content labels) and specify Operation→Attempt→workflow lookup through platform rows before calling DBOS by ID. Remove the claim that every Operation is directly filterable from the shared DBOS workflow, or replace scalar attributes with a separately specified bounded, concurrency-safe correlation design and prove its DBOS filter semantics.

## F6. Recursive DBOS cancellation bypasses reference checks on descendant workflows

- **Severity:** major
- **Class:** owner-decision
- **Plan contract:** §1.5 cancellation, ADR 0005, and §4.3 child/shared cancellation verification
- **Evidence:** `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v1/plan.md:360` checks exclusivity only for each directly referenced workflow and then calls `cancel_workflow(..., cancel_children=True)`; line 984 nevertheless requires child and shared-workflow coverage. Installed DBOS 2.26 recursively discovers and cancels every descendant without an application callback or reference predicate: `/Users/daniellerothermel/drotherm/repos/dr-platform/.venv/lib/python3.12/site-packages/dbos/_sys_db.py:845`, especially the unconditional breadth-first cascade at lines 880–887.
- **Consequence:** an Operation may exclusively reference parent P while a descendant C is content-scoped and still referenced by another live Operation. Cancelling P passes v1's direct exclusivity check and DBOS then cancels C, destroying shared paid work and violating ADR 0005. Locking only platform references to P cannot close the race for C.
- **Required plan change:** choose and enforce one contract: either prohibit shared/content-addressed child workflows and validate that invariant, or enumerate the descendant graph first, lock/check every descendant reference, and cancel only the individually exclusive workflows without DBOS recursive cascade. Specify how a newly created child/reference races with the frozen cancellation set.

## V0 coverage gaps

- **P0-1 workflow reconciliation:** execution-state reconciliation is specified, but it cannot advance a Whetstone domain attempt after a DBOS-successful error/partial/harness-failure outcome (F1).
- **P0-2 export consistency:** destination-local cursors and the source barrier still lack second-writer serialization and monotonic promotion (F2).
- **P1-6 searchable workflows:** scalar Operation attributes are incompatible with content-scoped workflows shared by multiple Operations (F5).
- **P1-7 typed inspector/control:** recursive child cancellation is not reference-aware below the directly referenced workflow (F6).
- **P1-9 through P1-11 registration/crosswalk/paging:** page-level accounting is present, but the exact bounded source, registrar winner, resume cursor, and registration-completion CAS remain unspecified (F3).

## Verdict

- **Gate:** REPEAT_CONVERGENCE
- **Reason:** F1 is an unresolved owner decision over domain-attempt ownership; F2 and F3 require new persistence/concurrency contracts; and F6 changes the cross-workflow cancellation contract. These meet the effort index's mandatory successor-draft conditions before focused audits.
- **Unverified:** No live MotherDuck or Neon credentials were available, so endpoint query parity, sink promotion behavior, and Vercel secret/runtime wiring could only be checked against current code and official product contracts, not exercised. The proposed adapters, schemas, export implementation, and acceptance fixtures do not yet exist. Broad suites were intentionally not run under the review prompt; installed-package introspection and static/focused checks were used instead.
