# Changelog

All notable changes to `dr-platform` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.2.4 - 2026-08-19

### Added

- Optional `application_version` and `executor_id` on `PlatformDbosConfig` /
  `build_dbos_config` for explicit deployment identity without
  `runtime_initializer`.

### Changed

- Sweep reads `DBOS.application_version` and unions `DBOS.executor_id` into
  live executor ids once per pass instead of requiring registration-time
  `LiveDbosIdentity.app_version`.
- `SweepSummary.identity_unavailable` replaces
  `executor_resolver_unavailable` and covers unavailable application version
  or executor identity axes.

### Fixed

- Default-path sweep no longer projects every healthy pending attempt as
  `stale_app_version` when dispatcher registration precedes `DBOS.launch()`.
- Sweep suppresses dependent pending abandonment projection when identity
  evidence is absent instead of inferring terminal failure from empty values
  or the default `"local"` executor sentinel.
- Identity-orphan projection applies to PENDING DBOS rows only; ENQUEUED and
  DELAYED queue backlog is excluded. Blank or whitespace identity fields are
  treated as absent end-to-end.

### Removed

- `LiveDbosIdentity.app_version` (breaking).

## 0.2.3 - 2026-08-19

### Added

- Filtered `list_predecessor_stage_outputs` with optional `stage_key`,
  `min_stage_index`, and `max_stage_index` (exclusive bounds) for scoped
  barrier join reads across multiple deferral episodes.
- `input_reference` on `PredecessorStageOutput`.
- `list_stage_executions` for scoped stage execution listing.
- `BarrierJoinCluster` and `resolve_barrier_join_cluster` for deferral
  episode discovery and topology validation.
- README documentation for the ledger-native barrier fan-out / fan-in pattern.

### Fixed

- `list_stage_executions` accepts a min-only exclusive lower bound.
- `resolve_barrier_join_cluster` rejects equal optim and eval stage keys.

## 0.2.2 - 2026-08-19

### Added

- Preemptible wrapped stage bodies so operator cancellation interrupts in-flight
  work without recording application failure.
- Optional `LabelQueueRoute` selectors on `StageDefinition` for enqueue-time
  queue selection from work-item labels.
- Work-item `priority` persisted at submission, used ahead of stable rank in
  admission, passed to DBOS enqueue when non-zero, and adjustable via
  `set_work_priority`.
- `LiveDbosIdentity.resolve_executor_ids` for dynamic sweep executor identity.
- Alembic revision `0004_work_priority` (irreversible).

### Changed

- Application-directed stage handoff via `StageCompletion` / `StageSuccessor`
  with fan-out, loops, and admission-gated join barriers.
- `list_predecessor_stage_outputs` and `PredecessorStageOutput` for join
  bodies to read succeeded lower-index sibling outputs.
- Canonical work-item status derivation in `_core.ledger.work_item_status`
  (`work_item_status_rows`, `work_item_status_rows_by_run`).
- `AdmissionPayload.work_item_id` and persisted per-stage `input_reference`.
- `CancelledStageExecution` on the public cancellation API.
- Alembic revision `0003_stage_index_identity` (irreversible).
- Stage execution identity is `(work_item_id, stage_index)`; stage workflow-id
  digests include `stage_index` (invalidates in-flight stage workflow ids).
- `str` stage returns are permitted only at the registration index; otherwise
  stages must return `StageCompletion`. On the linear path, the returned string
  becomes the successor's `input_reference`, not the work item's submission
  input.
- `WorkCancellationResult` is now `{work_item_id, cancellations}` only
  (breaking: removed top-level `disposition`, `stage_execution`, and
  `delegated_workflow_id`).
- `cancel_work` is item-level: every nonterminal execution is cancelled.
- Work-item status and run-barrier release counts derive from precedence over
  all executions, not `max(stage_index)`.
- Label queue route overlap validation accepts disjoint label keys; route queue
  names must be distinct at registration.

### Fixed

- Repeated `cancel_work` on already-cancelled fan-out work reissues every stored
  admitted workflow identity, not just one representative row.
- Sweep resolver failures and empty results suppress pending `dead_executor`
  projection and set `executor_resolver_unavailable` on sweep summaries
  instead of projecting every pending workflow as `dead_executor`.
- Successor stage execution insert replays sync priority after an operator boost.
- Alembic revision `0003_stage_index_identity` validates the schema prefix via
  `LedgerSchema` before interpolating privileged DDL.
- `StageSuccessor` rejects boolean `stage_index` values at construction time
  so application failures are recorded instead of durable workflow errors.

## 0.2.1 - 2026-08-12

### Added

- `dr-store==0.2.3` integration with enlisted failure evidence writes in the
  stage handoff checkpoint; Alembic revision `0002_dr_store_baseline` colocates
  the `dr_store` schema with the platform ledger on the migration bind
  connection and is irreversible on downgrade.
- Required `PlatformDbosConfig.max_recovery_attempts` for wrapped stage and run
  completion workflows; recovery-exhausted workflows project to platform failure
  for operator retry.
- Default-on abandoned-stage sweep with identity-based pending projection
  (`stale_app_version`, `dead_executor`) and live-identity skip for same-process
  crash recovery.
- INFO reconciliation logging for admission, run-barrier, and sweep dispatcher
  passes.
- `retry_run_completion` with `run_completion_attempts` attempt history and
  per-attempt workflow identity.
- `sweep_abandoned_run_completions` to project recovery-exhausted, errored, and
  identity-orphaned pending run-completion workflows onto platform failure for
  operator retry.

### Changed

- Hard-cut dev-mode defaults: 1 s dispatcher schedules, comically high
  batch/chunk/inspection ceilings, and `pool_size` on `PlatformDbosConfig`
  with checkpoint-pool validation against admission and barrier batch sizes.
- Renamed `AdmissionPayload.run_key` to `origin_run_key` on the DBOS enqueue
  wire format; removed `MAX_INSPECTION_LIMIT`.
- README sizing guidance now points at whetstone's sizing table and documents
  the per-stage-boundary latency model.
- Run completion schema hard cutover: execution rows track `current_attempt`;
  workflow identity lives on attempt rows.
- Sweep and recovery contracts now cover identity orphaning, recovery caps,
  run completion retry, and run-completion abandonment projection.
- Run-completion error and attempt summaries use pinned `TerminalSummaryField`
  wire keys without a producer tag; successful stage attempts use pinned
  outcome-only summaries via `build_terminal_outcome_summary`.
- PostgreSQL table-prefix validation now derives its limit from the longest
  generated identifier suffix rather than an unreachable inner guard.
- `.defs/terms.toml` aligns run completion execution with per-attempt identity
  and adds `payload`, `terminal` cross-notes, and `unbudgeted`.
- 2026-08-12 single-path consolidation (structural audit, wave 2). One workflow
  attribute binding mechanism in `execution/_workflow_binding.py` backs both the
  object-store and ledger-checkpoint bindings; one private generic
  `normalize_key` in `_core/identities.py` replaces the three `inspection`
  helpers, ~19 inline coercion ternaries, and seven hand-written pydantic
  key validators; one `utc_now` in `_core/clock.py` replaces nine per-module
  copies; `_core.frozen.immutable_mapping` replaces the inline
  `MappingProxyType(dict(...))` sites; `_core/validation.py` integer guards
  replace the hand-rolled copies in `insert_stage_execution` and the stage
  control writers, so `set_stage_capacity` and `set_selector_capacity` raise
  `TypeError` rather than `ValueError` for a non-integer capacity, and
  `AdmissionPayload` rejects a non-string origin run key with the generic
  `run key must be a string`. `MembershipDigestField` now derives the
  membership-digest byte fragments it declares, and `_core/ledger/schema.py`
  derives `MAX_PREFIX_BYTES` from the declared table metadata instead of a
  hand-maintained suffix list.
- Dispatcher registration hygiene: `_log_admission_summary`,
  `_log_barrier_summary`, and `_validate_dispatcher_settings` are module-level
  helpers, and `DispatcherRegistration` holds a required `_resources` field as
  its single close-once guard.
- Shared read paths: `current_stage_indexes_by_run` serves both the bulk run
  status reader and the run barrier; `_matches_recorded_outcome` names the
  idempotent-replay comparison; `_StageIdentity` is a `NamedTuple`; the three
  ledger record modules share one section grammar, `select(table)` idiom, and
  `_decode_<record>` naming.
- 2026-08-12 vocabulary and test homes (structural audit, wave 3). The public
  ledger schema class is `LedgerSchema`, matching the `platform ledger` term;
  the `0001_staging_baseline` revision identity is unchanged. `StateCount`
  moved to `_core/ledger/states.py` beside the states it counts, so
  `completion/` no longer imports it from `inspection/`. Test homes now follow
  the code: the run-completion workflow-id golden sits beside the stage golden
  in `tests/core/test_workflow_ids.py`; stage-sweep tests live in
  `tests/recovery/test_sweep.py`; admission control CRUD in
  `tests/admission/test_controls.py`; pure-unit candidate evaluation in
  `tests/admission/test_runner_units.py`; conftest-harness safety checks in
  `tests/test_conftest_database_safety.py`. One `_RecordingClient` and one
  `_WorkflowStatus` stub in `tests/conftest.py` replace the per-module copies,
  and the two `args_for`-failure isolation twins are one parametrized test.

### Removed

- `StageApplicationFailure.evidence_reference`; callers supply optional strict-JSON
  `evidence` and the platform writes the enlisted `dr-store` reference.
- Qualification harnesses under `qualification/` and recorded rate artifacts
  under `docs/qualification/` and `docs/plans/async-stages-and-run-fan-in/`.
- The abandoned per-row work-item submission path (`insert_work_item`,
  `insert_work_item_with_result`, `WorkItemInsertResult`, `get_work_item`, the
  submission-side `list_work_items`, `WorkItemRecord`) together with the public
  `WorkItemConflictError` export; set-oriented `submit` is the only work-item
  creation path.
- Unreferenced symbols `get_stage_control`, `validate_evidence_reference`, the
  duplicate run-completion workflow-id constants and
  `RunCompletionWorkflowIdField` in `completion/execution.py`, the
  `run_completion_workflow_id` pass-through wrapper, the `Engine | Connection`
  branch on `run_admission_pass` and `run_barrier_pass`, and the unreachable
  branch in `_decode_bulk_work_terminal_status`.
- The layout test enumerating pre-rebuild module names, the re-exports in
  `dr_platform/completion/__init__.py`, and the vestigial `scripts/` and
  `qualification/` directories that held only `__pycache__`.

### Design notes (from retired async-stages plan)

- Synchronous stage and run-completion checkpoints run through a
  dispatcher-owned dedicated executor, not the asyncio default pool.
- Checkpoint transactions use `READ COMMITTED` isolation.
- Run-barrier reconciliation uses a bounded fair cursor so blocked prefixes
  do not starve later runs.
- Loop-affine async application resources remain application-owned; the
  platform enqueues compact validated payloads without calling application
  code inside dispatcher transactions.

## 0.2.0 - 2026-08-09

### Added

- Added declared ordered run membership, canonical membership digests,
  set-oriented registration, stable closure receipts, and membership-based
  bulk run inspection.
- Added optional run completion with independent barrier reconciliation,
  immutable one-time release facts, stable durable workflow identity, and
  application-outcome inspection.

### Changed

- Made application stage and run completion workflows async.
- Moved argument derivation into durable wrappers so dispatcher transactions
  enqueue only compact validated platform payloads.
- Replaced the development schema baseline with the fresh membership and
  completion schema; existing valuable databases require archival before an
  explicit reset.

## 0.1.1 - 2026-08-05

### Changed

- Organized the implementation into functional packages while preserving the
  root `dr_platform` public API.
- Made the platform baseline migration irreversible so downgrade cannot delete
  the recorded ledger.
- Refreshed the README and definitions reference around the current functional
  boundaries, public vocabulary, and recovery limits.
- Unified local hooks and Depot CI on `pre-check.sh`, expanded validation to
  Python 3.12 through 3.14, and hardened tag-triggered trusted publishing with
  provenance, changelog, artifact metadata, and digest checks.
- Published the TOML-backed terms and contracts reference through GitHub Pages.
- Pinned the serialization boundary to the published `dr-serialize` 0.1.2
  release.
- Declared Python 3.14 support and advanced the package metadata to version
  0.1.1.

### Fixed

- Forwarded the application database URL through the public DBOS runtime
  bootstrap.
- Settled a retry-prepared attempt when its READY stage is cancelled, without
  delegating cancellation for a workflow that was never admitted.
- Sampled cancellation and retry timestamps only after locking the current
  stage so a concurrent newer transition cannot make an operator action fail
  with a stale timestamp.

## 0.1.0 - 2026-07-24

Initial release of the staged-work funnel built on PostgreSQL and DBOS.

### Added

- The staged-work funnel: streaming submission with campaign and run
  idempotency, randomized admission with capacity and pause controls, atomic
  stage handoff, sweep of abandoned stages, retry and cancellation intent, and
  inspection and operator actions, including bounded collection readers.
- DBOS-backed durable stage execution: `wrap_pipeline_workflows` replaces
  application stage callables with package-owned DBOS workflows that commit
  stage outcome and create the next READY stage atomically.
- PostgreSQL schema managed through a single Alembic baseline
  (`0001_staging_baseline`) as the root of the supported migration chain.
- The published [vocabulary sheet](https://danielle-rothermel.github.io/dr-platform/)
  (source: `.defs/vocab.html`) as the authoritative statement of the
  staged-work pipeline contract.
