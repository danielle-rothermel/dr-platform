# Changelog

All notable changes to `dr-platform` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dr-store==0.2.2` integration with enlisted failure evidence writes in the
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
  control writers. `MembershipDigestField` now derives the membership-digest
  byte fragments it declares, and `_core/ledger/schema.py` derives
  `MAX_PREFIX_BYTES` from the declared table metadata instead of a
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
