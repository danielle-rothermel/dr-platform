# Changelog

All notable changes to `dr-platform` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
