# Changelog

All notable changes to `dr-platform` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Organized the implementation into functional packages while preserving the
  root `dr_platform` public API.
- Made the platform baseline migration irreversible so downgrade cannot delete
  the recorded ledger.

### Fixed

- Forward the application database URL through the public DBOS runtime
  bootstrap.
- Settle a retry-prepared attempt when its READY stage is cancelled, without
  delegating cancellation for a workflow that was never admitted.

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
