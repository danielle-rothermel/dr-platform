# P7 export and destination publication matrix

P7a owns the source cut, frozen export contracts, incremental kernel bundle,
generic projection validation, and local DuckDB publication. P7b owns remote
destination fencing, bounded cross-source compatibility, and pin/cleanup
semantics. Whetstone-specific Analysis and Detail builders remain W7-owned.

## P7a source and local publication

| ID | Scenario | Expected result |
| --- | --- | --- |
| P7-S01 | Export races an in-flight writer | Exclusive session barrier precedes repeatable-read capture; no committed row through the high-water mark is omitted |
| P7-S02 | Incremental replay | Extract `previous_cursor < change_seq <= high_water`; crash replay is exact and full rebuild is equivalent |
| P7-S03 | Kernel inventory | Export Operations, Items, Attempts, Claims, next-Attempt requests, compensations, and throttle state; exclude DBOS payloads |
| P7-L01 | DuckDB publisher | Acquire sibling `fcntl.flock` before destination state; stage all members and atomically advance one bundle pointer |
| P7-L02 | Promotion replay | Newer snapshot promotes; equal snapshot/equal checksums is idempotent; older or equal/unequal is `STALE_PROMOTION` |
| P7-L03 | Lease/fence race | Foreign live owner returns `LEASE_HELD`; an older fencing token cannot promote after a newer owner |
| P7-P01 | Projection validation | Declared schemas, row counts, checksums, uniqueness, and cross-member references pass before promotion |
| P7-F01 | Partial sink failure | Structured destination failure preserves the prior readable pointer and independent retry state |

## P7b remote compatibility and retention

| ID | Scenario | Expected result |
| --- | --- | --- |
| P7-R01 | MotherDuck promotion | Conditional transactional state update uses `UPDATE ... RETURNING`; unmatched CAS fails closed |
| P7-R02 | Neon promotion | Row-lock/conditional-update transaction provides the same Lease/fence/pointer semantics without session advisory locks |
| P7-C01 | Application/DBOS capture | Persist truthful source identities and database-server timestamps; sequence equality never implies a shared source cut |
| P7-C02 | Bounded compatibility | Measured skew at or below 100 ms passes; missing coordinates, drift, or larger skew fails combined compatibility while preserving independent bundles |
| P7-B01 | Bundle pins | Active pins and current pointers survive cleanup; missing/checksum-invalid pinned members return `PINNED_BUNDLE_GONE` |

## Deferred live gates

Deterministic local tests are blocking. MotherDuck, Neon, and live DBOS source
checks are reported separately when credentials or endpoints are unavailable;
no secret or DSN enters export models, logs, or bundle metadata.

P7 verification on 2026-07-12:

- MotherDuck application-publication project hash `c248c2555063`: one
  non-empty Whetstone Analysis bundle (six members) and Detail bundle (seven
  members) were independently visible after their STAGED commits and final
  pointer CAS operations. A fresh connection resolved the Analysis pin;
  renewal, replacement-token, stale-renewal, and stale-promotion checks passed.
  MotherDuck application manifests must name its `main` schema; the generic
  Postgres `public` default fails before STAGED metadata commits.
- MotherDuck project hash `493872f2ab39`: the production fence acquired and
  renewed a Lease, promoted a physically present row-count/checksum-validated
  bundle with persisted source coordinates through conditional
  `UPDATE ... RETURNING`, resolved an active pin, and rejected a stale token.
  MotherDuck staging commits before the final short fence transaction so its
  transaction-stable timestamp cannot authorize a writer past Lease expiry.
  The adapter uses `CURRENT_TIMESTAMP` because the
  MotherDuck endpoint does not implement PostgreSQL `clock_timestamp()`, and
  its SQLAlchemy engine must set `use_native_hstore=False` because the endpoint
  does not implement savepoints used by hstore discovery.
- Application/DBOS topology project hash `56f797833dee`: 100 fresh samples
  measured p99 skew `0.297 ms`, median query quantum `0.188 ms`, and retained
  the pinned `100 ms` bound. `DBOS_SYSTEM_DATABASE_URL` was absent, so the
  specified application-endpoint fallback was exercised.
- The 2026-07-12 Phase-0 remediation rerun verified that same pinned topology
  and both expected hashes, measured p99 skew `0.339 ms`, and passed the
  configured/derived `100 ms` and measured-bound checks. The separate
  exploratory command is not treated as contract verification.
- Neon project hash `3bb33d910255`: the production fence promoted a physically
  present row-count/checksum-validated bundle with a database-server source
  coordinate and resolved an active pin. The endpoint was supplied through
  encrypted `DR_LLM_POSTGRES_SYNC_ADMIN_URL`; no URL value was printed or
  persisted. This closes the live Neon credential gate.
- The reusable preflight probes also made current and stale writers attempt the
  owner/token/unexpired-Lease/current-pointer guarded promotion CAS on Neon and
  MotherDuck. Current promotion returned one row, stale promotion returned no
  rows, atomic pointer visibility passed, and post-probe temporary-schema counts
  were zero on both providers.
