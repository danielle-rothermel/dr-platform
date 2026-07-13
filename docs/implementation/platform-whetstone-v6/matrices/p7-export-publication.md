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
