# Keep export commit state with each destination

Each export destination owns committed cursor and Snapshot metadata plus a
single-writer Lease and monotonic fencing token per consumer-visible
Publication Bundle: DuckDB state lives in DuckDB, MotherDuck state in
MotherDuck, and Detail Store state in Neon. Mutually referential Whetstone
Analysis tables promote through one atomic pointer, a Detail root manifest and
all root-cascaded tables promote through another, and all kernel-table deltas
plus cursor bookkeeping commit in one destination transaction. A writer holds
the bundle Lease through staging promotion and cursor commit, and promotion
rejects a stale token or a candidate older than the committed Snapshot. Local
DuckDB additionally uses an OS/process writer lock; remote stores enforce the
Lease, token allocation, and promotion check transactionally.

Unrelated bundles remain independently timed rather than sharing one universal
Snapshot. Each exposes its committed `snapshot_seq`; any cross-bundle reader
must explicitly tolerate skew or reject/check incompatible cuts. A destination
advances only after its own idempotent bundle commit, so overlapping exporters,
Lease expiry, a missing local file, or partial multi-sink success cannot regress
or skip another destination's state. Operational Postgres supplies stable
source high-water marks but does not claim that downstream state has committed.
