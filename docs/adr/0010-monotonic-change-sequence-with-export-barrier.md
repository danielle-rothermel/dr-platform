# Use monotonic change sequences behind a short export barrier

Every mutable exported platform row receives a trigger-maintained monotonic
`change_seq`. Because PostgreSQL sequence allocation can precede commit,
platform writers take a shared advisory transaction lock and export takes the
matching exclusive session lock before opening its repeatable-read snapshot;
export then captures the high water and extracts the bounded delta before
releasing the barrier. This briefly delays writes during extraction but avoids
silently skipping a lower sequence that commits after a destination advances.
The source Export Barrier does not serialize destination publication; each
destination and artifact uses its own Lease and Publication Fence through
promotion and cursor commit.
