# Keep export commit state with each destination

Each export destination owns its per-artifact committed cursor and snapshot
metadata: DuckDB state lives in DuckDB, MotherDuck state in MotherDuck, and
Detail Store state in Neon. A destination advances only after its own
idempotent transaction commits, so a missing local file, a second writer, or
partial multi-sink success cannot cause another destination to skip source
changes. Operational Postgres supplies stable source high-water marks but does
not claim that downstream state has committed.
