# Separate aggregate analysis from row-level detail

Operational Postgres remains the durable write model, while DuckDB/MotherDuck
serves aggregate analysis and Neon serves a bounded root-complete detail
surface for Unitbench. One export protocol feeds both destinations with
independent commit state. Within each destination, tables whose joins or root
closure promise one source cut publish as one atomic consumer-visible bundle;
unrelated kernel, DBOS-telemetry, Whetstone-projection, and Detail bundles may
advance independently only when readers explicitly tolerate or check their
`snapshot_seq` skew. This avoids analytical load on the operational database
and avoids forcing wide prompt/output/debug payloads into the columnar
aggregate model without exposing mixed referential table sets.
