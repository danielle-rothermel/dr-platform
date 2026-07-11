# Separate aggregate analysis from row-level detail

Operational Postgres remains the durable write model, while DuckDB/MotherDuck
serves aggregate analysis and Neon serves a bounded root-complete detail
surface for Unitbench. One export protocol feeds both destinations with
independent commit state. This avoids analytical load on the operational
database and avoids forcing wide prompt/output/debug payloads into the
columnar aggregate model.
