# Use local DuckDB and remote MotherDuck read adapters

Unitbench's analysis read-layer has two concrete adapters behind one
intent-named interface: local development opens the exported DuckDB file and
uses laptop compute, while deployed Vercel uses MotherDuck's Postgres endpoint
without native binaries or persistent local files. Query contracts are shared,
and each analytical surface declares whether remote execution is allowed,
requires confirmation, or is local-only so intensive exploration stays cheap
locally without making production deployment fragile.
