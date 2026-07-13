# Cut to fresh schemas without migrating legacy data

The platform, Whetstone, DBOS durable names, analysis projections, and
Unitbench stores cut to fresh canonical names with no stamp/adopt path,
backfill, compatibility view, or dual write. Legacy `dr_dspy_*` and
`published_*` data remains readable through old code for one-off reference.
This trades historical continuity for a smaller final interface before
external users and intensive experiments create irreversible state.
