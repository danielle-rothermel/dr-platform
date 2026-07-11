# Let dr-platform own attempt lineage

dr-platform allocates each Item's attempt ordinal under its reconciliation
compare-and-swap and persists the resulting lineage. Callers map that ordinal
one-to-one into their content-scoped execution identity; for Whetstone it is
the Generation Run `attempt_index`. This keeps retry eligibility, concurrency
control, and provenance in one owner while allowing different Operations to
converge on the same domain execution for the same content and ordinal.
