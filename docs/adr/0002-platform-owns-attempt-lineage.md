# Let dr-platform own attempt lineage

dr-platform is the only authority that allocates each Item's attempt ordinal
and persists the resulting lineage. Automatic retry policy, a caller-owned
domain outcome, or an explicit operator decision may request the next Attempt,
but dr-platform applies the compare-and-swap, idempotency, provenance, and
maximum-attempt policy. Callers map the allocated ordinal one-to-one into their
content-scoped execution identity; for Whetstone it is the Generation Run or
Score Attempt index. This avoids both a second caller retry counter and the
false classification of domain failure as platform execution failure.
