# Bound compatibility across independently captured sources

One export run owns one identity while preserving truthful, independent source
cuts for application Postgres and DBOS. Persist each source's own coordinate
and capture timestamp under that export-run identity. An application-derived
`snapshot_seq` remains an application coordinate only: assigning or comparing
the same value across bundles must never claim that independently captured
application and DBOS data came from an identical source cut.

Every cross-source export run declares a compatibility bound as explicit
implementation configuration and contract. Before the export implementation
slice lands, its state-machine and scenario matrix must pin the coordinate
fields, timestamp semantics, comparison rule, configured bound, and failure
behavior. The owner has not selected a numeric tolerance in this decision.
Readers and operators receive the declared bound and measured cross-source
skew rather than an inferred same-cut claim.

Publication fails closed when the exporter cannot establish that the captured
application and DBOS coordinates satisfy the declared bound. This cross-source
gate does not combine their publication transactions: each bundle retains its
own atomic staging and promotion boundary, and every destination retains its
existing Lease and monotonic fencing checks.
