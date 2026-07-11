# Preserve attempts in an append-only ledger

dr-platform stores every Item attempt in `<prefix>_item_attempts`, keyed by
Item and platform-owned attempt ordinal, while the Item points to its current
attempt. An attempt row may advance until terminal, but terminal attempts are
never reused or replaced by later retries; this keeps application-owned retry
provenance available for reconciliation, inspection, export, and future DBOS
retention. Caller-requested creation is recorded separately in an idempotent
next-Attempt request ledger, and a created Attempt links that request while the
terminal source remains immutable. This adds operational tables but preserves
both the execution history and the authorization history that advanced it.
The request's optional `max_attempts` may only tighten the immutable Operation
RetryPolicy; the ledger persists both requested and effective bounds so replay
and exhaustion remain reproducible.
