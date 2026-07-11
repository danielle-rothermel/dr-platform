# Preserve attempts in an append-only ledger

dr-platform stores every Item attempt in `<prefix>_item_attempts`, keyed by
Item and platform-owned attempt ordinal, while the Item points to its current
attempt. An attempt row may advance until terminal, but terminal attempts are
never reused or replaced by later retries; this keeps application-owned retry
provenance available for reconciliation, inspection, export, and future DBOS
retention at the cost of one additional operational table.
