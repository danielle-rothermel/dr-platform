# Accept paid-call overlap and outcome-linked undercount after cancellation

Cancellation of a platform-managed Whetstone execution is a logical lifecycle
transition, not a guarantee that an in-flight synchronous provider request was
aborted. Once DBOS records the workflow `CANCELLED` and dr-platform finalizes
the local Attempt as `CANCELLED`, an explicitly authorized replacement Attempt
may be created and enqueued without waiting for the prior provider call to
become observably quiescent. The older call may continue at the provider and
overlap the replacement, causing duplicate spend; the owner accepts that cost
risk for the pre-experiment cut.

This keeps the existing top-level-only, reference-aware, non-recursive
cancellation topology and avoids requiring abortable async semantics from every
provider adapter. Whetstone accounting remains outcome-linked: if the older
call returns after DBOS cancellation prevents the later Whetstone outcome
write, Whetstone may omit that call and its price rather than add a separate
provider-call ledger. The owner accepts this undercount because the discarded
outcome is not used, while provider-side receipts remain the billing source for
total spend. Inspection should distinguish logical DBOS cancellation from
proved upstream abort and surface known overlap when it can be inferred, but
the system must not claim Whetstone/export totals are complete for discarded
post-cancellation outcomes. DBOS replay payloads remain excluded from normal
inspection, export, and accounting truth.
