# Cancel shared executions only when exclusively referenced

Cancelling an Operation always cancels its own platform Attempts, but the
operator path sends a DBOS cancellation request for a directly referenced
workflow only when no other live Operation references that content-scoped
execution. The
pre-experiment topology prohibits DBOS child workflows beneath
platform-managed executions, and cancellation always uses
`cancel_children=False`; recursive DBOS cancellation cannot enforce the
platform reference predicate for descendants. Shared workflows continue for
their remaining Operations, while exclusively cancelled executions remain
sticky and require an explicit policy-gated next-Attempt request to replace.
This preserves cross-Operation execution sharing without introducing a
descendant-graph locking subsystem. Per ADR 0019, DBOS cancellation is not
upstream provider-call abort and may not prevent duplicate spend when a
replacement overlaps a continuing synchronous call.

A new Operation that links an execution cancelled through another Operation
records that foreign cancellation provenance and remains eligible only for a
new, locally confirmed operator retry. Cancellation intent is also separate
from execution outcome: an already-terminal DBOS `SUCCESS` or `ERROR` wins the
finalization race and is preserved beside an `OBSERVED_TERMINAL` cancellation
disposition; a committed local terminal row is never rewritten later.

Cancellation also prevents new enqueue after intent: claim eligibility
excludes cancellation, intent invalidates outstanding Claims, and a missing
DBOS row is recorded as `NOT_ENQUEUED` rather than a delivered cancellation.
If an invalidated claimant nevertheless creates the DBOS row before observing
its lost CAS, claimant and replay compensation take the standard workflow-lock
then Operation-row order and re-evaluate the same reference-exclusivity
predicate before physical cancellation. Another registered nonterminal
reference resolves the exact Claim-keyed compensation as `SKIPPED_SHARED`
without a DBOS call. Repeated absence through the bounded missing-workflow
window resolves `NO_WORKFLOW_FOUND`; both dispositions clear the invalidated-
Claim link hazard. Per ADR 0021, every expired or invalidated call-started
Claim remains append-only so compensation and replay retain their exact key.
