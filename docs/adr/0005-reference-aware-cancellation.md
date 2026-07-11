# Cancel shared executions only when exclusively referenced

Cancelling an Operation always cancels its own platform Attempts, but the
operator path physically cancels a DBOS workflow and its children only when no
other live Operation references that content-scoped execution. Shared
workflows continue for their remaining Operations; exclusively cancelled
executions remain sticky and require an explicit operator retry to replace.
This preserves both cost control and cross-Operation execution sharing rather
than letting either concern silently override the other.
