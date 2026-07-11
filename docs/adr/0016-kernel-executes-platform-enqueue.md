# Let dr-platform execute enqueue behind its interface

Callers provide a typed target—workflow, content identity, arguments,
registration hook, and failure classifier—but dr-platform owns Claim/Lease,
workflow attributes, queue options, DBOS enqueue, outcome persistence, and
reconciliation. This removes the shallow callback protocol that allowed each
application to reimplement idempotency and retry mechanics while keeping
domain identity and workflow code outside the kernel.
