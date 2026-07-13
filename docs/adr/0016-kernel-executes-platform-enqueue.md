# Let dr-platform execute enqueue behind its interface

Callers register a typed target—workflow, content identity, arguments, opaque
recipe producer, registration hook, and failure classifier—under an immutable
key/version/contract digest. Operations persist that reference, and every
lifecycle driver resolves it through the same startup registry; missing or
conflicting registration fails closed. dr-platform owns Claim/Lease,
workflow attributes, queue options, DBOS enqueue, outcome persistence, and
reconciliation. Every Claim that reaches the enqueue-call boundary is recorded
append-only before the external call; later replacement or Attempt
terminalization changes only the current pointer and cannot erase compensation
identity. This removes the shallow callback protocol that allowed each
application to reimplement idempotency and retry mechanics while keeping
domain identity and workflow code outside the kernel, including after process
restart.
