# P5 cancellation and late-enqueue compensation matrix

This matrix is the executable acceptance contract for P5. It specializes the
frozen v6 cancellation contract and closes implementation risk A2 without
changing the normative packet.

P5 lands in two dependency-ordered PRs. P5a owns logical cancellation,
reference/topology safety, terminal races, and foreign provenance (C, R, and F
rows). P5b owns invalidated-Claim compensation and delayed-enqueue repair (A2
rows). Neither PR claims the full P5 exit gate until both have landed.

## Logical cancellation and replay

| ID | Starting state | Event | Expected result |
| --- | --- | --- | --- |
| P5-C01 | Nonterminal current Attempts | First cancellation request | Persist immutable request identity and operator intent; mark local Attempts `CANCEL_REQUESTED`; invalidate every outstanding Claim |
| P5-C02 | Same cancellation request and payload | Exact replay | Return the stored plan/results without repeating a successful DBOS call |
| P5-C03 | Same request ID with unequal payload | Replay | Typed idempotency conflict; no lifecycle mutation |
| P5-C04 | Already-sticky local Attempt | Different request | Record `ALREADY_CANCELLED`; never reactivate or rewrite terminal history |
| P5-C05 | One DBOS call fails in a multi-Item request | Finalize results | Preserve logical intent; successful results remain durable and the Operation stays `CANCELLING` until every result is resolved or acknowledged |

P5-C04 is isolated pending an owner decision. The frozen schema has one
cancellation request/result tuple per Attempt, so it cannot preserve the first
request's exact replay while also durably recording a later request. P5a fails
closed with a typed idempotency conflict and no lifecycle mutation. Completing
C04 requires either a durable cancellation-request ledger, explicitly
non-durable `ALREADY_CANCELLED` responses for later requests, or acceptance of
the fail-closed behavior.

## Reference and topology safety

| ID | Locked reference set | DBOS observation | Expected result |
| --- | --- | --- | --- |
| P5-R01 | Exclusive top-level workflow | Live | Call `cancel_workflow(workflow_id, cancel_children=False)` once and persist `DBOS_CANCELLED` |
| P5-R02 | Another registered nonterminal current Attempt references the workflow | Any | Persist `SKIPPED_SHARED`; issue no physical cancellation |
| P5-R03 | Reference is created while cancellation races | Same workflow lock | Racer either precedes the predicate and is observed, or waits and fails closed on the committed guard |
| P5-R04 | Inspector finds a child/descendant | Topology drift | Persist typed failure, leave Operation incomplete, and never cancel parent or descendants |
| P5-R05 | DBOS was terminal before cancellation | `SUCCESS` or `ERROR` | Terminal execution truth wins; preserve intent with `OBSERVED_TERMINAL` |
| P5-R06 | Cancellation wins before a later provider/step return | `CANCELLED` | Finalize local Attempt `CANCELLED`; later non-cancel observations cannot rewrite it |

## Invalidated enqueue Claims and A2 repair

| ID | Claim/enqueue schedule | Repair event | Expected result |
| --- | --- | --- | --- |
| P5-A201 | Claim invalidated before `enqueue_call_started_at` | Claimant wakes | Claim cannot call DBOS and needs no compensation |
| P5-A202 | Call-started claimant enqueues after invalidation and loses outcome CAS | Cooperative claimant repair | Insert or exact-reload compensation by `(item_id, attempt, claim_id)` and recheck exclusivity before any cancel |
| P5-A203 | Claimant dies after enqueue before losing outcome CAS | Bounded replay | Discover every invalidated call-started Claim and create/replay its exact compensation without claimant cooperation |
| P5-A204 | Multiple stale Claims name one deterministic workflow | Concurrent replay | Workflow lock serializes repair; every Claim resolves independently and at most one exclusive cancel is required |
| P5-A205 | Another live reference appears before repair | Compensation exclusivity check | Resolve `SKIPPED_SHARED`; issue no cancellation and leave the other Attempt live |
| P5-A206 | Workflow absent below grace/count | Replay | Keep compensation unresolved and block new references |
| P5-A207 | Workflow absent through both grace and count | Replay | Resolve durable `NO_WORKFLOW_FOUND`; unblock new-reference creation |
| P5-A208 | DBOS commit appears after `NO_WORKFLOW_FOUND` | Periodic terminal-MISSING re-observation | Create a bounded successor hazard/compensation signal and escalate; never rewrite the terminal Attempt or silently accept the late work |
| P5-A209 | Compensation already resolved | Exact replay | No DBOS call and no mutation |
| P5-A210 | Existing compensation identity or workflow provenance differs | Replay | Typed integrity conflict; remain degraded and fail closed |

P5-A206 through P5-A208 are isolated pending an owner/schema decision. The
frozen compensation ledger has no durable grace/count observation fields, and
resolved rows are immutable with no append-compatible successor identity for
a workflow that appears after `NO_WORKFLOW_FOUND`. P5b therefore leaves an
absent call-started hazard `PENDING`, keeps health/reference creation blocked,
and continues bounded replay. Completing these rows requires either durable
absence observations plus a successor hazard ledger or a DBOS-side quiescence
guard; it cannot be inferred safely from repeated absence alone.

## Foreign cancellation provenance and retry seam

| ID | Starting state | Observation/request | Expected result |
| --- | --- | --- | --- |
| P5-F01 | New Operation links a workflow uniquely cancelled elsewhere | Reconcile | Local Attempt becomes sticky `CANCELLED` with originating Operation and request provenance |
| P5-F02 | Foreign cancellation provenance is ambiguous | Reconcile | Integrity failure; do not invent retry eligibility |
| P5-F03 | Confirmed `OPERATOR_CANCEL_RETRY` cites local or foreign request | Request next Attempt | Existing P4 reason/source and maximum-attempt rules create at most one successor |

## Locking and observability

Every create/link/cancel/compensate path acquires the Export Barrier writer
lock, lexical workflow advisory locks, ascending Operation rows, Items, and
Attempts before Claim/request/compensation rows. DBOS calls occur only after
the intent transaction releases row locks. Health remains degraded while any
compensation or cancellation result is unresolved or failed.

## Exit gate

P5a is complete when C01-C03, C05, R, and F scenarios pass, with C04 recorded
as the isolated owner/schema decision above. P5b is complete when A201-A205,
A209, and A210 pass, with A206-A208 recorded as the isolated owner/schema
decision above. P5 as a whole is complete when
all scenarios above pass against a fresh schema; recursive
DBOS cancellation is statically and dynamically absent; exact replay is
idempotent; a blocked delayed DBOS commit closes A2 through bounded detection
and escalation; cancellation cannot strand or erase a durable Claim identity;
and one fresh light review finds no unresolved cancellation, reference,
topology, compensation, or terminal-race blocker.
