# Persist every enqueue Claim identity append-only

dr-platform records each enqueue Claim and its one permitted enqueue-call
boundary as an immutable row keyed by `(item_id, attempt, claim_id)`. An
Attempt may keep only a nullable pointer to the current Claim; expiry,
replacement, invalidation, and terminalization never erase older Claim
identity or call-start facts. Enqueue compensation references the exact Claim
row with the same key, so claimant death, Lease reuse, several stale claimants,
and replay converge without inventing provenance. This adds a small durable
ledger instead of redefining the already accepted Claim-keyed compensation
identity or retaining mutable Claim fields that cannot represent history.
