# Platform/Whetstone v6 implementation risk register

Every risk starts open. A risk closes only when the owning PR links its executable matrix, implementation decision, focused tests, observability/escalation behavior, and fresh-agent review. A generic full-suite pass is insufficient.

| Risk | Owner | Required closure | Status |
| --- | --- | --- | --- |
| A1 — authorized `PARTIAL` acceptance | W4-W5 | Truthful selected-partial-policy state, immutable authorization and digest, exact run/Attempt proof, provenance, promotion/current-read fixtures. | Open |
| A2 — delayed enqueue after `NO_WORKFLOW_FOUND` | P3-P5 | Late-enqueue successor/escalation, periodic re-observation, claimant-success path, exclusivity re-check, blocked-enqueue delayed-commit fixture. | Open |
| A3 — independent application/DBOS source cuts | P0, P7, W7 | Truthful coordinates/timestamps, deterministic bound, transition-between-captures fixture, measured skew, fail-closed outcomes. | Open |
| L1 — exact populated-only predicate | P0, W3 | Pinned database character semantics, one ctype-independent boundary, aligned Python behavior, ASCII and non-ASCII corpus. | Open |
| L2 — plural membership versus accepted run | W3-W5 | Complete eligible Manifest membership, separately derived pinned winner, `SUPERSEDED_GENERATION` provenance. | Open |
| V1 — pinned-bundle survival or typed loss | P7, W7 | Active-pin retention, cleanup race, integrity loss, `PINNED_BUNDLE_GONE`, explicit COPRO restart. | Open |
