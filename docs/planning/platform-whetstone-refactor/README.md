# Platform and Whetstone refactor

This effort plans the hard-cut refactor across `dr-platform`, `whetstone-ai`,
and the affected `unitbench` boundary before intensive experiments begin.

**Current version:** v0 (`reviewed`)

**Tracker map:** not created. Until Wayfinder is introduced, this index and the
review packet are the navigation surface; unresolved decisions remain in the
unified feedback.

## Versions

| Version | Status | Review scope | Plan | Unified feedback |
| --- | --- | --- | --- | --- |
| v0 | reviewed | `dr-platform` plus immediate downstream impacts | [plan](v0/plan.md) | [feedback](v0/reviews/unified-feedback.md) |

v0 is immutable. When review feedback is accepted, create `v1/plan.md` by
copying v0, mark v0 `superseded`, and apply the revisions only in v1 while it
is `draft`.

## Process

Versions move through `draft` → `in-review` → `reviewed` → `superseded`.
Only `draft` is mutable. Each version's `reviews/` directory contains the exact
prompts, findings, and synthesis that evaluated that plan.

The issue tracker is the live decision store when a Wayfinder map exists. The
version packet is a historical snapshot. [`CONTEXT.md`](../../../CONTEXT.md)
and `docs/adr/` remain living canonical docs outside version packets. Reports,
prototypes, and future handoffs stay temporary; their durable conclusions are
copied into the tracker, active draft, glossary, or an ADR.

The review sequence anticipated by the original session was:

1. `dr-platform` and immediate impacts (complete in v0)
2. `whetstone-ai` and immediate impacts
3. the full repository constellation and operational dependencies

Those scopes may be adjusted before creating each successor; they are not a
reason to mutate an already reviewed version.

## Historical provenance

The [v0 session handoff](v0/session-handoff.md) is retained only as provenance
for the original planning session. It is not the current process contract.
