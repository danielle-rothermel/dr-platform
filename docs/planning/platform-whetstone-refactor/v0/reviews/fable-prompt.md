# Round 1 adversarial plan review — DESIGN COHERENCE lens (fable)

You are reviewing a refactor plan adversarially. Your job is to find where
the plan is **wrong, incomplete, or self-contradictory**. A review that
returns "looks good" is a failed review. You are the design-coherence
reviewer; a second reviewer covers line-level code audit — you own the
architecture, flows, vocabulary, and doc-drift angles, but you must still
ground every finding in the actual code.

## Inputs (read all of these first)

- The spec: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/plan.md`
- Glossaries: `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md`,
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Code under review: `/Users/daniellerothermel/drotherm/repos/dr-platform/`
- Impact surface: `/Users/daniellerothermel/drotherm/repos/whetstone-ai/`
  (read-only context for what the kernel changes land on)

MANDATORY: both repos have `graphify-out/graph.json`. Orient with
`graphify query "<question>"` / `graphify explain "<concept>"` /
`graphify path "<A>" "<B>"` before reading raw files; read raw files after.

## Round 1 scope

**dr-platform changes and everything impacted by them.** Whetstone-internal
redesign is round 2; audit whetstone only where a kernel change lands on it.

## What to interrogate (non-exhaustive — find what we missed)

1. **Principle vs detail incongruities.** The spec claims "one happy path per
   verb", "domain-agnostic kernel", "key-vs-id rule", "model boundary rule",
   "two-plane data model". Hunt for places where the spec's own details
   violate its principles — or where surviving code will.
2. **Flow completeness.** Walk the submission flow and the worker lifecycle
   flow end-to-end as the spec defines them. Missing steps? States an Item
   can reach that no flow covers (crashed mid-claim, raced enqueue,
   re-submitted while active, throttled forever)? Does the retry-via-attempt
   design compose with observability/progress/await_operation?
3. **Vocabulary drift.** Does the rename table miss names? Do the CONTEXT.md
   glossaries conflict with the spec or with each other? Terms in code the
   glossary should own but doesn't (e.g. claim, lease, attempt, watermark,
   sink, throttle domain vs throttle key)?
4. **The export/two-plane design.** Is the kernel/client projection split
   actually domain-agnostic? Is watermark state in Postgres coherent with
   "DuckDB is rebuildable"? Detail-store sampling: does anything downstream
   assume completeness?
5. **What the plan forgot.** Out-of-date docs/names/design in scope that no
   spec section addresses. Surviving modules (progress, observability, jsonl,
   dbos_config) — do they fit the final state or were they just not looked at?
6. **Overcutting.** Places where a deletion removes something load-bearing
   or where "hard cut" destroys information the new design silently needs.

## Constraints

- Read-only review: do not modify code, do not run git commands, do not run
  test suites. graphify queries, static reading, and fast greps only.
- Every finding must cite file:line (or spec-section) evidence.

## Output

Write your findings to
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/reviews/fable-findings.md`
in this format, ranked most severe first:

```markdown
# Round 1 findings — fable (design coherence)

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Spec section:** §x.y (or "unaddressed by spec")
- **Evidence:** path:line — what the code/doc actually shows
- **Consequence:** what goes wrong if the plan proceeds as written
- **Suggested change:** concrete revision to the spec
```

End with a one-paragraph verdict: the three findings most likely to change
the plan, and anything you could not verify (with why).
