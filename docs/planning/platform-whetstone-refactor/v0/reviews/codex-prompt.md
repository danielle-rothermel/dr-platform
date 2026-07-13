# Round 1 adversarial plan review — CODE AUDIT lens (codex)

You are reviewing a refactor plan adversarially. Your job is to find where
the plan is **wrong, incomplete, or self-contradictory** by auditing the
actual code it claims to describe. A review that returns "looks good" is a
failed review. You are the code-level auditor; a second reviewer covers
design coherence — stay close to the code.

## Inputs (read all of these first)

- The spec: `/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/plan.md`
- Glossaries: `/Users/daniellerothermel/drotherm/repos/dr-platform/CONTEXT.md`,
  `/Users/daniellerothermel/drotherm/repos/whetstone-ai/CONTEXT.md`
- Code under review: `/Users/daniellerothermel/drotherm/repos/dr-platform/src/dr_platform/` and `tests/`
- Impact surface: `/Users/daniellerothermel/drotherm/repos/whetstone-ai/src/whetstone/` (read-only context for what breaks)

## Round 1 scope

**dr-platform changes and everything impacted by them.** Whetstone-internal
redesign is round 2; only audit whetstone where a dr-platform change lands on
it. unitbench is round 3 except where a dr-platform decision (export sinks,
two-plane split) is directly contradicted by reality.

## What to audit (non-exhaustive — find what we missed)

1. **Verify every factual claim in the spec against the code.** Claimed dead
   code that actually has callers; claimed call-site inventories that are
   wrong or incomplete; claimed behavior (e.g. "dedup_enqueue treats any
   existing status as skip", "empty submission records ERROR") that the code
   contradicts.
2. **Deletion blast radius.** For each deletion in spec §1.1/§1.2: enumerate
   what actually references it (both repos, tests included). Find references
   the spec doesn't account for.
3. **The new designs' correctness.** EnqueueTarget + library-minted
   workflow_id from (operation_key, item_id, attempt): does the attempt
   counter interact correctly with the claim/CAS loop and DBOS recovery?
   Priority derivation from digests: collisions, range mapping, determinism.
   Watermark-based incremental export: what breaks it (updated rows, deleted
   rows, clock skew, concurrent writes during export)?
4. **Schema reset holes.** Lease columns vs the CAS logic in submission.py;
   the attempt column vs existing retry logic; constraints that must move.
5. **Interfaces that stay confusing or broken even after the plan.** Anything
   in the surviving API that is awkward, inconsistent, or underspecified.
6. **DBOS contract risks.** Check the pinned dbos version's actual APIs for
   priority queues, SetEnqueueOptions, workflow status introspection
   (read the installed package in .venv if present).
7. **Out-of-date names/docs/design in scope that the plan does not address.**

## Constraints

- Read-only review: do NOT modify code, do NOT run git commands, do NOT run
  test suites or any command expected to exceed ~2 minutes. Static reading
  and fast greps only.
- Every finding must cite file:line evidence. No vague "consider X".

## Output

Write your findings to
`/Users/daniellerothermel/drotherm/repos/dr-platform/docs/planning/platform-whetstone-refactor/v0/reviews/codex-findings.md`
in this format, ranked most severe first:

```markdown
# Round 1 findings — codex (code audit)

## F1. <one-line defect statement>
- **Severity:** blocker | major | minor
- **Spec section:** §x.y (or "unaddressed by spec")
- **Evidence:** path:line — what the code actually shows
- **Consequence:** what goes wrong if the plan proceeds as written
- **Suggested change:** concrete revision to the spec (or code)
```

End with a one-paragraph verdict: the three findings most likely to change
the plan, and anything you could not verify (with why).
