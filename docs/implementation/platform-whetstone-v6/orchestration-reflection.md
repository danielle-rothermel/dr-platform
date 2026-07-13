# Platform/Whetstone v6 orchestration reflection

## Outcome and scope

The implementation-first hard cut selected by [ADR 0022](../../adr/0022-implementation-first-final-hard-cut.md) reached `READY_WITH_EXTERNAL_GATES`. The Platform, Whetstone, and Unitbench stacks were implemented, reviewed at stack level, and repaired through their recorded top commits and PR stacks. This reflection concerns the orchestration method, not a replacement specification: v6 remains the reviewed immutable packet at [the effort index](../../planning/platform-whetstone-refactor/README.md), with its [plan](../../planning/platform-whetstone-refactor/v6/plan.md), [unified feedback](../../planning/platform-whetstone-refactor/v6/reviews/unified-feedback.md), contracts, and implementation matrices as the technical record.

Later fixes closed executable gaps without reversing accepted architecture,
ownership, identity, or hard-cut decisions. The outcome is therefore
convergence in implementation, not a claim that every live deployment gate has
already run.

## Convergence versus thrashing

The planning loop itself became non-convergent at v6: the reviewed packet
correctly stopped automatic successor generation and shifted to implementation
under ADR 0022. That was useful. It separated the question “is the frozen
architecture coherent enough to build?” from “does a real stack actually
perform its contracts?” The subsequent stack reviews found concrete executable
gaps and the repair work closed them without re-litigating the accepted model.

The risk was thrashing during the broad review periods. Some workers consumed
roughly 200k–340k tokens to reconstruct broad context, while late reviews
still found missing workers, non-executable publication, fresh-schema
persistence defects, and shallow tests. Those costs did not mean the work was
useless—the reviews exposed real faults—but they showed that accumulating
conversation and broad scope is not a substitute for an executable boundary
gate.

## What worked

- The immutable v6 packet gave implementation a stable target while the
  non-frozen matrices translated it into slice-level scenarios and exit gates.
- Coherent implementation phases and stack-level reviews exposed dependencies
  that per-file changes could not: Platform publication before Whetstone
  builders, then Whetstone output contracts before Unitbench readers.
- Fixing important cross-repository findings before moving downstream avoided
  baking invalid publisher assumptions into the reader stack. The recorded
  Platform/Whetstone publication fixes and the Unitbench fix artifacts are the
  evidence trail, rather than this document.
- The later backlog pass was disciplined: useful Unitbench P3 failures were
  closed, while no automatic re-review multiplied the loop.
- Keeping real live-gate outcomes distinct from local verification preserved
  credibility. The implementation matrices record the MotherDuck, Neon, DBOS,
  and browser boundaries without copying secrets or treating a mock as proof.

## What failed or cost too much

Green shallow tests were misleading. Earlier suites could pass while the
integration target was empty, a worker did not remain alive, persisted records
did not fit the fresh schema, publication specs had no executable builders,
or reader tests only inspected query text and empty fixtures. These are not
minor coverage-quality issues: each allowed the hard cut to appear complete
without exercising the contract that another repository consumed.

Skipping per-PR reviews was a deliberate trade-off. It saved expensive
repetition and kept the implementation moving, but it raised the importance of
non-empty phase gates and one high review after the full repository stack. The
right response to the late findings was targeted repair and a useful-backlog
pass, not a reflexive new review round after every commit.

Cross-repository order mattered more than local velocity. A late Platform or
Whetstone interface correction invalidates downstream reader confidence; it is
cheaper to repair it before beginning the dependent stack. Conversely,
upstream work should not wait for a downstream review to prove a contract that
can be made executable locally first.

## Decision, gate, and economics lessons

Owner questions need batching for evidence, then serialization for choice.
The two remaining concerns are referenced—not decided here—in the P5
cancellation matrix: the second cancellation-request representation and the
late-enqueue absence/successor-hazard protocol. A fresh orchestrator should
ask one owner question at a time while safe diagnostics continue.

External gates deserve the same precision. An absent MotherDuck endpoint,
unreachable CI Postgres service, missing browser-service worktrees, and the
fact that draft PRs are unmerged are external handoff concerns, not reasons to
invent success. Their current form and smallest next checks live in the final
handoff and P7 matrix.

The process changed from broad conversational investigation to artifact-led
dispatch: a thin root, fresh self-contained workers, restricted skills/tools,
one worker at a time under credit pressure, final-artifact consumption instead
of trace sampling, and end-to-end worker ownership through test, commit, push,
and PR update. That change protects root context; it does not forbid workers
from using the model tokens needed for a coherent task. The durable operating
rules are maintained in the [Codex token-efficiency guide](../../../../dotfiles/agents/config/codex/README.md).

## Recommendations for the next effort

1. Create executable scenario matrices before implementation and require every
   slice to preserve a non-empty integration gate.
2. Use Terra low for small durable documentation, Terra medium for a coherent
   implementation phase, and one Sol/high review only after each repository
   stack is integrated.
3. Make worker prompts self-contained and read only their final artifacts by
   default; treat verbose traces as targeted debugging evidence.
4. Sequence publisher and consumer work by tested interface dependency, and
   repair material cross-repository findings before the next stack.
5. Keep owner decisions explicit and one-at-a-time; classify unavailable
   infrastructure as an external gate with an exact prerequisite rather than
   weakening verification.
