# Prompt: normalize the v4 plan into a modular review packet

Work in `/Users/daniellerothermel/drotherm/repos/dr-platform` and restructure
the active v4 Platform and Whetstone refactor draft into a bounded modular plan
packet. This is a structure-preserving planning-doc change only. Do not change
implementation code, tests, dependencies, runtime configuration, canonical
ADRs/glossaries, historical v0-v3 packets, or the effort's review lifecycle.
Do not commit.

## Authoritative source

The exact pre-normalization v4 monolith is preserved read-only at:

`/private/tmp/platform-whetstone-v4-monolith-before-normalization.md`

Its SHA-256 is:

`bfbf13016cca60cd29241072f27aa746e15db8776d87c0e84574d4be7dcc6a59`

Verify that digest before editing. Treat that snapshot—not a partially edited
working file—as the complete source contract for semantic-preservation checks.

Also read:

- `docs/agents/planning.md` and `docs/agents/domain.md`;
- `docs/planning/platform-whetstone-refactor/README.md`;
- `docs/planning/platform-whetstone-refactor/v4/planning-prompt.md`;
- both existing v4 review prompts under `v4/reviews/` for current terminology
  and closure expectations; and
- `/Users/daniellerothermel/drotherm/repos/dotfiles/agents/skills/orchestrate-plan-review/references/artifact-contract.md`
  for the modular packet manifest contract.

## Required output shape

Create this packet:

```text
v4/
├── plan.md
├── plan-manifest.json
├── contracts/
│   ├── platform.md
│   ├── whetstone.md
│   ├── publication.md
│   └── delivery.md
├── traceability.md
├── planning-prompt.md
├── normalization-prompt.md
└── reviews/
    ├── codex-prompt.md
    └── fable-prompt.md
```

Do not edit the two review prompts during this task. A later worker will
refresh them against the normalized packet.

Write `plan-manifest.json` with `version`, `entrypoint`, and ordered
`documents` exactly as defined by the artifact contract. Declare `plan.md` and
the four files under `contracts/` as `normative`. Declare `traceability.md` as
`traceability`. Do not declare working prompts as plan documents.

## Document responsibilities

### `plan.md`

Make this the concise normative entrypoint, aiming for 250-400 lines. Include:

- status, scope, goals, and hard-cut assumptions;
- the unified invariants and accepted owner policies;
- a repository/domain ownership map;
- the one end-to-end lifecycle narrative;
- the ordered implementation/cutover phases and blocking gates at summary
  level;
- a normative-document map linking every contract; and
- the review protocol.

Do not repeat detailed schemas, field tables, state machines, crash matrices,
or historical finding-by-finding dispositions here.

### `contracts/platform.md`

Own the complete dr-platform kernel contract: vocabulary and public/API
crosswalks, schema and lifecycle crosswalk, scheduling, submission,
registration, Item/Attempt/Operation state, retry, cancellation, DBOS
correlation, pacing, inspection, control, telemetry, and platform hygiene.

### `contracts/whetstone.md`

Own Whetstone identity and boundary behavior, generation/scoring Operations,
accepted Generation Run selection, Experiment-acceptance schema/currentness,
outcome and cost truth, tests, and deletion/rename work.

### `contracts/publication.md`

Own export and publication contracts, bundle boundaries, fences/leases,
Analysis and Detail inventories, Unitbench two-plane readers, confidentiality,
local/deployed compute policy, and destination failure behavior.

### `contracts/delivery.md`

Own migration/cutover order, phase dependencies, transaction/concurrency/crash
verification, pre-experiment acceptance gates, repository verification,
rollback, failure handling, and explicit deferrals.

### `traceability.md`

Keep review provenance non-normative. Include:

- v0-v3 incorporation/disposition tables;
- reviewer disagreements and synthesis opinions;
- v2-v4 owner-decision provenance;
- the revision log; and
- a source-coverage appendix mapping every heading from the preserved monolith
  to one destination heading, with `moved`, `merged`, or `removed-duplicate`
  disposition and a short reason.

Selected owner decisions must still appear normatively in the relevant plan or
contract. Traceability owns why and reviewer provenance, not the only statement
of runtime behavior.

## Preservation constraints

- Preserve every normative v4 behavior, invariant, type/field requirement,
  state transition, transaction/CAS/lock/fence boundary, failure disposition,
  repository responsibility, test, live gate, and deletion requirement.
- Do not weaken a requirement while summarizing it.
- Remove repeated prose only when one authoritative destination retains the
  complete contract and all former locations link to it.
- Do not resolve a source contradiction silently. Preserve the stricter
  enforceable contract and record the ambiguity in traceability for the
  preservation auditor.
- Keep normative documents self-contained; they may link to each other but may
  not rely on v0-v3 packets to define current behavior.
- Keep traceability out of the fresh-review read order. It is read only during
  closure/provenance review.
- Preserve valid relative links or update them for the new file depth.
- Keep v4 `draft`. Do not change the effort index lifecycle, create a review
  baseline, create findings, or launch reviewers.

## Verification

Before finishing:

1. verify the source snapshot SHA-256;
2. validate `plan-manifest.json` as strict JSON and verify every declared path
   exists inside v4;
3. confirm every source heading has exactly one coverage disposition;
4. search the normalized packet for stale claims that the plan is monolithic
   or that review prompts are already issued;
5. check relative Markdown links;
6. confirm historical v0-v3 artifacts and canonical docs did not change;
7. run `git diff --check` scoped to v4; and
8. report per-file line/word counts, the total normative word count, every
   removed duplicate, every recorded ambiguity, and all changed files.

Do not optimize for the smallest total word count at the expense of contract
loss. Optimize for bounded documents, one authoritative home per contract,
and a reviewable fresh normative set.
