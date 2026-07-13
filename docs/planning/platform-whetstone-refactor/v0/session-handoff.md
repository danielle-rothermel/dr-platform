# Historical handoff — Platform Hard Cut planning

> Archived with v0 as session provenance. Future handoffs are temporary working
> artifacts; durable process state lives in the [effort index](../README.md).

**Written:** 2026-07-08, end of the grilling/spec session.
**You are:** a fable agent continuing Danielle's planning → review-incorporation →
issue-creation → orchestration process for the joint dr-platform + whetstone-ai
hard-cut refactor (+ unitbench two-plane swap).

## Where things stand

1. A full grilling session resolved every design decision. The canonical
   artifact is the spec: `docs/planning/platform-whetstone-refactor/v0/plan.md` (v0).
   Read it first, in full. Glossaries: `CONTEXT.md` here and in
   `../whetstone-ai/CONTEXT.md` (both created this session; maintain them
   inline as decisions evolve — domain-modeling discipline).
2. **Before anything else happens**, Danielle is merging two external chunks:
   the dr-code refactor and "the unitbench crazy stack". The spec was written
   against the pre-merge tree, so expect some file/line facts to be stale —
   trust the review findings over the spec's inventory claims.
3. **Then she manually launches round-1 review agents** (she runs them, not
   you): prompts already written at
   `docs/planning/platform-whetstone-refactor/v0/reviews/codex-prompt.md`
   (code-audit lens, gpt 5.5 fast) and `reviews/fable-prompt.md`
   (design-coherence lens, fable). They write to
   `reviews/codex-findings.md` / `reviews/fable-findings.md`.
4. **Your first job when she returns:** triage round-1 findings.

## The process contract (do not deviate without asking)

- Three adversarial review rounds: R1 = dr-platform + impacts (prompts done),
  R2 = whetstone-ai + impacts, R3 = whole constellation (incl. dr-providers/
  dr-serialize pinning, CI PAT for private git deps, secrets story, any other
  on-disk consumers of whetstone's tables).
- Findings triage: absorb mechanical/factual corrections directly into the
  spec (bump version, append revision log); **any finding that changes a
  decision she made goes back to her as a question before revising**.
- Grilling style she expects: one question at a time, each with a recommended
  answer, decisions are hers, facts you look up yourself. She often interrupts
  with refinements mid-flow — treat those as decisions and capture them.
- Write R2/R3 prompts only after the prior round is incorporated (mirror the
  R1 prompt structure: adversarial framing, severity-ranked findings,
  file:line evidence, fixed output paths, "'looks good' is a failed review").
- After R3: write 5 ADRs (dr-platform: two-plane analysis, adaptive backoff
  over DBOS limiters, priority-replaces-fairness, library-executed enqueue;
  whetstone: fresh tables/no migration — format in
  `~/.claude/skills/domain-modeling/ADR-FORMAT.md`), then file GitHub issues,
  then write two orchestration prompt files.
- Issues: filed per repo where the work lands (dr-platform, whetstone-ai,
  unitbench via `gh`), chunked by workstream, dependency ordering stated in
  each body, labeled `ready-for-agent` (see `docs/agents/triage-labels.md`).
- Orchestration prompts (written to local files, she launches): one codex
  orchestrator for all non-frontend work — **no git operations in codex
  tasks** (sandbox blocks .git writes; branches/commits happen outside),
  bounded verification (exact commands, expected runtime, abort rule),
  `cd` into the target repo per task; one fable orchestrator for the
  unitbench frontend track (read-layer.ts two-plane routing + page changes).

## Decision log (all locked; spec is canonical — this is the why-summary)

1. Deliverable: full refactor spec → adversarial review rounds → GH issues →
   two orchestration prompts.
2. Naming shim hard cut: PlatformNaming/ItemIdentity deleted; fixed canonical
   names; one defaulted `prefix` knob survives (apps share a DB by prefix).
3. Vocabulary: Operation/Item, "batch" dies; key-vs-id rule; protocol
   `item_id()` → `item_key()`.
4. Dedup contract: only ACTIVE/ENQUEUED/SUCCESS block; terminal failures
   retry via attempt increment → fresh workflow_id (fixes a real bug: the
   old truthiness check blocked failed workflows forever).
5. artifacts.py: deleted (zero consumers).
6. One happy path per verb (her explicit principle): dedup_enqueue goes
   private; per-operation EnqueueTarget, library-executed enqueue, library
   mints workflow_ids and always sets priority.
7. Fairness → integer priority (her idea): digest-derived deterministic
   default, explicit override; DBOS priority_enabled queues; whole fairness
   module + order_key die.
8. Pacing: adaptive backoff + holds stay THE mechanism; queue-per-throttle-
   domain convention; no DBOS static limiters; in-workflow durable sleep OK.
9. Data (her framing, refined together): duckdb core dep; one export verb,
   incremental watermark + full_rebuild; kernel exports its own + DBOS system
   tables; client augmentation hook for domain projections; **two planes, not
   full cutover** — MotherDuck = analytical plane, Neon = detail plane
   (row/log viewers, all-then-sampled), one pipeline two sinks; publish CLI
   + published_* retired; curation becomes a flag/view.
10. Dataclass boundary rule: frozen Pydantic at parse/persist boundaries;
    frozen slotted dataclasses for in-memory value/config/callable-carriers.
    (Overrides her global always-BaseModel rule for these repos.)
11. Schema reset: new single 0001, lease as real columns, JSONB caller-owned,
    priority column, enum collapse, stamp path deleted.
12. whetstone: analysis/ **deleted, not rebuilt** (her call, more aggressive
    than recommended — core analysis lives in unitbench, one-offs in marimo);
    migration/ + queue_worker + fair_order_key + prediction_projection +
    dr_dspy history deleted; frozen dr-dspy strings renamed; stable-ID scheme
    kept minus legacy-byte constraints; domain nouns kept, Item/Operation
    mapping only at the boundary; schema/backoff tests are expected
    casualties.
13. Spec/issues layout: one canonical spec in dr-platform; issues per repo.
14. ADRs: all five approved.

## Facts worth not re-deriving

- unitbench has ZERO code dependency on dr_platform today; its
  `docs/workbench/projections.md` was an aspirational consumer contract (now
  superseded by the two-plane design — needs rewriting).
- whetstone is the only real consumer; usage is concentrated in
  `src/whetstone/platform/`; unused kernel surface was: artifacts, fairness,
  projections modules + ~40 individual symbols.
- Architecture reviews (full reports live in this session's history, key
  conclusions baked into the spec): dr-platform's layering is clean — this is
  a deletion/hardening job, not a restructure. Keep: callable/Protocol
  boundary, pure status state machine, single-table backoff composition,
  deterministic jitter.
- Repo rules: run `graphify query` before reading source (both repos have
  graphs); `graphify update .` after code changes; dr-platform README is
  stale (claims empty skeleton) — rewrite is in the spec.
- Memory saved: `adversarial-plan-review-protocol` (her preferred
  plan-review workflow — reuse it for future big plans).

## Open items already flagged for implementation-time verification

See spec "Open items": DBOS priority API at pinned version; copro.py table
audit; detail-plane table enumeration from unitbench's detail pages;
MotherDuck client for Next.js server-side; whether scoring needs its own
EnqueueTarget.
