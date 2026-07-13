# Handoff: locked sweep halted — streaming conversion required

Written 2026-07-13, at the owner's direction, after deliberately halting the
locked paid sweep at 4,212/5,904 terminal cells. **The v6 implementation is
not "complete and validated" until (a) the execution pipeline is converted to
an end-to-end streaming design with separate worker pools, and (b) the sweep
is finished on that design.** The blocking behavior measured below is a
usability defect of the system itself, not an operational inconvenience:
a system where the longest single call blocks the entire pipeline is not
usable.

## What ran, and what it proved

All work below used the production code path (the canary is a locked 12-cell
first shard of the same campaign, not separate code), against the run-scoped
production stores `v6accept_0713e` (source Postgres, MotherDuck analysis,
Neon detail, SQLite DBOS store) and the locked campaign
`platform-v6-live-acceptance-161` (164 HumanEval+ tasks × 3 seeds × 3 models
× 4 budgets = 5,904 cells; manifest sha
`71584eb7…d0f0`, prompt protocol sha `0d44d268…acc3`).

1. **Store acceptance passed** — `stores prepare`/`verify` provisioned and
   ownership-proved fresh schemas on all three stores after whetstone-ai
   PR #41 (strict run-schema isolation; direct/unpooled Neon endpoint
   required and now enforced).
2. **The 12-cell canary passed end-to-end** after fixing three production
   defects it exposed (all fresh-reviewed and merged to whetstone-ai):
   - PR #43 — four call sites let dr_platform default to `platform_*` table
     names instead of `whetstone_*`; an AST contract test now guards the
     whole class.
   - PR #44 — the live-sweep CLI had no DBOS enqueue runtime, so nothing
     physically enqueued (claims stuck `call_started`); the review also
     caught that DBOS's listen-all default would have made the CLI execute
     paid work itself. Added `recover-enqueues`.
   - PR #46 — no operator entry point drove kernel lifecycle reconciliation;
     added `whetstone-live-sweep reconcile` with an in-process DBOS facade
     (DBOSClient cannot open SQLite system databases).
3. **4,200 further cells executed correctly** through `submit-remaining`
   pages: 4,206 succeeded + 6 typed failures (~0.14%, all
   `generation_error`, retry-eligible via `submit-retry`), everything
   reconciled — kernel, ledger, and durable generation runs agree.
4. **Publication was pre-fixed** — whetstone-ai PR #45 bumped dr-platform to
   `cbd2eaf` (publication operation recovery + MotherDuck root fixes) and
   PR #47 made the publication runtime SQLite-safe and worker-safe (shared
   `enqueue_runtime` module). Neither has run live yet.

## Why the sweep was halted

Measured on this campaign (DBOS workflow timings, 4,212 successes):

| Metric | Value |
| --- | --- |
| Mean generation execution | 7.7 s |
| Cells finishing < 10 s | 81% |
| Cells finishing 10–30 s | 16% |
| Slowest cell | 1,139.7 s (~19 min) |

The locked flow dispatches in 100-cell shards fixed at campaign-lock time and
refuses to record shard N+1 until every shard-N cell is terminal in the
ledger. One ~19-minute straggler therefore idles the whole campaign, worker
concurrency above the shard size is worthless (measured directly: raising the
worker from 16 to 150 slots could not raise effective parallelism above 100,
with straggler drain and reconcile gaps pushing the average far lower), and
scoring cannot start at all until the entire campaign is terminal and a
frozen cut is taken.

## The requirement (owner decision, 2026-07-13)

Recorded as whetstone-ai issues
[#48](https://github.com/danielle-rothermel/whetstone-ai/issues/48) and
[#49](https://github.com/danielle-rothermel/whetstone-ai/issues/49) (see the
owner-clarification comment on #49):

1. **Separate worker pools** — scoring workers defined and scaled
   independently of graph-running generation workers.
2. **End-to-end streaming, no blocking anywhere:**
   - generation must not block on submission — generation workers start as
     soon as even one row is available;
   - scoring must not block on generation — scoring workers pick up each
     finished generation as soon as it completes;
   - submission runs consistently at its own pace regardless of how long
     generation and scoring take — no terminal-lifecycle barriers between
     pages, no shard-size effects.
3. An explicit trade is accepted: if streaming costs some verifiability or
   exact determinism, that is worthwhile.
4. **Completion criterion changed:** the implementation is complete and
   validated only when the sweep has been *finished on the streaming
   design*. Downstream phases (scoring, acceptance, publication, hosted
   parity, UI validation, production cutover, operations report) remain
   pending behind it.

## Exact current state (resume inventory)

- Operator dir: `~/drotherm/data/platform-v6-operator/` — descriptor
  `stores-e.json` (run `v6accept_0713e`), journal, DBOS store
  `v6accept_0713e-dbos.sqlite3` (4,212 SUCCESS workflows), ledger
  `live-sweep.sqlite3` (4,206 succeeded / 6 typed_failure; 1,692 cells never
  dispatched), sweep driver `run-full-sweep.sh` (the paged driver — obsolete
  under streaming), page logs under `sweep-logs/`.
- Campaign dir: `/private/tmp/platform-v6-live-sweep-161` (volatile /tmp!)
  with a durable backup at `~/drotherm/data/platform-v6-live-sweep-161-backup`.
- All processes stopped (no worker, no driver, no monitors). The queue was
  drained and kernel+ledger fully reconciled before shutdown — no in-flight
  or uncertain state. The 6 typed failures await `submit-retry`.
- whetstone-ai main is `9fda7a7` (PRs #41–#47 merged); dr-platform main is
  `cbd2eaf`; unitbench parity workflow is dispatch-only.
- Cost telemetry gap (non-blocking, worth fixing during conversion): observed
  provider cost is `unknown` for all 4,212 cells.
- Secrets: all store URLs + provider keys in mise; `NEON_DATABASE_URL` is the
  direct (unpooled) endpoint.

## ADRs and frozen constraints to reconsider (owner directive)

The new requirements are not a bolt-on: several reviewed v6 decisions assume
barrier semantics and must be re-examined against streaming before
implementation, using the established adversarial plan-review process.
Likely-affected (not exhaustive, and reconsideration does not mean reversal):

- **Frozen scoring cut** — `submit-scoring`'s freeze-once/replay-exactly
  contract and the v5/v6 "ordered deterministic score selection" and
  "pre-scoring evaluations" closures assume scoring sees one complete,
  deterministic campaign cut. Streaming scoring (property 2) replaces the
  single cut with per-generation triggers.
- **ADR 0018 / ADR 0020 (strict, append-only experiment acceptance)** —
  acceptance evaluates a complete current cut; under streaming, "current"
  becomes a moving frontier and acceptance timing needs a new definition.
- **L2 closure (plural membership vs accepted run) and
  `SUPERSEDED_GENERATION` provenance** — winner selection ordering was
  pinned deterministic; streaming plus retries may reorder completion.
- **ADR 0022 operating constraints** — the hard-cut philosophy and rollback
  model stand, but the "final-intent slice" list and the completion
  criterion change (this document); the risk register's A2/V1 closure
  evidence moves to the streaming design.
- **Campaign-lock shard artifacts** — `generation-manifest-shards.json` as
  the dispatch unit disappears; the lock's identity role (manifest hashes,
  canary shard, cell identities) must be preserved while its barrier role is
  removed.
- The explicitly accepted trade — determinism/verifiability may be reduced
  where streaming requires it — should be recorded as a new ADR when the
  design lands, so reviewers stop enforcing the old constraint.

## Suggested conversion sequence

1. Design + implement issues #48/#49 in whetstone-ai (dr_platform kernel
   primitives — claims, reconciliation pages, recovery — already support
   continuous pumping; the barriers live in whetstone's `live_sweep.py`
   submission flow and the single shared worker).
2. Reuse the existing ledger and kernel state: submitted shards stay valid;
   the streaming submitter needs to dispatch the 1,692 remaining cells and
   retry the 6 typed failures under the same manifest identities.
3. Finish the sweep streaming; then proceed with the unchanged downstream
   plan (scoring → acceptance → publication → parity → UI → cutover → ops
   report), whose gates are unaffected.
