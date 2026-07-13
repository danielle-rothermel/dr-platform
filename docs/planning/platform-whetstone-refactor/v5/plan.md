# Platform Hard Cut — Joint Refactor Spec (v5)

**Status:** In review — frozen
**Date:** 2026-07-11
**Repos:** dr-platform (kernel), whetstone-ai (lockstep overhaul), unitbench (two-plane swap)
**Glossaries:** [dr-platform/CONTEXT.md](../../../../CONTEXT.md), [whetstone-ai/CONTEXT.md](../../../../../whetstone-ai/CONTEXT.md)
**Canonical decisions:** [Content-scoped execution identity](../../../adr/0001-content-scoped-execution-identity.md), [Platform-owned attempt lineage](../../../adr/0002-platform-owns-attempt-lineage.md), [Append-only attempt ledger](../../../adr/0003-append-only-attempt-ledger.md), [Kernel-owned failure taxonomy](../../../adr/0004-kernel-owned-failure-taxonomy.md), [Reference-aware cancellation](../../../adr/0005-reference-aware-cancellation.md), [Adaptive pacing and bounded slot occupancy](../../../adr/0006-accept-bounded-multi-domain-slot-occupancy.md), [Urgency versus shuffle order](../../../adr/0007-separate-urgency-from-shuffle-order.md), [Destination-local export state](../../../adr/0008-destination-local-export-state.md), [Manifest-backed transactional registration](../../../adr/0009-transactional-registration-hook.md), [Monotonic change sequence and export barrier](../../../adr/0010-monotonic-change-sequence-with-export-barrier.md), [DBOS export payload exclusion](../../../adr/0011-exclude-dbos-replay-payloads-from-export.md), [Scoring as a platform Operation](../../../adr/0012-scoring-as-platform-managed-operation.md), [Platform execution versus domain outcome](../../../adr/0013-separate-platform-execution-from-domain-outcome.md), [Dual analysis read adapters](../../../adr/0014-dual-analysis-read-adapters.md), [Two-plane stores](../../../adr/0015-two-plane-analysis-and-detail-stores.md), [Kernel-executed enqueue](../../../adr/0016-kernel-executes-platform-enqueue.md), [Fresh schemas without migration](../../../adr/0017-fresh-schemas-without-data-migration.md), [Strict Experiment acceptance](../../../adr/0018-strict-experiment-acceptance.md), [Accepted paid-call overlap](../../../adr/0019-accept-paid-call-overlap-after-cancellation.md), [Append-only Experiment acceptance](../../../adr/0020-append-only-experiment-acceptance.md)

## Mode and goals

All three repositories are in dev mode with no external users. This is a **hard cut to
the clean final state** before intensive experiments begin: breaking changes
are free, old data is abandoned in place (readable by old code, never
migrated), and every surface is hardened, simplified, and renamed to the
target domain — no compatibility shims, no deprecation paths.
See [ADR 0017](../../../adr/0017-fresh-schemas-without-data-migration.md).

### Design principles (enforced throughout)

1. **One happy path per verb.** Exactly one flow for submission, one for the
   worker lifecycle, one for export. Any second way to do a thing is deleted.
   Every flow step has exactly one named knob; unlabeled flow control is a bug.
2. **Domain-agnostic kernel.** dr-platform accepts callables and typed items,
   never step definitions; it knows nothing about LMs, prompts, or scoring.
   Its persisted `FailureClass` is a small neutral retry/pacing taxonomy;
   callers map domain-specific failures at the platform seam.
3. **Vocabulary is law.** Operation/Item in the kernel; whetstone's domain
   nouns (prediction, generation run, score attempt, experiment) map to
   Operation/Item only at the platform boundary. Key-vs-id rule: `*_key` is
   caller-supplied identity, `*_id` is derived/generated.
4. **Model boundary rule.** Structured records, options, callable-carrying
   targets, parsed input, persisted rows, and public results are frozen
   Pydantic `BaseModel`s with `extra="forbid"`. Protocols describe structural
   caller inputs. No dataclass exception is introduced; this follows the
   repository-wide Pydantic convention.
5. **Two-plane data model.** Operational Postgres is the durable system of
   record only. The Analysis Store (DuckDB → MotherDuck) serves all aggregate
   analysis/exploration. The Detail Store (Neon) serves row/log-level viewers
   and deep debugging, fed by the same export flow with sampling knobs. See
   [ADR 0015](../../../adr/0015-two-plane-analysis-and-detail-stores.md).

### Current-code re-audit (2026-07-11)

V5 carries forward v4's reviewed re-audit against these exact working trees;
no later application-code change is asserted by this successor:

- `dr-platform`: branch `07-08-refactor`,
  `7b9b340fd8f2717e44de36804396077b7beeb661`. The tree contains only this
  planning effort's index, canonical glossary/ADR updates, immutable v1-v3
  review packets, and the v4 prompt/draft; no application code, tests,
  dependency files, or runtime configuration changed from the v3 baseline.
- `whetstone-ai`: branch `codex/versioned-planning-docs`,
  `ccd9818d505ce45aafd7bd8503a2bcbd85f37289`, dirty only in the canonical
  glossary edits required by resolved owner decisions; no application drift.
- `unitbench`: branch `codex/versioned-planning-docs`,
  `cafd493ab9e9c1940106037209b1b218097f847e`, clean.
- DBOS: installed `2.26.0` at
  `dr-platform/.venv/lib/python3.12/site-packages/dbos`; both Python lockfiles
  resolve 2.26.0 although both project declarations still say
  `dbos>=2.25.0`.

No application-code revision has moved since the v3 convergence review, so
all cited defects still reproduce. dr-platform still persists enqueue-only
rows, progressively discovers `requested_count`, lacks execution
reconciliation and export cursors, exposes 94 root names, and depends on
`dr-providers` plus the pandas `frames` extra. Whetstone still passes
`database_url` as a durable generation/scoring workflow argument, catches and
persists domain failures before returning DBOS success, registers queues
without `priority_enabled=True`, imports `dr_platform.backoff.utc_now`, and
owns caller-selected generation/scoring attempt indexes. Its rescore selector
filters by experiment, allowed Generation Run statuses, optional generation
attempt, scoring/parser profile and dataset; excludes an existing Score
Attempt at the requested base index; advances beyond matching harness-failure
indexes; and orders by fair-order key, Prediction ID, and Generation Run ID.
Unitbench still reads Neon `published_*` tables through `DATABASE_URL`, has no
`ANALYSIS_DATABASE_URL` adapter, and retains `tools/unitbench_publish`.
Whetstone's lock also still points at the obsolete dr-platform
`drprov-v02-migration` branch/revision. These are implementation drift items,
not changes to the accepted architecture.

The installed DBOS contract remains: persisted live statuses are `PENDING`,
`ENQUEUED`, and `DELAYED`; queue registration defaults
`priority_enabled=False`; `DBOSClient.list_workflows` defaults to loading
inputs and outputs; workflow attributes are one execution-scoped object;
`cancel_workflow` defaults `cancel_children=False` and recursive cancellation
does not accept an application reference predicate. Its queue table stores
millisecond `created_at` and dequeue orders only by `(priority, created_at)`;
the owner accepts nondeterministic ties rather than adding a third key. DBOS
2.26 system-schema
and public-API assumptions remain exact-version contract-test obligations,
not compatibility claims for `>=2.25`.

### Unified invariants

These distinctions constrain every section below:

1. **Attempt authority is not eligibility.** dr-platform alone creates and
   numbers Attempts. Automatic retry policy, Whetstone domain policy, and an
   operator authorization are distinct reasons that may request creation.
2. **Execution terminality is not Experiment acceptance.** A DBOS workflow
   and platform Operation may succeed while Whetstone rejects the domain
   result or the Experiment remains incomplete.
3. **There are three independent lock scopes.** The source Export Barrier
   protects one extraction cut; the Operation row lock serializes
   registration, Item/Attempt mutation, and aggregate recomputation; the
   destination Publication Fence serializes one consumer-visible Publication
   Bundle's promotion.
4. **Execution identity is not reference identity.** DBOS owns one durable
   execution; dr-platform authoritatively stores every Operation/Attempt
   reference to that execution. DBOS attributes describe only the immutable
   execution.
5. **A transaction page is not an input set.** `page_size=500` bounds work in
   one transaction; only the caller-prepared immutable Manifest defines the
   complete Operation membership.
6. **Membership identity is not execution-recipe identity.** The Manifest
   defines which Items belong to an Operation. A separate versioned
   concrete `execution_recipe_digest` binds each Attempt to its complete
   domain input and immutable execution-affecting versions, while the
   Operation stores the ordered aggregate
   `operation_execution_recipe_digest`. Neither may substitute for the
   Manifest.
7. **Logical cancellation is not provider-call abort.** DBOS `CANCELLED`
   authorizes local terminalization but does not prove an in-flight synchronous
   provider request stopped. A confirmed replacement may overlap that request;
   duplicate spend is an explicit accepted risk, not a hidden failure mode.
   Whetstone accounting is outcome-linked and may omit a post-cancellation
   provider result that never reaches an accepted domain outcome; provider
   receipts, not DBOS replay payloads, remain the external billing record.

---

## Normative packet map

This entrypoint is normative together with the four contracts listed below.
The ordered document set and roles are machine-readable in
[`plan-manifest.json`](plan-manifest.json).

| Document | Normative responsibility |
| --- | --- |
| [`contracts/platform.md`](contracts/platform.md) | Complete dr-platform kernel vocabulary, schemas, scheduling, registration, submission, Attempt and Operation lifecycle, retry, cancellation, DBOS correlation, pacing, inspection, control, telemetry, and platform hygiene. |
| [`contracts/whetstone.md`](contracts/whetstone.md) | Whetstone identity and boundary behavior, generation/scoring Operations, accepted Generation Run selection, Experiment acceptance and currentness, outcome/cost truth, tests, and deletion/rename work. |
| [`contracts/publication.md`](contracts/publication.md) | Export and publication, bundle boundaries, Leases/fences, Analysis and Detail inventories, Unitbench two-plane readers, confidentiality, compute policy, and destination failure behavior. |
| [`contracts/delivery.md`](contracts/delivery.md) | Dependencies, migration and cutover order, transaction/concurrency/crash proof, pre-experiment gates, repository verification, rollback, failure handling, and explicit deferrals. |

[`traceability.md`](traceability.md) is non-normative. It records v0-v4
dispositions, reviewer disagreements, owner-decision provenance, revision
history, and source-heading coverage. Fresh reviewers must reconstruct the
normative design before consulting it for the closure pass.

## Scope and ownership

The hard cut spans `dr-platform`, `whetstone-ai`, and `unitbench` plus the
installed DBOS 2.26.0 behavior and the Analysis/Detail destinations they use.
It does not implement compatibility migration, dual reads or writes, data
adoption, generic replay, or a second orchestration path.

| Owner | Authority in the target system |
| --- | --- |
| dr-platform | Operation/Item identity, Attempt ordinals and lineage, Manifest registration, enqueue and reconciliation, retry-policy enforcement, logical cancellation, aggregate state, safe inspection/control, pacing state, and the export protocol. |
| DBOS | Durable workflow and step execution, raw workflow status, queue dequeue, durable sleeps, and worker slots. DBOS is never the authoritative store for mutable Operation references or domain acceptance. |
| Whetstone | Prediction, Generation Run, Score Attempt, and Experiment identity; domain eligibility and outcomes; concrete opaque recipe payloads; generation/scoring selection; one accepted Generation Manifest relationship and any accepted Scoring Manifest relationships per Experiment; acceptance evaluation; provider/model/cost facts linked to persisted outcomes. |
| Operational Postgres | Durable application truth, append-only provenance, Manifest relationships, lifecycle rows, domain outcomes, acceptance evaluations, and source change/snapshot sequencing. |
| DuckDB/MotherDuck | Rebuildable Analysis Store publication and aggregate/query execution under an explicit local or remote compute policy. |
| Neon | Rebuildable Detail Store publication and row/log-level reader truth behind one root-cascaded bundle pointer. |
| Unitbench | Typed two-plane read intent, local/deployed adapters, remote-compute confirmation, row validation, and independent fail-closed page behavior. |

No owner may silently reproduce another owner's allocation, lifecycle, flow
control, publication cursor, or acceptance decision. Every external boundary
uses frozen Pydantic models with forbidden extras; callable-bearing targets
remain runtime-only and their persisted identity is a frozen target reference.

## Accepted owner policies

The following policies are normative and are not reopened without new
correctness evidence:

1. Prediction ID remains Whetstone domain identity. A separate complete,
   versioned `execution_recipe_digest` binds execution behavior and enters the
   content-scoped workflow identity; exact canonical domain equality is
   required for `ALREADY_PRESENT`.
2. Mutually referential Whetstone Analysis members publish through one atomic
   pointer, the Detail root closure through one atomic pointer, and kernel
   tables plus cursor bookkeeping through one destination transaction.
   Independent families expose and enforce explicit skew policy.
3. DBOS cancellation of synchronous paid work is logical. A confirmed later
   Attempt may overlap the older provider call. Whetstone/export totals may
   omit a discarded post-cancellation result; provider receipts remain the
   external total-billing record and DBOS replay payloads remain excluded.
4. Experiment acceptance is append-only. Exact source Manifests, domain cut,
   platform cut, policy, observed matrix, override facts, and immutable member
   rows are persisted; one CAS-protected pointer names the current evaluation.
5. Deterministic kernel `shuffle_rank`, claim/enqueue ordering, and bounded
   model mixing are mandatory. Identical final DBOS order for same-priority,
   same-millisecond ties is not required.
6. For a Prediction with multiple successful Generation Runs at the pinned
   cut, the highest platform Attempt ordinal is accepted. Earlier successes
   remain superseded provenance and create no required scoring cells.
7. Acceptance currentness is proven with the sorted vector of contributing
   Operation `platform_cut_version` values, checked atomically at promotion
   and every current read. Mismatch fails closed as historical.
8. Each Experiment accepts exactly one Generation Operation/Manifest. The
   first accepted generation relationship fixes Experiment membership; exact
   replay is idempotent, a second unequal relationship returns typed
   `GENERATION_MEMBERSHIP_CONFLICT`, and membership expansion requires a new
   Experiment identity/version. Highest successful Attempt ordinal therefore
   selects only within that one Generation Operation/Item lineage. This
   specializes the strict and append-only acceptance decisions in
   [ADR 0018](../../../adr/0018-strict-experiment-acceptance.md) and
   [ADR 0020](../../../adr/0020-append-only-experiment-acceptance.md).
9. Accepted Scoring relationships receive immutable monotonically increasing
   ordinals per Experiment. For each logical scoring cell, acceptance selects
   from the newest relationship with a successful candidate, then selects the
   highest successful platform Attempt ordinal within that relationship. The
   ordered relationships, selected inputs, all candidates, and supersession
   provenance enter the immutable acceptance record and identity.
10. Populated `PARTIAL` Generation Runs remain eligible inputs to scoring to
    preserve rescore behavior. That eligibility is distinct from strict
    Generation acceptance: `PARTIAL` does not satisfy strict acceptance unless
    a separate explicit persisted acceptance policy authorizes it.
11. Experiment acceptance may be evaluated before any Scoring relationship
    exists. The empty canonical scoring-relationship set is valid acceptance
    identity, the durable evaluation is `PARTIAL` with explicit
    `MISSING_SCORE` members, and later scoring relationships append a new
    evaluation rather than rewriting the earlier one.

## One end-to-end lifecycle

There is one path from input preparation to a consumer-visible result:

1. The caller resolves a registered `ExecutionTargetRef`, freezes the complete
   ordered input set as an `OperationManifest`, computes every concrete recipe
   leaf and the ordered aggregate digest, and supplies a replayable source.
2. dr-platform creates or exact-reloads the Operation, obtains a database-time
   Registration Lease, validates one Manifest page at a time, and commits the
   typed Whetstone hook rows, Items, and Attempt 0 under the Export Barrier,
   workflow-reference locks where needed, and the Operation row lock.
3. For a non-empty Manifest, only the final-page CAS marks Registration
   complete. An empty Manifest instead completes atomically as
   `FAILED/empty_submission`. Before either completion branch, claim, enqueue,
   reconciliation, cancellation mutation, and accepted Experiment relationship
   publication are forbidden. An expired partial Registration can resume at
   its cursor or be explicitly abandoned after operator confirmation; it can
   never enqueue.
4. The kernel claims eligible current Attempts in deterministic service-class
   and shuffle order, calls DBOS outside the application transaction with the
   content-scoped workflow ID, and persists the outcome with a Claim CAS.
   Existing executions are linked without rewriting immutable attributes.
5. Bounded reconciliation normalizes DBOS state, records domain-neutral
   failures, advances automatic retries under `RetryPolicy`, detects missing
   work conservatively, and recomputes the Operation aggregate in the same
   transaction as every authoritative state change.
6. Whetstone may request a later Attempt only through the idempotent request
   ledger for a terminal domain outcome or confirmed sticky cancellation.
   dr-platform alone allocates the next ordinal. Concurrent requests converge
   or resolve `SOURCE_ADVANCED`; request bounds can tighten but never expand
   the immutable Operation policy.
7. Reference-aware cancellation records sticky local intent first, physically
   cancels only exclusively referenced top-level workflows, never recurses,
   and preserves partial failure. It invalidates outstanding Claims; a late
   claimant whose outcome CAS loses performs idempotent DBOS compensation and
   records the append-only compensation row.
8. `wait_operation` reconciles until the platform aggregate is terminal. A
   terminal platform success says only that durable execution succeeded;
   Whetstone separately derives append-only domain outcomes and strict or
   explicitly overridden Experiment acceptance.
9. Export captures a source cut behind the Export Barrier, then independently
   stages and fenced-promotes the kernel, DBOS telemetry, Whetstone Analysis,
   and Detail bundles. Each destination advances only its own cursor/pointer;
   partial destination success is returned structurally and never disguised
   as an all-or-nothing global commit.
10. COPRO and Unitbench consume only committed, pinned bundle identities.
    Unitbench routes analytical reads to local DuckDB or deployed MotherDuck
    and detail reads to Neon, with server-only secrets and explicit remote
    compute policy.

The detailed schemas, state machines, lock orders, crash matrices, reader
inventories, and exact tests that make this narrative enforceable live only in
the linked contracts.

## Ordered implementation and cutover

The full phase contracts and exit criteria are authoritative in
[`contracts/delivery.md`](contracts/delivery.md). The order is blocking:

1. Contract preflight pins DBOS 2.26.0 and proves the exact DBOS,
   MotherDuck, Neon, DuckDB, Vercel, and secret-wiring assumptions.
2. Platform vocabulary and the fresh baseline land the Pydantic contracts,
   schema crosswalk, Manifests, target refs, lineage/request/compensation
   ledgers, locks, triggers, and pure aggregate tests.
3. Platform lifecycle lands Registration, target resolution, deterministic
   scheduling, enqueue/reconciliation, retry, cancellation, inspection-facing
   state, and fresh-process recovery before callers migrate.
4. Whetstone generation moves to its registered top-level target, secret-free
   arguments, fresh schema, Manifest hook, platform-owned Attempt lineage, and
   model-mixing proof before legacy generation paths are deleted.
5. Whetstone scoring and Experiment acceptance move together: immutable
   selections, successive scoring Operations with monotonic accepted
   relationship ordinals, representable missing cells, deterministic
   generation and score-candidate selection, explicit `PARTIAL` scoring
   eligibility distinct from strict acceptance, accepted Manifest
   relationships, and checked source/platform cuts precede deletion of custom
   batching/replay.
6. Typed inspection, operator controls, payload-safe DBOS step metadata, and
   optional OTLP land before any expensive experiment.
7. Export and projections land fenced publication, atomic Analysis/Detail
   bundles, snapshot-built platform detail, explicit skew metadata, and
   wait→export→pinned-read COPRO/e2e proof before old analysis helpers vanish.
8. Unitbench swaps to the two real Analysis adapters and Neon detail plane,
   proves query/runtime/secret parity, then retires the copy publisher.
9. Final deletion and documentation remove only superseded paths whose
   replacement gates passed, refresh pins, search all repositories for stale
   names, and update the graph.

Rollback is code/config rollback to preserved old names and stores before the
environment switch. New durable work is never translated back: after it runs,
fix forward or abandon the fresh run. There is no dual-read/write interval.

## Blocking gates

No intensive experiment begins until the exact locked revisions pass every
gate in the delivery contract. At summary level they prove:

- immutable Manifest membership, bounded registration, deterministic mixing,
  exact resubmission, and original result ordering;
- content-scoped generation identity and restart-safe target resolution;
- generation/scoring/domain/acceptance separation, later-Attempt behavior,
  explicit missing cells, singular fixed generation membership,
  deterministic score selection across ordered relationships, `PARTIAL`
  scoring eligibility versus strict acceptance, highest-success selection
  within each chosen lineage, and stale-cut rejection;
- typed operator readiness, non-recursive cancellation, Claim invalidation,
  late-enqueue compensation, and honest accepted paid-call overlap;
- exact DBOS queue/status/inspection contracts, payload exclusion, pacing
  bounds, and accepted tie-local ordering variance;
- incremental/full export equivalence, atomic bundle visibility,
  destination-local fencing, live crash/partial-failure behavior, and explicit
  cross-family skew;
- local/deployed Unitbench query parity, remote compute enforcement, Vercel
  runtime safety, server-only secrets, and independent store failure;
- outcome-linked Whetstone cost truth without DBOS replay accounting; and
- COPRO plus zero-spend e2e continuity through typed wait, explicit export,
  and pinned Analysis reads.

Any failed gate blocks the dependent phase. Exact commands, transaction race
fixtures, load test, live-store proofs, and stale-symbol searches are specified
in the delivery contract; a slogan or mock-only test cannot satisfy them.

## Review protocol

V5 remains mutable as `draft` until review starts. The
next review is another independent whole-system convergence pass over
dr-platform, Whetstone, Unitbench, DBOS, export, and the runtime constellation.
Once review begins, the execution precondition changes both this status and the
effort index to `in-review` without changing contract content; v5 then freezes,
and decision-changing findings land only in a successor draft. No focused audit
or implementation begins before convergence completes.

The two v5 review prompts are prepared. No baseline, finding, or synthesis
artifact exists; v4 review outputs remain historical evidence, not v5 results.
