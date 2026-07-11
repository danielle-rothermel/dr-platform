# dr-platform

Conventions on top of DBOS for running large sweeps of durable work: stable
item identity, idempotent resumable submission, throttle/backoff with operator
holds, fair ordering, and progress observability. Deliberately not an
orchestrator — DBOS owns workflow execution; the library accepts callables and
typed items, never step definitions, and knows nothing about any domain.

## Language

**Operation**:
One submission of a set of Items, identified by a caller-supplied
`operation_key`. The unit whose lifecycle the kernel tracks from registration
through the aggregate terminal execution outcome of its Items. Its success
means durable workflow completion, not caller-domain success.
_Avoid_: batch, batch operation, sweep, run

**Domain Outcome**:
The caller-owned meaning of what a completed execution produced. It remains
separate from platform execution status.
_Avoid_: treating DBOS SUCCESS as proof that a domain result passed

**Workflow Role**:
The caller-owned stable label for the kind of execution an Operation manages.
The kernel records and correlates it but does not enumerate domain roles or
orchestrate dependencies between them.
_Avoid_: a kernel OperationType registry, domain branching in dr-platform

**Item**:
One unit of durable work inside an Operation, carried as a caller-defined
spec with stable identity.
_Avoid_: batch item, prediction, task

**Registration**:
The durable establishment of an Operation's complete caller-prepared Manifest,
Items, and any caller-owned domain records those Items require before enqueue.
One registrar advances bounded pages under a Lease until completion. After an
expired Lease, an operator may terminally abandon partial Registration without
deleting its committed provenance.
_Avoid_: fixture seeding, treating a transaction page as the input set, leaving
an unresumable Operation permanently REGISTERING

**Manifest**:
The immutable, caller-prepared identity of an Operation's complete ordered Item
set, including its count, canonical digest, and stable page boundaries.
_Avoid_: an unbounded iterator as registration authority, progressively
discovering Operation membership after writes begin

**Execution Recipe**:
The complete versioned identity of what one Item execution will do: its exact
canonical domain input plus the workflow, argument recipe, application, and
relevant profile, parser, dataset, and provider-configuration versions. Its
digest is persisted on the Attempt and participates in content-scoped workflow
identity; the Operation stores an ordered aggregate of its Item recipe digests.
_Avoid_: treating a Manifest, callable object, Prediction ID, or workflow name
alone as proof that two executions are equal

**Attempt**:
One numbered execution opportunity for an Item. dr-platform alone allocates
the ordinal and lineage; retry policy, a caller-owned Domain Outcome, or an
operator decision may establish eligibility for the next Attempt.
_Avoid_: maintaining a second caller retry counter, treating eligibility as
Attempt creation authority

**Cancellation**:
An explicit operator decision that an Item or Operation must not resume
automatically. Cancellation is terminal until another explicit operator
action permits a new Attempt. Platform-managed executions have no DBOS child
workflows in the pre-experiment topology, and cancelling one Operation does
not cancel an execution still referenced by another live Operation. DBOS
`CANCELLED` is a logical platform boundary, not proof that an in-flight
synchronous provider request stopped; an authorized replacement may overlap
that older paid call, and the resulting duplicate spend is accepted. If the
cancelled call returns after DBOS prevents its later Whetstone outcome write,
its price may remain only in the provider's billing receipts; Whetstone does
not add a separate provider-call ledger for an outcome it will not use.
_Avoid_: treating cancellation as a retryable failure, recursive DBOS
cancellation, claiming physical provider abort from DBOS status, claiming
Whetstone cost totals are complete for discarded post-cancellation outcomes

**Failure Class**:
The kernel-owned, domain-neutral category that determines retry and pacing
policy. Callers map their domain failure types into this vocabulary at the
platform boundary.
_Avoid_: persisting provider-library enums in the kernel

**Claim**:
The exclusive right of one submitter to perform the next enqueue transition
for an Attempt. Every Claim that reaches the enqueue-call boundary has an
append-only durable identity; expiry, replacement, invalidation, or Attempt
terminalization may change the current pointer but cannot erase that history.
_Avoid_: treating a claim as proof that DBOS accepted the workflow, storing
Claim history only on the mutable Attempt

**Lease**:
The bounded lifetime of exclusive transition authority, used for an enqueue
Claim or Operation Registration. Expiry permits recovery of the same durable
identity and cursor rather than creation of different work.
_Avoid_: creating a new Attempt or input set merely because an owner disappeared

**Spec**:
The caller-owned payload of an Item — opaque to the kernel.
_Avoid_: submit_spec, payload

### Identity

**Key (`*_key`)**:
A caller-supplied identity or grouping string: `operation_key`, `item_key`,
`group_key`, `throttle_key`.
_Avoid_: using `*_id` for caller-supplied identity

**Id (`*_id`)**:
A derived or generated identifier: `item_id` (digest derived from keys),
`workflow_id`, `claim_id`.
_Avoid_: using `*_key` for derived identifiers

**Service Class**:
The caller-selected urgency tier for an Item. It maps to a fixed DBOS priority
and does not encode shuffle order.
_Avoid_: fairness, using urgency to randomize work

**Shuffle Rank**:
The kernel-derived stable rank used to mix Item claim and enqueue order within
a Service Class when caller input is grouped. It is separate from original
Item position and DBOS priority. The rank and kernel enqueue order are
reproducible; DBOS may nondeterministically reorder workflows that share the
same priority and millisecond `created_at`, which is accepted.
_Avoid_: treating deterministic mixing as strict fairness, promising identical
final DBOS start order

**Throttle Domain**:
The unit of pacing for one rate-limited external resource, identified by
`throttle_key`. One workflow may cross multiple Throttle Domains, so durable
backoff can occupy a shared queue slot and must be bounded and observable.
_Avoid_: claiming queue topology eliminates multi-domain slot occupancy

**Projection**:
A kernel-owned, rebuildable export of platform and DBOS system tables into
the analysis store, which clients augment with domain-specific projections
of their Item specs and results.
_Avoid_: analysis tables in Postgres

**Export Snapshot**:
The stable source boundary captured for one export pass. Rows at or below the
Snapshot are eligible for that pass; later changes wait for the next pass.
_Avoid_: using wall-clock query completion as the boundary

**Export Cursor**:
A destination-owned record of the last source change it committed for one
artifact. Each destination advances its own Cursor only after its write
commits.
_Avoid_: one operational-Postgres watermark shared by every destination

**Export Barrier**:
The brief exclusion point between platform writers and source extraction that
makes an Export Snapshot complete through its captured change sequence.
_Avoid_: holding the barrier during destination upload or synchronization

**Platform Cut Version**:
The monotonic version on one Operation that changes with every
acceptance-relevant platform lifecycle mutation. A caller may pin a sorted set
of Operation versions as a platform cut and later verify it atomically without
the kernel knowing which domain object consumes that cut.
_Avoid_: domain-specific invalidation callbacks in dr-platform, treating a
previously read Operation/Attempt set as current without checking its versions

**Publication Fence**:
The destination-local, per-Publication-Bundle Lease and monotonic token that
permits one exporter to promote a staged Snapshot and advance its Cursor. It
rejects a stale exporter even after that exporter's Lease expires.
_Avoid_: treating the Export Barrier or invocation discipline as destination
writer serialization

**Publication Bundle**:
The smallest consumer-visible set of mutually referential tables that one
export promotes through a single atomic pointer or transaction. Kernel tables
and their cursor bookkeeping form one bundle, Whetstone Analysis projections
form one bundle, and a Detail root manifest plus all root-cascaded rows form one
bundle. Intentionally independent bundles carry their own Snapshot sequence;
cross-bundle readers must declare that they tolerate skew or check it.
_Avoid_: independent promotion of tables whose joins promise one Snapshot,
one universal Snapshot that couples unrelated artifact families

**Analysis Store**:
The DuckDB database (local file, synced to MotherDuck) that all aggregate
analysis, exploration, and mid-tier debugging reads from. Operational
Postgres remains solely the durable system of record.
_Avoid_: analyzing off operational Postgres, published tables

**Detail Store**:
The hosted Postgres (Neon) holding a bounded, optionally sampled dump of
row/log-level detail, serving direct table/row viewers and deep debugging.
Fed by the same export flow as the Analysis Store, never written directly.
_Avoid_: publish CLI, curated copy steps, using the Detail Store for
aggregate analysis

**Prefix**:
The single physical-naming knob: one string prepended to the kernel's table
names so multiple apps can share a database. Column names and digest recipes
are fixed.
_Avoid_: PlatformNaming, ItemIdentity, label parameterization
