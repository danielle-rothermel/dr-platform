# Phase 0 contract preflight evidence

Captured 2026-07-11 on the refreshed implementation baseline. This evidence
contains no credentials, URLs, or provider tokens.

## Reusable commands

```console
uv sync --all-extras --dev
uv run pytest tests/contracts/test_dbos_226_contract.py -q
uv run pytest tests/contracts/test_storage_preflight.py -q
uv run pytest -q
uv run ruff check .
uv run ty check
uv run python scripts/platform_v6_preflight.py postgres-fencing \
  --url-env UNITBENCH_TARGET_DATABASE_URL
uv run python scripts/platform_v6_preflight.py motherduck-fencing
uv run python scripts/platform_v6_preflight.py motherduck-parity
uv run python scripts/platform_v6_preflight.py capture-skew
```

The Postgres contract tests use `DR_PLATFORM_TEST_DATABASE_URL` when set and
otherwise connect to the local `dr_platform_test` database. The DBOS tests
create an isolated temporary system schema and drop it on completion. The
storage tests use a non-destructive connection fixture: they never invoke the
legacy `clean_pg` fixture and never drop or recreate `public`. The DuckDB test
creates only pytest-temporary files. Live probes create uniquely named schemas
and drop only those schemas in `finally` blocks.

## Installed DBOS contract

- `dbos[otel]==2.26.0` is an exact project dependency and the lock resolves
  DBOS 2.26.0.
- The public client, queue, enqueue-option, workflow-list, step-list, and
  cancellation signatures are snapshotted in a durable contract test.
- The full installed `workflow_status`, `operation_outputs`, and `queues`
  column order, application-version schema, and seven workflow statuses are
  snapshotted.
- A live isolated-schema fixture proves database-backed queues dequeue by
  `(priority ASC, created_at ASC)`.
- An exact same-priority/same-millisecond fixture deliberately asserts only set
  completeness. DBOS 2.26 has no final tie-break and the Platform contract does
  not depend on the observed incidental order.
- Queue registration/retrieval, `never_update` preservation of operator
  configuration, explicit workflow identity, enqueue options, attributes, and
  attribute filtering are live contract gates.
- A live cancellation fixture proves `cancel_children=False` leaves children
  active and cancellation of a missing workflow creates no tombstone. A
  separate fixture proves DBOS recursive cancellation reaches child and
  grandchild rows, while the Platform implementation remains required to call
  only `cancel_children=False`.
- `list_workflows` defaults to loading inputs and outputs, so every Platform
  call must set both flags false explicitly.
- Public `DBOSClient.list_workflow_steps` exposes no payload-loading flag and
  invokes the configured deserializer. The safe timeline projection is pinned
  to workflow/function identity, child workflow ID, and lifecycle timestamps;
  it excludes `output`, `error`, and `serialization` and reads successfully
  with a payload-rejecting serializer.
- Reviewed workflow, step, queue-configuration, and application-version
  allowlists cover every field named by the delivery contract. Workflow
  authentication, role, input, output, error, and serialization columns remain
  explicitly outside the allowlist.

## Postgres and populated-only boundary

The local Postgres 17.9 fixture reported `datcollate=en_US.UTF-8`,
`datctype=en_US.UTF-8`, and libc locale provider. PostgreSQL does not expose
`lc_ctype` through `current_setting` in this environment; the reusable query is:

```sql
SELECT datcollate, datctype, datlocprovider, datlocale
FROM pg_database
WHERE datname = current_database();
```

Transaction-scoped advisory lock contention and automatic release passed with
two independent connections. The ctype-independent populated-only predicate
uses `btrim(value, :unicode_whitespace) <> ''` with this exact closed 25-code
point Unicode White_Space set in both SQL and Python:

```text
U+0009..U+000D, U+0020, U+0085, U+00A0, U+1680,
U+2000..U+200A, U+2028, U+2029, U+202F, U+205F, U+3000
```

Null, empty, every individual set member, the complete set in forward order,
and the complete set in reverse order are unpopulated. Text containing any
character outside the set is populated. Boundary fixtures include U+0008,
U+180E, U+200B, and U+FEFF as populated non-members. No POSIX character class,
database ctype, or unconstrained `str.strip()` decides membership.

## DuckDB process lock

DuckDB 1.4.5 is pinned in the development group for Phase 0. A subprocess
fixture proves that a blocking exclusive `fcntl.flock` on a sibling
`analysis.duckdb.lock` file prevents a second writer from opening the database,
then becomes available automatically after the owner releases it. The proof
does not add or widen a production API.

## Live destination probes

Live probes were run with credentials supplied only through the existing
environment:

- `local-postgres`, project hash `56f797833dee`: advisory-lock
  contention/release, Lease renewal CAS, stale-writer rejection, and atomic
  bundle-plus-pointer transaction passed.
- `neon-postgres`, project hash `3bb33d910255`: Lease renewal CAS,
  stale-writer rejection, and atomic bundle-plus-pointer transaction passed.
- `motherduck-postgres`, non-secret endpoint label
  `aws-us-east-1/default-md`, project hash `493872f2ab39`: SSL connection,
  temporary schema/table round-trip, Lease renewal CAS, stale-writer rejection,
  and atomic bundle-plus-pointer transaction passed.
- One physical MotherDuck table read through the DuckDB `md:` client and the
  official Postgres endpoint ran the same SQL and returned identical strict
  Pydantic view models for VARCHAR/DECIMAL/BIGINT values with
  `str`/`Decimal`/`int` types. The normalized query hash was `b4aa8d622843`;
  the temporary schema was dropped.

The MotherDuck Postgres endpoint reports an unreliable affected-row count for
an unmatched plain `UPDATE`. The reusable probe therefore uses `UPDATE ...
RETURNING fencing_token`: a matching renewal returns the token and a stale
writer returns no rows. This is the checked CAS detection mechanism.

The reusable implementation pattern for either destination is one temporary
schema and two independently opened writer connections, one acting as current
owner and one as stale writer. The probe output includes
`independent_writer_connections=PASS`, and any failed fencing boolean exits
nonzero. The probes use a Lease row with owner/token and
expiry, conditional renewal matching owner/token, a stale-token update that
must affect zero rows, and a transaction that writes bundle members and swaps
the pointer together. Credentials remain environment inputs and must never be
embedded in evidence.

## Final application/DBOS topology and timestamp bound

Phase 0 pins the implementation topology: the Platform application schema and
DBOS system schema use the same configured Postgres endpoint through separate
database connections and separate schemas. An absent
`DBOS_SYSTEM_DATABASE_URL` intentionally selects the application endpoint;
this is the supported final fallback, not an unresolved topology choice.

The checked-in `capture-skew` command ran exactly 100 zero-work samples using
independent application and DBOS connections. `DBOS_SYSTEM_DATABASE_URL` is
currently absent, so the pinned fallback selected the same configured physical
endpoint for the system connection. Results:

```text
application_project_hash=56f797833dee
system_project_hash=56f797833dee
sample_count=100
p99_skew_ms=0.301
median_query_quantum_ms=0.178
max_capture_skew_ms=100
cap_exceeded=false
system_url_fell_back_to_application=true
```

The two database clocks are read with `SELECT clock_timestamp()`; query quantum
is measured with the process monotonic clock around each read. Raw absolute
skew samples in milliseconds for the recorded rerun are:

```text
5.172,0.229,0.301,0.166,0.175,0.200,0.198,0.178,0.219,0.256,
0.267,0.227,0.214,0.195,0.205,0.202,0.229,0.199,0.188,0.232,
0.238,0.175,0.226,0.218,0.216,0.190,0.211,0.193,0.184,0.193,
0.224,0.177,0.276,0.232,0.283,0.229,0.191,0.180,0.199,0.200,
0.171,0.201,0.177,0.199,0.185,0.185,0.197,0.200,0.152,0.183,
0.169,0.263,0.177,0.178,0.163,0.163,0.132,0.130,0.176,0.189,
0.175,0.109,0.145,0.145,0.165,0.127,0.132,0.154,0.161,0.188,
0.209,0.124,0.119,0.142,0.152,0.149,0.165,0.145,0.118,0.168,
0.224,0.155,0.149,0.192,0.167,0.152,0.170,0.136,0.109,0.145,
0.142,0.140,0.132,0.150,0.141,0.159,0.125,0.158,0.140,0.183
```

An earlier independent run observed p99 `0.248 ms` and median query quantum
`0.045 ms`; it produced the same 100 ms bound. The recorded rerun above is the
auditable sample set.

The bound is `ceil((p99 + 2 * median_query_quantum) / 100) * 100`, with a
minimum increment of 100 ms and a hard cap of 5,000 ms. The reusable command
also emits all raw samples plus non-secret endpoint hashes. P7 reruns the same
100 samples as verification of the pinned topology, not as topology selection.
Changing either endpoint or the resulting bound is a reviewed
configuration-contract change.

## Unitbench and Vercel evidence

The executable Unitbench matrix is linked at
`unitbench/docs/implementation/platform-v6/matrices/delivery-preflight.md`.
It records:

- Next.js on the Vercel Node 24.x runtime with an explicit Node root route;
- encrypted `DATABASE_URL` in Preview and Production;
- independent four-state Analysis/Detail missing-secret behavior;
- a passing production build and native-DuckDB trace exclusion check;
- the credential-safe MotherDuck Postgres round-trip above, proving that
  `ANALYSIS_DATABASE_URL` is constructible from the existing token and official
  endpoint rather than credential-blocked; and
- durable live same-query view-model parity across the DuckDB and Postgres
  interfaces to one physical MotherDuck fixture.

### Vercel completion evidence

```text
ANALYSIS_DATABASE_URL configured scopes: Preview, Production (Sensitive)
DATABASE_URL configured scopes: Preview, Production (Encrypted)
Preview deployment identifier: dpl_oPPfGdfEcxyvXFK3qjd5s1g5hgTW
Preview deployment state: READY
Preview route smoke: root, dashboard, and all six fixture galleries HTTP 200
Independent missing-secret behavior: PASS (four-state boundary matrix)
Native DuckDB exclusion: PASS (production dependency and generated-trace gate)
```

The Analysis DSN was constructed from the existing MotherDuck token and the
official PostgreSQL endpoint, validated before upload, and stored as a
sensitive Vercel value without printing it. The linked project and downloaded
local environment file were removed after deployment. Values recorded here
remain non-secret: only scopes, deployment identifiers, and dispositions are
allowed.

## Disposition

The Phase 0 feasibility gate is closed: installed DBOS, local
Postgres/DuckDB, live Neon, MotherDuck fencing, same-query strict view-model
parity, pinned-topology skew, Vercel runtime/secrets, independent boundary
behavior, preview build, route smoke, and native dependency exclusion all
pass. Full production-adapter parity remains the owning U1/U2/U5 exit gate,
and P7 repeats the pinned-topology skew measurement as verification. Those are
implementation acceptance gates, not prerequisites for proving the contracts
feasible. Risk A3 remains open through P7/W7; topology selection itself is
closed. Risk L1 remains open through W3 adoption by every persisted consumer.

Final verification on this baseline:

```text
uv run pytest tests/contracts -q  -> 21 passed
uv run pytest -q                  -> 124 passed
uv run ruff check .               -> All checks passed
uv run ty check                   -> All checks passed
```
