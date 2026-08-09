# Async stages and run fan-in are qualified

**Outcome: qualified.** On clean Git tip
`35ed53afbf558d0e8872fe7004f828132eb8f211`, the declared admission and run
completion schedules retained 44% configured headroom, the representative
200-workflow burst completed with cancellation fencing intact, and both
planner checks used their intended bounded/indexed paths.

The machine-readable [qualification result](async-stages-and-run-fan-in-results.json)
is authoritative. All numeric bounds here are qualification-only acceptance
bounds, not standing service-level objectives.

## What did the representative burst prove?

The run admitted 200 stage workflows and released 20 run completion workflows.
All 220 synchronous checkpoint transactions ran through the dispatcher-owned
dedicated executor: 200 stage submissions, 20 completion submissions, and zero
unknown submissions. The executor had a 200-worker limit and reached 185 active
workers.

The persisted stage population ended at 180 successful handoffs and 20 logical
cancellations. For every cancelled work item, the platform delegated the exact
persisted DBOS workflow identity and DBOS recorded `CANCELLED`. All 20
process-local coroutines remained active and none cleaned before the release
gate. After release, all 20 returned and attempted their stage checkpoint;
late-return fencing preserved the 180 succeeded / 20 cancelled terminal split.

The state-gated handoff took **1.416653 seconds**, below the **7.2-second
rate-equivalent bound** for 100,000 admissions per hour, and completed at
**127.060 successful handoffs per second**. Cleanup completed for all workflows.
Cancellation-request-to-cleanup was 17.760 ms minimum, 192.859
ms p50, 465.752 ms p95, and 503.631 ms maximum. Release-to-cleanup for the 20
cancelled coroutines was 8.173 ms minimum, 19.814 ms p50, 262.130 ms p95, and
270.307 ms maximum.

Runtime shutdown also cleaned the deliberately still-active cancellation probe
in **0.359158 seconds**.

## What contention and loop behavior were measured?

| Measurement | Samples | Minimum | p50 | p95 | p99 | Maximum |
|---|---:|---:|---:|---:|---:|---:|
| Dedicated checkpoint queue delay | 220 | 0.014 ms | 0.041 ms | 3.635 ms | 9.281 ms | 41.996 ms |
| Stage checkpoint application-pool wait | 200 | 0.000 ms | 221.700 ms | 578.628 ms | 707.584 ms | 1046.494 ms |
| Completion checkpoint application-pool wait | 20 | 0.001 ms | 0.001 ms | 0.002 ms | 0.002 ms | 0.002 ms |
| Event-loop lag | 90 | 0.024 ms | 0.253 ms | 10.120 ms | 872.129 ms | 872.129 ms |

The SQLAlchemy application pool had 20 connections and no overflow. Each known
checkpoint produced one aggregate pool-wait observation, summing any connection
acquisitions made during that checkpoint; there were no unknown checkpoint
submissions or pool-wait observations.

The event-loop result is not characterized as low: its maximum lag was
**872.129 ms**. It remained below the qualification-only five-second component
bound while the probe continued through all completion workflows and their
checkpoints.

## Did the declared schedules retain headroom?

| Schedule | Declared workload | Configuration | Configured capacity | Headroom | Measured pass | Measured burst rate |
|---|---:|---|---:|---:|---:|---:|
| Admission | 100,000/hour | 200 every 5 seconds | 144,000/hour | 44% | 0.537572 s | 1,339,355.576/hour |
| Run barrier | 10,000/hour | 20 every 5 seconds | 14,400/hour | 44% | 0.048216 s | 1,493,266.056/hour |

Both passes completed inside their five-second schedule intervals. The measured
single-pass rates are observations from this run, not promised sustained
throughput.

## Did the planner retain bounded indexed behavior?

The barrier planner retained 10,000 unrelated released historical runs and
their 10,000 memberships outside candidate selection. Its candidate population
contained 10,000 nonterminal runs, including one run with 2,000 memberships.
The pass materialized a 20-row indexed candidate page, used bounded
parameterized lateral eligibility probes, locked the one eligible run, and
returned only `planner-00000-eligible` in **0.151 ms** under normal planner
settings. The plan used:

- `platform_ix_pipeline_runs_completion_candidates`;
- `platform_ix_stage_executions_nonterminal_work`; and
- `platform_uq_run_memberships_run_work`.

The topology gate required one candidate-page loop with 20 rows, one evaluated
page loop with 20 rows, 20 lateral limit and membership-probe loops, and between
one and 20 parameterized nonterminal-execution probe loops. The reported
`rounded_plan_row_estimates` are diagnostic rounded per-loop output estimates,
not exact row visits: 21 pipeline-run rows, 20 membership rows, and 19 stage
execution rows. The 10,000 released-history runs and memberships remained
unrelated history rather than candidate work.

The `list_runs()` planner retained 10,000 historical runs and 250,000 historical
memberships. It selected 20 runs and their 40 memberships in **0.143 ms**, using
`platform_ix_pipeline_runs_campaign_cursor` and
`platform_uq_run_memberships_run_work`. Its topology gate required one indexed
run-page loop returning 20 rows and 20 parameterized membership loops. Its
rounded per-loop output estimates were 20 run rows and 40 membership rows; they
are diagnostics, not exact row-visit counts.

## What environment produced the authoritative result?

The qualification ran on password-authenticated PostgreSQL 17.9 (Homebrew)
with a clean tree at both recorded Git-status checks. The database URL includes
a generated credential and a dedicated `_test` database; the result masks the
password.
The host was arm64 macOS 26.5.2 with Python 3.12.5, DBOS 2.27.0, SQLAlchemy
2.0.51, and psycopg 3.3.4. PostgreSQL reported 1,000 maximum connections.

The run completed at `2026-08-09T22:15:44.710843+00:00` on branch
`08-08-async-stages-and-run-fan-in-plan`. Both `git_status_at_start` and
`git_status_before_result` were empty.

The redacted reproducible command was:

```console
DR_PLATFORM_TEST_DATABASE_URL='postgresql+psycopg://drp_qual_s4_e2b696c5858b:REDACTED@127.0.0.1:5432/drp_qual_s4_e2b696c5858b_test' \
  uv run python qualification/async_stages_and_run_fan_in.py \
  --reset-test-database
```

Before the run, catalog checks verified login role
`drp_qual_s4_e2b696c5858b` (OID 15653175) and its owned database
`drp_qual_s4_e2b696c5858b_test` (OID 15653176), then a TCP connection verified
the password-authenticated role/database identity. Cleanup revalidated the
names, OIDs, login flag, ownership, and absence of active sessions before
dropping only that database and role. Final catalog checks confirmed both were
absent; the temporary secret file was removed.

## Did real sibling resources survive the durable workflow paths?

Yes. A separate clean-tip qualification exercised the local tagged
`dr-store`, `dr-providers`, and `dr-exec` packages through two overlapping
stage workflows and the run-completion workflow. The authoritative
[cross-package result](cross-package-async-resources-results.json) records two
successful stage outcomes, two provider calls, two process-executor calls, and
one successful completion whose exact aggregate consumed both stage artifacts.
Its release facts were two succeeded, zero failed, and zero cancelled members.

The async SQLite object store opened, served both stage workflows and run
completion, and closed on one reused workflow event loop. Both stages entered
before their explicit release gate (`peak_active_stages` was 2). The
application-owned bridge serialized provider access with its own lock, handled
all synchronous provider, executor, status, and DBOS-result calls, and reported
a completed shutdown. The runner's owned temporary artifact root was also
removed.

The qualification ran at `2026-08-09T21:22:49.698300+00:00` from clean
dr-platform tip `381a1aeffeb8d89546bba2e1f8d80350397e9860`; both source-status
snapshots were empty. Its exact sibling provenance was:

| Package | Tag | Commit | Imported module |
|---|---|---|---|
| `dr-store` | `v0.2.0` | `9787e72190c7fe1b2d3579c0179cae7d00a396d5` | `/Users/daniellerothermel/drotherm/repos/dr-store/src/dr_store/__init__.py` |
| `dr-providers` | `v0.3.0` | `f4931d71c3a2cec4c03caae03b02ccb8188000c6` | `/Users/daniellerothermel/drotherm/repos/dr-providers/src/dr_providers/__init__.py` |
| `dr-exec` | `v0.1.7` | `c06b45796b741dd2cac3c87955b8f3f239a7991e` | `/Users/daniellerothermel/drotherm/repos/dr-exec/src/dr_exec/__init__.py` |

Each sibling tree was clean, and each editable installation pointed to the
listed local repository. The redacted command shape was:

```console
uv run --offline \
  --with-editable /Users/daniellerothermel/drotherm/repos/dr-store \
  --with-editable /Users/daniellerothermel/drotherm/repos/dr-providers \
  --with-editable /Users/daniellerothermel/drotherm/repos/dr-exec \
  python qualification/cross_package_async_resources.py \
  --database-url 'postgresql+psycopg://drp_qual_b8d14d33c31e:REDACTED@127.0.0.1:5432/drp_qual_b8d14d33c31e_test' \
  --reset-test-database
```

The database setup independently verified a password-authenticated TCP
connection as the dedicated role `drp_qual_b8d14d33c31e` to its owned database
`drp_qual_b8d14d33c31e_test`. After success, cleanup revalidated the exact role
OID, database OID, login flag, database name, and ownership before dropping
them; final catalog checks found neither object.

## Why are there earlier failed measurements?

Two precursor runs established causes but are not qualification results. The
first measured the default executor and failed because shared-pool contention
starved checkpoint progress. After checkpoint work moved to the dedicated
dispatcher-owned executor, the next rerun failed only because `SERIALIZABLE`
checkpoint retries amplified contention. Explicit `READ COMMITTED`, row-locked
checkpoint transactions removed that retry amplification.

The clean-tip result linked above measures the resulting dedicated-executor
design and is the sole authoritative qualification result.
