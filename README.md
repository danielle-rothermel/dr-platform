# dr-platform

[![CI](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dr-platform.svg)](https://pypi.org/project/dr-platform/)

`dr-platform` is an alpha staged-work funnel built on PostgreSQL and DBOS. It
accepts application-owned work as a stream, moves each item through a linear
pipeline, and exposes durable controls and inspection. The public API is under
active development; there are no compatibility promises yet.

The ownership boundary is deliberate:

- `dr-platform` owns the funnel mouth and its gates: campaign and work
  identity, streaming submission, stage state, randomized admission, capacity,
  pause, retry, cancellation intent, and inspection.
- DBOS owns the conveyor belt: durable workflow execution, queues, recovery,
  and replay.
- Applications own meaning: which work should exist, what input and output
  references identify, how configuration is resolved, and what each stage
  does.

The package does not interpret application payloads or privilege a source
transport. A database query, API iterator, generated sequence, or file reader
can all yield the same `WorkInput` values.

The [vocabulary sheet](https://danielle-rothermel.github.io/dr-platform/)
(source: `.defs/vocab.html`) is the authoritative statement of the
staged-work pipeline contract this repo implements: the terms, the
guarantees, what is in and out of scope, and the mapping from each term to
the exported names.

## Installation

```console
pip install dr-platform
```

```console
uv add dr-platform
```

`dr-platform` requires Python >= 3.12 and a PostgreSQL database. The library
creates its schema in that database and colocates with the DBOS system schema;
see [Operational preconditions](#operational-preconditions) for the colocation
requirement and migration lineage.

`dr-platform` pins its DBOS dependency to an exact version
(`dbos[otel]==2.27.0`).
The package couples to DBOS internals, and recovery and sweep behavior is
validated against exactly this release, so each `dr-platform` release pins the
DBOS version it was proven against. Consumers get the exact combination that
was tested.

## Pipeline and execution model

A `PipelineDefinition` is an immutable, versioned, non-empty sequence of
`StageDefinition` values. Each stage names an application-owned queue, a
callable, and an `args_for` adapter. `wrap_pipeline_workflows` replaces those
callables with package-owned DBOS workflows that commit stage outcome and
create the next READY stage atomically. Register and submit the wrapped
definition, not the original declaration.

Application stage callables return a non-empty immutable output reference.
Stage execution is at-least-once, not exactly-once: if DBOS recovers a
workflow that crashed before its completion transaction checkpointed, the
whole stage body runs again, even effects the application considers already
done. The platform's completion transaction itself commits exactly once.
Put non-idempotent effects inside DBOS steps, or design the callable around
immutable output references so re-execution is safe.

Application exceptions become an in-band platform `FAILED` stage and the
wrapper returns normally. This is intentional: DBOS can report `SUCCESS` for
a workflow whose logical stage is `FAILED`. Platform inspection is
authoritative for stage outcome. A failed stage stays terminal until an
operator calls `retry_stage`; retry appends a new attempt and returns the same
logical stage to READY for later admission.

## Neutral end-to-end example

This example submits a plain generator. The scheduled dispatcher repeatedly
runs bounded admission passes; DBOS workers execute each admitted stage.

```python
import time

from dbos import DBOS, Queue
from sqlalchemy import create_engine

from dr_platform import (
    AdmissionPayload,
    PipelineDefinition,
    PipelineKey,
    PipelineRegistry,
    StageDefinition,
    StageExecutionState,
    StageKey,
    WorkInput,
    build_platform_dbos_config,
    bulk_work_statuses,
    initialize_dbos_runtime,
    inspect_campaign,
    register_scheduled_dispatcher,
    set_stage_capacity,
    submit,
    upgrade_platform_schema,
    wrap_pipeline_workflows,
)


def args_for(payload: AdmissionPayload) -> tuple[object, ...]:
    return (payload.input_reference,)


def prepare(input_reference: str) -> str:
    return f"prepared:{input_reference}"


def execute(input_reference: str) -> str:
    return f"executed:{input_reference}"


def score(input_reference: str) -> str:
    return f"scored:{input_reference}"


config = build_platform_dbos_config(database_url=None)  # resolves DATABASE_URL
engine = create_engine(config.database_url)
upgrade_platform_schema(config.database_url)
initialize_dbos_runtime(config, app_name="staged-work-example")

declared = PipelineDefinition(
    key=PipelineKey("generic-work"),
    version=1,
    stages=(
        StageDefinition(
            key=StageKey("prepare"),
            queue_name="prepare",
            workflow=prepare,
            args_for=args_for,
        ),
        StageDefinition(
            key=StageKey("execute"),
            queue_name="execute",
            workflow=execute,
            args_for=args_for,
        ),
        StageDefinition(
            key=StageKey("score"),
            queue_name="score",
            workflow=score,
            args_for=args_for,
        ),
    ),
)
pipeline = wrap_pipeline_workflows(declared)
registry = PipelineRegistry()
registry.register(pipeline)

# A stage-wide control is required for every stage before admission.
for stage in pipeline.stages:
    Queue(stage.queue_name)
    set_stage_capacity(
        pipeline=pipeline.identity,
        stage_key=stage.key,
        capacity=4,
        engine=engine,
    )

dispatcher = register_scheduled_dispatcher(
    config=config,
    engine=engine,
    registry=registry,
)
DBOS.launch()


def work_inputs():
    for index in range(10):
        yield WorkInput(
            work_key=f"work-{index}",
            input_reference=f"input:{index}",
            labels={"group": "example"},
        )


try:
    receipt = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        execution_config_reference="config:1",
        items=work_inputs(),
        registry=registry,
        engine=engine,
    )

    requested = [f"work-{index}" for index in range(10)]
    deadline = time.monotonic() + 60
    while True:
        statuses = bulk_work_statuses(
            "campaign-1", requested, engine=engine
        ).statuses.values()
        if all(
            status.state is StageExecutionState.SUCCEEDED
            for status in statuses
        ):
            break
        if any(
            status.state
            in {StageExecutionState.FAILED, StageExecutionState.CANCELLED}
            for status in statuses
        ):
            raise RuntimeError("work reached a terminal failure state")
        if time.monotonic() >= deadline:
            raise TimeoutError("work did not finish before the deadline")
        time.sleep(0.25)

    campaign = inspect_campaign("campaign-1", engine=engine)
    print(receipt.inserted_count, campaign.work_item_count)
finally:
    dispatcher.close()
    DBOS.destroy()
    engine.dispose()
```

## Submission and campaign idempotency

`submit` consumes any iterable and commits bounded chunks before requesting
the next values. A producer failure can therefore leave useful committed work
and an incomplete run. Replaying the same run key with the same immutable
campaign, pipeline version, and configuration reference resumes safely;
changing those facts raises `PipelineRunConflictError`.

Work identity is `(campaign_key, work_key)`. The first matching item fixes its
input reference and labels. Replays and later runs in the same campaign count
matching items as already existing; conflicting immutable facts raise
`WorkItemConflictError`. The receipt reports only what this call actually
committed: its run key, inserted count, and already-existing count.

For top-ups, derive the desired work keys in the application, read them with
`bulk_work_statuses`, submit only absent keys in a new run, leave READY or
ADMITTED keys alone, and explicitly retry FAILED stage executions. CANCELLED
keys are not reusable in that campaign; recover them with new work keys.

## Admission, capacity, and pause

Admission considers READY work in a stable randomized order so repeated passes
do not permanently favor submission order. Capacity is desired concurrent
occupancy, not a worker count. `set_stage_capacity` creates the required `{}`
stage-wide control; a stage admits nothing until that control exists. Creating
one for every declared stage is part of pipeline setup.

`set_selector_capacity` adds an exact-label gate. `pause` and `resume` modify
an existing control without changing its capacity or interrupting running
work; they never create one. Pausing a label subset requires that exact
selector control to already exist, created with `set_selector_capacity`.
`pause` or `resume` on a selector that was never configured raises
`LookupError`.

Applications register one scheduled dispatcher per process configuration with
`register_scheduled_dispatcher`. The dispatcher owns its DBOS client and runs
bounded admission passes. Close its registration during shutdown.
`register_scheduled_dispatcher` requires every pipeline in the registry to be
the return value of `wrap_pipeline_workflows`; registering an unwrapped
declaration raises `UnwrappedPipelineError`, since a raw declaration would
admit work whose completion transaction never runs.

## Failure, sweep, retry, and cancellation

Call `sweep_abandoned_stages` to lazily project DBOS `CANCELLED`, `ERROR`, or
recovery-exhausted workflows that remain platform-ADMITTED into terminal
platform state. The sweep does not retry or wait. Use `retry_stage` explicitly
for FAILED stages.

Nothing releases ADMITTED capacity for an abandoned workflow automatically.
Pass `sweep_cron` to `register_scheduled_dispatcher` to schedule
`sweep_abandoned_stages` alongside admission, or call it manually on your own
schedule; without one or the other, abandoned slots stay ADMITTED forever and
starve real work of capacity. Set `sweep_cron` in production-like runs.

An application obtains a `WorkflowCanceller` by constructing a `DBOSClient`
against `PlatformDbosConfig.system_database_url`, the colocated system
database URL.

`cancel_work` makes platform state terminal before delegating cancellation of
the exact admitted DBOS workflow. READY work has no workflow to delegate.
FAILED work is cancellable too: the attempt is already terminal, so nothing is
delegated, but the CANCELLED stage fences the item against a later
`retry_stage`. Cancellation is non-recursive and has several important
consequences:

- CANCELLED work is permanently terminal within its campaign. Submit a new
  work key to recover it.
- A cancellation racing with a successful handoff targets whatever stage is
  current once it holds the row lock, so it cancels the freshly created next
  stage rather than misreporting already-terminal work.
- If DBOS cancellation delegation fails after the platform commit, calling
  `cancel_work` again re-issues the idempotent delegation for the recorded
  attempt; repeated calls self-heal a lost delegation.

## Inspection and operations

`inspect_campaign` returns one campaign summary. The bounded readers
`list_campaigns`, `list_runs`, and `list_work_items` use stable cursors;
`get_work_item_stages` exposes stage and attempt lineage. Current-state counts
are available through `campaign_state_counts` and `run_state_counts`, controls
through `read_controls`, and application-sized desired sets through the
chunked `bulk_work_statuses` reader.

These readers derive outcomes from platform tables. DBOS workflow status is
execution evidence, not the source of truth for logical success or failure.

Across the public API, a well-formed identity that does not exist raises
`LookupError` (an unknown campaign, run, work item, or control selector);
malformed input raises `ValueError`. `campaign_state_counts` and
`run_state_counts` follow this too: an unknown campaign or run raises rather
than returning an empty tuple, so a typo'd key is distinguishable from a
drained one.

## Operational preconditions

The platform tables and the DBOS system schema must share one PostgreSQL
database. `PlatformDbosConfig`, runtime bootstrap, and dispatcher registration
validate colocation and fail fast when the URLs identify different databases.
The staging tables use the same `upgrade_platform_schema` Alembic chain as the
rest of the package.

**Migration lineage.** `0001_staging_baseline` is the root of the supported
Alembic chain. Apply the chain only to a database that has no platform schema.
If a database already contains platform tables outside this lineage, archive
it and initialize a replacement instead of attempting an in-place upgrade.

Register wrapped workflows, application queues, and the scheduled dispatcher
before `DBOS.launch()`. Keep the returned dispatcher registration alive while
the runtime is active. Optional OTLP initialization is fail-open and reports a
typed `TelemetryInitializationResult`; database, migration, workflow, and queue
startup failures are not fail-open.

## Development

Clone the repository and install the locked environment:

```console
git clone https://github.com/danielle-rothermel/dr-platform
cd dr-platform
uv sync
uv run pre-commit install
```

The test suite needs a PostgreSQL database. Create the default with
`createdb dr_platform_test`, or set `DR_PLATFORM_TEST_DATABASE_URL` to any
PostgreSQL database whose name ends in `_test`; the suite refuses other names
and resets the database destructively between tests. Then run the checks:

```console
uv run ruff check .
uv run ty check
uv run pytest
```
