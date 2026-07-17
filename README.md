# dr-platform

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

## Pipeline and execution model

A `PipelineDefinition` is an immutable, versioned, non-empty sequence of
`StageDefinition` values. Each stage names an application-owned queue, a
callable, and an `args_for` adapter. `wrap_pipeline_workflows` replaces those
callables with package-owned DBOS workflows that commit stage outcome and
create the next READY stage atomically. Register and submit the wrapped
definition, not the original declaration.

Application stage callables return a non-empty immutable output reference.
They may execute again during DBOS crash recovery. Put non-idempotent effects
inside DBOS steps, or design the callable around immutable output references
so replay is safe.

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
    return (payload.input_ref,)


def prepare(input_ref: str) -> str:
    return f"prepared:{input_ref}"


def execute(input_ref: str) -> str:
    return f"executed:{input_ref}"


def score(input_ref: str) -> str:
    return f"scored:{input_ref}"


config = build_platform_dbos_config(database_url=None)
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
            input_ref=f"input:{index}",
            labels={"group": "example"},
        )


try:
    receipt = submit(
        campaign_key="campaign-1",
        run_key="run-1",
        pipeline=pipeline.identity,
        config_ref="config:1",
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
work. They require the control row for that exact selector to exist; pausing a
missing selector raises `LookupError`.

Applications register one scheduled dispatcher per process configuration with
`register_scheduled_dispatcher`. The dispatcher owns its DBOS client and runs
bounded admission passes. Close its registration during shutdown.

## Failure, sweep, retry, and cancellation

Call `sweep_abandoned_stages` to lazily project DBOS `CANCELLED`, `ERROR`, or
recovery-exhausted workflows that remain platform-ADMITTED into terminal
platform state. The sweep does not retry or wait. Use `retry_stage` explicitly
for FAILED stages.

An application obtains a `WorkflowCanceller` by constructing a `DBOSClient`
against `PlatformDbosConfig.system_database_url`, the colocated system
database URL.

`cancel_work` makes platform state terminal before delegating cancellation of
the exact admitted DBOS workflow. READY work has no workflow to delegate;
already-terminal cancellation is an idempotent no-op. Cancellation is
non-recursive and has several important consequences:

- CANCELLED work is permanently terminal within its campaign. Submit a new
  work key to recover it.
- If DBOS cancellation delegation fails after the platform commit, the package
  does not re-drive that external call.
- A cancellation racing with successful handoff can find that the next stage
  was created after the first call selected its target. Call `cancel_work`
  again to cancel that newly current stage.

## Inspection and operations

`inspect_campaign` returns one campaign summary. The bounded readers
`list_campaigns`, `list_runs`, and `list_work_items` use stable cursors;
`get_work_item_stages` exposes stage and attempt lineage. Current-state counts
are available through `campaign_state_counts` and `run_state_counts`, controls
through `read_controls`, and application-sized desired sets through the
chunked `bulk_work_statuses` reader.

These readers derive outcomes from platform tables. DBOS workflow status is
execution evidence, not the source of truth for logical success or failure.

## Operational preconditions

The platform tables and the DBOS system schema must share one PostgreSQL
database. `PlatformDbosConfig`, runtime bootstrap, and dispatcher registration
validate colocation and fail fast when the URLs identify different databases.
The staging tables use the same `upgrade_platform_schema` Alembic chain as the
rest of the package.

Register wrapped workflows, application queues, and the scheduled dispatcher
before `DBOS.launch()`. Keep the returned dispatcher registration alive while
the runtime is active. Optional OTLP initialization is fail-open and reports a
typed `TelemetryInitializationResult`; database, migration, workflow, and queue
startup failures are not fail-open.

## Development

Install the locked environment with `uv sync`. The repository checks are
`uv run ruff check .`, `uv run ty check`, and
`uv run pytest -q`.
