# dr-platform

`dr-platform` is a typed durable-execution kernel built on DBOS. It owns
Operation and Item identity, append-only Attempt lineage, single-read
registration, kernel-executed enqueue, reconciliation and retry policy,
reference-aware logical cancellation, inspection, and health reporting.

DBOS owns durable workflow and step execution. Applications own workflow
definitions and domain outcomes. The kernel does not persist workflow inputs,
application configuration, external-service payloads, credentials, or DBOS
replay payloads. It validates workflow arguments only for serialization and
does not log or emit them as telemetry; applications own their secret policy.

The supported root-package flow is:

1. build the platform DBOS configuration, initialize the runtime, and upgrade
   the platform schema;
2. register an immutable `ExecutionTarget` in a `TargetRegistry`;
3. call `submit` with a caller-owned `SubmissionSource`, or call
   `submit_jsonl` for a JSONL file;
4. run bounded `reconcile` calls or `wait_operation`;
5. use `inspect_operation`, the bounded `list_*` readers, and `health_report`;
6. call `cancel_operation` or `request_next_attempt` for explicit operator
   actions.

The root `dr_platform` package exposes these verbs, the direct input and result
types needed to call them, and the `TargetResolver` and `WorkflowCanceller`
contracts required at application boundaries. For example, once an application
has constructed its target and source:

```python
from dr_platform import TargetRegistry, inspect_operation, reconcile, submit

registry = TargetRegistry()
target = registry.register(application_target)

receipt = submit(
    operation_key="daily-import-2026-07-15",
    workflow_role="import-item",
    group_key="2026-07-15",
    target=target,
    source=application_source,
    engine=engine,
    resolver=registry,
)
reconcile(engine=engine, resolver=registry)
operation = inspect_operation(receipt.operation_key, engine=engine)
```

Advanced record storage and mutation, enqueue recovery, throttling/backoff,
cancellation repair, and scheduling APIs remain importable from their
responsibility modules; they are not part of the supported root facade. Record
types returned by the supported inspection API remain available at the root.

Cancellation is non-recursive and reference-aware. Attempts and enqueue
Claims are durable history rather than mutable work slots. Optional OTLP
telemetry is diagnostic and fail-open; no exporter is the normal default.

## Development

Install the locked environment and run the repository checks:

```bash
uv sync
uv run ruff check .
uv run ty check
uv run pytest
```
