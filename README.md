# dr-platform

`dr-platform` is a typed durable-execution kernel built on DBOS. It owns
Operation and Item identity, append-only Attempt lineage, manifest-backed
registration, kernel-executed enqueue, reconciliation and retry policy,
reference-aware logical cancellation, inspection, and health reporting.

DBOS owns durable workflow and step execution. Applications own workflow
definitions and domain outcomes. The kernel does not persist workflow inputs,
application configuration, external-service payloads, credentials, or DBOS
replay payloads.

The public flow is:

1. register an immutable execution target;
2. prepare and submit an Operation manifest;
3. run bounded reconciliation or `wait_operation`;
4. inspect typed Operation, Item, Attempt, and health state;
5. use explicit cancellation or next-Attempt requests for operator actions.

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
