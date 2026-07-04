# dr-platform

Conventions on top of [DBOS](https://www.dbos.dev/) for running large
sweeps of durable work: stable work-item identity, idempotent resumable
batch submission, throttle/backoff with operator holds, fair ordering,
progress and attempt observability, artifact offload, and rebuildable
analysis projections.

Deliberately **not** an orchestrator: no step definitions, no handler
registries, no retry scheduling — DBOS owns workflow execution and
recovery; workflows and steps stay app-side. The library accepts
callables and typed items, never step definitions, and knows nothing
about any domain (no LM calls, prompts, scoring, or model configs).

Design doc: `docs/composable/platform.md` in the whetstone-ai repo.

## Status

v0.1 — the pure kernel (extraction stage 6a):

- `dr_platform.items` — `SubmittableItem` protocol, `ItemIdentity`
  digest configuration, `stable_item_id` axis hashing.
- `dr_platform.fairness` — order-key sorting and windowing.
- `dr_platform.jsonl` — byte-offset JSONL indexing/windowed loading
  with parameterized field names.
- `dr_platform.batch_status` — the batch operation/item status state
  machine (pure counts, no I/O).
- `dr_platform.dbos_config` — DBOS config/bootstrap helpers and the
  single compatibility shim for DBOS's private race-error classes.
- `dr_platform.progress` — heartbeat progress logging for long
  operations.

Coming (6b): library-owned schema + Alembic lineage, claim/lease batch
submission, dedup enqueue, throttle/backoff with tags and holds,
`await_operation`, projections, artifact store.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ty check
```
