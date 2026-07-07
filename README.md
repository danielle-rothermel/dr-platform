# dr-platform

Conventions on top of [DBOS](https://www.dbos.dev/) for running large
sweeps of durable work — the batch-submission/platform kernel of the
`dr-*` library family, extracted from whetstone: stable work-item
identity, idempotent resumable batch submission, throttle/backoff with
operator holds, fair ordering, and progress/attempt observability.

Deliberately **not** an orchestrator: DBOS owns workflow execution and
recovery; workflows and steps stay app-side. The library accepts
callables and typed items, never step definitions, and knows nothing
about any domain (no LM calls, prompts, scoring, or model configs).

## Status

`main` is a skeleton: an empty typed package (`src/dr_platform/`) plus
project scaffolding (uv, ruff, ty, pytest, pre-commit). There is no
library code and there are no tests yet.

The actual library lands with
[PR #1](https://github.com/danielle-rothermel/dr-platform/pull/1)
(the whetstone platform extraction). Until it merges, don't build
against this repo.

## Development

`pyproject.toml` pins `dr-serialize` as an editable **path dependency**
(`../dr-serialize` — temporary migration wiring, to become a real pin
before PR #1 merges). A fresh clone's `uv sync` fails unless
`dr-serialize` is checked out as a sibling directory. With that in
place:

```bash
uv sync
uv run ruff check .
uv run ty check
uv run pytest   # collects nothing on main — tests arrive with PR #1
```
