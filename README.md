# dr-platform

[![CI](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/danielle-rothermel/dr-platform/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/dr-platform.svg)](https://pypi.org/project/dr-platform/)

| [Repo Definitions](https://danielle-rothermel.github.io/dr-platform/) | [dr-serialize v0.1.0](https://github.com/danielle-rothermel/dr-serialize) |
| --- | --- |

**dr-platform durably moves application-owned work through staged pipelines.**
It is built on PostgreSQL and DBOS and organized into these functional areas:

- **Pipeline definitions** describe ordered, versioned stages while
  applications retain ownership of stage behavior and the meaning of input and
  output references.
- **Submission and identity** record streamed work in bounded chunks and
  organize it into campaigns and runs with stable identities and replay-safe
  conflict detection.
- **Admission and controls** select ready work fairly within stage and
  label-specific capacity, with pause and resume controls that leave running
  work uninterrupted.
- **Execution and handoff** run admitted stages durably, recover interrupted
  workflows, record outcomes, and create the next ready stage.
- **Recovery and operator actions** reconcile abandoned workflows and provide
  explicit retry and cancellation while preserving stage-attempt history.
- **Inspection** exposes campaigns, runs, work items, stage and attempt history,
  current state counts, and configured controls through bounded readers.
- **Runtime support** manages database migrations, validates PostgreSQL and
  DBOS colocation, initializes the runtime, schedules dispatch, and optionally
  configures telemetry.
