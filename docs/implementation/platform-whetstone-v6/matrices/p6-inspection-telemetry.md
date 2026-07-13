# P6 inspection, wait, health, and telemetry matrix

This matrix specializes the reviewed v6 inspection seam without changing the
frozen packet. P6a owns typed kernel reads, wait, and health. P6b owns optional
OTLP configuration, public-surface hygiene, and the README cut. Whetstone CLI
adapters land in the Whetstone stack after the Platform API is pinned.

## P6a inspection and wait

| ID | Scenario | Expected result |
| --- | --- | --- |
| P6-I01 | List Operations | Stable bounded page ordered by creation and key; unknown cursor fails closed |
| P6-I02 | Inspect Operation | Frozen typed aggregate plus current Item/Attempt facts from authoritative Platform rows |
| P6-I03 | List Items | Stable `(item_index, item_id)` order and bounded pagination |
| P6-I04 | List Attempts | Full append-only lineage ordered by `(item_id, attempt)` |
| P6-I05 | Workflow/step join | Persisted workflow IDs drive payload-disabled DBOS reads; only reviewed step columns are selected |
| P6-I06 | Health | Typed counts and threshold breaches cover age, no progress, failures, missing/retry exhaustion, holds/backoff, drift, and incomplete cancellation/compensation |
| P6-W01 | Nonterminal wait | Bounded reconciliation then authoritative inspection; injected sleeper/clock controls polling |
| P6-W02 | Terminal wait | Every terminal aggregate, including `PARTIAL`, cancellation, abandoned registration, and permanent enqueue failure, returns a typed result |
| P6-W03 | Timeout | Typed timeout contains the last inspection and never invents domain acceptance |

## P6b telemetry and hygiene

| ID | Scenario | Expected result |
| --- | --- | --- |
| P6-T01 | OTLP disabled | Normal operation with no exporter and no failure |
| P6-T02 | OTLP initialization/export failure | Visible degraded telemetry state; experiment execution continues |
| P6-T03 | Span attributes | Only safe correlation and already-available cost/pacing facts; prompts, outputs, credentials, URLs, and raw provider metadata are rejected |
| P6-H01 | Root imports | Intentional reviewed public API only; adapters, row helpers, and DBOS internals remain private |
| P6-H02 | Documentation | README describes the v6 kernel rather than the legacy skeleton/batch/fairness interface |
| P6-H03 | Legacy search | Removed artifacts, fairness, naming, batch, projection, and callback-enqueue paths stay absent |

## Deferred live gates

No credentials are needed for deterministic P6 tests. Live OTLP export is a
separate credential/environment gate and may be reported as deferred without
weakening the disabled/failure-path contract.
