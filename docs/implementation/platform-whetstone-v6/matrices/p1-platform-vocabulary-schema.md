# P1 platform vocabulary and schema matrix

This matrix is the executable acceptance contract for the P1 clean cut. It
specializes the frozen v6 platform contract; it does not amend it. Scenario IDs
are stable references for implementation and review findings.

Run the focused gate with:

```console
uv run pytest \
  tests/contracts/test_platform_v6_vocabulary.py \
  tests/contracts/test_platform_v6_fresh_schema.py -q
```

The schema suite uses `DR_PLATFORM_TEST_DATABASE_URL` when configured and the
existing local `dr_platform_test` fallback otherwise. It destroys only that
explicit scratch database's `public` schema through the existing `clean_pg`
fixture. It must never run against an application database.

## Fixed public vocabulary

| ID | Contract | Expected result | Executable assertion |
| --- | --- | --- | --- |
| P1-V01 | Operation states | Exactly `registering`, `enqueuing`, `running`, `cancelling`, `succeeded`, `partial`, `failed`, `cancelled` | `test_final_lifecycle_enums_are_closed` |
| P1-V02 | Item insertion states | Exactly `inserted`, `already_present` | `test_final_lifecycle_enums_are_closed` |
| P1-V03 | Attempt enqueue states | Exactly `pending`, `claiming`, `enqueued`, `workflow_already_present`, `enqueue_error` | `test_final_lifecycle_enums_are_closed` |
| P1-V04 | Attempt execution states | Exactly `not_started`, `active`, `succeeded`, `error`, `recovery_exhausted`, `cancel_requested`, `cancelled`, `missing` | `test_final_lifecycle_enums_are_closed` |
| P1-V05 | Retry disposition | Exactly `retryable`, `permanent`, `exhausted` | `test_final_lifecycle_enums_are_closed` |
| P1-V06 | Service class | Semantic values are exactly `urgent`, `standard`, `backfill`; their DBOS priorities are respectively `100`, `1_000`, `10_000` | `test_final_lifecycle_enums_are_closed`; priority mapping remains a P2 scheduling test |
| P1-V07 | Persisted records | Operation, Item, Attempt, Claim, and compensation records are Pydantic models with `extra="forbid"` and `frozen=True` | `test_persisted_record_models_are_frozen_and_strict` |
| P1-V08 | Historical vocabulary | No root export contains `Batch`; callback enqueue, `PlatformNaming`, stamp, and old record/status names are absent | `test_legacy_root_exports_are_removed` |

## Operation registration and aggregate status

These rows define the complete pure status precedence that the P1/P2 state
tests must implement. First matching row wins. P1 pins the enum and schema
shape; P2 supplies the table-driven aggregate implementation using these same
IDs.

| ID | Registration/current-Attempt observation | Operation result | Required facts |
| --- | --- | --- | --- |
| P1-O01 | `registration_abandoned_at` is set | `FAILED` | `terminal_reason=registration_abandoned`; immutable operator and reason facts present |
| P1-O02 | Registration incomplete, Lease held | `REGISTERING` | Lease triple is all present; cursor is within page bounds |
| P1-O03 | Registration incomplete, Lease expired but resumable | `REGISTERING` | Expiry alone is not terminal |
| P1-O04 | Empty Manifest completes atomically | `FAILED` | `requested_count=0`; `terminal_reason=empty_submission`; hook is not invoked |
| P1-O05 | Registration completes on final page | `ENQUEUEING` | cursor equals page count; Lease triple cleared; completion timestamp set |
| P1-O06 | Any cancellation disposition unresolved | `CANCELLING` | Takes precedence over enqueue/running mixtures |
| P1-O07 | Any current Attempt pending, claiming, or retryable enqueue error | `ENQUEUEING` | Includes a newly requested Attempt |
| P1-O08 | Any current Attempt confirmed enqueued but `NOT_STARTED`, active, or automatically retryable | `RUNNING` | Confirmed enqueue transfers authority to DBOS |
| P1-O09 | All current Attempts succeeded | `SUCCEEDED` | completed timestamp present; no terminal reason required |
| P1-O10 | All current Attempts cancelled | `CANCELLED` | cancellation provenance remains typed |
| P1-O11 | At least one success and at least one other terminal result | `PARTIAL` | Includes selected retries succeeding while other Items remain cancelled |
| P1-O12 | All terminal without any success and not all cancelled | `FAILED` | Exhaustion reason remains stable |
| P1-O13 | Impossible count/state/timestamp mixture | validation failure | Both Pydantic and database checks reject it |
| P1-O14 | Exact no-op replay | status unchanged | Does not increment `platform_cut_version` or `change_seq` |

## Manifest, Item, and Attempt invariants

| ID | Scenario | Expected result | P1 evidence |
| --- | --- | --- | --- |
| P1-M01 | Operation records accepted Manifest identity | Immutable version, digest, page size/count, target ref, recipe aggregate, requested count | Required columns in `test_platform_schema_has_final_tables_and_columns` |
| P1-M02 | Item identity | `item_id` is derived; caller identity is `item_key`; `(operation_key,item_index)` and `(operation_key,item_key)` are unique | `test_platform_schema_has_final_tables_and_columns`; `test_item_and_request_idempotency_keys_are_unique` |
| P1-M03 | Item scheduling | Caller supplies `service_class`; schema persists matching `service_priority` and derived `shuffle_rank`; no `order_key` | Required/forbidden columns in `test_platform_schema_has_final_tables_and_columns` |
| P1-M04 | Item current Attempt | `current_attempt >= 0` and deferred composite FK resolves `(item_id,current_attempt)` to an Attempt | `test_item_current_attempt_has_deferred_composite_foreign_key` |
| P1-M05 | Attempt 0 | Same transaction inserts Item and Attempt 0; no source Attempt/workflow/request provenance | Schema supports the deferred FK; lifecycle execution moves to P2 |
| P1-M06 | Later Attempt | Inserts ordinal `source+1`; old terminal Attempt remains; source and request/retry provenance are required | Append-only table and no-delete trigger are P1; transition execution moves to P2 |
| P1-M07 | Shared execution | `workflow_id` and `execution_key` are indexed but not unique | `test_shared_execution_identity_is_not_unique_per_attempt` |
| P1-M08 | Terminal Attempt mutation | Any non-no-op update is rejected | `test_fresh_upgrade_installs_append_only_and_terminal_guards` |

## Claim and compensation ledgers

| ID | Source/event | Durable result | Required invariant |
| --- | --- | --- | --- |
| P1-C01 | Attempt wins a Claim CAS | New `(item_id,attempt,claim_id)` row in `CLAIMED`; Attempt points to it | Claim identity is never stored only on the Attempt |
| P1-C02 | Current valid Claim crosses call boundary | Same row advances once to `CALL_STARTED` with timestamp committed before DBOS | Workflow identity and call-start fact become immutable |
| P1-C03 | Claim expires or is replaced | Old row advances to `EXPIRED` or `REPLACED`; replacement inserts a new Claim | Old row is neither deleted nor reused |
| P1-C04 | Cancellation invalidates Claim | Same row advances to `INVALIDATED`; terminalizing Attempt may clear only its current pointer | Claim history survives terminalization |
| P1-C05 | Winning outcome CAS | Same Claim advances to `OUTCOME_RECORDED` | One external-call boundary per Claim |
| P1-C06 | Late enqueue loses outcome CAS | Insert/exact-reload compensation with the same composite key and exact Claim FK | Terminal Attempt remains immutable |
| P1-C07 | Exact compensation replay | Existing equal row is returned | No duplicate side effect or new identity |
| P1-C08 | Unequal compensation replay | Integrity conflict | Workflow and reason cannot be redefined |
| P1-C09 | Claim/compensation deletion | Database rejects deletion | `test_fresh_upgrade_installs_append_only_and_terminal_guards` |
| P1-C10 | Multiple stale claimants | One Claim and compensation row per distinct `claim_id` | Composite primary/FK shapes in `test_claim_and_compensation_keys_are_exact` |

Closed Claim dispositions are `claimed`, `call_started`, `outcome_recorded`,
`expired`, `replaced`, and `invalidated`. Closed compensation dispositions are
`pending`, `failed`, `cancelled`, `observed_terminal`, `skipped_shared`, and
`no_workflow_found`.

## Physical schema, migration, and change ownership

| ID | Contract | Expected result | Executable assertion |
| --- | --- | --- | --- |
| P1-S01 | Prefix API | `PlatformSchema(prefix="platform")` has exactly one defaulted configuration knob | `test_platform_schema_constructor_has_only_prefix` |
| P1-S02 | Fixed tables | Exactly operations, items, item attempts, next-Attempt requests, enqueue Claims, enqueue compensations, and throttle state under the prefix | `test_platform_schema_has_final_tables_and_columns` |
| P1-S03 | Fresh lineage | Exactly one new `0001`; upgrade from empty `public` succeeds | `test_migration_lineage_is_one_fresh_baseline`; `test_fresh_upgrade_creates_only_final_tables` |
| P1-S04 | No adoption | No `stamp_platform_schema`, `PlatformNaming`, `naming=` migration argument, compatibility view, or old `0002` | `test_legacy_root_exports_are_removed`; `test_migration_lineage_is_one_fresh_baseline` |
| P1-S05 | Change sequence owner | One prefix-scoped Postgres sequence; database triggers assign `change_seq` on insert/update for operations, items, attempts, requests, Claims, compensations, and throttle state | `test_fresh_upgrade_installs_change_sequence_ownership` |
| P1-S06 | Operation cut | `platform_cut_version` is positive; owning lifecycle transaction increments it exactly once; reads/no-op replays do not | Column/check pinned in P1; transactional transition tests move to P2 |
| P1-S07 | No hard deletion | Lifecycle rows cannot be deleted | `test_fresh_upgrade_installs_append_only_and_terminal_guards` |
| P1-S08 | Enum checks | Migration imports closed sets through `enum_check`; it does not hand-type divergent lists | `test_schema_enum_checks_use_the_closed_values` plus migration source review |
| P1-S09 | No source export state | No `<prefix>_export_state` or projections table exists | `test_platform_schema_has_final_tables_and_columns`; `test_fresh_upgrade_creates_only_final_tables` |

## Exit gate

P1 is complete only when the focused command passes against a fresh scratch
schema, `uv run ruff check .` and `uv run ty check` pass, and all IDs above are
either executed in P1 or explicitly carried into their named P2 lifecycle
test. Passing against an adopted/stamped legacy schema is not acceptable
evidence.
