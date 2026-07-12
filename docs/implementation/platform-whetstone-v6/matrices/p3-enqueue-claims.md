# P3 kernel enqueue and append-only Claim matrix

This matrix is the executable acceptance contract for P3. It specializes the
frozen v6 Platform contract without changing it. Scenario IDs are stable
references for implementation, tests, evidence, and review findings.

Run the focused gate with:

```console
uv run pytest tests/contracts/test_platform_v6_enqueue_claims.py -q
```

Database scenarios use the explicit scratch database selected by the shared
`clean_pg` fixture. DBOS behavior is supplied by a deterministic adapter; no
test uses a live DBOS runtime, wall-clock sleeps, or remote services.

## Scheduling and admission

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P3-S01 | Registered Items in caller/model-grouped order | Build bounded claim page | Order by `(service_priority, shuffle_rank, item_id)` | Caller `item_index` remains result order and never controls scheduling |
| P3-S02 | Same Manifest replay | Rebuild claim page | Identical Item IDs, ranks, and claim order | Shuffle is deterministic and positive signed-63-bit |
| P3-S03 | Mixed Service Classes | Claim page | `URGENT(100)` before `STANDARD(1_000)` before `BACKFILL(10_000)` | Shuffle never becomes a wide DBOS priority |
| P3-S04 | Registration incomplete or abandoned | Claim requested | Typed ineligible result; no Claim | Only fully registered Operations are claimable |
| P3-S05 | Registration complete | Queue missing | Fail closed before Claim mutation | Startup/runtime configuration is never silently created or overwritten |
| P3-S06 | Registration complete | Queue exists with `priority_enabled=False` | Fail closed before Claim mutation | Every platform enqueue uses a priority-enabled database queue |

## Claim acquisition and lock order

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P3-C01 | Current Attempt `PENDING`, no Claim | One claimant wins | Insert `CLAIMED`; Attempt becomes `CLAIMING`, points at Claim, increments `enqueue_try` | Claim identity is append-only and Attempt holds only the current pointer |
| P3-C02 | Same eligible Attempt | Two claimers race | Exactly one wins; loser reloads typed current truth | No duplicate Claim, DBOS call, or raw uniqueness error |
| P3-C03 | Several eligible Items | Concurrent claim pages | Each Item/Attempt is claimed at most once | Page bound is execution-only and never changes identity |
| P3-C04 | Claim transaction | Mutation begins | Shared Export Barrier, sorted workflow advisory locks, ascending Operation/Item/Attempt rows, then Claim | DBOS is not called while application locks are held |
| P3-C05 | Two workflows requested in reverse caller order | Acquire reference locks | Database observes lexical workflow-ID lock order | No code path reverses the global lock hierarchy |
| P3-C06 | Cancellation guard or terminal Attempt exists | Claim requested | Ineligible, no Claim | Claim eligibility requires no cancellation intent and nonterminal execution |

## Claim Lease and call-start boundary

| ID | Claim state | Event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P3-L01 | `CLAIMED`, current, live | Start-call CAS | Same Claim becomes `CALL_STARTED`; immutable timestamp commits before DBOS | CAS key is exact `(item_id,attempt,current_claim_id,CLAIMING)` |
| P3-L02 | Stale/replaced/invalidated Claim | Start-call CAS | Typed CAS loss; DBOS adapter is not called | A claimant that cannot commit call-start never crosses the boundary |
| P3-L03 | `CLAIMED`, no call start, Lease expires | Recovery | Old Claim resolves `EXPIRED`/`REPLACED`; fresh Claim uses a new ID on the same Attempt/workflow | Old identity is never reused or deleted |
| P3-L04 | `CALL_STARTED`, process dies before outcome | Recovery observes DBOS | Existing workflow records accepted/already-present truth; absence follows the explicit same-ID recovery policy | Uncertainty is resolved before any later external call |
| P3-L05 | `CALL_STARTED`, DBOS lookup unavailable/ambiguous | Recovery | Leave hazard unresolved and fail closed | No blind second enqueue while call state is unknown |
| P3-L06 | Replaced claimant returns late | Outcome CAS | Loses without rewriting current Attempt or old Claim identity | Stale result reloads authoritative truth or opens exact compensation |

## DBOS enqueue outcomes and priority truth

| ID | DBOS observation | Durable result | Required facts |
| --- | --- | --- | --- |
| P3-D01 | Queue accepts new workflow | Attempt `ENQUEUED`; Claim `OUTCOME_RECORDED` | `enqueued_at`, effective priority=requested, `priority_source=ENQUEUED_HERE` |
| P3-D02 | Same workflow already exists live/successful | Attempt `WORKFLOW_ALREADY_PRESENT`; Claim `OUTCOME_RECORDED` | Read existing DBOS priority, `priority_source=LINKED_EXISTING`; never mutate shared workflow |
| P3-D03 | DBOS returns retryable enqueue error | Attempt `ENQUEUE_ERROR`; Claim resolves with typed failure | Later reconciliation may reset the same Attempt only below `max_enqueue_tries` |
| P3-D04 | Permanent or exhausted enqueue error | Attempt remains `ENQUEUE_ERROR`, terminal for Item | No execution Attempt was started and no replacement is allocated in P3 |
| P3-D05 | DBOS call succeeds, matching outcome CAS wins | Record once | Exact replay is a no-op and does not increment cut/version twice |
| P3-D06 | DBOS call succeeds, outcome CAS loses after invalidation/cancellation | Insert or exact-reload pending compensation by `(item_id,attempt,claim_id)` | Terminal Attempt stays immutable; no second enqueue |
| P3-D07 | Same compensation key, unequal workflow/reason | Integrity conflict | Compensation cannot redefine Claim provenance |
| P3-D08 | Enqueue adapter invoked | Context contains workflow ID, mapped priority, and allowlisted immutable attributes | No Operation ID, Item ID, prompt/output, endpoint, credential, or provider payload is attached |

## Crash and replay cuts

| ID | Crash point | Replay outcome | Invariant |
| --- | --- | --- | --- |
| P3-R01 | Before Claim transaction commits | Attempt remains `PENDING`; no Claim | Retry may claim normally |
| P3-R02 | After Claim commit, before call-start commit | Expired Claim is replaced; same Attempt/workflow | No external side effect was possible |
| P3-R03 | After call-start commit, before DBOS call | Observe DBOS first, then recover exact same workflow ID when absence is authoritative | Never allocate a new Attempt |
| P3-R04 | After DBOS accepts, before outcome write | Existing workflow is linked and Claim resolves | Never enqueue a second workflow ID |
| P3-R05 | After outcome write | Exact replay returns stored state | No extra Claim, call, compensation, or cut increment |

## Static ownership and hard cut

| ID | Check | Expected result |
| --- | --- | --- |
| P3-H01 | Search application code outside kernel enqueue adapter | No direct `DBOS.enqueue_workflow`, `start_workflow`, queue-options, or Claim-table mutation |
| P3-H02 | Search repository | No callback-era `dedup_enqueue`, `EnqueueOutcome`, `EnqueueItem`, `enqueue_callback`, `order_key`, or `PlatformNaming` |
| P3-H03 | Search repository | No `fairness.py`, `fair_ordered*`, `windows`, or fairness exports |
| P3-H04 | Public imports | Kernel owns one typed enqueue/reconcile facade; Claim/compensation records are intentional exports and row/CAS helpers remain private |

## Executable coverage

| Scenario IDs | Focused assertion |
| --- | --- |
| P3-S01–P3-S03 | `test_claim_order_is_service_then_deterministic_shuffle`; `test_claim_page_persists_service_then_shuffle_order` |
| P3-S04 | `test_registration_completion_gates_claim_admission` |
| P3-S05–P3-S06 | `test_queue_admission_fails_before_claim_mutation` |
| P3-C01–P3-C03 | `test_claim_page_persists_service_then_shuffle_order`; `test_concurrent_claimers_create_one_append_only_claim` |
| P3-C04–P3-C05 | `test_claim_transaction_observes_global_lock_order` |
| P3-C06 | `test_cancelled_or_terminal_attempt_is_not_claimable` |
| P3-L01–P3-L02 | `test_call_start_cas_is_exact_and_blocks_expired_claim`; `test_call_start_cas_precedes_physical_enqueue_and_blocks_loser` |
| P3-L03 | `test_expired_uncalled_claim_is_replaced_without_identity_reuse` |
| P3-L04–P3-L05, P3-R03–P3-R04 | `test_uncertain_physical_outcome_remains_unresolved`; runtime-owner crash fixture |
| P3-L06, P3-D06–P3-D07 | `test_lost_success_outcome_creates_exact_compensation_signal`; Claims-store compensation tests |
| P3-D01–P3-D05 | `test_physical_enqueue_outcomes_are_recorded_once`; Claims-store outcome tests |
| P3-D08 | `test_enqueue_adapter_receives_only_allowlisted_context` |
| P3-R01–P3-R05 | Claim, call-start, uncertainty, and store idempotency tests across the independent and owning suites |
| P3-H01–P3-H04 | `test_enqueue_ownership_and_legacy_paths_are_absent` |

## Exit gate

P3 is complete only when every scenario above is executable, the focused
suite passes against a fresh scratch schema, DBOS calls are fully deterministic
test-adapter observations, repo lint and type checks pass, legacy paths are
absent, and a fresh reviewer finds no unresolved Claim identity, lock-order,
call-boundary, priority, or mutation-ownership issue.
