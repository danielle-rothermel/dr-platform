# P4 reconciliation, retries, and next-Attempt matrix

This matrix is the executable acceptance contract for P4. It specializes the
frozen v6 Platform contract without changing the normative packet. Scenario
IDs are stable references for implementation, tests, and review findings.

Run the focused gate with:

```console
uv run pytest tests/contracts/test_platform_v6_reconciliation_retries.py -q
```

Database scenarios use the disposable PostgreSQL schema selected by the
shared `clean_pg` fixture. DBOS observations, clocks, queue admission, and
physical enqueue are deterministic fakes; no test calls a remote service.

## Bounded lifecycle cycle and candidate loading

| ID | Starting state | Event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P4-L01 | Current Attempts include terminal `SUCCEEDED`, `CANCELLED`, terminal `ERROR`, `RECOVERY_EXHAUSTED`, and `MISSING` plus actionable work | Load a bounded reconciliation page | Irreversible terminal states are excluded; actionable work is loaded before a separate lower-priority terminal-`MISSING` re-observation page consumes residual capacity; multiple `MISSING` rows rotate oldest-reobserved first | Terminal history cannot starve repairable work, while durable scheduling facts prevent one `MISSING` prefix from starving its peers |
| P4-L02 | Current `PENDING`, `CLAIMING`, confirmed nonterminal, and `ENQUEUE_ERROR` Attempts coexist | Load a bounded page | `PENDING`/`CLAIMING` are owned by enqueue Claim processing; confirmed nonterminal and `ENQUEUE_ERROR` are reconciliation candidates | One owner mutates each state family |
| P4-L03 | Recovery, actionable lifecycle observation, terminal-`MISSING` re-observation, expired never-started replacement, and pending work exceed one cycle bound | Public `reconcile` | The stages run in that order; each receives only the capacity left by actual prior-stage results | Total processed work is at most one shared page bound |
| P4-L04 | Exact completed Manifest replay | Public `submit`/`submit_jsonl` | Reconcile precedes Claim repair and new pending enqueue through the sole facade | Lifecycle progress is not delegated to an application-owned background loop |

## Crash, replay, and late completion

| ID | Starting state | Observation/replay | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P4-R01 | Confirmed enqueue, execution `NOT_STARTED` | DBOS live then terminal success | Attempt becomes `ACTIVE`, then terminal `SUCCEEDED`; exact terminal replay is a no-op | Every authoritative change is durable and terminal history is immutable |
| P4-R02 | Source Attempt becomes terminal while another actor allocates a successor | Late result for the old workflow | Current Item pointer and successor remain unchanged | A stale completion cannot rewrite a later Attempt |
| P4-R03 | DBOS lookup is unavailable, ambiguous, identity-mismatched, or topology-invalid | Normalize observation | Persist no lifecycle mutation | Uncertainty is not absence and cannot authorize retry or missing terminalization |
| P4-R04 | DBOS reports `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | Reconcile and replay | Current Attempt becomes terminal `RECOVERY_EXHAUSTED`; replay allocates nothing | Recovery exhaustion is distinct and never automatically retried |
| P4-R05 | Current Attempt is `CANCEL_REQUESTED` | DBOS reports live or a non-cancel terminal without proof it preceded cancellation | No Attempt mutation or Operation cut change | Cancellation intent is sticky; only cancellation or separately proven pre-cancel terminal truth may finalize it |

## Missing workflow policy

| ID | Starting state | Absence observation | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P4-M01 | Unconfirmed enqueue | DBOS row absent | No missing count or terminal transition | Absence before confirmed enqueue belongs to Claim recovery |
| P4-M02 | Confirmed enqueue | Fewer than required observations or grace not elapsed | Increment durable first/last/count only | One lookup never proves missing work |
| P4-M03 | Confirmed enqueue | Required count and grace both satisfied | Terminal `MISSING` with preserved diagnostics | Both independent bounds are required |
| P4-M04 | Current terminal `MISSING` | Periodic late success/error/active observation | Observe again, advance only the separate domain-neutral re-observation scheduling fact, and do not rewrite the terminal Attempt | P4 preserves immutable truth while keeping the A2 late-commit signal observable and fairly rotating multiple missing rows |
| P4-M05 | Two terminal `MISSING` Attempts, page size 1 | Run three re-observation cycles | Oldest marker order selects A, B, then A; marker counts/change sequences advance while both Attempts remain byte-for-byte unchanged | No terminal missing candidate starves |

## Enqueue and execution retry bounds

| ID | Starting state | Reconciliation | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P4-E01 | Retryable `ENQUEUE_ERROR`, `enqueue_try < max_enqueue_tries` | Reconcile | Same Attempt returns to `PENDING`; no new ordinal | Enqueue retry never becomes execution retry |
| P4-E02 | Permanent failure or enqueue bound exhausted | Reconcile | Attempt remains `ENQUEUE_ERROR` | Retry policy fails closed and is bounded |
| P4-E03 | Current `ENQUEUE_ERROR` resets to `PENDING` | Commit reconciliation | Recompute all three enqueue counters from current Attempts | Operation and `SubmitResult` do not retain superseded enqueue failure |
| P4-X01 | Confirmed Attempt observes retryable DBOS `ERROR` below `max_attempts` | Reconcile | Old Attempt becomes terminal `ERROR`; exactly `attempt + 1` is inserted and becomes current | Execution retry is append-only and platform allocates the ordinal |
| P4-X02 | Retryable DBOS `ERROR` at execution bound | Reconcile | Source records `EXHAUSTED`; no successor | `max_attempts` counts attempt 0 and cannot be exceeded |
| P4-X03 | Permanent/unclassifiable failure | Reconcile | Source records `PERMANENT`; no successor | Only the frozen allowlist authorizes automatic retry |
| P4-X04 | Retryable execution error derives a successor workflow | Begin reconciliation mutation | The shared helper sorts/deduplicates source and successor workflow IDs before Operation, Item, or Attempt rows, including when the successor sorts first | Every caller follows the global workflow-reference lock order |
| P4-X05 | Workflow lock helper receives reverse and duplicate IDs | Acquire advisory locks | Observe one lexical acquisition per unique ID | Helper-level ordering cannot drift between callers |
| P4-X06 | Automatic or requested successor becomes current `PENDING` | Commit successor creation | Current-Attempt enqueue aggregates become zero in the same transaction | Prior source enqueue state cannot leak through the new pointer |

Repeated observation has exact replay semantics: an unchanged `ACTIVE` state
and DBOS status does not rewrite `updated_at` or increment the Operation cut.
An `ACTIVE` observation after a prior absence streak clears the missing count
and timestamps and is a real cut-changing mutation.

## Caller-requested next Attempt

| ID | Starting state | Request race/replay | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P4-N01 | Terminal `SUCCEEDED` source plus `DOMAIN_OUTCOME` evidence | Exact request replay | One request row, one successor, equal result | `request_id` is stable and replay is idempotent |
| P4-N02 | Same `(item_id, request_key)` with unequal payload | Replay | Typed idempotency conflict; no new Attempt | A stable key cannot redefine authorization |
| P4-N03 | Two identical requests race | Concurrent calls | Both return the same `CREATED` result; one request and one successor persist | Concurrency converges under the Operation lock |
| P4-N04 | Different request keys name one source | Concurrent calls | One `CREATED`; loser is `SOURCE_ADVANCED` and never creates `source + 2` | A winner is not authorization for another ordinal |
| P4-N05 | `DOMAIN_OUTCOME` from non-`SUCCEEDED`, cancel retry from non-`CANCELLED` or without provenance, `MISSING`, `RECOVERY_EXHAUSTED`, or enqueue-only failure | Request | Persist `INELIGIBLE`; no successor | The reason/source matrix is closed |
| P4-N06 | Optional request bound is below or above immutable RetryPolicy | Request | Effective bound is the minimum; exhausted requests persist `MAX_ATTEMPTS_EXHAUSTED` | A request may tighten but never expand policy |

## A2 delayed-enqueue precursor

| ID | Starting state | Forced schedule | Expected P4 precursor | Gate status |
| --- | --- | --- | --- | --- |
| P4-A201 | A call-started enqueue is blocked before its independent DBOS commit; repeated absence reaches the current grace/count | Release DBOS commit after terminal `MISSING`, then kill claimant before return/outcome CAS | Later reconciliation must still observe the workflow; terminal Attempt is not rewritten in P4 | Precursor only: proves periodic observation and deterministic delayed-commit fixture |
| P4-A202 | Claimant reports enqueue success after the missing hazard resolved | Late claimant outcome | The durable Claim identity remains available for the A2 successor protocol; no blind replacement is authorized | Open through P5: this matrix does not claim safe compensation closure |

The A2 rows deliberately do **not** mark the frozen v6 review finding closed.
They preserve the blocked-enqueue schedule and the periodic observation needed
by its successor. Safe late-commit cancellation, shared-reference resolution,
and final hazard disposition remain tracked in the implementation risk
register.

Implementation discovered that immutable terminal Attempt evidence alone
cannot provide fair periodic scheduling: its frozen first/last missing facts
cannot be rewritten after terminalization. P4 therefore adds a minimal,
domain-neutral operational re-observation fact keyed by `(item_id, attempt)`
with only last-reobserved time and count. It contains no DBOS payload or
diagnostic content, is change-tracked and delete-guarded, and orders the
lower-priority MISSING pass oldest-first. This closes the scheduling durability
gap without changing the frozen terminal Attempt lifecycle truth; A2 physical
late-commit compensation remains open through P5.

## Executable coverage

| Scenario IDs | Focused assertion |
| --- | --- |
| P4-L01–P4-L02 | `test_actionable_loader_excludes_terminal_and_claim_owned_states`; `test_leading_missing_cannot_starve_later_actionable_candidate` |
| P4-L03 | `test_public_reconcile_uses_one_shared_budget_and_enqueues_new_work` |
| P4-L04 | `test_exact_resubmit_reconciles_before_claim_repair_and_pending_enqueue` |
| P4-R01–P4-R02, P4-R05 | `test_terminal_success_replay_is_a_noop`; `test_identical_active_is_noop_but_active_resets_missing_streak`; `test_late_source_completion_cannot_rewrite_successor`; `test_active_observation_cannot_reverse_cancel_requested` |
| P4-R03 | `test_ambiguous_lookup_is_uncertain_and_does_not_mutate` |
| P4-R04 | `test_recovery_exhaustion_is_terminal_and_never_retried` |
| P4-M01–P4-M03 | `test_missing_requires_confirmed_enqueue_count_and_grace` |
| P4-M04–P4-M05, P4-A201 | `test_terminal_missing_is_periodically_reobserved_without_rewrite`; `test_terminal_missing_reobservation_rotates_oldest_without_rewrite` |
| P4-E01–P4-E03 | `test_enqueue_retry_reuses_attempt_and_respects_bound` |
| P4-X01–P4-X06 | `test_execution_retry_allocates_one_attempt_and_respects_policy`; `test_automatic_retry_locks_successor_workflow_before_domain_rows`; `test_workflow_reference_lock_helper_sorts_and_deduplicates` |
| P4-N01–P4-N06 | request idempotency, concurrency, eligibility, and bound tests |
| P4-A202 | durable Claim/recovery tests remain in the P3 matrix; P5 owns safe compensation closure |

## Exit gate

P4 is complete when every non-A2 scenario above is executable against a fresh
schema, the focused suite passes, the root facade owns the bounded lifecycle
cycle, terminal history cannot starve actionable candidates, exact replay is
idempotent, and a fresh reviewer finds no unresolved retry-bound, ordinal,
missing-observation, or mutation-ownership issue. A2 remains explicitly open
until its successor compensation protocol and blocked-commit gate land.
