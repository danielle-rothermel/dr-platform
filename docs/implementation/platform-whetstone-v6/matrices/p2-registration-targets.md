# P2 manifest registration and target-resolution matrix

This matrix is the executable acceptance contract for P2. It specializes the
frozen v6 Platform contract without changing it. Scenario IDs are stable
references for tests, implementation decisions, and review findings.

Run the focused gate with:

```console
uv run pytest tests/contracts/test_platform_v6_registration_targets.py -q
```

Database scenarios use the explicit scratch database selected by the shared
`clean_pg` fixture. They do not call DBOS or any remote service. All simulated
time is passed explicitly; tests never sleep.

## Manifest identity and replay

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P2-M01 | No Operation | Valid frozen Manifest | Create `REGISTERING` Operation and acquire Registration Lease | Accepted Manifest identity and immutable Operation inputs are persisted before any page rows |
| P2-M02 | Existing Operation | Exact Manifest and immutable fields replayed | Resume or return the existing receipt | No duplicate Item, Attempt, or hook fact; no no-op cut-version increment |
| P2-M03 | Existing Operation | Same Items reordered | Hard manifest conflict before hook execution | Caller order is identity through leaf and page digests |
| P2-M04 | Existing Operation | Source truncated | Hard manifest conflict before hook execution | Existing committed pages remain unchanged |
| P2-M05 | Existing Operation | Source extended | Hard manifest conflict before hook execution | Existing committed pages remain unchanged |
| P2-M06 | Existing Operation | Same Manifest digest but unequal immutable Operation field | Integrity conflict before hook execution | `group_key`, `workflow_role`, `spec`, `metadata`, Retry Policy, target ref, and aggregate recipe are exact-replay fields |
| P2-M07 | Manifest model | Descriptor gap, overlap, wrong page index, short non-final page, or invalid digest | Validation failure | Pages are contiguous, bounded by `page_size`, cover exactly `[0,item_count)`, and enter Manifest identity |
| P2-M08 | No Operation | Empty Manifest | Atomically persist `FAILED/empty_submission` | No Lease, page transaction, Item, Attempt, or hook invocation |

## Page registration, Lease, and hook transaction

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P2-R01 | Cursor 0, live owning Lease | Register page 0 | Insert page Items and Attempt 0, apply hook, advance cursor once | Hook and kernel rows commit in one transaction |
| P2-R02 | Cursor 0, live foreign Lease | Competing registrar | `REGISTRATION_LEASE_HELD` | No hook invocation or row mutation |
| P2-R03 | Cursor 1 after committed page; registrar crashed | Exact replay after Lease expiry | New Lease resumes at cursor 1 | Page 0 is digest-checked but never re-applied |
| P2-R04 | Cursor 1 | Stale holder attempts cursor CAS | CAS loss and reload | Stale holder cannot mutate hook or kernel rows |
| P2-R05 | Page transaction open | Crash/exception before commit | Transaction rolls back | Cursor, hook rows, Items, and Attempts all remain at the prior cut |
| P2-R06 | Hook has no row | Hook returns `INSERTED` for every ordered Item | Page commits | Result keys and order exactly match page input |
| P2-R07 | Hook has equal canonical row | Hook returns `ALREADY_PRESENT` | Page commits idempotently | Existing domain row is proven exactly equal |
| P2-R08 | Hook has unequal row or malformed accounting | Hook conflict, missing/extra/duplicate/reordered result | Entire page rolls back | No partial domain/kernel provenance survives |
| P2-R09 | Final page | Owning Lease and final cursor CAS win | Set completion marker, clear Lease, transition to `ENQUEUEING` | Only the final page transaction makes Items claimable |

## Target registration and restart resolution

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P2-T01 | Empty registry | Register one target declaration | Resolve the exact runtime target by persisted ref | Callable identity is runtime-only; declaration digest is persisted identity |
| P2-T02 | Existing equal target | Register exact same target again | Idempotent success | Registry remains one entry |
| P2-T03 | Existing key/version | Register unequal declaration or callable/name/topology tuple | `TARGET_CONFLICT` | Original registration remains authoritative |
| P2-T04 | Fresh process registry | Register equivalent reconstructed target | Persisted ref resolves | Restart does not depend on Python object identity |
| P2-T05 | Registry lacks persisted ref | Resolve lifecycle target | `TARGET_UNAVAILABLE` | No lifecycle mutation occurs |
| P2-T06 | Registry has key/version but digest differs | Resolve persisted ref | `TARGET_CONFLICT` | No ad hoc target substitution is allowed |
| P2-T07 | Target declaration | Non-top-level topology | Registration rejection | Pre-experiment cut permits `TOP_LEVEL_ONLY` only |

## Abandonment and cross-Operation identities

| ID | Starting state | Input/event | Expected result | Invariant |
| --- | --- | --- | --- | --- |
| P2-A01 | Incomplete registration, live Lease | Abandon requested | Ineligible | Operator cannot preempt a live registrar |
| P2-A02 | Incomplete registration, expired Lease | Named operator confirms with reason | Sticky `FAILED/registration_abandoned` | Lease clears; committed Item, Attempt, and hook rows remain provenance |
| P2-A03 | Registration completed concurrently | Abandon locks after completion | Ineligible | Completion wins under the Operation row lock |
| P2-A04 | Already abandoned Operation | Exact abandonment replay | Return stored result | No cut-version increment or fact rewrite |
| P2-A05 | Same `item_key` in two Operations | Derive Item identities | Different Operation-local `item_id` values | Item row identity never deduplicates across Operations |
| P2-A06 | Same content and Attempt in two Operations | Target derives execution identity | Same content-scoped execution key/workflow ID | DBOS execution may deduplicate while both Operation-local references remain distinct |
| P2-A07 | Different recipe or Attempt ordinal | Target derives execution identity | Different execution key/workflow ID | Deduplication never crosses recipe or Attempt identity |

## Executable coverage map

| Scenario IDs | Focused assertion |
| --- | --- |
| P2-M01, P2-M02, P2-R01, P2-R06, P2-R09 | `test_new_registration_and_exact_replay_apply_each_hook_page_once` |
| P2-M03–P2-M06 | `test_changed_replay_conflicts_before_hook`; `test_manifest_mutation_cannot_reuse_an_issued_digest` |
| P2-M07 | `test_manifest_rejects_invalid_page_coverage_even_with_a_fresh_digest` |
| P2-M08 | `test_empty_manifest_is_failed_without_invoking_hook` |
| P2-R02 | `test_live_lease_blocks_competing_submit_and_abandonment` |
| P2-R03 | `test_expired_lease_resumes_after_a_committed_page` |
| P2-R04 | `test_registration_cursor_cas_rejects_authority_lost_during_hook` |
| P2-R05, P2-R08 | `test_hook_accounting_conflict_rolls_back_the_complete_page`; `test_expired_lease_resumes_after_a_committed_page` |
| P2-R07 | `test_hook_already_present_accounting_commits_idempotently` |
| P2-T01, P2-T02 | `test_target_registration_and_exact_duplicate_are_idempotent` |
| P2-T03 | `test_target_key_version_conflict_retains_original_registration`; `test_same_declaration_with_different_workflow_callable_conflicts`; `test_managed_workflow_name_cannot_belong_to_two_target_refs` |
| P2-T04 | `test_fresh_registry_resolves_a_serialized_persisted_reference` |
| P2-T05, P2-T06 | `test_unavailable_and_digest_mismatch_resolution_fail_closed`; `test_registration_rejects_a_ref_that_does_not_match_declaration` |
| P2-T07 | `test_target_declaration_rejects_non_top_level_topology` |
| P2-A01 | `test_live_lease_blocks_competing_submit_and_abandonment` |
| P2-A02, P2-A04 | `test_abandonment_after_expiry_is_sticky_and_preserves_committed_rows` |
| P2-A03 | `test_completed_registration_cannot_be_abandoned` |
| P2-A05 | `test_item_identity_is_operation_local` |
| P2-A06, P2-A07 | `test_content_execution_identity_deduplicates_across_operations` |

## Exit gate

P2 is complete only when every row above is either an executable assertion in
the focused suite or explicitly named as a later P3 enqueue-Claim scenario,
the focused suite passes against a fresh scratch schema, repo lint and type
checks pass, the legacy callback/`Batch`/naming paths remain absent, and a
fresh reviewer finds no unresolved contract or transaction-boundary issue.
