# Require a caller-prepared manifest and preserve its hook transaction

Before registration writes begin, the caller supplies an immutable Manifest of
the Operation's complete ordered Item set. dr-platform durably validates its
count, canonical digest, and page boundaries, grants one registrar a Lease,
and advances a persisted cursor under compare-and-swap until completion. Each
page retains a typed `RegistrationHook` that creates caller-owned domain rows
inside the same transaction as its Items; Whetstone uses it for Experiment and
Prediction Spec rows. `ALREADY_PRESENT` requires exact canonical equality with
the submitted domain row; an unequal identity conflict rolls back the page.
Enqueue is prohibited until registration completion, and every resubmission
must prove exact Manifest and Operation execution-recipe equality. This
proof uses concrete recipe digests recomputed through the persisted target's
startup resolver. The hook receives a typed final-page context so Whetstone
can accept its Experiment/Operation/Manifest relationship in the same
transaction as Registration completion, never for a partial Registration. This
requires callers to materialize or freeze their selection before submission,
but prevents bounded transaction pages, competing callers, or truncated
sources from redefining an Operation's membership.

If a non-empty Registration is partially committed and cannot be resumed, a
named operator may terminally mark it `FAILED/registration_abandoned` only
after the Registration Lease expires. Committed Items, Attempt-0 rows, and hook
rows remain immutable; no remaining page is invented and no hard deletion is
available.
