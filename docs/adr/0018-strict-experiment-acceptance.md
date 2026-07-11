# Require strict Experiment acceptance by default

Whetstone accepts an Experiment by default only when every expected Prediction
has an accepted domain-successful Generation Run and every required scoring
profile has an accepted Score Attempt. DBOS and dr-platform terminal success
remain necessary execution facts but do not satisfy this domain predicate. A
Prediction with multiple successful Generation Runs accepts the run with the
highest platform Attempt ordinal at the evaluation's pinned source cut within
the Experiment's one accepted Generation Operation/Manifest lineage. The first
accepted generation relationship fixes membership; an unequal second
relationship is rejected, and growth requires a new Experiment identity/version.
Earlier successful runs remain immutable superseded provenance and do not add
required Score Attempt cells. A `PARTIAL` Generation Run is scoring-eligible
only when persisted `terminal_submission_text IS NOT NULL AND
terminal_submission_text ~ '[^[:space:]]'`; empty and POSIX-whitespace-only
rows are excluded before Manifest identity. Populated `PARTIAL` runs do not
satisfy this strict Generation predicate unless a separate explicit persisted
policy authorizes them. Across accepted Scoring relationships, a logical cell
first pins the evaluation's selected accepted Generation Run. It then selects
from the newest monotonically ordered relationship containing a run-matched
success and that relationship's highest successful platform Attempt; other-
run candidates are immutable `SUPERSEDED_GENERATION` provenance and cannot
satisfy the cell.
A
partial result may be accepted only through an explicit policy that persists
minimums and observed counts per meaningful stratum and records operator
confirmation; a global percentage alone cannot hide model-, task-, or
profile-correlated failure. This makes exploratory partial use possible while
preventing incomplete or biased results from being labeled complete silently.
Per ADR 0020, the predicate is enforced through append-only evaluations that
pin exact Manifest, domain-row, policy, observed-matrix, and platform cuts;
one source-version-guarded pointer identifies the candidate current evaluation.
Promotion and every current read also atomically compare the evaluation's
sorted Operation/`platform_cut_version` vector with dr-platform. A mismatch
makes the evaluation historical even if no new Whetstone domain row exists.
Promotion additionally requires every accepted-relationship Operation to be
terminal and every selected exact platform Attempt to be terminal `SUCCEEDED`
with DBOS `SUCCESS`; a partial override cannot waive execution success for a
selected candidate.
Before any Scoring relationship exists, the empty canonical relationship set
is valid and yields a durable `PARTIAL` evaluation with explicit
`MISSING_SCORE` members.
