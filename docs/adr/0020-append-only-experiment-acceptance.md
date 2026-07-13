# Persist append-only Experiment acceptance with one current pointer

Whetstone records every Experiment-acceptance evaluation as an immutable row
with immutable membership rows naming the exact Generation Runs, Score
Attempts, generation and scoring Manifest digests, platform Operation/Attempt
cuts, required profiles, observed matrix, policy version, and any partial
override and operator facts. The Experiment carries a monotonically increasing
acceptance-source version and a nullable current-acceptance pointer.

Each Experiment accepts exactly one Generation Operation/Manifest, fixed by
its first accepted relationship; membership growth uses a new Experiment
identity/version. Accepted Scoring relationships remain plural and receive
immutable monotonically increasing per-Experiment ordinals.

For each Prediction, the evaluation selects the successful Generation Run
with the highest platform Attempt ordinal in its pinned single-Generation
lineage. Earlier successful runs remain recorded as superseded provenance but
do not create expected scoring cells. For each logical scoring cell, the newest
accepted Scoring relationship with a successful candidate for the cell's
pinned accepted Generation Run wins, then its highest successful Attempt in
that Item lineage wins. Other-run candidates remain
`SUPERSEDED_GENERATION` and cannot win. The ordered relationship vector, selected
inputs, every candidate, and supersession reason enter immutable evaluation
identity/provenance.

The empty canonical accepted-Scoring-relationship vector is valid. Before the
first scoring relationship, Whetstone persists a `PARTIAL` evaluation with
explicit `MISSING_SCORE` members; later scoring acceptance appends a new
evaluation and never rewrites that earlier decision.

Every relevant new domain outcome or accepted Manifest relationship increments
the source version and clears the pointer in the same transaction, making the
prior evaluation historical without rewriting it. Evaluation reads one source
version and a sorted vector of each contributing dr-platform Operation's
monotonic `platform_cut_version`, inserts its immutable record and members,
then advances the current pointer only in a transaction that locks and proves
both the domain source version and every pinned Operation version still match.
Before promotion, every accepted-relationship Operation must be terminal and
every selected Generation/Score candidate's exact platform Attempt must be
terminal `SUCCEEDED` with DBOS `SUCCESS`.
Every current read performs the same version-vector check in one consistent
database snapshot. A lost comparison or later platform mutation makes the
evaluation historical and requires reevaluation even when no Whetstone domain
row changed. This adds durable rows and version checks without coupling
dr-platform mutations to Experiment invalidation, while making every past
decision reproducible and currentness unambiguous.
