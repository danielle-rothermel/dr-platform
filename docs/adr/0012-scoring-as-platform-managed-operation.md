# Manage scoring through a second platform Operation role

Whetstone uses the same dr-platform submission and lifecycle module for two
caller-owned roles: generation and scoring. A Scoring Operation contains
content-addressed scoring targets and uses platform claims, attempts, retry,
cancellation, correlation, and inspection; Whetstone still decides eligibility,
owns Score Attempt identity/persistence, may request a later platform Attempt
after a harness-failed domain outcome, and explicitly submits scoring after
generation. dr-platform remains the only ordinal-allocation authority. The
kernel stores an open `workflow_role` string but does not enumerate domain
Operation types or orchestrate dependencies.
Each frozen candidate selection has its own digest and therefore its own
Scoring Operation. One Experiment may accumulate multiple Scoring Operations
after late Generation Runs become eligible. Each accepted relationship receives
a monotonic per-Experiment ordinal. For an overlapping logical scoring cell,
Experiment acceptance selects from the newest relationship containing a
success, then its highest successful platform Attempt, while persisting all
candidate and supersession provenance rather than mutating an earlier Manifest.
