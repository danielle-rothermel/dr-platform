# Manage scoring through a second platform Operation role

Whetstone uses the same dr-platform submission and lifecycle module for two
caller-owned roles: generation and scoring. A Scoring Operation contains
content-addressed scoring targets and uses platform claims, attempts, retry,
cancellation, correlation, and inspection; Whetstone still decides eligibility,
owns Score Attempt identity/persistence, and explicitly submits scoring after
generation. The kernel stores an open `workflow_role` string but does not
enumerate domain Operation types or orchestrate dependencies.
