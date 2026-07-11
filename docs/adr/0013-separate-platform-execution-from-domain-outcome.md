# Separate platform execution success from domain outcome

An Operation succeeds when its required DBOS workflows complete durably under
the platform lifecycle; it does not assert that a model answer passed or that
scoring produced a successful domain result. Whetstone derives Experiment
outcome from its append-only Generation Run and Score Attempt records and
reports that beside the linked platform Operation statuses. This preserves one
owner for domain truth while keeping platform execution failures visible.
