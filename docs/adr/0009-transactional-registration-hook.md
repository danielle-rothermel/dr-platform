# Preserve caller registration inside the platform transaction

dr-platform retains a typed `RegistrationHook` that runs caller-owned,
idempotent domain registration inside each bounded Item registration
transaction and before those Items become enqueue-eligible. Whetstone uses it
to create Experiment and Prediction Spec rows that its workflows later load;
this keeps the kernel domain-agnostic while avoiding a separate ordered call
and the race where DBOS starts before required domain inputs exist.
