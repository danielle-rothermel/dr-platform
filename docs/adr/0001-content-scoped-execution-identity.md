# Keep execution identity content-scoped across Operations

Whetstone executions retain content-scoped identity: submitting the same
Prediction through multiple dr-platform Operations converges on the same
Generation Run for a given attempt. The caller supplies the stable execution
identity used to derive the DBOS workflow ID, while dr-platform owns safe
claiming and enqueue mechanics; this preserves cross-Operation deduplication
and avoids duplicate provider spend at the cost of allowing multiple
Operations to reference one durable execution history.
