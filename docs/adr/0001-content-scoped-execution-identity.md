# Keep execution identity content-scoped across Operations

Whetstone executions retain content-scoped identity: submitting the same
Prediction through multiple dr-platform Operations converges on the same
Generation Run for a given attempt only when their complete execution recipes
are equal. Prediction ID remains Whetstone's domain identity; a separate
versioned concrete `execution_recipe_digest` covers the exact canonical domain
input, workflow name/implementation/version, argument-recipe version,
application version, and relevant profile, parser, dataset, and
provider-configuration versions. The digest is persisted on the Attempt and
participates in Generation Run and DBOS workflow identity. The Operation
persists an ordered aggregate of its Item recipe digests for exact
resubmission. Registration may accept an existing domain row only after exact
canonical equality, never from identity conflict alone.

dr-platform defines only a minimal versioned recipe envelope and treats its
canonical caller payload as opaque; Whetstone owns and validates the domain
recipe model. Every lifecycle-driving process registers the complete runtime
target under an immutable persisted key/version/contract digest. The shared
resolver fails closed on missing or conflicting registration, and final
Registration recomputes every concrete recipe leaf plus the ordered aggregate.

The caller supplies the stable execution identity used to derive the DBOS
workflow ID, while dr-platform owns safe claiming and enqueue mechanics; this
preserves cross-Operation deduplication
for genuinely identical recipes and avoids both duplicate provider spend and
stale-work reuse. It adds explicit recipe models and causes any
execution-affecting version change to create new work without coupling domain
Prediction identity to infrastructure evolution.
