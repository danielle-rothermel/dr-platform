# Keep the retry failure taxonomy in dr-platform

dr-platform owns the small domain-neutral `FailureClass` used by persisted
attempt failures, retry policy, and throttle backoff. Domain clients map their
own failure types at the platform seam; Whetstone maps
`dr_providers.FailureClass` rather than making the kernel depend on the
LM-provider package. This adds an explicit adapter but keeps the platform
independently reusable and its persisted policy vocabulary stable.
