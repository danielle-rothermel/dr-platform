# Separate service urgency from deterministic shuffle order

dr-platform uses fixed service classes mapped to DBOS priority for genuine
urgency, while a kernel-derived stable shuffle rank controls claim and enqueue
order within a class. This preserves automatic mixing when callers generate
Items in model-grouped blocks without using random DBOS priorities that can
starve older work under sustained arrivals; original Item position remains a
separate result-ordering fact. Deterministic shuffling is a required
pre-experiment safety property, not an optimization: model-blocked execution
order has invalidated or brought down multiple prior experiments.

The deterministic contract ends at kernel rank, claim, and enqueue order. DBOS
2.26.0 dequeues by `(priority, created_at)` and may reorder same-priority rows
whose millisecond timestamps tie, especially across multiple dequeuers. The
owner accepts that final-order nondeterminism because the required property is
bounded mixing before enqueue, not exact reproduction of workflow start order.
No DBOS fork or separate scheduler is introduced for a stronger guarantee.
