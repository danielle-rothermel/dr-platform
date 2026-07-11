# Separate service urgency from deterministic shuffle order

dr-platform uses fixed service classes mapped to DBOS priority for genuine
urgency, while a kernel-derived stable shuffle rank controls claim and enqueue
order within a class. This preserves automatic mixing when callers generate
Items in model-grouped blocks without using random DBOS priorities that can
starve older work under sustained arrivals; original Item position remains a
separate result-ordering fact. Deterministic shuffling is a required
pre-experiment safety property, not an optimization: model-blocked execution
order has invalidated or brought down multiple prior experiments.
