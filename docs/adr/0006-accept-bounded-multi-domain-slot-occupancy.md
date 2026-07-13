# Keep adaptive in-workflow pacing and bound multi-domain slot occupancy

Whetstone keeps one graph execution as one DBOS workflow even when its nodes
cross throttle domains. Durable in-workflow backoff may therefore occupy a
shared generation-queue slot; the system bounds and observes that residual
risk with sleep caps, queue headroom, runtime concurrency controls, health
checks, and guarded cancellation rather than splitting the graph workflow or
claiming one queue per throttle domain provides isolation it cannot enforce.
Adaptive backoff plus operator holds remains the sole pacing policy; DBOS
static rate limiters are not layered on as a second, independently configured
mechanism.
