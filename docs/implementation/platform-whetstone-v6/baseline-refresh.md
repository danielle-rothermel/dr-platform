# Refreshed implementation baseline

Captured 2026-07-11 before implementation began.

| Repository | Reviewed head | Reviewed merge base | Refreshed canonical `origin/main` | Disposition |
| --- | --- | --- | --- | --- |
| `dr-platform` | `0999347daba4a701716703d6097815ad387a4c03` | `49b135f3053196ce0f592666dd22eb6be9a8c736` | `1a4b22a1ffa06df7f9e9a2bb6ef08aa1d3b0214a` | README-only ecosystem note; no implementation-contract change. |
| `whetstone-ai` | `e8f3c60dea1d7470305d2f7aa6aecc81de7cf77a` | `0be846a004b63747dfc32948e24f3be99ea70a0f` | `23254e87b12dd16c173a396cd09326abe0708a1d` | Retain snapshot validation/provenance, replace path identity and durable path/DSN workflow arguments with content identity. |
| `unitbench` | `cafd493ab9e9c1940106037209b1b218097f847e` | `ea16c3c91ad39f2dfdd28849ddb4be5baa86ca5a` | `b0b6556314778eaa08cd29f38196a5a824a4f548` | R1-R6 patches are already canonical; extend two-plane matrices rather than replaying them. |

Implementation stacks are rooted directly on these refreshed canonical commits. Existing planning branches are retained as history only.

## Whetstone snapshot correction

The refreshed Whetstone baseline makes dataset snapshots concrete implementation inputs. The final contract keeps SHA-256, validated header/version fields, raw-row validation, and deterministic sampling while applying these corrections:

- content bytes and validated header fields define immutable identity;
- an absolute or relative local path is a retrieval locator and never canonical identity;
- injected rows require an explicit matching content identity;
- scoring selection, item recipes, Score Attempt identity, relationships, acceptance, inspection, and publication carry the snapshot digest;
- managed workflow arguments contain stable identifiers, not a database URL or filesystem path;
- one step resolves and verifies the expected content once, avoiding path-keyed cache and double-read races; and
- the additive migration and fabricated unknown provenance are replaced by the fresh final schema.

## Unitbench surface correction

The page and adapter matrices include the canonical R1-R6 surfaces:

- Analysis: sweep dashboard, bootstrap variance, headroom heatmap, and compression summary;
- Detail: extraction flow, pipeline trace, and per-prediction compression evidence;
- the six `/dev/*` galleries remain fixture-only and require neither store; and
- harness failure is distinct from model-test failure without carrying the legacy Python publisher forward.
