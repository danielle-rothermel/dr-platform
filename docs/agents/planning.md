# Planning docs

Versioned planning efforts in this repo live under
`docs/planning/<effort>/`. Each effort has a `README.md` index and one
directory per plan version:

```text
docs/planning/<effort>/
├── README.md
├── v0/
│   ├── plan.md
│   └── reviews/
│       ├── fable-prompt.md
│       ├── fable-findings.md
│       ├── codex-prompt.md
│       ├── codex-findings.md
│       └── unified-feedback.md
└── v1/
    ├── plan.md
    └── reviews/
```

The effort index names the current version and lifecycle status, links its
tracker map when one exists, and records the review scope and artifacts for
each version.

## Version lifecycle

- **draft** — mutable plan under active investigation or grilling
- **in-review** — frozen plan while prompts and findings accumulate
- **reviewed** — unified feedback complete; version immutable
- **superseded** — a successor version exists; version immutable

Create a successor by copying the reviewed plan into the next version and
applying accepted feedback there. Never revise an `in-review`, `reviewed`, or
`superseded` plan.

## Artifact ownership

- The issue-tracker map and tickets hold live questions and detailed decision
  resolutions.
- A version packet is the immutable snapshot of one plan and the reviews that
  evaluated it.
- `CONTEXT.md` and `docs/adr/` remain living canonical artifacts outside the
  packet. Plans link them rather than copying them.
- Reports, prototypes, and handoffs are temporary working artifacts. Capture
  durable conclusions in a ticket, active draft, glossary, or ADR.

Review prompts point to the exact plan version they evaluate and record the
code revision or date used for factual claims.

## Current efforts

- [Platform and Whetstone refactor](../planning/platform-whetstone-refactor/README.md)
