---
name: danielle-code-quality
description: Always use this skill when interacting with code in any way, including writing, editing, reviewing, debugging, explaining, refactoring, testing, or planning code changes.
---

# Danielle Code Quality

When interacting with code, apply these preferences as defaults unless the repo has a more local convention or the user explicitly asks for a different approach.

## Related Skills

Load these additional skills when their narrower scope applies:

- `danielle-python` for Python conventions around JSON I/O, `uv`, and Typer.
- `danielle-review-flow` when preparing changes for review or producing review findings.
- `danielle-diff-check` before finalizing code changes, refactors, commits, pull requests, or reviews with suggested patches.

## Working Agreements

### Surgical changes

Make the smallest coherent change that satisfies the request.

- Keep edits inside the requested behavior, file, function, or module unless a wider change is necessary.
- Preserve existing behavior, interfaces, diagnostics, logging, error messages, and debugging workflow.
- Leave unrelated cleanup for a separate change, including formatting, naming, dead paths, internal approaches, and adjacent refactors.
- If the request appears to require a broader rewrite, stop and explain the tradeoff before proceeding.

### Change-caused dead code only

Remove dead code only when the current change made it unused or invalid.

- This applies to code, tests, fixtures, docs, config, and examples.
- Leave pre-existing dead code, deprecated paths, commented-out code, unused helpers, and stale tests in place when they are unrelated.
- Mention unrelated cleanup separately instead of folding it into the current change.
- When the user asks for cleanup or refactoring, keep dead-code removal inside that requested boundary.

### Clean breaks over shims

When an interface, data shape, command, config key, or behavior must change, prefer one clear breaking change over compatibility layers.

- Avoid shims, aliases, adapters, migration layers, and dual paths unless the user asks for compatibility.
- Before making a breaking change, warn the user and name what will break.
- Update the affected callers, tests, configs, and docs as part of the same change.

### Surface assumptions

Inspect the repo first, then make the smallest reasonable assumption that fits existing patterns.

- Proceed when the ambiguity is small, local, and reversible. State the assumption briefly when it matters.
- Call out assumptions during planning when they affect ownership boundaries, module layout, data flow, persistence, API shape, abstraction strategy, or responsibility splits.
- Ask before proceeding when a choice affects public behavior, data shape, security, persistence, migrations, irreversible operations, user workflow, or structural direction.
- When multiple approaches are viable, choose the simplest one that matches the codebase and explain the tradeoff.

### Earned abstractions

Add abstractions only when they pay for themselves in the current code.

- Do not add interfaces, base classes, configuration layers, factories, registries, strategies, builders, or extension points speculatively.
- Add an abstraction when it removes real complexity, eliminates meaningful duplication, clarifies a stable boundary, or matches an established local pattern.
- Prefer interfaces or Python `Protocol`s over abstract base classes when a contract is useful.
- Keep single-use logic direct until there is concrete pressure to generalize.

## Code Structure

### Responsibility-revealing modules

Organize code so files and modules reveal their purpose from the repository tree.

- Separate different responsibilities into clearly named modules instead of mixing unrelated concerns for convenience.
- Keep business rules, validation, and domain transformations separate from HTTP handlers, database queries, file I/O, UI state, and external-service clients.
- Write adapters as thin boundary code: parse input, call focused domain code, and translate output.
- Keep transport, storage, UI, and framework details out of domain code unless they are the domain being modeled.

### Domain-facing contracts

Public contracts should expose domain-facing shapes, not infrastructure details.

- Do not expose ORM models, raw database rows, framework request/response objects, untyped dictionaries, or external API payloads unless that is the explicit boundary.
- When data crosses a boundary, translate internal and external shapes into domain-facing models.

### Exports define APIs

Use exports and module boundaries to signal public and private APIs.

- Treat exported or documented classes, functions, and models as public contracts.
- Treat internal, unexported code as private implementation detail.
- Communicate public API through module exports, `__all__`, package exports, documentation, and file or module placement.
- Do not use `_` class-name prefixes as the primary privacy signal.

### Inject replaceable dependencies

Inject dependencies when they are replaceable infrastructure.

- Pass varying infrastructure dependencies through constructors, providers, or explicit function parameters at the boundary.
- Avoid hard-coding external clients, repositories, clocks, loggers, metrics, feature flags, or state providers deep inside domain logic.
- Do not plumb a helper through constructors when it is not replaceable, external, or meaningful for testing.

### One job per function

Prefer functions with one clear responsibility and one level of abstraction.

- When two closely related functions serve the same goal, merge them into one coherent responsibility or separate them into clearly different jobs, names, inputs, and call sites.
- Treat a function as too large when it mixes concerns, hides steps, repeats logic, needs comments to follow, or makes error handling and verification hard.
- Do not use a universal line-count threshold.
- Extract helpers when they clarify intent, name a meaningful step, isolate validation or parsing, remove real duplication, or make testing easier.
- Do not extract helpers just to satisfy a size preference.

### Constraint comments

Use comments for constraints the code cannot express clearly.

- Prefer clearer code over comments when naming, a small helper, clearer branching, or a more explicit domain model can make the behavior self-explanatory.
- Do not add comments that restate what the code does.
- Add comments for external constraints, protocol quirks, migration context, performance tradeoffs, security assumptions, ordering requirements, or why an obvious alternative is wrong.
- When related behavior changes, remove or update nearby comments.

## Domain Data and Boundaries

### Explicit domain concepts

Represent domain meaning in code instead of passing around anonymous primitives.

- Prefer named models, enums, constants, typed identifiers, and small parsing helpers when meaning is not obvious locally.
- Avoid raw strings, dicts, tuples, booleans, and numbers when they obscure domain meaning.
- Add domain modeling when it clarifies a boundary, prevents value mixups, improves validation, or makes call sites inspectable.
- Do not add wrapper types speculatively.

### Type new and changed code

Add explicit types to new and changed code.

- Type added or modified functions, methods, classes, module constants, and public APIs.
- Make absence, failure, and optional values visible in return types.
- Avoid `Any` and untyped dictionaries unless the data is truly dynamic or intentionally untyped.
- Prefer named models, protocols, enums, aliases, or typed containers when structured values recur or cross boundaries.
- Do not type unrelated existing code during a targeted change.
- When untyped surrounding code blocks a clean implementation, type the smallest necessary boundary.

### Boundary parsing

Convert raw external data into domain-facing types at the boundary.

- Parse external strings, dictionaries, payloads, rows, CLI arguments, environment values, and user input as they enter the system.
- Once data is past the boundary, use typed models, `StrEnum`s, typed identifiers, constants, and validated values.
- Do not repeatedly parse raw shapes after boundary conversion.
- Do not pass raw external data into functions unless they are explicitly for parsing, validation, or transport adaptation.

### Domain literals as constants

Put domain-meaningful literals in `UPPER_SNAKE_CASE` constants near the top of the file, even when used once, to prevent inline-copy drift, make references searchable, and keep future reuse cheap.

- This includes MIME types, encoding names, magic prefixes, separators, format strings, sentinel values, magic numbers, environment variable names, and protocol keys.

### `StrEnum` value sets

Use `StrEnum` for closed, named sets of string values that represent distinct cases, so the value set is discoverable, extensible, inspectable, statically checkable, and centralized.

- Convert inline literals, `Literal[...]`, grouped constants, and fixed dict keys to `StrEnum` when they represent a named value set.
- Use `StrEnum` even for one-value sets when growth is anticipated.
- Use a named enum, usually `StrEnum`, when a parameter selects a mode, kind, role, strategy, workflow, or behavior variant, even if there are only two values.
- Do not add boolean flags that create hidden combinations or impossible states.
- A boolean such as `dry_run=True` is acceptable when a toggle is genuinely binary and clear at the call site.

### Pydantic for structured data

Use Pydantic `BaseModel` for structured data and validation boundaries.

- Use Pydantic for structured domain data, parsed input, persisted shapes, API payloads, and validation boundaries.
- Prefer Pydantic over dataclasses unless the repo strongly prefers dataclasses or the object is truly internal and behavior-free.
- Keep parent models declarative when parsing raw nested data.
- Prefer explicit child construction like `ChildModel(**value)` or `ItemModel(item_id=id, **value)` over hidden wiring.
- Put raw-key parsing in the model that owns those fields, typically in a `model_validator(mode="before")`.
- Prefer explicit helpers over dense validators or comprehensions when parsing is intricate.
- Use strict types and `ConfigDict(extra="forbid")` when ingest models should detect schema drift.

### Composition over inheritance

Prefer composition over inheritance for reuse and model structure.

- Do not introduce base classes solely to share fields, helper methods, configuration, or wiring.
- Use nested fields when one structured model is conceptually contained by another.
- When one model is a strict superset of another, compose the smaller model as a field on the larger model.
- Prefer `prepared.ref.field` over duplicating fields or using inheritance only for field reuse.
- Prefer composition, small shared functions, explicit collaborators, or Python `Protocol`s/interfaces when code needs shared behavior or a common contract.
- Use inheritance only for true subtype relationships where callers can rely on the subclass anywhere the parent is expected.
- Accept the verbosity of nested call sites when composition exposes structure and lets future fields propagate naturally.

## Errors, Defaults, and Resilience

### Named validation

Make validation-only code obvious so discarded-return statements do not look removable.

- Name code that exists only to raise on invalid input as validation with `validate_*`, or write an explicit conditional raise.
- Do not discard the return value of a parser, constructor, or checker unless the function name clearly communicates validation.

### Boundary error handling

Handle errors at boundaries when the boundary can make the failure more useful.

- Handle errors where the code can add useful context, choose a recovery path, convert to a user-facing message, or preserve invariants.
- Do not catch an exception just to log and re-raise, swallow it, return ambiguous sentinels, or convert it to a generic error when the result is not more actionable.
- Preserve the original cause when translating errors with exception chaining, such as `raise DomainError(...) from e`.
- Include the domain context needed to debug translated failures.

### Explicit, bounded resilience

Retry only plausibly transient failures, and make resilience explicit and bounded.

- Do not retry bugs, validation errors, bad requests, authentication failures, or other permanent failures.
- Bound retry attempts or duration.
- Use backoff, and add jitter when many clients may retry.
- Make retry behavior visible with logging or metrics.
- Set an explicit timeout when calling an external network or service dependency.

### Explicit defaults

Use defaults only when they are explicit contract, not silent recovery from failed correctness paths.

- Declared defaults are acceptable in data models, schemas, CLI options, configuration contracts, and clear function APIs.
- Do not recover from missing data, failed reads or writes, failed validation, failed security checks, or failed correctness checks with runtime defaults, cached substitutes, empty results, `{}`, `None`, or degraded behavior unless the user agrees or the operation is clearly non-critical.
- Make fallback behavior for non-critical paths visible through logging, metrics, or the returned result.

### Batch partial failures

Define batch failure semantics explicitly.

- Prefer continue-when-possible semantics when batch items can continue safely after one item fails.
- Abort on first failure when ordering, transactions, invariants, or downstream effects require it.
- Document abort-on-first-failure behavior in the code path or contract.
- Return structured successes and failures with stable item identifiers or indexes when partial success is allowed.
- Do not silently drop failed items.
- Make full success, partial success, and failure distinct when summarizing a batch.

## Verification and Tests

### Define verification

For non-trivial code changes, identify the narrowest checks that prove the change works.

- Prefer tests or deterministic scripts over manual inspection.
- Distinguish temporary checks from long-term tests. Verification is required; permanent tests are not always the right artifact.
- Use temporary verification when a long-term check would be brittle, overly specific, expensive, or implementation-bound.
- Temporary verification can include one-off scripts, focused REPL checks, intermediate snapshots, or throwaway tests.
- Remove temporary checks before landing unless the user wants them kept.
- Add long-term verification for public contracts, realistic regression risk, important edge cases, and data, security, or integration concerns.
- Prefer durable regression tests for user-visible or contract bugs.
- When refactoring, verify behavior before and after without locking in implementation structure.
- When changing contracts or data shapes, update and run checks for affected callers, configs, schemas, docs, and migrations.
- When verification cannot be run, say why and name the smallest useful next check.

### One behavior per test

Make each test prove one clear behavior, scenario, or contract.

- Avoid broad tests that make failures hard to diagnose.
- Name tests with the scenario and expected outcome.
- Use parameterization for equivalent cases when it improves scanability.

### Failure and boundary tests

Include failure and boundary behavior in long-term verification when it is part of the contract.

- Cover failure paths, edge cases, validation failures, retry exhaustion, timeout behavior, and partial failures when they are contract behavior.
- Do not rely only on happy-path tests when code validates, persists, calls external systems, or handles user input.

### Controlled nondeterminism

Make nondeterministic sources explicit and controllable where practical.

- This includes time, clocks, expiry, backoff, scheduling, ordering, random numbers, generated IDs, concurrency, external state, and eventual consistency.
- Prefer injected clocks, seeded RNGs, deterministic ID providers, stable ordering, fake schedulers, and explicit consistency boundaries.
- Do not make tests depend on wall-clock timing, sleeps, real retry delays, live randomness, uncontrolled generated values, or incidental ordering when a controlled source would prove behavior reliably.
- Assert invariants or allowed ranges instead of incidental values when true nondeterminism is part of the contract.

### Observable scripts

Make scripts produce stable evidence of progress and success.

- Log enough progress to show what is happening, what systems or files are touched, the current step, and whether the script completed successfully.
- Make logging and validation explicit for demos, examples, and end-to-end checks.
- Verify important outputs when practical.
- Write or print stable evidence of success.
- When post-run verification depends on generated data, write stable identifiers, output paths, event IDs, artifact IDs, or summary records.
- When adding or changing a demo script, create or update `TESTING.md` with the command to run and the concrete success criteria to check after execution.
