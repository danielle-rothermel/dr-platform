# dr-platform API naming notes

Naming observations surfaced while writing `.defs/vocab.html`. These are **proposals only** — nothing here has been implemented. No rename, alias, or fixture change was made from the doc pass. Each item records the current name, the problem, proposed rename(s) with trade-offs, and blast radius.

## 1. `PipelineIdentity` is a nominal-looking name for a bare tuple alias

- **Current name:** `PipelineIdentity` (`definitions.py:16`), defined as `tuple[PipelineKey, int]`.
- **Problem:** The name reads like a dedicated identity class, but it is only a two-element tuple alias. A caller may assume it is a hashed or validated identity object with attribute access, when it is positional `(key, version)`. The vocab term "Pipeline Identity" also has to explain "a plain pair of values, not a reference or hash" specifically to counter this.
- **Proposed:** `PipelineVersionRef` (keeps "it's a lightweight pointer" framing) or promote to a small frozen dataclass `PipelineIdentity(key, version)` so `.key` / `.version` access is explicit and the name matches the shape. Trade-off: the dataclass is a runtime/behavior change (equality/hashing semantics, construction sites) — heavier than the alias rename; the rename alone is type-only.
- **Blast radius (alias rename):** `definitions.py`, `registry.py`, `submission.py`, `operations.py`, README example. Type alias only — no runtime, schema, or fixture change. (Dataclass promotion additionally touches every construction/destructuring site.)

## 2. Contract record types are internal while their `*Summary` siblings are exported

- **Current names:** `PipelineRunRecord`, `WorkItemRecord`, `StageExecutionRecord`, `StageControlRecord`, `CampaignWorkIdentity` live in `records.py` / `identities.py` but are **not** in `__all__`. The parallel `*Summary` types (`CampaignSummary`, `RunSummary`, `WorkItemSummary`, `StageExecutionSummary`) and `BulkWorkStatus` **are** exported.
- **Problem:** Term/name mismatch between the vocabulary and the public surface. The vocab's Exported Names bridge has to map contract concepts (which read naturally as "record") onto the `Summary` reader types plus the one exported record, `StageAttemptRecord`. A reader who sees the record names in the code but not in `__all__` cannot tell which family is the public vocabulary.
- **Proposed:** Decide which family is public and align names. Either (a) export the record types so the record vocabulary is first-class, or (b) treat `*Summary` as the sole public reader vocabulary and keep records fully internal (and stop exporting `StageAttemptRecord` — see item 3). Do not silently alias one to the other.
- **Blast radius:** `__init__.py` (`__all__`), `records.py`, `identities.py`, plus the vocab doc's Exported Names rows. Doc/mapping impact; no schema change required for either direction.

## 3. `StageAttemptRecord` is the only exported `*Record`, an asymmetric sibling

- **Current name:** `StageAttemptRecord` is exported; `StageExecutionRecord`, `StageControlRecord`, `PipelineRunRecord`, `WorkItemRecord` are not.
- **Problem:** Asymmetric-sibling situation — one `*Record` is public while its structural peers stay internal, so the export list mixes two vocabularies.
- **Proposed:** Either export the sibling records for symmetry, or rename `StageAttemptRecord`'s public role into the `Summary` family (e.g. `StageAttemptSummary`) so the public surface is uniformly reader-shaped. Trade-off: renaming to `Summary` implies a reader view, which fits if callers only read it; keep `Record` if it is genuinely the persisted row shape.
- **Blast radius:** `__init__.py`, `records.py`, `handoff.py`/`stage_attempts.py` construction sites, vocab doc Stage Attempt row. Couples with item 2's decision.

## 4. `terminal_summary` is a field, not a type — flag so the doc does not read it as a result payload

- **Current name:** `terminal_summary`, a `Mapping[str, object] | None` field on `StageAttemptRecord` (`records.py:68`; set in `handoff.py`, `stage_attempts.py`).
- **Problem:** The contract crosswalk names it as a distinct concept ("qualified attempt diagnostic only, not a result payload"). Because it is prominent in handoff code, it can be misread as a typed result object.
- **Proposed:** No rename needed. Keep it described as a field of Stage Attempt in the doc, not a type. Optionally document that it is diagnostic-only.
- **Blast radius:** Documentation only.

## 5. `_stage_workflow_name` truncated-SHA slug reads like an identity hash

- **Current name:** `_stage_workflow_name` (`handoff.py:410`), which builds a DBOS workflow name from `hashlib.sha256(identity.encode()).hexdigest()[:12]` (`handoff.py:419`).
- **Problem:** A truncated `sha256(...).hexdigest()[:12]` is a routing/display slug, not an identity or content hash — but in a repo whose cross-repo contract bans truncated hashes for identity, a bare truncated digest is a readability hazard. A reader could mistake it for an identity hash prefix.
- **Proposed:** Name the local helper/return value to make the intent explicit, e.g. `_stage_workflow_slug` / `workflow_name_slug`, and/or a short comment stating it is a workflow-name slug, not an identity or content hash. Trade-off: purely internal (leading underscore), low cost, high clarity.
- **Blast radius:** `handoff.py` (helper name + its one call site). Internal; no public surface, schema, or fixture change.

## 6. `input_ref` vs `input_reference` — the same transport string is spelled two ways

- **Current names:** `input_ref` on the public dataclasses (`WorkInput`, `AdmissionPayload`, `submission.py:52`) vs `input_reference` in records / DB column (`work_items.py`, schema). `output_reference` uses the long spelling.
- **Problem:** The same opaque transport string has two public spellings across the boundary, producing a name/term mismatch. The vocab term is "Input Reference"; a reader sees `input_ref` on the dataclass and `input_reference` in records.
- **Proposed:** Pick one spelling for the public vocabulary term. Prefer `input_reference` everywhere for consistency with `output_reference` and the DB column, or standardize both to `input_ref`/`output_ref` if brevity is wanted. Trade-off: aligning the dataclass field is a public field rename (touches callers and any positional/keyword construction).
- **Blast radius:** `submission.py` (`WorkInput`, `AdmissionPayload`), `admission.py`, callers passing `input_ref`, plus doc term. If the DB column is kept as `input_reference`, only the dataclass field spelling changes; no schema/fixture change if the column name is untouched.

## 7. `CampaignWorkIdentity` is unexported but documented as work identity

- **Current name:** `CampaignWorkIdentity` (`identities.py`), the `(campaign_key, work_key)` uniqueness identity; not in `__all__`. README documents work identity as "(campaign_key, work_key)".
- **Problem:** If work identity is a first-class vocabulary term it should have a stable exported name; otherwise the doc should describe it as a composite key rather than a type. Currently it is a type in code but absent from the public surface.
- **Proposed:** Either export `CampaignWorkIdentity` (making work identity first-class), or keep it internal and describe work identity in the doc/README strictly as a composite key `(campaign key, work key)` rather than a named type. Trade-off: exporting adds public surface; keeping internal keeps the doc describing a composite key with no type row.
- **Blast radius:** `__init__.py`, `identities.py`, README wording, vocab doc. The current doc already treats it as a composite key (no term row), consistent with the "keep internal" option.

## 8. `AdmissionPayload` name vs the `Admission` operation term — do not conflate

- **Current name:** `AdmissionPayload` (the `args_for` per-candidate input); the contract term is "Admission" (the operation).
- **Problem:** Name is acceptable, but the term (Admission = the transactional pass) and the name (AdmissionPayload = its per-candidate routing input) are different things and can be conflated in a doc row.
- **Proposed:** No rename required. Keep the doc's Admission and Admission Payload as separate rows and note explicitly that the payload is the input to admission, not the operation (the current doc row already does this). If a rename were desired for maximum clarity, `AdmissionCandidateContext` / `StageArgsContext` would separate it from the operation name — but this is optional and adds churn.
- **Blast radius (only if renamed):** `admission.py`, `submission.py`, callers of `args_for`, vocab doc. Otherwise documentation only.
