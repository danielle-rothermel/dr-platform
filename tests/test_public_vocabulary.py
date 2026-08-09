from __future__ import annotations

import tomllib
from collections import defaultdict
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import dr_platform
from dr_platform._core import identities
from dr_platform._core.ledger import attempts, executions, schema, states
from dr_platform.admission import controls, runner
from dr_platform.completion import execution as completion_execution
from dr_platform.execution import handoff
from dr_platform.inspection import campaigns, statuses
from dr_platform.inspection import work_items as inspection_work_items
from dr_platform.pipeline import definitions, registry
from dr_platform.recovery import cancellation, retry, sweep
from dr_platform.runtime import database, dbos, dispatcher, telemetry
from dr_platform.submission import runs, stream
from dr_platform.submission import work_items as submission_work_items

_ROOT_BINDINGS = {
    "AdmissionPayload": runner.AdmissionPayload,
    "BulkStatusResult": statuses.BulkStatusResult,
    "BulkWorkStatus": statuses.BulkWorkStatus,
    "CampaignKey": identities.CampaignKey,
    "CampaignSummary": campaigns.CampaignSummary,
    "CancellationDisposition": cancellation.CancellationDisposition,
    "DispatcherRegistration": dispatcher.DispatcherRegistration,
    "PipelineConflictError": registry.PipelineConflictError,
    "PipelineDefinition": definitions.PipelineDefinition,
    "PipelineIdentity": definitions.PipelineIdentity,
    "PipelineKey": identities.PipelineKey,
    "PipelineRegistry": registry.PipelineRegistry,
    "PipelineRunConflictError": runs.PipelineRunConflictError,
    "PlatformDbosConfig": dbos.PlatformDbosConfig,
    "RegistrationClosureError": stream.RegistrationClosureError,
    "RunCompletionDefinition": definitions.RunCompletionDefinition,
    "RunCompletionExecutionRecord": (
        completion_execution.RunCompletionExecutionRecord
    ),
    "RunCompletionExecutionState": states.RunCompletionExecutionState,
    "RunCompletionKey": identities.RunCompletionKey,
    "RunCompletionPayload": completion_execution.RunCompletionPayload,
    "RunKey": identities.RunKey,
    "RunMemberInput": stream.RunMemberInput,
    "RunMembershipConflictError": stream.RunMembershipConflictError,
    "RunRegistrationDeclaration": stream.RunRegistrationDeclaration,
    "RunSummary": campaigns.RunSummary,
    "StageAttemptRecord": attempts.StageAttemptRecord,
    "StageControlRecord": controls.StageControlRecord,
    "StageDefinition": definitions.StageDefinition,
    "StageExecutionRecord": executions.StageExecutionRecord,
    "StageExecutionState": states.StageExecutionState,
    "StageExecutionSummary": inspection_work_items.StageExecutionSummary,
    "StageHandoffMismatchError": handoff.StageHandoffMismatchError,
    "StageKey": identities.StageKey,
    "StageRetryResult": retry.StageRetryResult,
    "StagingSchema": schema.StagingSchema,
    "StateCount": statuses.StateCount,
    "SubmissionReceipt": stream.SubmissionReceipt,
    "SweepProjection": sweep.SweepProjection,
    "SweepSummary": sweep.SweepSummary,
    "TelemetryInitializationResult": telemetry.TelemetryInitializationResult,
    "UnwrappedPipelineError": dispatcher.UnwrappedPipelineError,
    "WorkCancellationResult": cancellation.WorkCancellationResult,
    "WorkInput": stream.WorkInput,
    "WorkItemConflictError": submission_work_items.WorkItemConflictError,
    "WorkItemSummary": inspection_work_items.WorkItemSummary,
    "WorkKey": identities.WorkKey,
    "WorkflowCanceller": cancellation.WorkflowCanceller,
    "build_platform_dbos_config": dbos.build_platform_dbos_config,
    "bulk_work_statuses": statuses.bulk_work_statuses,
    "bulk_run_state_counts": statuses.bulk_run_state_counts,
    "campaign_state_counts": statuses.campaign_state_counts,
    "cancel_work": cancellation.cancel_work,
    "compute_run_membership_digest": stream.compute_run_membership_digest,
    "get_work_item_stages": inspection_work_items.get_work_item_stages,
    "initialize_dbos_runtime": dbos.initialize_dbos_runtime,
    "initialize_telemetry_safely": telemetry.initialize_telemetry_safely,
    "inspect_campaign": campaigns.inspect_campaign,
    "inspect_run_completion": completion_execution.inspect_run_completion,
    "list_campaigns": campaigns.list_campaigns,
    "list_runs": campaigns.list_runs,
    "list_work_items": inspection_work_items.list_work_items,
    "pause": controls.pause,
    "read_controls": controls.read_controls,
    "register_scheduled_dispatcher": dispatcher.register_scheduled_dispatcher,
    "resume": controls.resume,
    "retry_stage": retry.retry_stage,
    "run_state_counts": statuses.run_state_counts,
    "set_selector_capacity": controls.set_selector_capacity,
    "set_stage_capacity": controls.set_stage_capacity,
    "submit": stream.submit,
    "sweep_abandoned_stages": sweep.sweep_abandoned_stages,
    "upgrade_platform_schema": database.upgrade_platform_schema,
    "wrap_pipeline_workflows": handoff.wrap_pipeline_workflows,
}


def test_root_exports_are_the_public_contract() -> None:
    assert len(dr_platform.__all__) == len(_ROOT_BINDINGS)
    assert set(dr_platform.__all__) == set(_ROOT_BINDINGS)


def test_root_exports_are_bound_to_the_contract_objects() -> None:
    for name, expected in _ROOT_BINDINGS.items():
        assert getattr(dr_platform, name) is expected


_DEFS_DIR = Path(__file__).resolve().parents[1] / ".defs"
_RELATIONSHIP_FIELDS = ("is_a", "part_of")
_TERM_FIELDS = {
    "name",
    "definition",
    "exported_symbols",
    "categories",
    *_RELATIONSHIP_FIELDS,
}
_CONTRACT_FIELDS = {"title", "statement", "rationale", "date", "check"}


def _load_toml(name: str) -> dict[str, Any]:
    with (_DEFS_DIR / name).open("rb") as file:
        return tomllib.load(file)


def _assert_nonblank_string(value: object, *, field: str) -> None:
    assert isinstance(value, str), f"{field} must be a string"
    assert value.strip(), f"{field} must not be blank"


def _assert_optional_string_list(
    item: dict[str, Any], field: str, *, owner: str
) -> None:
    if field not in item:
        return

    values = item[field]
    assert isinstance(values, list), f"{owner}.{field} must be a list"
    assert values, f"{owner}.{field} must not be empty"
    for value in values:
        _assert_nonblank_string(value, field=f"{owner}.{field}")
    assert len(values) == len(set(values)), (
        f"{owner}.{field} must not contain duplicates"
    )


def _terms() -> list[dict[str, Any]]:
    document = _load_toml("terms.toml")
    assert set(document) == {"terms"}
    terms = document["terms"]
    assert isinstance(terms, list)
    assert terms
    return terms


def _contracts() -> list[dict[str, Any]]:
    document = _load_toml("contracts.toml")
    assert set(document) == {"contracts"}
    contracts = document["contracts"]
    assert isinstance(contracts, list)
    assert contracts
    return contracts


def _relationship_edges(
    terms: list[dict[str, Any]],
) -> list[tuple[str, str, str]]:
    return [
        (term["name"], relationship, target)
        for term in terms
        for relationship in _RELATIONSHIP_FIELDS
        for target in term.get(relationship, [])
    ]


def _find_cycle(graph: dict[str, list[str]]) -> list[str] | None:
    visited: set[str] = set()
    active_positions: dict[str, int] = {}
    path: list[str] = []

    def visit(name: str) -> list[str] | None:
        visited.add(name)
        active_positions[name] = len(path)
        path.append(name)

        for target in graph[name]:
            if target in active_positions:
                cycle_start = active_positions[target]
                return [*path[cycle_start:], target]
            if target not in visited:
                cycle = visit(target)
                if cycle is not None:
                    return cycle

        path.pop()
        del active_positions[name]
        return None

    for name in graph:
        if name not in visited:
            cycle = visit(name)
            if cycle is not None:
                return cycle
    return None


class _DefinitionsPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.elements: list[tuple[str, dict[str, str | None]]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        self.elements.append((tag, dict(attrs)))


def test_terms_file_has_valid_shape_and_unique_names() -> None:
    terms = _terms()
    names: list[str] = []

    for index, term in enumerate(terms):
        assert isinstance(term, dict), f"terms[{index}] must be a table"
        assert set(term) <= _TERM_FIELDS, f"terms[{index}] has unknown fields"
        assert {"name", "definition"} <= set(term), (
            f"terms[{index}] is missing required fields"
        )
        _assert_nonblank_string(term["name"], field=f"terms[{index}].name")
        _assert_nonblank_string(
            term["definition"], field=f"terms[{index}].definition"
        )
        names.append(term["name"])
        for field in (
            "exported_symbols",
            "categories",
            *_RELATIONSHIP_FIELDS,
        ):
            _assert_optional_string_list(term, field, owner=term["name"])

    assert len(names) == len(set(names)), "term names must be unique"


def test_relationship_targets_are_exact_canonical_terms() -> None:
    terms = _terms()
    canonical_terms = {term["name"] for term in terms}
    missing_targets = [
        f"{source} --{relationship}--> {target}"
        for source, relationship, target in _relationship_edges(terms)
        if target not in canonical_terms
    ]

    assert not missing_targets, (
        "Relationship targets must exactly name existing terms:\n"
        + "\n".join(missing_targets)
    )


def test_relationship_graph_has_no_self_links_or_cycles() -> None:
    terms = _terms()
    canonical_terms = {term["name"] for term in terms}
    edges = _relationship_edges(terms)
    self_links = [
        f"{source} --{relationship}--> {target}"
        for source, relationship, target in edges
        if source == target
    ]
    assert not self_links, (
        "Terms must not relate to themselves:\n" + "\n".join(self_links)
    )

    graph = {name: [] for name in canonical_terms}
    for source, _, target in edges:
        if target in canonical_terms:
            graph[source].append(target)

    cycle = _find_cycle(graph)
    assert cycle is None, (
        "The combined is_a/part_of graph contains a cycle: "
        + " -> ".join(cycle or [])
    )


def test_exported_symbols_are_unique_public_and_resolvable() -> None:
    symbol_terms: dict[str, list[str]] = defaultdict(list)
    for term in _terms():
        for symbol in term.get("exported_symbols", []):
            symbol_terms[symbol].append(term["name"])

    duplicate_symbols = {
        symbol: names
        for symbol, names in symbol_terms.items()
        if len(names) > 1
    }
    assert not duplicate_symbols, (
        "Each exported symbol must map to exactly one term: "
        f"{duplicate_symbols}"
    )

    mapped_symbols = set(symbol_terms)
    public_symbols = set(dr_platform.__all__)
    assert mapped_symbols == public_symbols, (
        "Exported-symbol mappings must exactly cover dr_platform.__all__. "
        f"Unmapped public names: {sorted(public_symbols - mapped_symbols)}. "
        f"Mapped non-public names: {sorted(mapped_symbols - public_symbols)}."
    )
    for symbol in mapped_symbols:
        assert getattr(dr_platform, symbol) is _ROOT_BINDINGS[symbol]


def test_contracts_file_has_valid_shape_and_unique_titles() -> None:
    titles: list[str] = []

    for index, contract in enumerate(_contracts()):
        assert isinstance(contract, dict), (
            f"contracts[{index}] must be a table"
        )
        assert set(contract) <= _CONTRACT_FIELDS, (
            f"contracts[{index}] has unknown fields"
        )
        required_fields = {"title", "statement", "rationale", "date"}
        assert required_fields <= set(contract), (
            f"contracts[{index}] is missing required fields"
        )
        for field in required_fields:
            _assert_nonblank_string(
                contract[field], field=f"contracts[{index}].{field}"
            )
        if "check" in contract:
            _assert_nonblank_string(
                contract["check"], field=f"contracts[{index}].check"
            )

        contract_date = contract["date"]
        assert (
            date.fromisoformat(contract_date).isoformat() == contract_date
        ), f"contracts[{index}].date must use YYYY-MM-DD"
        titles.append(contract["title"])

    assert len(titles) == len(set(titles)), "contract titles must be unique"


def test_definitions_page_wires_required_slots_and_assets() -> None:
    parser = _DefinitionsPageParser()
    parser.feed((_DEFS_DIR / "index.html").read_text(encoding="utf-8"))

    render_slots = [
        {
            key: value
            for key, value in attrs.items()
            if key.startswith("data-defs-")
        }
        for tag, attrs in parser.elements
        if tag == "tbody" and "data-defs-kind" in attrs
    ]
    assert render_slots == [
        {
            "data-defs-file": "terms.toml",
            "data-defs-kind": "terms",
        },
        {
            "data-defs-file": "contracts.toml",
            "data-defs-kind": "contracts",
        },
    ]

    assert any(
        tag == "link"
        and attrs.get("rel") == "stylesheet"
        and attrs.get("href") == "doc.css"
        for tag, attrs in parser.elements
    )
    assert any(
        tag == "script"
        and attrs.get("type") == "module"
        and attrs.get("src") == "defs-render.js"
        for tag, attrs in parser.elements
    )
    for asset in (
        "index.html",
        "terms.toml",
        "contracts.toml",
        "terms.schema.json",
        "favicon.svg",
        "doc.css",
        "defs-render.js",
        "smol-toml.js",
    ):
        assert (_DEFS_DIR / asset).is_file(), (
            f"missing canonical asset: {asset}"
        )
    renderer = (_DEFS_DIR / "defs-render.js").read_text(encoding="utf-8")
    assert 'from "./smol-toml.js"' in renderer


def test_vendored_parser_retains_complete_bsd_license_header() -> None:
    source = (_DEFS_DIR / "smol-toml.js").read_text(encoding="utf-8")
    banner = "/**\n * Bundled by jsDelivr"
    assert banner in source

    license_header, _ = source.split(banner, maxsplit=1)
    assert "SPDX-License-Identifier: BSD-3-Clause" in license_header
    assert (
        "EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE." in license_header
    )
    assert license_header.rstrip().endswith("*/")
