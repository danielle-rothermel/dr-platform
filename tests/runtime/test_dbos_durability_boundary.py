"""Structural guards for the platform's DBOS durability boundary.

The platform source owns workflow and transaction wiring but no DBOS steps.
These scans guard that declared structure; runtime recovery behavior is proven
separately through an interrupted-worker integration test.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import dr_platform

_PACKAGE_ROOT = Path(dr_platform.__file__).resolve().parent


def _platform_source_files() -> tuple[Path, ...]:
    return tuple(sorted(_PACKAGE_ROOT.rglob("*.py")))


def _dbos_import_bindings(
    tree: ast.AST,
) -> tuple[frozenset[str], frozenset[str]]:
    direct_names: set[str] = set()
    module_names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "dbos":
            direct_names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "DBOS"
            )
        elif isinstance(node, ast.Import):
            module_names.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "dbos"
            )

    return frozenset(direct_names), frozenset(module_names)


def _is_dbos_reference(
    node: ast.expr,
    *,
    direct_names: frozenset[str],
    module_names: frozenset[str],
) -> bool:
    if isinstance(node, ast.Name):
        return node.id in direct_names
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "DBOS"
        and isinstance(node.value, ast.Name)
        and node.value.id in module_names
    )


def _is_dbos_attribute(
    node: ast.expr,
    *,
    attribute: str,
    direct_names: frozenset[str],
    module_names: frozenset[str],
) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attribute
        and _is_dbos_reference(
            node.value,
            direct_names=direct_names,
            module_names=module_names,
        )
    )


def _dbos_attribute_calls(tree: ast.AST) -> list[ast.Call]:
    """Every imported DBOS attribute call in a parsed module."""
    direct_names, module_names = _dbos_import_bindings(tree)
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and _is_dbos_reference(
            node.func.value,
            direct_names=direct_names,
            module_names=module_names,
        )
    ]


@pytest.mark.parametrize(
    "source",
    [
        "from dbos import DBOS\nDBOS.step()",
        "from dbos import DBOS as RuntimeDBOS\nRuntimeDBOS.step()",
        "import dbos\ndbos.DBOS.step()",
        "import dbos as runtime\nruntime.DBOS.step()",
    ],
)
def test_dbos_call_scan_resolves_supported_import_forms(source: str) -> None:
    calls = _dbos_attribute_calls(ast.parse(source))

    assert len(calls) == 1
    assert isinstance(calls[0].func, ast.Attribute)
    assert calls[0].func.attr == "step"


def test_dbos_call_scan_ignores_other_module_bindings() -> None:
    tree = ast.parse("import other as runtime\nruntime.DBOS.step()")

    assert _dbos_attribute_calls(tree) == []


def test_platform_declares_no_dbos_steps() -> None:
    """The platform leaves DBOS step and retry policy to applications.

    Stage bodies are workflows and completions are transactions. Applications
    may call their own DBOS steps, but the platform package declares none.
    """
    step_usages: list[str] = []
    for path in _platform_source_files():
        tree = ast.parse(path.read_text())
        for call in _dbos_attribute_calls(tree):
            attr = call.func
            assert isinstance(attr, ast.Attribute)
            if attr.attr == "step":
                step_usages.append(f"{path.name}:{call.lineno}")

    assert step_usages == []


def test_stage_wrapper_is_declared_as_dbos_workflow() -> None:
    """The generated stage wrapper is structurally a DBOS workflow."""
    handoff_source = (_PACKAGE_ROOT / "execution" / "handoff.py").read_text()
    tree = ast.parse(handoff_source)
    direct_names, module_names = _dbos_import_bindings(tree)

    workflow_decorated: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            target = call.func if call is not None else decorator
            if _is_dbos_attribute(
                target,
                attribute="workflow",
                direct_names=direct_names,
                module_names=module_names,
            ):
                workflow_decorated.append(node.name)

    assert "run_stage" in workflow_decorated
