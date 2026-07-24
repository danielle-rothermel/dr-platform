"""Structural guards for the platform's DBOS durability boundary.

The platform source owns workflow and transaction wiring but no DBOS steps.
These scans guard that declared structure; runtime recovery behavior is proven
separately through an interrupted-worker integration test.
"""

from __future__ import annotations

import ast
from pathlib import Path

import dr_platform

_PACKAGE_ROOT = Path(dr_platform.__file__).resolve().parent


def _platform_source_files() -> tuple[Path, ...]:
    return tuple(sorted(_PACKAGE_ROOT.rglob("*.py")))


def _dbos_attribute_calls(tree: ast.AST) -> list[ast.Call]:
    """Every ``DBOS.<attr>(...)`` call node in a parsed module."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DBOS"
    ]


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
    handoff_source = (_PACKAGE_ROOT / "staging" / "handoff.py").read_text()
    tree = ast.parse(handoff_source)

    workflow_decorated: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            target = call.func if call is not None else decorator
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "DBOS"
                and target.attr == "workflow"
            ):
                workflow_decorated.append(node.name)

    assert "run_stage" in workflow_decorated
