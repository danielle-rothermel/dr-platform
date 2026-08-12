from __future__ import annotations

import ast
from pathlib import Path

import dr_platform

_PACKAGE_ROOT = Path(dr_platform.__file__).resolve().parent


def _platform_source_files() -> tuple[Path, ...]:
    return tuple(sorted(_PACKAGE_ROOT.rglob("*.py")))


def _binds_dbos(tree: ast.AST) -> bool:
    """Detect the one form the package uses: ``from dbos import DBOS``."""
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "dbos"
        and any(
            alias.name == "DBOS" and alias.asname is None
            for alias in node.names
        )
        for node in ast.walk(tree)
    )


def _is_dbos_attribute(node: ast.expr, *, attribute: str) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attribute
        and isinstance(node.value, ast.Name)
        and node.value.id == "DBOS"
    )


def _dbos_attribute_calls(tree: ast.AST) -> list[ast.Call]:
    if not _binds_dbos(tree):
        return []
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "DBOS"
    ]


def test_platform_declares_no_dbos_steps() -> None:
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
    handoff_source = (_PACKAGE_ROOT / "execution" / "handoff.py").read_text()
    tree = ast.parse(handoff_source)
    assert _binds_dbos(tree)

    workflow_decorated: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            target = call.func if call is not None else decorator
            if _is_dbos_attribute(target, attribute="workflow"):
                workflow_decorated.append(node.name)

    assert "run_stage" in workflow_decorated
