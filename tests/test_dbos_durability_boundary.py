"""Source-invariant proofs for the platform's DBOS durability boundary.

The design requires that stage bodies run under a DBOS workflow identity per
Platform Stage Attempt, that automatic DBOS step retries are enabled nowhere,
and that Durability Replay stays within one Platform Stage Attempt.  These are
structural properties of the platform source, so they are proven by scanning
that source rather than by exercising a live DBOS runtime.
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


def test_platform_registers_no_dbos_step() -> None:
    """No stage or handoff logic is a ``@DBOS.step``.

    Automatic step retries are a property of ``DBOS.step``; the platform runs
    stage bodies as workflows and completions as transactions, so it exposes no
    step whose retries could be automatically re-driven.  A step introduced
    here would silently gain DBOS's default retry behavior.
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


def test_platform_enables_no_automatic_retry_keyword() -> None:
    """No DBOS decorator enables retries anywhere in the platform.

    ``retries_allowed`` / ``max_attempts`` keywords express automatic retry
    policy.  The platform must not enable them: recovery is the explicit
    ``retry_stage`` operation appending a new Platform Stage Attempt, never an
    automatic in-attempt re-drive.
    """
    retry_enablements: list[str] = []
    forbidden = {"retries_allowed", "max_attempts", "max_retries"}
    for path in _platform_source_files():
        tree = ast.parse(path.read_text())
        for call in _dbos_attribute_calls(tree):
            for keyword in call.keywords:
                if keyword.arg not in forbidden:
                    continue
                enables = not (
                    isinstance(keyword.value, ast.Constant)
                    and keyword.value.value in (False, 0)
                )
                if enables:
                    retry_enablements.append(
                        f"{path.name}:{call.lineno}:{keyword.arg}"
                    )

    assert retry_enablements == []


def test_stage_body_runs_under_a_workflow_identity() -> None:
    """The wrapped stage callable is a ``@DBOS.workflow``, not a bare call.

    Each Platform Stage Attempt owns one workflow identity, so the wrapper the
    platform registers must be decorated as a DBOS workflow.  This anchors the
    per-attempt identity that Durability Replay reconstructs within.
    """
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
