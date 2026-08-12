from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

_MISSING = object()


class _WorkflowBinding:
    """Owns one runtime payload attached to a set of wrapped workflows."""

    def __init__(
        self,
        *,
        workflows: tuple[Callable[..., object], ...],
        previous: tuple[object, ...],
        payload: object,
        attribute: str,
    ) -> None:
        self._workflows = workflows
        self._previous = previous
        self._payload = payload
        self._attribute = attribute
        self._released = False

    @property
    def payload(self) -> object:
        return self._payload

    def release(self) -> None:
        if self._released:
            return
        for workflow, previous in zip(
            self._workflows,
            self._previous,
            strict=True,
        ):
            if (
                getattr(workflow, self._attribute, _MISSING)
                is not self._payload
            ):
                continue
            if previous is _MISSING:
                delattr(workflow, self._attribute)
            else:
                setattr(workflow, self._attribute, previous)
        self._released = True


def _distinct_workflows(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    distinct: list[Callable[..., object]] = []
    identities: set[int] = set()
    for workflow in workflows:
        identity = id(workflow)
        if identity in identities:
            continue
        identities.add(identity)
        distinct.append(workflow)
    return tuple(distinct)


def preflight(
    workflows: Iterable[Callable[..., object]],
    *,
    attribute: str,
    is_live: Callable[[object], bool],
) -> tuple[Callable[..., object], ...]:
    """Return the distinct workflows, rejecting any with a live prior owner."""
    bound_workflows = _distinct_workflows(workflows)
    for workflow in bound_workflows:
        existing = getattr(workflow, attribute, None)
        if existing is not None and is_live(existing):
            raise RuntimeError(
                "wrapped workflow already has a live runtime owner"
            )
    return bound_workflows


def bind[B: _WorkflowBinding](
    workflows: Iterable[Callable[..., object]],
    payload: object,
    *,
    attribute: str,
    is_live: Callable[[object], bool],
    binding_type: type[B],
) -> B:
    """Attach ``payload`` to every distinct workflow under ``attribute``."""
    bound_workflows = preflight(
        workflows,
        attribute=attribute,
        is_live=is_live,
    )
    previous = tuple(
        getattr(workflow, attribute, _MISSING) for workflow in bound_workflows
    )
    changed: list[tuple[Callable[..., object], object]] = []
    try:
        for workflow, prior in zip(
            bound_workflows,
            previous,
            strict=True,
        ):
            setattr(workflow, attribute, payload)
            changed.append((workflow, prior))
    except Exception:
        for workflow, prior in reversed(changed):
            if prior is _MISSING:
                delattr(workflow, attribute)
            else:
                setattr(workflow, attribute, prior)
        raise
    return binding_type(
        workflows=bound_workflows,
        previous=previous,
        payload=payload,
        attribute=attribute,
    )


def require(
    workflow: Callable[..., object],
    *,
    attribute: str,
) -> object:
    """Return the payload bound to ``workflow`` or raise if unbound."""
    payload = getattr(workflow, attribute, None)
    if payload is None:
        raise RuntimeError(
            "wrapped workflow requires a live dispatcher registration"
        )
    return payload
