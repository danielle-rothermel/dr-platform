from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from dr_store.object_store import ObjectStore

_OBJECT_STORE_ATTRIBUTE = "_dr_platform_object_store"
_ACTIVE_OBJECT_STORE: ContextVar[ObjectStore | None] = ContextVar(
    "dr_platform_active_object_store",
    default=None,
)
_MISSING = object()


class _ObjectStoreBinding:
    def __init__(
        self,
        *,
        workflows: tuple[Callable[..., object], ...],
        previous: tuple[object, ...],
        object_store: ObjectStore,
    ) -> None:
        self._workflows = workflows
        self._previous = previous
        self._object_store = object_store
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        for workflow, previous in zip(
            self._workflows,
            self._previous,
            strict=True,
        ):
            if (
                getattr(workflow, _OBJECT_STORE_ATTRIBUTE, _MISSING)
                is not self._object_store
            ):
                continue
            if previous is _MISSING:
                delattr(workflow, _OBJECT_STORE_ATTRIBUTE)
            else:
                setattr(workflow, _OBJECT_STORE_ATTRIBUTE, previous)
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


def _preflight_object_store(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    bound_workflows = _distinct_workflows(workflows)
    for workflow in bound_workflows:
        if getattr(workflow, _OBJECT_STORE_ATTRIBUTE, None) is not None:
            raise RuntimeError(
                "wrapped workflow already has a live runtime owner"
            )
    return bound_workflows


def _bind_object_store(
    workflows: Iterable[Callable[..., object]],
    object_store: ObjectStore,
) -> _ObjectStoreBinding:
    bound_workflows = _preflight_object_store(workflows)
    previous = tuple(
        getattr(workflow, _OBJECT_STORE_ATTRIBUTE, _MISSING)
        for workflow in bound_workflows
    )
    changed: list[tuple[Callable[..., object], object]] = []
    try:
        for workflow, prior in zip(
            bound_workflows,
            previous,
            strict=True,
        ):
            setattr(workflow, _OBJECT_STORE_ATTRIBUTE, object_store)
            changed.append((workflow, prior))
    except Exception:
        for workflow, prior in reversed(changed):
            if prior is _MISSING:
                delattr(workflow, _OBJECT_STORE_ATTRIBUTE)
            else:
                setattr(workflow, _OBJECT_STORE_ATTRIBUTE, prior)
        raise
    return _ObjectStoreBinding(
        workflows=bound_workflows,
        previous=previous,
        object_store=object_store,
    )


def _require_object_store(workflow: Callable[..., object]) -> ObjectStore:
    object_store = getattr(workflow, _OBJECT_STORE_ATTRIBUTE, None)
    if object_store is None:
        raise RuntimeError(
            "wrapped workflow requires a live dispatcher registration"
        )
    return cast("ObjectStore", object_store)


def _active_object_store() -> ObjectStore:
    object_store = _ACTIVE_OBJECT_STORE.get()
    if object_store is None:
        raise RuntimeError(
            "stage failure checkpoint requires a live object store context"
        )
    return object_store


@contextmanager
def _object_store_context(
    object_store: ObjectStore,
) -> Generator[None, None, None]:
    token: Token[ObjectStore | None] = _ACTIVE_OBJECT_STORE.set(object_store)
    try:
        yield
    finally:
        _ACTIVE_OBJECT_STORE.reset(token)
