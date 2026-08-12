from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, cast

from dr_platform.execution._workflow_binding import (
    _WorkflowBinding,
    bind,
    preflight,
    require,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from dr_store.object_store import ObjectStore

_OBJECT_STORE_ATTRIBUTE = "_dr_platform_object_store"
_ACTIVE_OBJECT_STORE: ContextVar[ObjectStore | None] = ContextVar(
    "dr_platform_active_object_store",
    default=None,
)


def _object_store_is_live(_existing: object) -> bool:
    """Any prior object store owns the workflow, live or not."""
    return True


class _ObjectStoreBinding(_WorkflowBinding):
    @property
    def _object_store(self) -> ObjectStore:
        return cast("ObjectStore", self.payload)


def _preflight_object_store(
    workflows: Iterable[Callable[..., object]],
) -> tuple[Callable[..., object], ...]:
    return preflight(
        workflows,
        attribute=_OBJECT_STORE_ATTRIBUTE,
        is_live=_object_store_is_live,
    )


def _bind_object_store(
    workflows: Iterable[Callable[..., object]],
    object_store: ObjectStore,
) -> _ObjectStoreBinding:
    return bind(
        workflows,
        object_store,
        attribute=_OBJECT_STORE_ATTRIBUTE,
        is_live=_object_store_is_live,
        binding_type=_ObjectStoreBinding,
    )


def _require_object_store(workflow: Callable[..., object]) -> ObjectStore:
    return cast(
        "ObjectStore",
        require(workflow, attribute=_OBJECT_STORE_ATTRIBUTE),
    )


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
