"""Submission-time fair ordering: order-key sort plus windowing.

Fairness lives at submission time (the order work enters the queue),
not claim time — claim-time round-robin fights DBOS's queue ownership.
The sort key is ``(order_key, item_id)``: order keys interleave sweep
axes; item ids break ties deterministically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from dr_platform.items import SubmittableItem


@runtime_checkable
class Orderable(Protocol):
    """Anything with the fair-ordering key pair (items, jsonl refs)."""

    @property
    def item_id(self) -> str: ...

    @property
    def order_key(self) -> str: ...


def fair_ordered[ItemT: Orderable](
    items: Iterable[ItemT],
) -> tuple[ItemT, ...]:
    return tuple(
        sorted(items, key=lambda item: (item.order_key, item.item_id))
    )


def fair_ordered_windows[ItemT: Orderable](
    items: Iterable[ItemT],
    *,
    window_size: int,
) -> Iterator[tuple[ItemT, ...]]:
    validate_window_size(window_size)
    ordered = fair_ordered(items)
    for index in range(0, len(ordered), window_size):
        yield ordered[index : index + window_size]


def fair_ordered_item_windows(
    items: Iterable[SubmittableItem],
    *,
    window_size: int,
) -> Iterator[tuple[SubmittableItem, ...]]:
    """Alias fixing the element type for facade call sites."""
    return fair_ordered_windows(items, window_size=window_size)


def validate_window_size(window_size: int) -> None:
    if window_size < 1:
        raise ValueError("window_size must be positive")


def windows[ItemT](
    ordered: Sequence[ItemT],
    *,
    window_size: int,
) -> Iterator[tuple[ItemT, ...]]:
    """Windowing without re-sorting, for already-ordered sequences."""
    validate_window_size(window_size)
    materialized = tuple(ordered)
    for index in range(0, len(materialized), window_size):
        yield materialized[index : index + window_size]
