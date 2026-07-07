from __future__ import annotations

import pytest
from pydantic import BaseModel

from dr_platform import (
    JsonlItemRef,
    fair_ordered,
    fair_ordered_windows,
    windows,
)


class Ref(BaseModel):
    item_id: str
    order_key: str


def test_fair_ordered_sorts_by_order_key_then_item_id() -> None:
    items = (
        Ref(item_id="b", order_key="2"),
        Ref(item_id="a", order_key="2"),
        Ref(item_id="c", order_key="1"),
    )
    ordered = fair_ordered(items)
    assert [item.item_id for item in ordered] == ["c", "a", "b"]


def test_fair_ordered_windows_interleaves_axes() -> None:
    # Order keys constructed so the two "models" interleave rather than
    # run back-to-back — the point of submission-time fairness.
    refs = tuple(
        JsonlItemRef(
            item_id=f"{model}-{index}",
            order_key=f"{index:02d}-{model}",
            byte_offset=0,
        )
        for model in ("model-a", "model-b")
        for index in range(8)
    )

    ordered = tuple(fair_ordered_windows(refs, window_size=4))

    assert len(ordered) == 4
    first_window_models = {ref.item_id.rsplit("-", 1)[0] for ref in ordered[0]}
    assert len(first_window_models) > 1


def test_window_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="window_size must be positive"):
        tuple(fair_ordered_windows((), window_size=0))
    with pytest.raises(ValueError, match="window_size must be positive"):
        tuple(windows((), window_size=-1))


def test_windows_preserves_existing_order() -> None:
    values = ("z", "a", "m")
    chunked = tuple(windows(values, window_size=2))
    assert chunked == (("z", "a"), ("m",))
