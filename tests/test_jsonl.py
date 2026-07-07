from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import BaseModel

from dr_platform import (
    JsonlFieldNames,
    index_jsonl_items,
    load_jsonl_items,
)

WHETSTONE_FIELDS = JsonlFieldNames(
    item_id="prediction_id",
    order_key="fair_order_key",
    group_key="experiment_name",
)


class Item(BaseModel):
    item_id: str
    order_key: str
    group_key: str
    payload: str


def _write_jsonl(path: Path, items: tuple[Item, ...]) -> None:
    lines = [item.model_dump_json() for item in items]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _item(index: int, *, group_key: str = "exp") -> Item:
    return Item(
        item_id=f"item-{index}",
        order_key=f"{index:02d}",
        group_key=group_key,
        payload=f"payload-{index}",
    )


def test_index_rejects_group_key_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_jsonl(path, (_item(0),))

    with pytest.raises(
        ValueError,
        match="group_key must match the submit operation",
    ):
        index_jsonl_items(path, group_key="other")


def test_index_rejects_duplicate_item_id(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_jsonl(path, (_item(0), _item(0)))

    with pytest.raises(
        ValueError,
        match="duplicate item_id in submit operation",
    ):
        index_jsonl_items(path, group_key="exp")


def test_index_rejects_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text("{not json}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid item JSON on line 1"):
        index_jsonl_items(path, group_key="exp")


def test_index_requires_index_fields(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    path.write_text(
        json.dumps({"item_id": "abc"}) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid item JSON on line 1"):
        index_jsonl_items(path, group_key="exp")


def test_index_skips_blank_lines_and_records_offsets(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    first = _item(0).model_dump_json()
    second = _item(1).model_dump_json()
    path.write_text(f"{first}\n\n{second}\n", encoding="utf-8")

    refs = index_jsonl_items(path, group_key="exp")

    assert [ref.item_id for ref in refs] == ["item-0", "item-1"]
    assert refs[1].byte_offset == len(first.encode()) + 2


def test_custom_field_names_index_whetstone_shaped_files(
    tmp_path: Path,
) -> None:
    path = tmp_path / "specs.jsonl"
    payload = {
        "prediction_id": "pred-1",
        "fair_order_key": "00",
        "experiment_name": "exp",
        "graph": {"nodes": []},
    }
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    refs = index_jsonl_items(path, group_key="exp", fields=WHETSTONE_FIELDS)

    assert refs[0].item_id == "pred-1"
    assert refs[0].order_key == "00"


def test_custom_field_names_appear_in_errors(tmp_path: Path) -> None:
    path = tmp_path / "specs.jsonl"
    payload = {
        "prediction_id": "pred-1",
        "fair_order_key": "00",
        "experiment_name": "exp",
    }
    path.write_text(
        json.dumps(payload) + "\n" + json.dumps(payload) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="duplicate prediction_id in submit operation",
    ):
        index_jsonl_items(path, group_key="exp", fields=WHETSTONE_FIELDS)


def test_load_returns_items_in_ref_order(tmp_path: Path) -> None:
    items = (_item(2), _item(0), _item(1))
    path = tmp_path / "items.jsonl"
    _write_jsonl(path, items)
    refs = index_jsonl_items(path, group_key="exp")
    ordered_refs = tuple(
        sorted(refs, key=lambda ref: (ref.order_key, ref.item_id))
    )

    loaded = load_jsonl_items(
        path,
        ordered_refs,
        parse=Item.model_validate_json,
    )

    assert [item.item_id for item in loaded] == ["item-0", "item-1", "item-2"]
    assert loaded[0].payload == "payload-0"


def test_load_surfaces_parse_errors_with_byte_offset(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_jsonl(path, (_item(0),))
    refs = index_jsonl_items(path, group_key="exp")

    def parse(_line: str) -> Item:
        raise ValueError("bad line")

    with pytest.raises(
        ValueError,
        match="invalid item JSON at byte offset 0",
    ):
        load_jsonl_items(path, refs, parse=parse)


def test_load_empty_refs_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "items.jsonl"
    _write_jsonl(path, (_item(0),))
    assert load_jsonl_items(path, (), parse=Item.model_validate_json) == ()
