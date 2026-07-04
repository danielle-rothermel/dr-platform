"""Byte-offset JSONL indexing and windowed loading.

Index a JSONL file of work items without materializing it, then load
windows by seeking. Field names are parameterized so adopters with
existing file formats (whetstone: ``prediction_id`` /
``fair_order_key`` / ``experiment_name``) keep their bytes; new
adopters use the neutral defaults. Full-record validation belongs to
the caller via ``parse``.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, BinaryIO

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from pathlib import Path


class JsonlFieldNames(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: StrictStr = "item_id"
    order_key: StrictStr = "order_key"
    group_key: StrictStr = "group_key"


class JsonlItemRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_id: StrictStr
    order_key: StrictStr
    byte_offset: StrictInt


def index_jsonl_items(
    path: Path,
    *,
    group_key: str,
    fields: JsonlFieldNames | None = None,
) -> tuple[JsonlItemRef, ...]:
    resolved = fields or JsonlFieldNames()
    refs: list[JsonlItemRef] = []
    seen_item_ids: set[str] = set()
    with path.open("rb") as file:
        for line_number, line in _iter_nonempty_jsonl_lines(file):
            payload = _parse_jsonl_index_payload(line, line_number=line_number)
            item_group_key = _required_string_field(
                payload,
                resolved.group_key,
                line_number=line_number,
            )
            if item_group_key != group_key:
                raise ValueError(
                    f"item {resolved.group_key} must match the submit "
                    "operation"
                )
            item_id = _required_string_field(
                payload,
                resolved.item_id,
                line_number=line_number,
            )
            if item_id in seen_item_ids:
                raise ValueError(
                    f"duplicate {resolved.item_id} in submit operation: "
                    f"{item_id}"
                )
            seen_item_ids.add(item_id)
            order_key = _required_string_field(
                payload,
                resolved.order_key,
                line_number=line_number,
            )
            refs.append(
                JsonlItemRef(
                    item_id=item_id,
                    order_key=order_key,
                    byte_offset=line.byte_offset,
                )
            )
    return tuple(refs)


def load_jsonl_items[ItemT](
    path: Path,
    refs: Sequence[JsonlItemRef],
    *,
    parse: Callable[[str], ItemT],
) -> tuple[ItemT, ...]:
    """Load the referenced lines, returned in ``refs`` order.

    Reads seek in byte-offset order (one forward pass); ``parse``
    receives the raw line and owns validation. Parse failures surface
    with the byte offset for operator-grade errors.
    """
    if not refs:
        return ()
    items_by_id: dict[str, ItemT] = {}
    refs_by_offset = sorted(refs, key=lambda ref: ref.byte_offset)
    with path.open("rb") as file:
        for ref in refs_by_offset:
            file.seek(ref.byte_offset)
            line = file.readline()
            try:
                item = parse(line.decode("utf-8"))
            except ValueError as error:
                raise ValueError(
                    f"invalid item JSON at byte offset {ref.byte_offset}"
                ) from error
            items_by_id[ref.item_id] = item
    return tuple(items_by_id[ref.item_id] for ref in refs)


class _JsonlLine(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    byte_offset: StrictInt
    content: bytes


def _iter_nonempty_jsonl_lines(
    file: BinaryIO,
) -> Iterator[tuple[int, _JsonlLine]]:
    line_number = 0
    while True:
        byte_offset = file.tell()
        line = file.readline()
        if not line:
            break
        if not line.strip():
            continue
        line_number += 1
        yield line_number, _JsonlLine(byte_offset=byte_offset, content=line)


def _parse_jsonl_index_payload(
    line: _JsonlLine,
    *,
    line_number: int,
) -> dict[str, object]:
    try:
        payload = json.loads(line.content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"invalid item JSON on line {line_number}") from error
    if not isinstance(payload, dict):
        raise ValueError(  # noqa: TRY004 -- malformed input, not a bug
            f"invalid item JSON on line {line_number}"
        )
    return payload


def _required_string_field(
    payload: dict[str, object],
    field_name: str,
    *,
    line_number: int,
) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValueError(  # noqa: TRY004 -- malformed input, not a bug
            f"invalid item JSON on line {line_number}"
        )
    return value
