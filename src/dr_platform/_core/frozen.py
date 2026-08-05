from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import cast


def immutable_mapping(value: Mapping[str, str]) -> Mapping[str, str]:
    return MappingProxyType(dict(value))


def immutable_json_mapping(
    value: Mapping[str, object],
) -> Mapping[str, object]:
    return MappingProxyType(
        {key: _immutable_json_value(item) for key, item in value.items()}
    )


def _immutable_json_value(value: object) -> object:
    if isinstance(value, Mapping):
        return immutable_json_mapping(cast("Mapping[str, object]", value))
    if isinstance(value, list):
        return tuple(_immutable_json_value(item) for item in value)
    return value
