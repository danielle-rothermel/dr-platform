"""Shared validation at staging persistence boundaries."""

from __future__ import annotations

from collections.abc import Mapping


def validate_non_empty_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def validate_labels(value: Mapping[str, str], *, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        raise ValueError(f"{label} must map strings to strings")
    return dict(value)
