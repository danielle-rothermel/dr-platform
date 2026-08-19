from __future__ import annotations

from collections.abc import Mapping

WORK_PRIORITY_MAX = 2_147_483_647


def validate_positive_integer(value: int, *, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value <= 0:
        raise ValueError(f"{label} must be positive")


def validate_nonnegative_integer(value: int, *, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value < 0:
        raise ValueError(f"{label} must be non-negative")


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


def validate_work_priority(value: int, *, label: str = "priority") -> None:
    validate_nonnegative_integer(value, label=label)
    if value > WORK_PRIORITY_MAX:
        raise ValueError(
            f"{label} must be at most {WORK_PRIORITY_MAX}, got {value}"
        )
