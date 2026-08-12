from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

KEY_MAX_LENGTH = 128
_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")


def validate_key_value(value: str, *, label: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value:
        raise ValueError(f"{label} must not be empty")
    if len(value) > KEY_MAX_LENGTH:
        raise ValueError(
            f"{label} must be at most {KEY_MAX_LENGTH} characters"
        )
    if _KEY_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{label} must start with an ASCII letter or digit and contain "
            "only ASCII letters, digits, '.', '_', ':', '/', or '-'"
        )


@dataclass(frozen=True, slots=True)
class _ValidatedKey:
    value: str

    _label: ClassVar[str] = "key"

    def __post_init__(self) -> None:
        validate_key_value(self.value, label=self._label)

    def __str__(self) -> str:
        return self.value


def normalize_key[K: _ValidatedKey](value: K | str, key_type: type[K]) -> K:
    """Coerce a string into ``key_type``, passing through existing keys."""
    if isinstance(value, key_type):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{key_type._label} must be a string")
    return key_type(value)


@dataclass(frozen=True, slots=True)
class CampaignKey(_ValidatedKey):
    _label: ClassVar[str] = "campaign key"


@dataclass(frozen=True, slots=True)
class RunKey(_ValidatedKey):
    _label: ClassVar[str] = "run key"


@dataclass(frozen=True, slots=True)
class WorkKey(_ValidatedKey):
    _label: ClassVar[str] = "work key"


@dataclass(frozen=True, slots=True)
class StageKey(_ValidatedKey):
    _label: ClassVar[str] = "stage key"


@dataclass(frozen=True, slots=True)
class RunCompletionKey(_ValidatedKey):
    _label: ClassVar[str] = "run completion key"


@dataclass(frozen=True, slots=True)
class PipelineKey(_ValidatedKey):
    _label: ClassVar[str] = "pipeline key"


@dataclass(frozen=True, slots=True)
class CampaignWorkIdentity:
    campaign_key: CampaignKey
    work_key: WorkKey

    def __post_init__(self) -> None:
        if not isinstance(self.campaign_key, CampaignKey):
            raise TypeError("campaign_key must be a CampaignKey")
        if not isinstance(self.work_key, WorkKey):
            raise TypeError("work_key must be a WorkKey")
