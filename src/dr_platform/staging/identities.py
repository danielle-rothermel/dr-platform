"""Validated identities for the staged platform rebuild."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

KEY_MAX_LENGTH = 128
_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]*\Z")


def validate_key_value(value: str, *, label: str) -> None:
    """Validate one opaque key at its construction boundary."""
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


@dataclass(frozen=True, slots=True)
class CampaignKey(_ValidatedKey):
    """Application namespace for related logical work."""

    _label: ClassVar[str] = "campaign key"


@dataclass(frozen=True, slots=True)
class RunKey(_ValidatedKey):
    """Identity of one immutable pipeline submission invocation."""

    _label: ClassVar[str] = "run key"


@dataclass(frozen=True, slots=True)
class WorkKey(_ValidatedKey):
    """Opaque stable work identity within a campaign."""

    _label: ClassVar[str] = "work key"


@dataclass(frozen=True, slots=True)
class StageKey(_ValidatedKey):
    """Identity of one position in a declared linear pipeline."""

    _label: ClassVar[str] = "stage key"


@dataclass(frozen=True, slots=True, init=False)
class CampaignWorkIdentity:
    """The campaign-scoped uniqueness identity for logical work."""

    campaign_key: CampaignKey
    work_key: WorkKey

    def __init__(
        self,
        campaign_key: CampaignKey | str,
        work_key: WorkKey | str,
    ) -> None:
        object.__setattr__(
            self,
            "campaign_key",
            campaign_key
            if isinstance(campaign_key, CampaignKey)
            else CampaignKey(campaign_key),
        )
        object.__setattr__(
            self,
            "work_key",
            work_key if isinstance(work_key, WorkKey) else WorkKey(work_key),
        )
