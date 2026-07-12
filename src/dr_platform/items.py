"""Final caller Item contract and fixed kernel identity recipes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dr_serialize import sha256_json_digest

if TYPE_CHECKING:
    from typing import Any

    from dr_platform.status import ServiceClass

ITEM_ID_LENGTH = 32
SHUFFLE_RANK_BITS = 63
SHUFFLE_RANK_HEX_LENGTH = 16
SHUFFLE_RANK_MAX = (1 << SHUFFLE_RANK_BITS) - 1


@runtime_checkable
class SubmittableItem(Protocol):
    @property
    def item_key(self) -> str: ...

    @property
    def spec(self) -> dict[str, Any]: ...

    @property
    def service_class(self) -> ServiceClass: ...


def item_id(*, operation_key: str, item_key: str) -> str:
    """Derive the fixed Operation-local Item identity."""
    return sha256_json_digest(
        {"operation_key": operation_key, "item_key": item_key},
        length=ITEM_ID_LENGTH,
    )


def shuffle_rank(*, item_id: str) -> int:
    """Derive a stable positive signed-63-bit scheduling rank."""
    digest = sha256_json_digest(
        {"item_id": item_id}, length=SHUFFLE_RANK_HEX_LENGTH
    )
    return (int(digest, 16) % SHUFFLE_RANK_MAX) + 1
