"""Work-item identity: the protocol and the digest recipes.

`SubmittableItem` is everything the library needs to know about a work
item. `ItemIdentity` exists for byte-compatibility: persisted digest
recipes hash JSON payloads whose key names are caller words (whetstone's
rows were written with ``"prediction_id"``), so adopters with existing
data configure the label instead of breaking their IDs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from dr_serialize import sha256_json_digest
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

DEFAULT_ITEM_KEY_LABEL = "item_id"
DEFAULT_ITEM_ID_LENGTH = 32
DEFAULT_STABLE_ITEM_ID_LENGTH = 16


@runtime_checkable
class SubmittableItem(Protocol):
    """What batch submission needs from a work item — nothing else."""

    @property
    def item_id(self) -> str: ...

    @property
    def order_key(self) -> str: ...

    @property
    def group_key(self) -> str: ...


class ItemIdentity(BaseModel):
    """Digest-recipe configuration for persisted batch-item IDs.

    ``item_key_label`` is the JSON key used for the item id inside the
    hashed payloads (and, in 6b, the physical column name). It is part
    of the persisted bytes: changing it changes every derived ID.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key_label: StrictStr = DEFAULT_ITEM_KEY_LABEL
    id_length: StrictInt = DEFAULT_ITEM_ID_LENGTH


def stable_item_id(
    namespace: str,
    *,
    axes: Mapping[str, Any],
    length: int = DEFAULT_STABLE_ITEM_ID_LENGTH,
) -> str:
    """Canonical-hash identity from declared axes.

    The payload shape ``{"namespace": ..., "axes": {...}}`` is frozen:
    IDs derived from it are persisted by adopters.
    """
    return sha256_json_digest(
        {"namespace": namespace, "axes": dict(axes)},
        length=length,
    )


def batch_item_id(
    *,
    operation_key: str,
    item_id: str,
    identity: ItemIdentity | None = None,
) -> str:
    """Deterministic batch-item primary key (idempotent re-submission)."""
    resolved = identity or ItemIdentity()
    return sha256_json_digest(
        {
            "operation_key": operation_key,
            resolved.item_key_label: item_id,
        },
        length=resolved.id_length,
    )


def claim_token(
    *,
    operation_key: str,
    item_id: str,
    claimed_at: str,
    identity: ItemIdentity | None = None,
) -> str:
    """Per-attempt lease token for the CLAIMING compare-and-swap."""
    resolved = identity or ItemIdentity()
    return sha256_json_digest(
        {
            "operation_key": operation_key,
            resolved.item_key_label: item_id,
            "claimed_at": claimed_at,
        },
        length=resolved.id_length,
    )
