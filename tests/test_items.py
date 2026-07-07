from __future__ import annotations

import pytest
from dr_serialize import sha256_json_digest
from pydantic import BaseModel

from dr_platform import (
    ItemIdentity,
    SubmittableItem,
    batch_item_id,
    claim_token,
    stable_item_id,
)


class WorkItem(BaseModel):
    name: str
    experiment: str

    @property
    def item_id(self) -> str:
        return f"id-{self.name}"

    @property
    def order_key(self) -> str:
        return f"order-{self.name}"

    @property
    def group_key(self) -> str:
        return self.experiment


def test_typed_model_satisfies_submittable_item_protocol() -> None:
    item = WorkItem(name="a", experiment="exp")
    assert isinstance(item, SubmittableItem)
    assert item.item_id == "id-a"
    assert item.order_key == "order-a"
    assert item.group_key == "exp"


def test_stable_item_id_is_deterministic_and_axis_sensitive() -> None:
    axes = {"model": "m1", "layer": 4}
    first = stable_item_id("ns", axes=axes)
    assert first == stable_item_id("ns", axes={"layer": 4, "model": "m1"})
    assert first != stable_item_id("other", axes=axes)
    assert first != stable_item_id("ns", axes={"model": "m1", "layer": 5})
    assert len(first) == 16


def test_stable_item_id_payload_shape_is_frozen() -> None:
    # The payload shape {"namespace": ..., "axes": {...}} is persisted
    # identity for adopters; never change it silently.
    assert stable_item_id("ns", axes={"a": 1}) == sha256_json_digest(
        {"namespace": "ns", "axes": {"a": 1}},
        length=16,
    )


def test_batch_item_id_neutral_default_recipe() -> None:
    assert batch_item_id(
        operation_key="op-1",
        item_id="item-1",
    ) == sha256_json_digest(
        {"operation_key": "op-1", "item_id": "item-1"},
        length=32,
    )


def test_batch_item_id_reproduces_whetstone_bytes() -> None:
    # Byte-compat gate: whetstone's persisted batch_submit_item_id
    # hashed the literal key "prediction_id" (platform.md ItemIdentity).
    identity = ItemIdentity(item_key_label="prediction_id")
    assert batch_item_id(
        operation_key="op-1",
        item_id="pred-123",
        identity=identity,
    ) == sha256_json_digest(
        {"operation_key": "op-1", "prediction_id": "pred-123"},
        length=32,
    )


def test_claim_token_reproduces_whetstone_bytes() -> None:
    identity = ItemIdentity(item_key_label="prediction_id")
    claimed_at = "2026-07-04T12:00:00+00:00"
    assert claim_token(
        operation_key="op-1",
        item_id="pred-123",
        claimed_at=claimed_at,
        identity=identity,
    ) == sha256_json_digest(
        {
            "operation_key": "op-1",
            "prediction_id": "pred-123",
            "claimed_at": claimed_at,
        },
        length=32,
    )


def test_item_identity_is_frozen_config() -> None:
    identity = ItemIdentity()
    assert identity.item_key_label == "item_id"
    assert identity.id_length == 32
    with pytest.raises(Exception, match="frozen"):
        identity.item_key_label = "other"  # type: ignore[misc]
