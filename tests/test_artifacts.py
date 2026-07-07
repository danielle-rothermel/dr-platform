from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from dr_platform import (
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ArtifactRef,
    ArtifactStore,
    LocalDirArtifactStore,
)


def test_put_get_round_trip(tmp_path: Path) -> None:
    store = LocalDirArtifactStore(tmp_path)
    data = b"tensor bytes" * 100

    ref = store.put_bytes(data, content_type="application/x-tensor")

    assert ref.sha256 == hashlib.sha256(data).hexdigest()
    assert ref.size_bytes == len(data)
    assert ref.content_type == "application/x-tensor"
    assert store.exists(ref.sha256)
    assert store.get_bytes(ref) == data
    assert store.get_bytes(ref.sha256) == data


def test_put_is_content_addressed_and_idempotent(tmp_path: Path) -> None:
    store = LocalDirArtifactStore(tmp_path)
    first = store.put_bytes(b"same")
    second = store.put_bytes(b"same")
    assert first.sha256 == second.sha256
    files = [
        path
        for path in tmp_path.rglob("*")
        if path.is_file() and not path.name.startswith(".tmp-")
    ]
    assert len(files) == 1


def test_get_missing_raises_not_found(tmp_path: Path) -> None:
    store = LocalDirArtifactStore(tmp_path)
    with pytest.raises(ArtifactNotFoundError):
        store.get_bytes("0" * 64)


def test_verify_on_read_detects_corruption(tmp_path: Path) -> None:
    store = LocalDirArtifactStore(tmp_path)
    ref = store.put_bytes(b"original")
    path = tmp_path / ref.sha256[:2] / ref.sha256[2:4] / ref.sha256
    path.write_bytes(b"tampered")

    with pytest.raises(ArtifactIntegrityError, match="verify-on-read"):
        store.get_bytes(ref)


def test_store_satisfies_protocol(tmp_path: Path) -> None:
    store = LocalDirArtifactStore(tmp_path)
    assert isinstance(store, ArtifactStore)
    assert isinstance(store.put_bytes(b"x"), ArtifactRef)
