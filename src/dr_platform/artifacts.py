"""Content-addressed artifact offload.

Rows keep pointers (the three ``ArtifactRef`` fields); bytes live in
the store. One local-directory backend; S3 arrives when a real demand
exists. Reads verify the digest (corruption surfaces at read time,
not analysis time).
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr

DEFAULT_CONTENT_TYPE = "application/octet-stream"


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sha256: StrictStr
    size_bytes: StrictInt
    content_type: StrictStr = DEFAULT_CONTENT_TYPE


class ArtifactIntegrityError(RuntimeError):
    pass


class ArtifactNotFoundError(FileNotFoundError):
    pass


@runtime_checkable
class ArtifactStore(Protocol):
    def put_bytes(
        self,
        data: bytes,
        *,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> ArtifactRef: ...

    def get_bytes(self, ref: ArtifactRef | str) -> bytes: ...

    def exists(self, sha256: str) -> bool: ...


class LocalDirArtifactStore:
    """Sharded content-addressed directory with atomic writes."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).expanduser()

    def _path_for(self, sha256: str) -> Path:
        return self.root / sha256[:2] / sha256[2:4] / sha256

    def put_bytes(
        self,
        data: bytes,
        *,
        content_type: str = DEFAULT_CONTENT_TYPE,
    ) -> ArtifactRef:
        digest = hashlib.sha256(data).hexdigest()
        path = self._path_for(digest)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            file_descriptor, temp_name = tempfile.mkstemp(
                dir=path.parent,
                prefix=".tmp-",
            )
            try:
                with os.fdopen(file_descriptor, "wb") as file:
                    file.write(data)
                Path(temp_name).replace(path)
            finally:
                Path(temp_name).unlink(missing_ok=True)
        return ArtifactRef(
            sha256=digest,
            size_bytes=len(data),
            content_type=content_type,
        )

    def get_bytes(self, ref: ArtifactRef | str) -> bytes:
        sha256 = ref.sha256 if isinstance(ref, ArtifactRef) else ref
        path = self._path_for(sha256)
        if not path.exists():
            raise ArtifactNotFoundError(sha256)
        data = path.read_bytes()
        actual = hashlib.sha256(data).hexdigest()
        if actual != sha256:
            raise ArtifactIntegrityError(
                f"artifact {sha256} failed verify-on-read "
                f"(stored bytes hash to {actual})"
            )
        return data

    def exists(self, sha256: str) -> bool:
        return self._path_for(sha256).exists()
