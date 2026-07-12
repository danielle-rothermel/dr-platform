"""Re-readable JSONL Manifest source and submission adapter."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path  # noqa: TC003 -- Pydantic resolves this at runtime
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    ValidationError,
)

from dr_platform.status import ServiceClass
from dr_platform.submission import (
    RegistrationConflictError,
    SubmitOptions,
    SubmitResult,
    prepare_manifest,
    submit,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy import Engine

    from dr_platform.db import PlatformSchema
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.items import SubmittableItem
    from dr_platform.manifests import OperationManifest
    from dr_platform.targets import ExecutionTarget, TargetResolver

DEFAULT_ITEM_KEY_FIELD = "item_key"
DEFAULT_GROUP_KEY_FIELD = "group_key"
UTF8_ENCODING = "utf-8"
SOURCE_HASH_CHUNK_BYTES = 1024 * 1024

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
JSON_OBJECT_ADAPTER = TypeAdapter(dict[StrictStr, Any])


class JsonlFieldNames(BaseModel):
    """Map a caller JSON object into the final Item contract.

    When ``service_class`` is omitted every Item is ``STANDARD``. When
    ``spec`` is omitted the complete JSON object is the Item spec.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr = DEFAULT_ITEM_KEY_FIELD
    group_key: NonEmptyStr = DEFAULT_GROUP_KEY_FIELD
    service_class: NonEmptyStr | None = None
    spec: NonEmptyStr | None = None


class JsonlItemRef(BaseModel):
    """Preflight descriptor for one non-empty JSONL record."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr
    item_index: NonNegativeInt
    byte_offset: NonNegativeInt
    service_class: ServiceClass


class _JsonlFileIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    device: NonNegativeInt
    inode: NonNegativeInt
    size: NonNegativeInt
    mtime_ns: NonNegativeInt


class JsonlManifestSource(BaseModel):
    """Bounded page reader backed by immutable preflight descriptors."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: Path
    group_key: NonEmptyStr
    fields: JsonlFieldNames
    refs: tuple[JsonlItemRef, ...]
    content_sha256: NonEmptyStr
    byte_length: NonNegativeInt
    file_identity: _JsonlFileIdentity

    @property
    def item_count(self) -> int:
        return len(self.refs)

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[SubmittableItem, ...]:
        if not 0 <= start_index <= end_index <= self.item_count:
            raise ValueError("JSONL Item range is outside the preflight index")
        refs = self.refs[start_index:end_index]
        if not refs:
            self._validate_file_identity()
            return ()

        self._validate_file_identity()
        items: list[SubmittableItem] = []
        try:
            with self.path.open("rb") as file:
                for ref in refs:
                    file.seek(ref.byte_offset)
                    line = file.readline()
                    item = _parse_item(
                        line,
                        line_number=None,
                        fields=self.fields,
                    )
                    if item.group_key != self.group_key:
                        raise RegistrationConflictError(
                            "JSONL group_key changed after Manifest "
                            "preparation"
                        )
                    if (
                        item.item_key != ref.item_key
                        or item.service_class is not ref.service_class
                    ):
                        raise RegistrationConflictError(
                            "JSONL Item descriptor changed after Manifest "
                            "preparation"
                        )
                    items.append(item.as_submittable())
        finally:
            self._validate_file_identity()
        return tuple(items)

    def validate_source_cut(self) -> None:
        """Strongly validate the complete source at a pass boundary."""
        self._validate_file_identity()
        try:
            content_sha256, byte_length = _source_cut(self.path)
        finally:
            self._validate_file_identity()
        if (
            content_sha256 != self.content_sha256
            or byte_length != self.byte_length
        ):
            raise RegistrationConflictError(
                "JSONL source changed after Manifest preparation"
            )

    def _validate_file_identity(self) -> None:
        if _file_identity(self.path) != self.file_identity:
            raise RegistrationConflictError(
                "JSONL source changed after Manifest preparation"
            )


class _JsonlItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr
    group_key: NonEmptyStr
    service_class: ServiceClass
    spec: dict[StrictStr, Any]

    def as_submittable(self) -> _SubmittableJsonlItem:
        return _SubmittableJsonlItem(
            item_key=self.item_key,
            service_class=self.service_class,
            spec=self.spec,
        )


class _SubmittableJsonlItem(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    item_key: NonEmptyStr
    service_class: ServiceClass
    spec: dict[StrictStr, Any]


def index_jsonl_manifest_source(
    path: Path,
    *,
    group_key: str,
    fields: JsonlFieldNames | None = None,
) -> JsonlManifestSource:
    """Preflight a complete JSONL source without writing domain state."""
    resolved_fields = fields or JsonlFieldNames()
    refs: list[JsonlItemRef] = []
    seen_item_keys: set[str] = set()
    content_hasher = sha256()
    byte_length = 0
    initial_identity = _file_identity(path)
    with path.open("rb") as file:
        for line_number, byte_offset, line in _iter_lines(file):
            content_hasher.update(line)
            byte_length += len(line)
            if not line.strip():
                continue
            item = _parse_item(
                line,
                line_number=line_number,
                fields=resolved_fields,
            )
            if item.group_key != group_key:
                raise ValueError(
                    f"JSONL {resolved_fields.group_key!r} must match "
                    f"Operation group_key {group_key!r} on line {line_number}"
                )
            if item.item_key in seen_item_keys:
                raise ValueError(f"duplicate JSONL item_key {item.item_key!r}")
            seen_item_keys.add(item.item_key)
            refs.append(
                JsonlItemRef(
                    item_key=item.item_key,
                    item_index=len(refs),
                    byte_offset=byte_offset,
                    service_class=item.service_class,
                )
            )
    final_identity = _file_identity(path)
    if (
        initial_identity != final_identity
        or byte_length != final_identity.size
    ):
        raise RegistrationConflictError(
            "JSONL source changed during Manifest preparation"
        )
    return JsonlManifestSource(
        path=path,
        group_key=group_key,
        fields=resolved_fields,
        refs=tuple(refs),
        content_sha256=content_hasher.hexdigest(),
        byte_length=byte_length,
        file_identity=final_identity,
    )


def prepare_jsonl_manifest(  # noqa: PLR0913 -- explicit adapter contract
    *,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    path: Path,
    fields: JsonlFieldNames | None = None,
    options: SubmitOptions | None = None,
) -> OperationManifest:
    """Prepare a JSONL Manifest in a no-write preflight pass."""
    source = index_jsonl_manifest_source(
        path,
        group_key=group_key,
        fields=fields,
    )
    return prepare_manifest(
        operation_key=operation_key,
        workflow_role=workflow_role,
        group_key=group_key,
        target=target,
        source=source,
        options=options,
    )


def submit_jsonl(  # noqa: PLR0913 -- explicit public facade contract
    manifest: OperationManifest,
    path: Path,
    fields: JsonlFieldNames | None = None,
    *,
    engine: Engine,
    resolver: TargetResolver,
    spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    options: SubmitOptions | None = None,
    source_application_version: str = "unknown",
    schema: PlatformSchema | None = None,
    queue_lookup: QueueLookup | None = None,
    enqueue_adapter: PhysicalEnqueueAdapter | None = None,
    workflow_observer: WorkflowObserver | None = None,
) -> SubmitResult:
    """Submit a fresh JSONL read through the sole registration pipeline."""
    selected_options = options or SubmitOptions(page_size=manifest.page_size)
    if selected_options.page_size != manifest.page_size:
        raise RegistrationConflictError(
            "SubmitOptions.page_size must match the prepared Manifest"
        )
    source = index_jsonl_manifest_source(
        path,
        group_key=manifest.group_key,
        fields=fields,
    )
    return submit(
        manifest,
        source,
        engine=engine,
        resolver=resolver,
        spec=spec,
        metadata=metadata,
        options=selected_options,
        source_application_version=source_application_version,
        schema=schema,
        queue_lookup=queue_lookup,
        enqueue_adapter=enqueue_adapter,
        workflow_observer=workflow_observer,
    )


def _iter_lines(
    file: BinaryIO,
) -> Iterator[tuple[int, int, bytes]]:
    line_number = 0
    while True:
        byte_offset = file.tell()
        line = file.readline()
        if not line:
            return
        line_number += 1
        yield line_number, byte_offset, line


def _source_cut(path: Path) -> tuple[str, int]:
    content_hasher = sha256()
    byte_length = 0
    with path.open("rb") as file:
        while chunk := file.read(SOURCE_HASH_CHUNK_BYTES):
            content_hasher.update(chunk)
            byte_length += len(chunk)
    return content_hasher.hexdigest(), byte_length


def _file_identity(path: Path) -> _JsonlFileIdentity:
    stat = path.stat()
    return _JsonlFileIdentity(
        device=stat.st_dev,
        inode=stat.st_ino,
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )


def _parse_item(
    line: bytes,
    *,
    line_number: int | None,
    fields: JsonlFieldNames,
) -> _JsonlItem:
    location = f" on line {line_number}" if line_number is not None else ""
    try:
        payload = JSON_OBJECT_ADAPTER.validate_json(
            line.decode(UTF8_ENCODING),
            strict=True,
        )
    except (UnicodeDecodeError, ValidationError) as error:
        raise ValueError(f"invalid JSONL Item JSON{location}") from error

    item_key = _required_nonempty_string(
        payload,
        fields.item_key,
        location=location,
    )
    group_key = _required_nonempty_string(
        payload,
        fields.group_key,
        location=location,
    )
    service_class = _service_class(payload, fields, location=location)
    spec = _spec(payload, fields, location=location)
    return _JsonlItem(
        item_key=item_key,
        group_key=group_key,
        service_class=service_class,
        spec=spec,
    )


def _required_nonempty_string(
    payload: dict[str, Any],
    field_name: str,
    *,
    location: str,
) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"JSONL field {field_name!r} must be a non-empty string{location}"
        )
    return value


def _service_class(
    payload: dict[str, Any],
    fields: JsonlFieldNames,
    *,
    location: str,
) -> ServiceClass:
    if fields.service_class is None:
        return ServiceClass.STANDARD
    value = payload.get(fields.service_class)
    try:
        return ServiceClass(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"JSONL field {fields.service_class!r} must be a valid "
            f"ServiceClass{location}"
        ) from error


def _spec(
    payload: dict[str, Any],
    fields: JsonlFieldNames,
    *,
    location: str,
) -> dict[str, Any]:
    if fields.spec is None:
        return dict(payload)
    value = payload.get(fields.spec)
    if not isinstance(value, dict):
        raise ValueError(  # noqa: TRY004 -- malformed input, not a bug
            f"JSONL field {fields.spec!r} must be an object{location}"
        )
    return value
