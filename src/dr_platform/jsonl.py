"""Single-read JSONL submission adapter."""

from __future__ import annotations

from pathlib import Path  # noqa: TC003 -- Pydantic resolves this at runtime
from typing import TYPE_CHECKING, Annotated, Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictStr,
    TypeAdapter,
    ValidationError,
)

from dr_platform.status import ServiceClass
from dr_platform.submission import (
    SubmitOptions,
    SubmitResult,
    submit,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from dr_platform.db import PlatformSchema
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.items import SubmittableItem
    from dr_platform.targets import ExecutionTarget, TargetResolver

DEFAULT_ITEM_KEY_FIELD = "item_key"
DEFAULT_GROUP_KEY_FIELD = "group_key"
UTF8_ENCODING = "utf-8"

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
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


class JsonlSubmissionSource(BaseModel):
    """Parsed JSONL Items owned by one submission call."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    items: tuple[_SubmittableJsonlItem, ...]

    @property
    def item_count(self) -> int:
        return len(self.items)

    def read_items(
        self,
        *,
        start_index: int,
        end_index: int,
    ) -> tuple[SubmittableItem, ...]:
        if not 0 <= start_index <= end_index <= self.item_count:
            raise ValueError("JSONL Item range is outside the submission")
        return self.items[start_index:end_index]


def read_jsonl_source(
    path: Path,
    *,
    group_key: str,
    fields: JsonlFieldNames | None = None,
) -> JsonlSubmissionSource:
    """Read and validate a complete JSONL source once."""
    resolved_fields = fields or JsonlFieldNames()
    items: list[_SubmittableJsonlItem] = []
    seen_item_keys: set[str] = set()
    with path.open("rb") as file:
        for line_number, line in enumerate(file, start=1):
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
            items.append(item.as_submittable())
    return JsonlSubmissionSource(items=tuple(items))


def submit_jsonl(  # noqa: PLR0913 -- explicit public facade contract
    *,
    operation_key: str,
    workflow_role: str,
    group_key: str,
    target: ExecutionTarget,
    path: Path,
    engine: Engine,
    resolver: TargetResolver,
    fields: JsonlFieldNames | None = None,
    spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    options: SubmitOptions | None = None,
    source_application_version: str = "unknown",
    schema: PlatformSchema | None = None,
    queue_lookup: QueueLookup | None = None,
    enqueue_adapter: PhysicalEnqueueAdapter | None = None,
    workflow_observer: WorkflowObserver | None = None,
) -> SubmitResult:
    """Read JSONL once and submit it through the registration pipeline."""
    source = read_jsonl_source(
        path,
        group_key=group_key,
        fields=fields,
    )
    return submit(
        operation_key=operation_key,
        workflow_role=workflow_role,
        group_key=group_key,
        target=target,
        source=source,
        engine=engine,
        resolver=resolver,
        spec=spec,
        metadata=metadata,
        options=options,
        source_application_version=source_application_version,
        schema=schema,
        queue_lookup=queue_lookup,
        enqueue_adapter=enqueue_adapter,
        workflow_observer=workflow_observer,
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
