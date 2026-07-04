"""Durable, idempotent, resumable batch submission.

The claim/lease loop ported from whetstone's platform: per item,
PENDING -> CLAIMING (CAS on ``enqueue_status='pending'``) -> one of
ENQUEUED / WORKFLOW_ALREADY_PRESENT / FAILED (CAS on the claim
token). Before each pass, FAILED items and stale CLAIMING leases
reset to PENDING, so re-running an operation reconciles instead of
duplicating.

Domain rows are the caller's business: the optional ``seed`` hook runs
inside the same transaction that registers each window's items, and
returns which item ids were newly inserted app-side (drives
``insert_status`` accounting).
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from sqlalchemy import null, select, update
from sqlalchemy.dialects.postgresql import insert

from dr_platform.batch_status import (
    BatchItemEnqueueStatus,
    BatchItemInsertStatus,
    BatchOperationStatus,
    operation_status_from_counts,
)
from dr_platform.fairness import fair_ordered_windows, validate_window_size
from dr_platform.items import batch_item_id, claim_token
from dr_platform.jsonl import index_jsonl_items, load_jsonl_items
from dr_platform.records import (
    ENQUEUE_CLAIM_ID_METADATA_KEY,
    ENQUEUE_CLAIMED_AT_METADATA_KEY,
    WORKFLOW_ID_METADATA_KEY,
    BatchItemRecord,
    BatchOperationRecord,
    EnqueueFailure,
)

if TYPE_CHECKING:
    from collections.abc import (
        Callable,
        Iterable,
        Sequence,
    )
    from collections.abc import (
        Set as AbstractSet,
    )
    from pathlib import Path

    from sqlalchemy.engine import Connection, Engine

    from dr_platform.db.schema import PlatformSchema
    from dr_platform.enqueue import EnqueueItem
    from dr_platform.items import SubmittableItem
    from dr_platform.jsonl import JsonlFieldNames

DEFAULT_SUBMIT_CHUNK_SIZE = 500

type SeedHook = Callable[
    ["Connection", "Sequence[SubmittableItem]"],
    "AbstractSet[str] | None",
]
type ClassifyEnqueueError = Callable[[BaseException], EnqueueFailure]


class SubmittedItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: StrictStr
    order_key: StrictStr
    insert_status: BatchItemInsertStatus
    enqueue_status: BatchItemEnqueueStatus
    workflow_id: StrictStr | None = None
    workflow_metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    failure: EnqueueFailure | None = None


class BatchSubmitResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    operation_key: StrictStr
    group_key: StrictStr
    requested_count: StrictInt
    inserted_count: StrictInt
    already_present_count: StrictInt
    enqueued_count: StrictInt
    already_scheduled_count: StrictInt
    failed_count: StrictInt
    items: tuple[SubmittedItem, ...]


class EnqueueCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: StrictStr
    order_key: StrictStr
    item_index: StrictInt
    insert_status: BatchItemInsertStatus


def enqueue_failure_from_exception(error: BaseException) -> EnqueueFailure:
    """Structural default classifier (apps may inject their own)."""
    failure_class = getattr(error, "failure_class", None)
    if isinstance(failure_class, StrEnum):
        failure_class = failure_class.value
    if not isinstance(failure_class, str):
        failure_class = None
    metadata = getattr(error, "metadata", None)
    underlying = getattr(error, "underlying", None) or error.__cause__
    return EnqueueFailure(
        failure_class=failure_class,
        error_type=f"{type(error).__module__}.{type(error).__qualname__}",
        underlying_exception_type=(
            f"{type(underlying).__module__}.{type(underlying).__qualname__}"
            if isinstance(underlying, BaseException)
            else None
        ),
        message=str(error),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
    )


def submit_batch(  # noqa: PLR0913 -- the facade surface
    engine: Engine,
    *,
    operation_key: str,
    group_key: str,
    items: Iterable[SubmittableItem],
    enqueue: EnqueueItem,
    schema: PlatformSchema,
    seed: SeedHook | None = None,
    submit_spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_SUBMIT_CHUNK_SIZE,
    classify_error: ClassifyEnqueueError = enqueue_failure_from_exception,
) -> BatchSubmitResult:
    windows = list(fair_ordered_windows(items, window_size=chunk_size))
    return _submit_windows(
        engine,
        operation_key=operation_key,
        group_key=group_key,
        windows=windows,
        enqueue=enqueue,
        schema=schema,
        seed=seed,
        submit_spec=submit_spec,
        metadata=metadata,
        chunk_size=chunk_size,
        classify_error=classify_error,
    )


def submit_batch_jsonl(  # noqa: PLR0913 -- the facade surface
    engine: Engine,
    *,
    operation_key: str,
    group_key: str,
    items_file: Path,
    parse: Callable[[str], SubmittableItem],
    enqueue: EnqueueItem,
    schema: PlatformSchema,
    fields: JsonlFieldNames | None = None,
    seed: SeedHook | None = None,
    submit_spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    chunk_size: int = DEFAULT_SUBMIT_CHUNK_SIZE,
    classify_error: ClassifyEnqueueError = enqueue_failure_from_exception,
) -> BatchSubmitResult:
    """Windowed submission from a JSONL file without materializing it."""
    validate_window_size(chunk_size)
    refs = index_jsonl_items(items_file, group_key=group_key, fields=fields)

    def window_stream() -> Iterable[Sequence[SubmittableItem]]:
        for ref_window in fair_ordered_windows(
            refs,
            window_size=chunk_size,
        ):
            yield load_jsonl_items(items_file, ref_window, parse=parse)

    return _submit_windows(
        engine,
        operation_key=operation_key,
        group_key=group_key,
        windows=window_stream(),
        enqueue=enqueue,
        schema=schema,
        seed=seed,
        submit_spec=submit_spec,
        metadata=metadata,
        chunk_size=chunk_size,
        classify_error=classify_error,
    )


def _submit_windows(  # noqa: PLR0913 -- internal fan-in of both entrypoints
    engine: Engine,
    *,
    operation_key: str,
    group_key: str,
    windows: Iterable[Sequence[SubmittableItem]],
    enqueue: EnqueueItem,
    schema: PlatformSchema,
    seed: SeedHook | None,
    submit_spec: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
    chunk_size: int,
    classify_error: ClassifyEnqueueError,
) -> BatchSubmitResult:
    validate_window_size(chunk_size)
    seen_item_ids: set[str] = set()
    item_index_offset = 0
    for window in windows:
        _validate_window(
            window,
            group_key=group_key,
            seen_item_ids=seen_item_ids,
        )
        with engine.begin() as connection:
            prepare_submission_records(
                connection,
                operation_key=operation_key,
                group_key=group_key,
                ordered_items=window,
                schema=schema,
                seed=seed,
                submit_spec=submit_spec,
                metadata=metadata,
                item_index_offset=item_index_offset,
            )
        item_index_offset += len(window)

    if item_index_offset == 0:
        with engine.begin() as connection:
            prepare_submission_records(
                connection,
                operation_key=operation_key,
                group_key=group_key,
                ordered_items=(),
                schema=schema,
                seed=seed,
                submit_spec=submit_spec,
                metadata=metadata,
            )
    else:
        with engine.begin() as connection:
            prepare_enqueue_retries(
                connection,
                operation_key=operation_key,
                schema=schema,
            )
        enqueue_pending_batch_items(
            engine,
            operation_key=operation_key,
            page_size=chunk_size,
            enqueue=enqueue,
            schema=schema,
            classify_error=classify_error,
        )

    with engine.begin() as connection:
        return update_operation_summary(
            connection,
            operation_key=operation_key,
            group_key=group_key,
            schema=schema,
        )


def _validate_window(
    window: Sequence[SubmittableItem],
    *,
    group_key: str,
    seen_item_ids: set[str],
) -> None:
    for item in window:
        if item.group_key != group_key:
            raise ValueError("item group_key must match the submit operation")
        if item.item_id in seen_item_ids:
            raise ValueError(
                f"duplicate item_id in submit operation: {item.item_id}"
            )
        seen_item_ids.add(item.item_id)


def prepare_submission_records(  # noqa: PLR0913 -- one registration transaction
    connection: Connection,
    *,
    operation_key: str,
    group_key: str,
    ordered_items: Sequence[SubmittableItem],
    schema: PlatformSchema,
    seed: SeedHook | None = None,
    submit_spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    item_index_offset: int = 0,
) -> None:
    created_at = datetime.now(UTC)
    existing_operation = load_batch_operation(
        connection,
        operation_key=operation_key,
        schema=schema,
    )
    if existing_operation is not None:
        ensure_batch_operation_matches_request(
            existing_operation,
            group_key=group_key,
            submit_spec=submit_spec,
            metadata=metadata,
        )
    else:
        operation = BatchOperationRecord(
            operation_key=operation_key,
            group_key=group_key,
            status=BatchOperationStatus.ENQUEUING,
            requested_count=item_index_offset + len(ordered_items),
            spec=submit_spec or {},
            metadata=metadata or {},
            created_at=created_at,
        )
        connection.execute(
            insert(schema.batch_operations)
            .values(batch_operation_row(operation, schema=schema))
            .on_conflict_do_nothing(index_elements=["operation_key"])
        )
    mark_operation_enqueuing(
        connection,
        operation_key=operation_key,
        requested_count=item_index_offset + len(ordered_items),
        schema=schema,
    )

    if not ordered_items:
        return
    inserted_ids = seed(connection, ordered_items) if seed else None
    for item_index, item in enumerate(ordered_items, start=item_index_offset):
        insert_status = (
            BatchItemInsertStatus.INSERTED
            if inserted_ids is None or item.item_id in inserted_ids
            else BatchItemInsertStatus.ALREADY_PRESENT
        )
        record = BatchItemRecord(
            batch_submit_item_id=batch_item_id(
                operation_key=operation_key,
                item_id=item.item_id,
                identity=schema.naming.identity,
            ),
            operation_key=operation_key,
            item_index=item_index,
            item_id=item.item_id,
            order_key=item.order_key,
            insert_status=insert_status,
            enqueue_status=BatchItemEnqueueStatus.PENDING,
            created_at=created_at,
        )
        connection.execute(
            insert(schema.batch_items)
            .values(batch_item_insert_values(record, schema=schema))
            .on_conflict_do_nothing(
                index_elements=[
                    "operation_key",
                    schema.naming.item_key_label,
                ]
            )
        )


def load_batch_operation(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> BatchOperationRecord | None:
    row = (
        connection.execute(
            select(schema.batch_operations).where(
                schema.batch_operations.c.operation_key == operation_key
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return batch_operation_record_from_row(dict(row), schema=schema)


def ensure_batch_operation_matches_request(
    existing: BatchOperationRecord,
    *,
    group_key: str,
    submit_spec: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> None:
    if existing.group_key != group_key:
        raise ValueError(
            "batch submit operation group_key does not match request"
        )
    if existing.spec != (submit_spec or {}):
        raise ValueError(
            "batch submit operation submit_spec does not match request"
        )
    if existing.metadata != (metadata or {}):
        raise ValueError(
            "batch submit operation metadata does not match request"
        )


def mark_operation_enqueuing(
    connection: Connection,
    *,
    operation_key: str,
    requested_count: int,
    schema: PlatformSchema,
) -> None:
    connection.execute(
        update(schema.batch_operations)
        .where(schema.batch_operations.c.operation_key == operation_key)
        .values(
            status=BatchOperationStatus.ENQUEUING.value,
            requested_count=requested_count,
            completed_at=None,
        )
    )


def reset_failed_enqueue_items(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> None:
    connection.execute(
        update(schema.batch_items)
        .where(schema.batch_items.c.operation_key == operation_key)
        .where(
            schema.batch_items.c.enqueue_status
            == BatchItemEnqueueStatus.FAILED.value
        )
        .values(
            enqueue_status=BatchItemEnqueueStatus.PENDING.value,
            enqueue_metadata={},
            failure=null(),
        )
    )


def reset_stale_enqueue_claims(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> None:
    connection.execute(
        update(schema.batch_items)
        .where(schema.batch_items.c.operation_key == operation_key)
        .where(
            schema.batch_items.c.enqueue_status
            == BatchItemEnqueueStatus.CLAIMING.value
        )
        .values(
            enqueue_status=BatchItemEnqueueStatus.PENDING.value,
            enqueue_metadata={},
        )
    )


def prepare_enqueue_retries(
    connection: Connection,
    *,
    operation_key: str,
    schema: PlatformSchema,
) -> None:
    reset_failed_enqueue_items(
        connection,
        operation_key=operation_key,
        schema=schema,
    )
    reset_stale_enqueue_claims(
        connection,
        operation_key=operation_key,
        schema=schema,
    )


def enqueue_pending_batch_items(  # noqa: PLR0913 -- claim/enqueue drive loop
    engine: Engine,
    *,
    operation_key: str,
    page_size: int,
    enqueue: EnqueueItem,
    schema: PlatformSchema,
    classify_error: ClassifyEnqueueError = enqueue_failure_from_exception,
) -> None:
    validate_window_size(page_size)
    while True:
        with engine.begin() as connection:
            candidates = load_pending_enqueue_candidates(
                connection,
                operation_key=operation_key,
                limit=page_size,
                schema=schema,
            )
        if not candidates:
            return
        for candidate in candidates:
            with engine.begin() as connection:
                claim_id = claim_pending_batch_item(
                    connection,
                    operation_key=operation_key,
                    item_id=candidate.item_id,
                    schema=schema,
                )
            if claim_id is None:
                continue
            item = _enqueue_candidate(
                candidate,
                enqueue=enqueue,
                classify_error=classify_error,
            )
            with engine.begin() as connection:
                update_batch_item_outcome(
                    connection,
                    operation_key=operation_key,
                    item=item,
                    claim_id=claim_id,
                    schema=schema,
                )


def _enqueue_candidate(
    candidate: EnqueueCandidate,
    *,
    enqueue: EnqueueItem,
    classify_error: ClassifyEnqueueError,
) -> SubmittedItem:
    try:
        outcome = enqueue(candidate.item_id)
        return SubmittedItem(
            item_id=candidate.item_id,
            order_key=candidate.order_key,
            insert_status=candidate.insert_status,
            enqueue_status=(
                BatchItemEnqueueStatus.ENQUEUED
                if outcome.enqueued
                else BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT
            ),
            workflow_id=outcome.workflow_id,
            workflow_metadata=dict(outcome.metadata),
        )
    except Exception as error:  # noqa: BLE001 -- failures become row state
        return SubmittedItem(
            item_id=candidate.item_id,
            order_key=candidate.order_key,
            insert_status=candidate.insert_status,
            enqueue_status=BatchItemEnqueueStatus.FAILED,
            failure=classify_error(error),
        )


def load_pending_enqueue_candidates(
    connection: Connection,
    *,
    operation_key: str,
    limit: int,
    schema: PlatformSchema,
) -> tuple[EnqueueCandidate, ...]:
    validate_window_size(limit)
    naming = schema.naming
    rows = connection.execute(
        select(schema.batch_items)
        .where(schema.batch_items.c.operation_key == operation_key)
        .where(
            schema.batch_items.c.enqueue_status
            == BatchItemEnqueueStatus.PENDING.value
        )
        .order_by(
            schema.batch_items.c[naming.order_key_label],
            schema.batch_items.c[naming.item_key_label],
        )
        .limit(limit)
    ).mappings()
    return tuple(
        EnqueueCandidate(
            item_id=row[naming.item_key_label],
            order_key=row[naming.order_key_label],
            item_index=row["item_index"],
            insert_status=BatchItemInsertStatus(row["insert_status"]),
        )
        for row in rows
    )


def claim_pending_batch_item(
    connection: Connection,
    *,
    operation_key: str,
    item_id: str,
    schema: PlatformSchema,
) -> str | None:
    claimed_at = datetime.now(UTC).isoformat()
    claim_id = claim_token(
        operation_key=operation_key,
        item_id=item_id,
        claimed_at=claimed_at,
        identity=schema.naming.identity,
    )
    result = connection.execute(
        update(schema.batch_items)
        .where(schema.batch_items.c.operation_key == operation_key)
        .where(schema.batch_items.c[schema.naming.item_key_label] == item_id)
        .where(
            schema.batch_items.c.enqueue_status
            == BatchItemEnqueueStatus.PENDING.value
        )
        .values(
            enqueue_status=BatchItemEnqueueStatus.CLAIMING.value,
            enqueue_metadata={
                ENQUEUE_CLAIM_ID_METADATA_KEY: claim_id,
                ENQUEUE_CLAIMED_AT_METADATA_KEY: claimed_at,
            },
        )
    )
    if result.rowcount != 1:
        return None
    return claim_id


def update_batch_item_outcome(
    connection: Connection,
    *,
    operation_key: str,
    item: SubmittedItem,
    claim_id: str,
    schema: PlatformSchema,
) -> None:
    item_col = schema.batch_items.c[schema.naming.item_key_label]
    existing = (
        connection.execute(
            select(schema.batch_items)
            .where(schema.batch_items.c.operation_key == operation_key)
            .where(item_col == item.item_id)
        )
        .mappings()
        .one()
    )
    updated_item = item.model_copy(
        update={
            "insert_status": BatchItemInsertStatus(existing["insert_status"])
        }
    )
    result = connection.execute(
        update(schema.batch_items)
        .where(schema.batch_items.c.operation_key == operation_key)
        .where(item_col == item.item_id)
        .where(
            schema.batch_items.c.enqueue_status
            == BatchItemEnqueueStatus.CLAIMING.value
        )
        .where(
            schema.batch_items.c.enqueue_metadata[
                ENQUEUE_CLAIM_ID_METADATA_KEY
            ].astext
            == claim_id
        )
        .values(
            insert_status=updated_item.insert_status.value,
            enqueue_status=updated_item.enqueue_status.value,
            enqueue_metadata=enqueue_metadata_for_item(updated_item),
            failure=(
                updated_item.failure.model_dump(mode="json")
                if updated_item.failure is not None
                else null()
            ),
        )
    )
    if result.rowcount != 1:
        raise RuntimeError(
            "batch submit item outcome update matched no rows: "
            f"operation_key={operation_key!r} "
            f"item_id={item.item_id!r} "
            f"claim_id={claim_id!r}"
        )


def update_operation_summary(
    connection: Connection,
    *,
    operation_key: str,
    group_key: str,
    schema: PlatformSchema,
) -> BatchSubmitResult:
    naming = schema.naming
    rows = tuple(
        connection.execute(
            select(schema.batch_items)
            .where(schema.batch_items.c.operation_key == operation_key)
            .order_by(
                schema.batch_items.c[naming.order_key_label],
                schema.batch_items.c[naming.item_key_label],
            )
        ).mappings()
    )
    items = tuple(
        submitted_item_from_row(dict(row), schema=schema) for row in rows
    )
    requested_count = len(items)
    inserted_count = sum(
        item.insert_status is BatchItemInsertStatus.INSERTED for item in items
    )
    already_present_count = sum(
        item.insert_status is BatchItemInsertStatus.ALREADY_PRESENT
        for item in items
    )
    enqueued_count = sum(
        item.enqueue_status is BatchItemEnqueueStatus.ENQUEUED
        for item in items
    )
    already_scheduled_count = sum(
        item.enqueue_status is BatchItemEnqueueStatus.WORKFLOW_ALREADY_PRESENT
        for item in items
    )
    failed_count = sum(
        item.enqueue_status is BatchItemEnqueueStatus.FAILED for item in items
    )
    status = operation_status_from_counts(
        requested_count=requested_count,
        enqueued_count=enqueued_count,
        already_scheduled_count=already_scheduled_count,
        failed_count=failed_count,
    )
    completed_at = (
        datetime.now(UTC)
        if status is not BatchOperationStatus.ENQUEUING
        else None
    )
    connection.execute(
        update(schema.batch_operations)
        .where(schema.batch_operations.c.operation_key == operation_key)
        .values(
            status=status.value,
            requested_count=requested_count,
            inserted_count=inserted_count,
            already_present_count=already_present_count,
            enqueued_count=enqueued_count,
            already_scheduled_count=already_scheduled_count,
            failed_count=failed_count,
            completed_at=completed_at,
        )
    )
    return BatchSubmitResult(
        operation_key=operation_key,
        group_key=group_key,
        requested_count=requested_count,
        inserted_count=inserted_count,
        already_present_count=already_present_count,
        enqueued_count=enqueued_count,
        already_scheduled_count=already_scheduled_count,
        failed_count=failed_count,
        items=items,
    )


def enqueue_metadata_for_item(item: SubmittedItem) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    if item.workflow_id is not None:
        metadata[WORKFLOW_ID_METADATA_KEY] = item.workflow_id
    metadata.update(item.workflow_metadata)
    return metadata


def submitted_item_from_row(
    row: dict[str, Any],
    *,
    schema: PlatformSchema,
) -> SubmittedItem:
    naming = schema.naming
    metadata = dict(row["enqueue_metadata"])
    workflow_id = metadata.pop(WORKFLOW_ID_METADATA_KEY, None)
    metadata.pop(ENQUEUE_CLAIM_ID_METADATA_KEY, None)
    metadata.pop(ENQUEUE_CLAIMED_AT_METADATA_KEY, None)
    failure = row["failure"]
    return SubmittedItem(
        item_id=row[naming.item_key_label],
        order_key=row[naming.order_key_label],
        insert_status=BatchItemInsertStatus(row["insert_status"]),
        enqueue_status=BatchItemEnqueueStatus(row["enqueue_status"]),
        workflow_id=workflow_id,
        workflow_metadata=metadata,
        failure=(
            EnqueueFailure.model_validate(failure)
            if failure is not None
            else None
        ),
    )


def batch_operation_row(
    record: BatchOperationRecord,
    *,
    schema: PlatformSchema,
) -> dict[str, Any]:
    return {
        "operation_key": record.operation_key,
        schema.naming.group_key_label: record.group_key,
        "status": record.status.value,
        "requested_count": record.requested_count,
        "inserted_count": record.inserted_count,
        "already_present_count": record.already_present_count,
        "enqueued_count": record.enqueued_count,
        "already_scheduled_count": record.already_scheduled_count,
        "failed_count": record.failed_count,
        "spec": record.spec,
        "metadata": record.metadata,
        "created_at": record.created_at,
        "completed_at": record.completed_at,
    }


def batch_operation_record_from_row(
    row: dict[str, Any],
    *,
    schema: PlatformSchema,
) -> BatchOperationRecord:
    return BatchOperationRecord(
        operation_key=row["operation_key"],
        group_key=row[schema.naming.group_key_label],
        status=BatchOperationStatus(row["status"]),
        requested_count=row["requested_count"],
        inserted_count=row["inserted_count"],
        already_present_count=row["already_present_count"],
        enqueued_count=row["enqueued_count"],
        already_scheduled_count=row["already_scheduled_count"],
        failed_count=row["failed_count"],
        spec=row["spec"],
        metadata=row["metadata"],
        created_at=row["created_at"],
        completed_at=row["completed_at"],
    )


def batch_item_insert_values(
    record: BatchItemRecord,
    *,
    schema: PlatformSchema,
) -> dict[str, Any]:
    return {
        "batch_submit_item_id": record.batch_submit_item_id,
        "operation_key": record.operation_key,
        "item_index": record.item_index,
        schema.naming.item_key_label: record.item_id,
        schema.naming.order_key_label: record.order_key,
        "insert_status": record.insert_status.value,
        "enqueue_status": record.enqueue_status.value,
        "enqueue_metadata": record.enqueue_metadata,
        "failure": (
            record.failure.model_dump(mode="json")
            if record.failure is not None
            else null()
        ),
        "created_at": record.created_at,
    }
