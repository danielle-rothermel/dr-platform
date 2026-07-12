"""Incremental kernel export and local DuckDB publication.

Only the platform kernel is owned here.  Application projections, DBOS data,
and remote destinations deliberately remain outside this module.
"""
# ruff: noqa: BLE001, E501, FBT001, PLR0911, PLR0912, PLR0913, PLR0915, S608, TRY300

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Literal, Protocol

import duckdb
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_serializer,
)
from sqlalchemy import Connection, Engine, Table, text
from sqlalchemy.sql import sqltypes

from dr_platform.db import PlatformSchema
from dr_platform.reconciliation_runtime import (
    LifecycleObservationReader,
    ReconcileOptions,
    ReconcileResult,
)
from dr_platform.submission import EXPORT_BARRIER_ADVISORY_KEY
from dr_platform.telemetry import validated_telemetry_attributes

if TYPE_CHECKING:
    from dr_platform.cancellation import WorkflowCanceller
    from dr_platform.enqueue_runtime import (
        PhysicalEnqueueAdapter,
        QueueLookup,
        WorkflowObserver,
    )
    from dr_platform.targets import TargetResolver

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
FullRebuildBuilder = Callable[
    [Connection, "ApplicationSnapshot"], Sequence[Mapping[str, Any]]
]
DbosTelemetryHook = Callable[[Mapping[str, str | int | float | bool]], None]


class ProjectionSpec(BaseModel):
    """The declared shape and integrity rules for one published member."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    member: NonEmptyStr
    columns: tuple[NonEmptyStr, ...]
    column_schema: tuple[ProjectionColumn, ...] = ()
    unique_key: tuple[NonEmptyStr, ...]
    references: tuple[tuple[NonEmptyStr, NonEmptyStr, NonEmptyStr], ...] = ()
    full_rebuild_builder: FullRebuildBuilder | None = None

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return the ordered names used by builders and destination DDL."""

        return self.columns


class ProjectionColumnType(StrEnum):
    """Closed destination types supported by application publication."""

    TEXT = "text"
    INTEGER = "integer"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    JSON = "json"


class ProjectionColumn(BaseModel):
    """One ordered, typed application projection column."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: NonEmptyStr
    type: ProjectionColumnType


class ApplicationSnapshot(BaseModel):
    """One repeatable-read application source cut shared by every builder."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_database: NonEmptyStr
    captured_at: datetime
    snapshot_seq: NonNegativeInt


@dataclass(frozen=True, slots=True, kw_only=True)
class ExportReconciliationDependencies:
    """Explicit collaborators for export-owned all-platform reconciliation."""

    resolver: TargetResolver
    queue_lookup: QueueLookup
    reader: LifecycleObservationReader
    dbos_engine: Engine
    options: ReconcileOptions = field(default_factory=ReconcileOptions)
    max_cycles: int = 10
    recovery_observer: WorkflowObserver | None = None
    enqueue_adapter: PhysicalEnqueueAdapter | None = None
    compensation_canceller: WorkflowCanceller | None = None

    def __post_init__(self) -> None:
        if self.options.operation_key is not None:
            raise ValueError(
                "export reconciliation must cover all platform operations"
            )
        if self.max_cycles <= 0:
            raise ValueError(
                "export reconciliation max_cycles must be positive"
            )


class IncompleteExportReconciliationError(RuntimeError):
    """The bounded driver could not establish a complete lifecycle pass."""


class _ReconciledSourceCut(BaseModel):
    """Library-owned source coordinate produced only after reconciliation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: NonEmptyStr
    database_server: NonEmptyStr
    reconciled_at: datetime


class LocalBundleIntegritySigner(Protocol):
    """Injected signer for local DuckDB bundle records."""

    key_id: str

    def sign(self, message: bytes) -> bytes: ...


class ExportOptions(BaseModel):
    """Options for one local kernel publication attempt."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, arbitrary_types_allowed=True
    )

    destination_path: NonEmptyStr
    destination_id: NonEmptyStr = "local-duckdb"
    bundle_key: NonEmptyStr = "platform-kernel"
    run_id: NonEmptyStr = Field(default_factory=lambda: uuid.uuid4().hex)
    lease_seconds: PositiveInt = 60
    full_rebuild: StrictBool = False
    projections: tuple[ProjectionSpec, ...] = ()
    source_change_sequence: NonEmptyStr = "platform_change_seq"
    # Every new local promotion is attested. Legacy rows are signed by the
    # explicit backfill API before readers begin enforcing this rule.
    integrity_signer: Any = None


class DestinationResult(BaseModel):
    """The independently recoverable result for one destination."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    destination_id: NonEmptyStr
    status: Literal[
        "PROMOTED", "IDEMPOTENT", "LEASE_HELD", "STALE_PROMOTION", "FAILED"
    ]
    bundle_id: StrictStr | None = None
    fencing_token: NonNegativeInt | None = None
    error: StrictStr | None = None


class LocalDestinationResult(DestinationResult):
    """Outcome for the local DuckDB bundle destination."""

    destination_kind: Literal["local_duckdb"] = "local_duckdb"


class PostgresDestinationResult(DestinationResult):
    """Outcome for a PostgresPublicationFence-backed destination."""

    destination_kind: Literal["postgres"] = "postgres"


class ExportResult(BaseModel):
    """Frozen source-cut facts and independent destination outcomes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_database: NonEmptyStr
    source_captured_at: datetime
    snapshot_seq: NonNegativeInt
    member_counts: Mapping[StrictStr, NonNegativeInt]
    member_checksums: Mapping[StrictStr, NonEmptyStr]
    destinations: tuple[
        LocalDestinationResult | PostgresDestinationResult, ...
    ]

    def model_post_init(self, __context: Any) -> None:
        """Deep-freeze the mapping-shaped source facts."""

        object.__setattr__(
            self, "member_counts", MappingProxyType(dict(self.member_counts))
        )
        object.__setattr__(
            self,
            "member_checksums",
            MappingProxyType(dict(self.member_checksums)),
        )

    @field_serializer("member_counts", "member_checksums")
    def serialize_member_facts(
        self, value: Mapping[str, Any]
    ) -> dict[str, Any]:
        return dict(value)


_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_STATE_TABLE = "__dr_platform_export_state"
_MEMBER_TABLE = "__dr_platform_export_members"
_BUNDLE_TABLE = "__dr_platform_export_bundles"
_PIN_TABLE = "__dr_platform_export_pins"
_SENSITIVE_KERNEL_COLUMNS = {
    "operations": frozenset({"spec", "metadata"}),
    "items": frozenset({"spec"}),
    "throttle_state": frozenset({"last_message", "metadata"}),
}


def capture_dbos_publication_telemetry(
    hook: DbosTelemetryHook | None,
    *,
    destination_id: str,
    disposition: str,
    snapshot_seq: int,
) -> None:
    """Emit only allowlisted DBOS publication facts to an optional hook."""

    if hook is None:
        return
    hook(
        validated_telemetry_attributes(
            {
                "platform.publication.destination_id": destination_id,
                "platform.publication.disposition": disposition,
                "platform.publication.snapshot_seq": snapshot_seq,
            }
        )
    )


def export(
    source: Engine,
    options: ExportOptions,
    *,
    reconciliation: ExportReconciliationDependencies,
    schema: PlatformSchema | None = None,
    remote_destinations: Sequence[Any] = (),
) -> ExportResult:
    """Capture and publish the seven platform kernel members to local DuckDB.

    The source barrier is intentionally released before destination work.  A
    failed destination therefore leaves its prior pointer and cursors intact,
    while a retry captures the same uncommitted delta again.
    """

    selected = schema or PlatformSchema()
    _validate_remote_destinations(remote_destinations)
    if options.projections and any(
        spec.full_rebuild_builder is not None for spec in options.projections
    ):
        return _export_application(
            source,
            options,
            remote_destinations,
            reconciliation_schema=selected,
            reconciliation=reconciliation,
        )
    members = _kernel_specs(selected, options.projections)
    database_path = Path(options.destination_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with _duckdb_lock(database_path):
        connection = duckdb.connect(str(database_path))
        try:
            _create_destination_tables(connection)
            lease = _acquire_lease(connection, options)
            if lease is None and not remote_destinations:
                return _empty_result(
                    selected, options, "", 0, {}, {}, "LEASE_HELD"
                )
            token, cursors = lease if lease is not None else (None, {})
            source_name = ""
            captured_at: datetime | None = None
            high_water = 0
            counts: dict[str, int] = {}
            checksums: dict[str, str] = {}
            try:
                reconciled_cut = _reconcile_for_export(
                    source, selected, reconciliation
                )
                source_name, captured_at, high_water, rows, remote_rows = (
                    _capture_source(
                        source,
                        selected,
                        members,
                        cursors,
                        options.full_rebuild if lease is not None else True,
                        reconciled_cut=reconciled_cut,
                        capture_complete_bundle=bool(remote_destinations),
                    )
                )
                counts, checksums = _validate_members(members, rows)
            except Exception as exc:
                if token is not None:
                    _release_lease(connection, options, token)
                return _empty_result(
                    selected,
                    options,
                    source_name,
                    high_water,
                    counts,
                    checksums,
                    "FAILED" if lease is not None else "LEASE_HELD",
                    token=token,
                    error=type(exc).__name__,
                    captured_at=captured_at,
                    failed_remotes=remote_destinations,
                )

            destinations: list[
                LocalDestinationResult | PostgresDestinationResult
            ]
            if token is None:
                destinations = [
                    LocalDestinationResult(
                        destination_id=options.destination_id,
                        status="LEASE_HELD",
                    )
                ]
            else:
                try:
                    status, bundle_id, counts, checksums = _stage_and_promote(
                        connection,
                        options,
                        token,
                        high_water,
                        members,
                        rows,
                    )
                    destinations = [
                        LocalDestinationResult(
                            destination_id=options.destination_id,
                            status=status,
                            bundle_id=bundle_id,
                            fencing_token=token,
                        )
                    ]
                except Exception as exc:
                    _release_lease(connection, options, token)
                    destinations = [
                        LocalDestinationResult(
                            destination_id=options.destination_id,
                            status="FAILED",
                            fencing_token=token,
                            error=type(exc).__name__,
                        )
                    ]

            destinations.extend(
                _publish_kernel_remotes(
                    remote_destinations,
                    options,
                    members,
                    remote_rows,
                    source_name,
                    captured_at,
                    high_water,
                    reconciled_cut,
                )
            )
            return ExportResult(
                source_database=source_name,
                source_captured_at=captured_at,
                snapshot_seq=high_water,
                member_counts=counts,
                member_checksums=checksums,
                destinations=tuple(destinations),
            )
        finally:
            connection.close()


def _reconcile_for_export(
    source: Engine,
    schema: PlatformSchema,
    dependencies: ExportReconciliationDependencies,
) -> _ReconciledSourceCut:
    """Drive bounded lifecycle work and return a library-owned source cut."""

    from dr_platform.reconciliation_runtime import reconcile  # noqa: PLC0415

    for _cycle in range(dependencies.max_cycles):
        result = reconcile(
            source,
            resolver=dependencies.resolver,
            queue_lookup=dependencies.queue_lookup,
            options=dependencies.options,
            schema=schema,
            reader=dependencies.reader,
            recovery_observer=dependencies.recovery_observer,
            enqueue_adapter=dependencies.enqueue_adapter,
            compensation_canceller=dependencies.compensation_canceller,
        )
        if (
            _reconciliation_work_count(result) < dependencies.options.page_size
            and result.recovered_call_started_count == 0
            and result.replacement_enqueue_count == 0
            and result.pending_enqueue_count == 0
        ):
            break
    else:
        raise IncompleteExportReconciliationError(
            "bounded all-platform reconciliation did not complete"
        )

    with dependencies.dbos_engine.connect() as connection:
        database_server, reconciled_at = connection.execute(
            text("SELECT current_database(), clock_timestamp()")
        ).one()
    return _ReconciledSourceCut(
        source_id=f"dbos:{database_server}",
        database_server=database_server,
        reconciled_at=reconciled_at,
    )


def _reconciliation_work_count(result: ReconcileResult) -> int:
    return (
        result.recovered_call_started_count
        + result.observed_count
        + result.replacement_enqueue_count
        + result.pending_enqueue_count
    )


def _validate_reconciled_capture(
    reconciled_cut: _ReconciledSourceCut,
    source_database: str,
    captured_at: datetime,
) -> None:
    """Consume the proof by binding it to the captured application cut."""

    from dr_platform.publication import (  # noqa: PLC0415 -- import cycle
        SourceCoordinate,
        require_compatible_snapshot,
    )

    require_compatible_snapshot(
        (
            SourceCoordinate(
                source_id=f"application:{source_database}",
                database_server=source_database,
                captured_at=captured_at,
            ),
            SourceCoordinate(
                source_id=reconciled_cut.source_id,
                database_server=reconciled_cut.database_server,
                captured_at=reconciled_cut.reconciled_at,
            ),
        )
    )


def _validate_remote_destinations(destinations: Sequence[Any]) -> None:
    from dr_platform.publication import (  # noqa: PLC0415 -- import cycle
        PostgresPublicationFence,
    )

    if any(
        not isinstance(destination, PostgresPublicationFence)
        for destination in destinations
    ):
        raise TypeError("remote destinations must be PostgresPublicationFence")


def _publish_kernel_remotes(
    destinations: Sequence[Any],
    options: ExportOptions,
    members: tuple[tuple[ProjectionSpec, Table], ...],
    rows: Mapping[str, list[dict[str, Any]]],
    source_name: str,
    captured_at: datetime,
    snapshot_seq: int,
    reconciled_cut: _ReconciledSourceCut,
) -> tuple[PostgresDestinationResult, ...]:
    """Physically stage and fenced-promote the complete canonical kernel."""

    from dr_platform.publication import (  # noqa: PLC0415 -- import cycle
        RemoteBundleManifest,
        RemoteBundleMember,
        SourceCoordinate,
    )

    coordinates = (
        SourceCoordinate(
            source_id=f"application:{source_name}",
            database_server=source_name,
            captured_at=captured_at,
            snapshot_seq=snapshot_seq,
        ),
        SourceCoordinate(
            source_id=reconciled_cut.source_id,
            database_server=reconciled_cut.database_server,
            captured_at=reconciled_cut.reconciled_at,
        ),
    )
    results: list[PostgresDestinationResult] = []
    for fence in destinations:
        token: int | None = None
        try:
            fence.ensure_schema()
            lease = fence.acquire_lease(
                bundle_key=options.bundle_key,
                run_id=options.run_id,
                lease_seconds=options.lease_seconds,
            )
            if lease.disposition == "LEASE_HELD":
                results.append(
                    PostgresDestinationResult(
                        destination_id=fence.destination_id,
                        status="LEASE_HELD",
                    )
                )
                continue
            assert lease.fencing_token is not None
            token = lease.fencing_token
            normalized_rows = {
                spec.member: [
                    {
                        column: _storage_value(
                            row[column], source_table.c[column].type
                        )
                        for column in spec.columns
                    }
                    for row in rows[spec.member]
                ]
                for spec, source_table in members
            }
            counts, checksums = _validate_members(members, normalized_rows)

            def stage(
                connection: Connection,
                *,
                destination: Any = fence,
                fencing_token: int = lease.fencing_token,
                staged_rows: Mapping[
                    str, list[dict[str, Any]]
                ] = normalized_rows,
                staged_counts: Mapping[str, int] = counts,
                staged_checksums: Mapping[str, str] = checksums,
            ) -> RemoteBundleManifest:
                staged: list[RemoteBundleMember] = []
                for spec, source_table in members:
                    table_name = destination.stage_table_name(
                        member=spec.member,
                        run_id=options.run_id,
                        fencing_token=fencing_token,
                        snapshot_seq=snapshot_seq,
                    )
                    connection.execute(
                        text(
                            f"CREATE TABLE {_pg_identifier(table_name)} ("
                            + ", ".join(
                                f"{_pg_identifier(column)} "
                                f"{_remote_kernel_sql_type(source_table.c[column].type)}"
                                for column in spec.columns
                            )
                            + ")"
                        )
                    )
                    member_rows = staged_rows[spec.member]
                    if member_rows:
                        placeholders = ", ".join(
                            f":{column}" for column in spec.columns
                        )
                        connection.execute(
                            text(
                                f"INSERT INTO {_pg_identifier(table_name)} "
                                f"({', '.join(_pg_identifier(column) for column in spec.columns)}) "
                                f"VALUES ({placeholders})"
                            ),
                            member_rows,
                        )
                    staged.append(
                        RemoteBundleMember(
                            member=spec.member,
                            schema_name=(
                                "main"
                                if destination.kind == "motherduck"
                                else "public"
                            ),
                            table_name=table_name,
                            key_columns=spec.unique_key,
                            row_count=staged_counts[spec.member],
                            checksum=staged_checksums[spec.member],
                        )
                    )
                return RemoteBundleManifest(
                    members=tuple(staged),
                    source_families=("application", "dbos"),
                )

            promoted = fence.promote(
                bundle_key=options.bundle_key,
                run_id=options.run_id,
                fencing_token=token,
                snapshot_seq=snapshot_seq,
                bundle_id=(f"kernel_{snapshot_seq}_{token}_{options.run_id}"),
                cursors={spec.member: snapshot_seq for spec, _ in members},
                source_coordinates=coordinates,
                source_families=("application", "dbos"),
                stage=stage,
            )
            results.append(
                PostgresDestinationResult(
                    destination_id=fence.destination_id,
                    status=promoted.disposition,
                    bundle_id=promoted.bundle_id,
                    fencing_token=token,
                )
            )
        except Exception as exc:
            results.append(
                PostgresDestinationResult(
                    destination_id=fence.destination_id,
                    status="FAILED",
                    fencing_token=token,
                    error=type(exc).__name__,
                )
            )
    return tuple(results)


def _kernel_specs(
    schema: PlatformSchema,
    declared: tuple[ProjectionSpec, ...],
) -> tuple[tuple[ProjectionSpec, Table], ...]:
    tables = (
        schema.operations,
        schema.items,
        schema.item_attempts,
        schema.enqueue_claims,
        schema.next_attempt_requests,
        schema.enqueue_compensations,
        schema.enqueue_compensation_hazards,
        schema.throttle_state,
        schema.missing_reobservations,
    )
    supplied = {item.member: item for item in declared}
    unknown = set(supplied).difference(table.name for table in tables)
    if unknown:
        raise ValueError(
            f"unknown kernel projection members: {sorted(unknown)}"
        )
    result: list[tuple[ProjectionSpec, Table]] = []
    for table in tables:
        suffix = table.name.removeprefix(f"{schema.prefix}_")
        excluded = _SENSITIVE_KERNEL_COLUMNS.get(suffix, frozenset())
        columns = tuple(
            column.name
            for column in table.columns
            if column.name not in excluded
        )
        key = tuple(column.name for column in table.primary_key.columns)
        default = ProjectionSpec(
            member=table.name, columns=columns, unique_key=key
        )
        spec = supplied.get(table.name, default)
        if spec.column_names != columns or spec.unique_key != key:
            raise ValueError(
                f"{table.name} must declare its canonical schema and key"
            )
        result.append((spec, table))
    return tuple(result)


@contextmanager
def _duckdb_lock(database_path: Path):
    lock_path = database_path.with_name(f"{database_path.name}.lock")
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _capture_source(
    source: Engine,
    schema: PlatformSchema,
    members: tuple[tuple[ProjectionSpec, Table], ...],
    cursors: Mapping[str, int],
    full_rebuild: bool,
    *,
    reconciled_cut: _ReconciledSourceCut,
    capture_complete_bundle: bool,
) -> tuple[
    str,
    datetime,
    int,
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
]:
    # A session-level exclusive lock must precede the repeatable-read BEGIN.
    with source.connect() as connection:
        connection.execute(
            text("SELECT pg_advisory_lock(:lock_key)"),
            {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
        )
        # SQLAlchemy autobegins for the lock SELECT.  Commit that transaction:
        # the session lock remains held while the repeatable-read source
        # transaction starts cleanly afterward.
        connection.commit()
        try:
            connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                source_name = connection.execute(
                    text("SELECT current_database()")
                ).scalar_one()
                captured_at = connection.execute(
                    text("SELECT clock_timestamp()")
                ).scalar_one()
                _validate_reconciled_capture(
                    reconciled_cut, source_name, captured_at
                )
                sequence_name = f"{schema.prefix}_change_seq"
                # Allocate the cut while writers are excluded.  All preceding
                # committed mutations are below H and every later mutation is
                # above H, including when no rows changed since the last run.
                high_water = int(
                    connection.execute(
                        text(
                            f"SELECT nextval('{_pg_identifier(sequence_name)}'::regclass)"
                        )
                    ).scalar_one()
                )
                output: dict[str, list[dict[str, Any]]] = {}
                complete: dict[str, list[dict[str, Any]]] = {}
                for spec, table in members:
                    statement = (
                        table.select()
                        .with_only_columns(
                            *(table.c[column] for column in spec.column_names)
                        )
                        .where(table.c.change_seq <= high_water)
                    )
                    captured_rows = [
                        dict(row)
                        for row in connection.execute(statement).mappings()
                    ]
                    output[spec.member] = (
                        captured_rows
                        if full_rebuild
                        else [
                            row
                            for row in captured_rows
                            if row["change_seq"] > cursors.get(spec.member, 0)
                        ]
                    )
                    complete[spec.member] = (
                        captured_rows if capture_complete_bundle else []
                    )
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
            )
            connection.commit()
    return source_name, captured_at, high_water, output, complete


def _validate_members(
    members: tuple[tuple[ProjectionSpec, Table], ...],
    rows: Mapping[str, list[dict[str, Any]]],
) -> tuple[dict[str, int], dict[str, str]]:
    counts: dict[str, int] = {}
    checksums: dict[str, str] = {}
    all_rows = rows
    for spec, _ in members:
        member_rows = all_rows[spec.member]
        keys = [
            tuple(row[column] for column in spec.unique_key)
            for row in member_rows
        ]
        if len(keys) != len({_canonical(value) for value in keys}):
            raise ValueError(f"{spec.member} contains duplicate declared keys")
        for local_column, target_member, target_column in spec.references:
            target_values = {
                row[target_column] for row in all_rows.get(target_member, [])
            }
            for row in member_rows:
                value = row[local_column]
                if value is not None and value not in target_values:
                    # Incremental members may legitimately reference old rows.
                    # The promoted bundle's reference check happens after merge.
                    continue
        ordered = sorted(
            member_rows,
            key=lambda row: _canonical(
                tuple(row[key] for key in spec.unique_key)
            ),
        )
        checksums[spec.member] = hashlib.sha256(
            _canonical(ordered).encode()
        ).hexdigest()
        counts[spec.member] = len(member_rows)
    return counts, checksums


def _create_destination_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_STATE_TABLE} ("
        "destination_id VARCHAR, bundle_key VARCHAR, committed_snapshot_seq BIGINT, "
        "cursors_json VARCHAR, checksums_json VARCHAR, bundle_id VARCHAR, "
        "owner VARCHAR, lease_expires_at BIGINT, fencing_token BIGINT, "
        "updated_at BIGINT, PRIMARY KEY(destination_id, bundle_key))"
    )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_MEMBER_TABLE} ("
        "destination_id VARCHAR, bundle_key VARCHAR, member VARCHAR, table_name VARCHAR, "
        "PRIMARY KEY(destination_id, bundle_key, member))"
    )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_BUNDLE_TABLE} ("
        "destination_id VARCHAR, bundle_key VARCHAR, bundle_id VARCHAR, "
        "snapshot_seq BIGINT, manifest_json VARCHAR, created_at BIGINT, "
        "PRIMARY KEY(destination_id, bundle_key, bundle_id))"
    )
    for column in (
        "integrity_version VARCHAR",
        "integrity_key_id VARCHAR",
        "integrity_payload_json VARCHAR",
        "integrity_signature VARCHAR",
        "physical_digest_algorithm VARCHAR",
    ):
        connection.execute(
            f"ALTER TABLE {_BUNDLE_TABLE} ADD COLUMN IF NOT EXISTS {column}"
        )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_PIN_TABLE} ("
        "destination_id VARCHAR, bundle_key VARCHAR, pin_id VARCHAR, "
        "bundle_id VARCHAR, expires_at BIGINT, created_at BIGINT, "
        "PRIMARY KEY(destination_id, bundle_key, pin_id))"
    )


def _acquire_lease(
    connection: duckdb.DuckDBPyConnection, options: ExportOptions
) -> tuple[int, dict[str, int]] | None:
    connection.execute("BEGIN TRANSACTION")
    try:
        row = connection.execute(
            f"SELECT * FROM {_STATE_TABLE} WHERE destination_id = ? AND bundle_key = ?",
            [options.destination_id, options.bundle_key],
        ).fetchone()
        columns = [item[0] for item in connection.description]
        now = _duckdb_scalar(connection, "SELECT epoch_ms(now())")
        if row is not None:
            state = dict(zip(columns, row, strict=True))
            if (
                state["owner"] not in (None, options.run_id)
                and state["lease_expires_at"] > now
            ):
                connection.execute("ROLLBACK")
                return None
            token = int(state["fencing_token"] or 0) + 1
            cursors = json.loads(state["cursors_json"] or "{}")
            connection.execute(
                f"UPDATE {_STATE_TABLE} SET owner = ?, fencing_token = ?, "
                "lease_expires_at = epoch_ms(now()) + ?, "
                "updated_at = epoch_ms(now()) "
                "WHERE destination_id = ? AND bundle_key = ?",
                [
                    options.run_id,
                    token,
                    options.lease_seconds * 1000,
                    options.destination_id,
                    options.bundle_key,
                ],
            )
        else:
            token, cursors = 1, {}
            connection.execute(
                f"INSERT INTO {_STATE_TABLE} VALUES (?, ?, 0, '{{}}', '{{}}', NULL, ?, "
                "epoch_ms(now()) + ?, ?, epoch_ms(now()))",
                [
                    options.destination_id,
                    options.bundle_key,
                    options.run_id,
                    options.lease_seconds * 1000,
                    token,
                ],
            )
        connection.execute("COMMIT")
        return token, {str(key): int(value) for key, value in cursors.items()}
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _local_signed_integrity(
    connection: duckdb.DuckDBPyConnection,
    options: ExportOptions,
    *,
    bundle_id: str,
    snapshot_seq: int,
    specs: Sequence[ProjectionSpec],
    candidate_tables: Mapping[str, str],
    checksums: Mapping[str, str],
) -> tuple[str, str, str, str, str]:
    """Sign native DuckDB facts without moving the mutable current pointer."""

    if options.integrity_signer is None:
        raise ValueError(
            "local promotion requires an injected integrity signer"
        )
    # Avoid an import cycle: publication owns the shared wire payload while
    # export owns DuckDB's physical tables.
    from dr_platform.publication import (  # noqa: PLC0415
        RemoteBundleMember,
        SignedBundleIntegrityPayload,
        canonical_integrity_payload,
        integrity_message,
    )

    algorithm = "duckdb-json-length-framed-sha256-v1"
    members = []
    for spec in specs:
        table = _quoted(candidate_tables[spec.member])
        ordering = ", ".join(_quoted(key) for key in spec.unique_key)
        row = connection.execute(
            "SELECT COUNT(*) AS row_count, "
            "sha256(COALESCE(string_agg(length(to_json(t)::VARCHAR)::VARCHAR "
            f"|| ':' || to_json(t)::VARCHAR, '' ORDER BY {ordering}), '')) "
            f"AS physical_digest FROM {table} t"
        ).fetchone()
        if row is None or int(row[0]) < 0 or not isinstance(row[1], str):
            raise ValueError("DuckDB physical digest validation failed")
        members.append(
            RemoteBundleMember(
                member=spec.member,
                schema_name="main",
                table_name=candidate_tables[spec.member],
                key_columns=spec.unique_key,
                row_count=int(row[0]),
                checksum=checksums[spec.member],
                physical_digest=row[1],
                column_schema=spec.column_schema,
            )
        )
    payload = SignedBundleIntegrityPayload(
        destination_id=options.destination_id,
        bundle_key=options.bundle_key,
        bundle_id=bundle_id,
        snapshot_seq=snapshot_seq,
        integrity_version="dr-platform.bundle-integrity.v1",
        source_coordinates_sha256=hashlib.sha256(b"[]").hexdigest(),
        physical_digest_algorithm=algorithm,
        members=tuple(members),
    )
    signature = options.integrity_signer.sign(integrity_message(payload))
    return (
        payload.integrity_version,
        options.integrity_signer.key_id,
        canonical_integrity_payload(payload).decode(),
        base64.b64encode(signature).decode(),
        algorithm,
    )


def _stage_and_promote(
    connection: duckdb.DuckDBPyConnection,
    options: ExportOptions,
    token: int,
    snapshot_seq: int,
    members: tuple[tuple[ProjectionSpec, Table], ...],
    rows: Mapping[str, list[dict[str, Any]]],
) -> tuple[
    Literal["PROMOTED", "IDEMPOTENT", "STALE_PROMOTION"],
    str | None,
    dict[str, int],
    dict[str, str],
]:
    connection.execute("BEGIN TRANSACTION")
    try:
        state = connection.execute(
            f"SELECT committed_snapshot_seq, checksums_json, bundle_id, owner, fencing_token, lease_expires_at "
            f"FROM {_STATE_TABLE} WHERE destination_id = ? AND bundle_key = ?",
            [options.destination_id, options.bundle_key],
        ).fetchone()
        if state is None or state[3] != options.run_id or state[4] != token:
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        now = _duckdb_scalar(connection, "SELECT epoch_ms(now())")
        if state[5] <= now:
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        committed = int(state[0] or 0)
        old_checksums = json.loads(state[1] or "{}")
        if state[2] is not None and snapshot_seq < committed:
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        bundle_id = f"kernel_{snapshot_seq}_{token}_{options.run_id}"
        candidate_tables: dict[str, str] = {}
        for spec, source_table in members:
            if not _renew_lease(connection, options, token):
                connection.execute("ROLLBACK")
                return "STALE_PROMOTION", None, {}, {}
            stage = _quoted(f"__dr_platform_stage_{token}_{spec.member}")
            target = _quoted(f"__dr_platform_bundle_{bundle_id}_{spec.member}")
            connection.execute(
                f"CREATE TABLE {stage} ("
                + ", ".join(
                    f"{_quoted(column)} {_duckdb_type(source_table.c[column].type)}"
                    for column in spec.columns
                )
                + ")"
            )
            values = [
                tuple(
                    _storage_value(row[column], source_table.c[column].type)
                    for column in spec.columns
                )
                for row in rows[spec.member]
            ]
            if values:
                placeholders = ", ".join("?" for _ in spec.columns)
                connection.executemany(
                    f"INSERT INTO {stage} VALUES ({placeholders})", values
                )
            old = connection.execute(
                f"SELECT table_name FROM {_MEMBER_TABLE} WHERE destination_id = ? AND bundle_key = ? AND member = ?",
                [options.destination_id, options.bundle_key, spec.member],
            ).fetchone()
            if options.full_rebuild or old is None:
                connection.execute(
                    f"CREATE TABLE {target} AS SELECT * FROM {stage}"
                )
            else:
                old_table = _quoted(old[0])
                connection.execute(
                    f"CREATE TABLE {target} AS SELECT * FROM {old_table}"
                )
                conditions = " AND ".join(
                    f"t.{_quoted(key)} = s.{_quoted(key)}"
                    for key in spec.unique_key
                )
                connection.execute(
                    f"DELETE FROM {target} t USING {stage} s WHERE {conditions}"
                )
                connection.execute(
                    f"INSERT INTO {target} SELECT * FROM {stage}"
                )
            connection.execute(f"DROP TABLE {stage}")
            candidate_tables[spec.member] = target.strip('"')

        candidate_counts: dict[str, int] = {}
        candidate_checksums: dict[str, str] = {}
        for spec, _ in members:
            target = _quoted(candidate_tables[spec.member])
            _validate_destination_member(
                connection, target, spec, candidate_tables
            )
            candidate_rows = [
                dict(zip(spec.columns, row, strict=True))
                for row in connection.execute(
                    f"SELECT {', '.join(f'CAST({_quoted(column)} AS VARCHAR)' for column in spec.columns)} "
                    f"FROM {target} ORDER BY {', '.join(_quoted(key) for key in spec.unique_key)}"
                ).fetchall()
            ]
            candidate_counts[spec.member] = len(candidate_rows)
            candidate_checksums[spec.member] = hashlib.sha256(
                _canonical(candidate_rows).encode()
            ).hexdigest()

        if state[2] is not None and snapshot_seq == committed:
            if old_checksums != candidate_checksums:
                connection.execute("ROLLBACK")
                return (
                    "STALE_PROMOTION",
                    None,
                    candidate_counts,
                    candidate_checksums,
                )
            if not _renew_lease(connection, options, token):
                connection.execute("ROLLBACK")
                return (
                    "STALE_PROMOTION",
                    None,
                    candidate_counts,
                    candidate_checksums,
                )
            connection.execute("ROLLBACK")
            _release_lease(connection, options, token)
            return (
                "IDEMPOTENT",
                str(state[2]),
                candidate_counts,
                candidate_checksums,
            )

        if not _renew_lease(connection, options, token):
            connection.execute("ROLLBACK")
            return (
                "STALE_PROMOTION",
                None,
                candidate_counts,
                candidate_checksums,
            )

        for spec, _ in members:
            connection.execute(
                f"INSERT OR REPLACE INTO {_MEMBER_TABLE} VALUES (?, ?, ?, ?)",
                [
                    options.destination_id,
                    options.bundle_key,
                    spec.member,
                    candidate_tables[spec.member],
                ],
            )
        manifest = {
            spec.member: {
                "table": candidate_tables[spec.member],
                "columns": list(spec.columns),
                "unique_key": list(spec.unique_key),
                "checksum": candidate_checksums[spec.member],
            }
            for spec, _ in members
        }
        integrity = _local_signed_integrity(
            connection,
            options,
            bundle_id=bundle_id,
            snapshot_seq=snapshot_seq,
            specs=tuple(spec for spec, _ in members),
            candidate_tables=candidate_tables,
            checksums=candidate_checksums,
        )
        connection.execute(
            f"INSERT INTO {_BUNDLE_TABLE} (destination_id, bundle_key, bundle_id, snapshot_seq, manifest_json, created_at, integrity_version, integrity_key_id, integrity_payload_json, integrity_signature, physical_digest_algorithm) VALUES (?, ?, ?, ?, ?, epoch_ms(now()), ?, ?, ?, ?, ?)",
            [
                options.destination_id,
                options.bundle_key,
                bundle_id,
                snapshot_seq,
                _canonical(manifest),
                *integrity,
            ],
        )
        cursors = {spec.member: snapshot_seq for spec, _ in members}
        promoted = connection.execute(
            f"UPDATE {_STATE_TABLE} SET committed_snapshot_seq = ?, cursors_json = ?, checksums_json = ?, bundle_id = ?, "
            "owner = NULL, lease_expires_at = NULL, updated_at = epoch_ms(now()) "
            "WHERE destination_id = ? AND bundle_key = ? AND owner = ? AND fencing_token = ? "
            "AND lease_expires_at > epoch_ms(now()) RETURNING fencing_token",
            [
                snapshot_seq,
                _canonical(cursors),
                _canonical(candidate_checksums),
                bundle_id,
                options.destination_id,
                options.bundle_key,
                options.run_id,
                token,
            ],
        ).fetchone()
        if promoted is None:
            connection.execute("ROLLBACK")
            return (
                "STALE_PROMOTION",
                None,
                candidate_counts,
                candidate_checksums,
            )
        connection.execute("COMMIT")
        return "PROMOTED", bundle_id, candidate_counts, candidate_checksums
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _stage_and_promote_application(
    connection: duckdb.DuckDBPyConnection,
    options: ExportOptions,
    token: int,
    snapshot_seq: int,
    specs: tuple[ProjectionSpec, ...],
    rows: Mapping[str, list[dict[str, Any]]],
) -> tuple[
    Literal["PROMOTED", "IDEMPOTENT", "STALE_PROMOTION"],
    str | None,
    dict[str, int],
    dict[str, str],
]:
    """Build and fence the complete application bundle in one transaction."""

    connection.execute("BEGIN TRANSACTION")
    try:
        state = connection.execute(
            f"SELECT committed_snapshot_seq, checksums_json, bundle_id, owner, fencing_token, lease_expires_at "
            f"FROM {_STATE_TABLE} WHERE destination_id = ? AND bundle_key = ?",
            [options.destination_id, options.bundle_key],
        ).fetchone()
        if state is None or state[3] != options.run_id or state[4] != token:
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        if state[5] <= _duckdb_scalar(connection, "SELECT epoch_ms(now())"):
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        committed = int(state[0] or 0)
        if state[2] is not None and snapshot_seq < committed:
            connection.execute("ROLLBACK")
            return "STALE_PROMOTION", None, {}, {}
        candidate_tables: dict[str, str] = {}
        bundle_id = f"application_{snapshot_seq}_{token}_{options.run_id}"
        for spec in specs:
            if not _renew_lease(connection, options, token):
                connection.execute("ROLLBACK")
                return "STALE_PROMOTION", None, {}, {}
            stage = _quoted(f"__dr_platform_stage_{token}_{spec.member}")
            target = _quoted(f"__dr_platform_bundle_{bundle_id}_{spec.member}")
            connection.execute(
                f"CREATE TABLE {stage} ("
                + ", ".join(
                    f"{_quoted(column.name)} {_application_sql_type(column.type, remote=False)}"
                    for column in spec.column_schema
                )
                + ")"
            )
            values = [
                tuple(
                    _application_storage_value(row[column.name], column.type)
                    for column in spec.column_schema
                )
                for row in rows[spec.member]
            ]
            if values:
                connection.executemany(
                    f"INSERT INTO {stage} VALUES ({', '.join('?' for _ in spec.column_schema)})",
                    values,
                )
            connection.execute(
                f"CREATE TABLE {target} AS SELECT * FROM {stage}"
            )
            connection.execute(f"DROP TABLE {stage}")
            candidate_tables[spec.member] = target.strip('"')
        candidate_counts, candidate_checksums = _application_destination_facts(
            connection, specs, candidate_tables
        )
        old_checksums = json.loads(state[1] or "{}")
        if state[2] is not None and snapshot_seq == committed:
            connection.execute("ROLLBACK")
            _release_lease(connection, options, token)
            if old_checksums == candidate_checksums:
                return (
                    "IDEMPOTENT",
                    str(state[2]),
                    candidate_counts,
                    candidate_checksums,
                )
            return (
                "STALE_PROMOTION",
                None,
                candidate_counts,
                candidate_checksums,
            )
        for spec in specs:
            connection.execute(
                f"INSERT OR REPLACE INTO {_MEMBER_TABLE} VALUES (?, ?, ?, ?)",
                [
                    options.destination_id,
                    options.bundle_key,
                    spec.member,
                    candidate_tables[spec.member],
                ],
            )
        manifest = {
            spec.member: {
                "table": candidate_tables[spec.member],
                "column_schema": [
                    column.model_dump(mode="json")
                    for column in spec.column_schema
                ],
                "unique_key": list(spec.unique_key),
                "checksum": candidate_checksums[spec.member],
            }
            for spec in specs
        }
        integrity = _local_signed_integrity(
            connection,
            options,
            bundle_id=bundle_id,
            snapshot_seq=snapshot_seq,
            specs=specs,
            candidate_tables=candidate_tables,
            checksums=candidate_checksums,
        )
        connection.execute(
            f"INSERT INTO {_BUNDLE_TABLE} (destination_id, bundle_key, bundle_id, snapshot_seq, manifest_json, created_at, integrity_version, integrity_key_id, integrity_payload_json, integrity_signature, physical_digest_algorithm) VALUES (?, ?, ?, ?, ?, epoch_ms(now()), ?, ?, ?, ?, ?)",
            [
                options.destination_id,
                options.bundle_key,
                bundle_id,
                snapshot_seq,
                _canonical(manifest),
                *integrity,
            ],
        )
        promoted = connection.execute(
            f"UPDATE {_STATE_TABLE} SET committed_snapshot_seq = ?, cursors_json = ?, checksums_json = ?, bundle_id = ?, owner = NULL, lease_expires_at = NULL, updated_at = epoch_ms(now()) "
            "WHERE destination_id = ? AND bundle_key = ? AND owner = ? AND fencing_token = ? "
            "AND lease_expires_at > epoch_ms(now()) RETURNING fencing_token",
            [
                snapshot_seq,
                _canonical({spec.member: snapshot_seq for spec in specs}),
                _canonical(candidate_checksums),
                bundle_id,
                options.destination_id,
                options.bundle_key,
                options.run_id,
                token,
            ],
        ).fetchone()
        if promoted is None:
            connection.execute("ROLLBACK")
            return (
                "STALE_PROMOTION",
                None,
                candidate_counts,
                candidate_checksums,
            )
        connection.execute("COMMIT")
        return "PROMOTED", bundle_id, candidate_counts, candidate_checksums
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _application_destination_facts(
    connection: duckdb.DuckDBPyConnection,
    specs: tuple[ProjectionSpec, ...],
    candidate_tables: Mapping[str, str],
) -> tuple[dict[str, int], dict[str, str]]:
    rows: dict[str, list[dict[str, Any]]] = {}
    for spec in specs:
        rows[spec.member] = _application_destination_rows(
            connection,
            member=spec.member,
            table_name=candidate_tables[spec.member],
            column_schema=spec.column_schema,
        )
    return _validate_application_rows(specs, rows)


def _application_destination_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    member: str,
    table_name: str,
    column_schema: tuple[ProjectionColumn, ...],
) -> list[dict[str, Any]]:
    """Read one DuckDB application member with its declared typed semantics."""

    table = _quoted(table_name)
    actual_schema = tuple(
        (item[0], item[1])
        for item in connection.execute(f"DESCRIBE {table}").fetchall()
    )
    expected_schema = tuple(
        (column.name, _duckdb_application_describe_type(column.type))
        for column in column_schema
    )
    if actual_schema != expected_schema:
        raise ValueError(f"{member} failed destination schema validation")
    return [
        {
            column.name: _normalize_application_value(
                value, column.type, destination=True
            )
            for column, value in zip(column_schema, values, strict=True)
        }
        for values in connection.execute(
            "SELECT "
            + ", ".join(
                (
                    f"CAST({_quoted(column.name)} AS VARCHAR)"
                    if column.type
                    in {
                        ProjectionColumnType.TIMESTAMP,
                        ProjectionColumnType.JSON,
                    }
                    else _quoted(column.name)
                )
                for column in column_schema
            )
            + f" FROM {table}"
        ).fetchall()
    ]


def _validate_destination_member(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    spec: ProjectionSpec,
    candidate_tables: Mapping[str, str],
) -> None:
    duplicate = connection.execute(
        f"SELECT 1 FROM {table} GROUP BY {', '.join(_quoted(key) for key in spec.unique_key)} HAVING count(*) > 1 LIMIT 1"
    ).fetchone()
    if duplicate is not None:
        raise ValueError(
            f"{spec.member} failed destination uniqueness validation"
        )
    for local, target_member, target_column in spec.references:
        target = candidate_tables.get(target_member)
        if target is None:
            raise ValueError(
                f"{spec.member} references unavailable member {target_member}"
            )
        missing = connection.execute(
            f"SELECT 1 FROM {table} child LEFT JOIN {_quoted(target)} parent "
            f"ON child.{_quoted(local)} = parent.{_quoted(target_column)} "
            f"WHERE child.{_quoted(local)} IS NOT NULL AND parent.{_quoted(target_column)} IS NULL LIMIT 1"
        ).fetchone()
        if missing is not None:
            raise ValueError(f"{spec.member} failed reference validation")


def _release_lease(
    connection: duckdb.DuckDBPyConnection, options: ExportOptions, token: int
) -> None:
    connection.execute(
        f"UPDATE {_STATE_TABLE} SET owner = NULL, lease_expires_at = NULL, updated_at = epoch_ms(now()) "
        "WHERE destination_id = ? AND bundle_key = ? AND owner = ? AND fencing_token = ?",
        [options.destination_id, options.bundle_key, options.run_id, token],
    )


def _renew_lease(
    connection: duckdb.DuckDBPyConnection,
    options: ExportOptions,
    token: int,
) -> bool:
    renewed = connection.execute(
        f"UPDATE {_STATE_TABLE} SET lease_expires_at = epoch_ms(now()) + ?, "
        "updated_at = epoch_ms(now()) "
        "WHERE destination_id = ? AND bundle_key = ? AND owner = ? "
        "AND fencing_token = ? AND lease_expires_at > epoch_ms(now()) "
        "RETURNING fencing_token",
        [
            options.lease_seconds * 1000,
            options.destination_id,
            options.bundle_key,
            options.run_id,
            token,
        ],
    ).fetchone()
    return renewed == (token,)


def _empty_result(
    schema: PlatformSchema,
    options: ExportOptions,
    source_name: str,
    snapshot_seq: int,
    counts: dict[str, int],
    checksums: dict[str, str],
    status: Literal["LEASE_HELD", "FAILED"],
    *,
    token: int | None = None,
    error: str | None = None,
    captured_at: datetime | None = None,
    failed_remotes: Sequence[Any] = (),
) -> ExportResult:
    destinations: list[LocalDestinationResult | PostgresDestinationResult] = [
        LocalDestinationResult(
            destination_id=options.destination_id,
            status=status,
            fencing_token=token,
            error=error,
        )
    ]
    destinations.extend(
        PostgresDestinationResult(
            destination_id=destination.destination_id,
            status="FAILED",
            error=error,
        )
        for destination in failed_remotes
    )
    return ExportResult(
        source_database=source_name or schema.prefix,
        source_captured_at=captured_at or datetime.now().astimezone(),
        snapshot_seq=snapshot_seq,
        member_counts=counts,
        member_checksums=checksums,
        destinations=tuple(destinations),
    )


def _export_application(
    source: Engine,
    options: ExportOptions,
    remote_destinations: Sequence[Any],
    *,
    reconciliation_schema: PlatformSchema,
    reconciliation: ExportReconciliationDependencies,
) -> ExportResult:
    """Publish one application-owned full-rebuild bundle to local DuckDB.

    Builders execute together in the application repeatable-read transaction.
    The same destination Lease, fencing token, and reader-visible pointer used
    by kernel publication then promote every validated member atomically.
    """

    specs = _application_specs(options)
    database_path = Path(options.destination_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with _duckdb_lock(database_path):
        connection = duckdb.connect(str(database_path))
        try:
            _create_destination_tables(connection)
            lease = _acquire_lease(connection, options)
            if lease is None:
                return _empty_result(
                    PlatformSchema(), options, "", 0, {}, {}, "LEASE_HELD"
                )
            token, _ = lease
            source_name = ""
            captured_at: datetime | None = None
            snapshot_seq = 0
            counts: dict[str, int] = {}
            checksums: dict[str, str] = {}
            try:
                reconciled_cut = _reconcile_for_export(
                    source, reconciliation_schema, reconciliation
                )
                source_name, captured_at, snapshot_seq, rows = (
                    _capture_application_source(
                        source,
                        specs,
                        options,
                        reconciled_cut=reconciled_cut,
                    )
                )
                rows = _normalize_application_rows(specs, rows)
                counts, checksums = _validate_application_rows(specs, rows)
                status, bundle_id, counts, checksums = (
                    _stage_and_promote_application(
                        connection, options, token, snapshot_seq, specs, rows
                    )
                )
                destinations: list[
                    LocalDestinationResult | PostgresDestinationResult
                ] = [
                    LocalDestinationResult(
                        destination_id=options.destination_id,
                        status=status,
                        bundle_id=bundle_id,
                        fencing_token=token,
                    )
                ]
                destinations.extend(
                    _publish_application_remotes(
                        remote_destinations,
                        options,
                        specs,
                        rows,
                        source_name,
                        captured_at,
                        snapshot_seq,
                        counts,
                        checksums,
                    )
                )
                return ExportResult(
                    source_database=source_name,
                    source_captured_at=captured_at,
                    snapshot_seq=snapshot_seq,
                    member_counts=counts,
                    member_checksums=checksums,
                    destinations=tuple(destinations),
                )
            except Exception as exc:
                _release_lease(connection, options, token)
                return _empty_result(
                    PlatformSchema(),
                    options,
                    source_name,
                    snapshot_seq,
                    counts,
                    checksums,
                    "FAILED",
                    token=token,
                    error=type(exc).__name__,
                    captured_at=captured_at,
                    failed_remotes=remote_destinations,
                )
        finally:
            connection.close()


def _publish_application_remotes(
    destinations: Sequence[Any],
    options: ExportOptions,
    specs: tuple[ProjectionSpec, ...],
    rows: Mapping[str, list[dict[str, Any]]],
    source_name: str,
    captured_at: datetime,
    snapshot_seq: int,
    counts: Mapping[str, int],
    checksums: Mapping[str, str],
) -> tuple[PostgresDestinationResult, ...]:
    """Promote an already captured application bundle to fenced destinations.

    This adapter deliberately knows only the public publication protocol.  It
    does not name an application, inspect application tables, or re-run a
    builder: every destination receives the same bounded source coordinates.
    """

    from dr_platform.publication import (  # noqa: PLC0415 -- import cycle
        PostgresPublicationFence,
        RemoteBundleManifest,
        RemoteBundleMember,
        SourceCoordinate,
    )

    results: list[PostgresDestinationResult] = []
    coordinate = SourceCoordinate(
        source_id=f"application:{source_name}",
        database_server=source_name,
        captured_at=captured_at,
        snapshot_seq=snapshot_seq,
    )
    for fence in destinations:
        if not isinstance(fence, PostgresPublicationFence):
            raise TypeError(
                "remote destinations must be PostgresPublicationFence"
            )
        try:
            fence.ensure_schema()
            lease = fence.acquire_lease(
                bundle_key=options.bundle_key,
                run_id=options.run_id,
                lease_seconds=options.lease_seconds,
            )
            if lease.disposition == "LEASE_HELD":
                results.append(
                    PostgresDestinationResult(
                        destination_id=fence.destination_id,
                        status="LEASE_HELD",
                    )
                )
                continue
            assert lease.fencing_token is not None

            def stage(
                connection: Connection,
                *,
                destination: Any = fence,
                token: int = lease.fencing_token,
            ) -> Any:
                members: list[RemoteBundleMember] = []
                for spec in specs:
                    table_name = destination.stage_table_name(
                        member=spec.member,
                        run_id=options.run_id,
                        fencing_token=token,
                        snapshot_seq=snapshot_seq,
                    )
                    connection.execute(
                        text(
                            f"CREATE TABLE {_pg_identifier(table_name)} ("
                            + ", ".join(
                                f"{_pg_identifier(column.name)} "
                                f"{_application_sql_type(column.type, remote=True, motherduck=destination.kind == 'motherduck')}"
                                for column in spec.column_schema
                            )
                            + ")"
                        )
                    )
                    if rows[spec.member]:
                        placeholders = ", ".join(
                            f":{column}" for column in spec.column_names
                        )
                        connection.execute(
                            text(
                                f"INSERT INTO {_pg_identifier(table_name)} "
                                f"({', '.join(_pg_identifier(column) for column in spec.column_names)}) "
                                f"VALUES ({placeholders})"
                            ),
                            [
                                {
                                    column.name: _application_storage_value(
                                        row[column.name], column.type
                                    )
                                    for column in spec.column_schema
                                }
                                for row in rows[spec.member]
                            ],
                        )
                    members.append(
                        RemoteBundleMember(
                            member=spec.member,
                            schema_name=(
                                "main"
                                if destination.kind == "motherduck"
                                else "public"
                            ),
                            table_name=table_name,
                            key_columns=spec.unique_key,
                            row_count=counts[spec.member],
                            checksum=checksums[spec.member],
                            column_schema=spec.column_schema,
                        )
                    )
                return RemoteBundleManifest(
                    members=tuple(members), source_families=("application",)
                )

            promoted = fence.promote(
                bundle_key=options.bundle_key,
                run_id=options.run_id,
                fencing_token=lease.fencing_token,
                snapshot_seq=snapshot_seq,
                bundle_id=(
                    f"application_{snapshot_seq}_{lease.fencing_token}_{options.run_id}"
                ),
                cursors={spec.member: snapshot_seq for spec in specs},
                source_coordinates=(coordinate,),
                source_families=("application",),
                stage=stage,
            )
            results.append(
                PostgresDestinationResult(
                    destination_id=fence.destination_id,
                    status=promoted.disposition,
                    bundle_id=promoted.bundle_id,
                    fencing_token=lease.fencing_token,
                )
            )
        except Exception as exc:
            results.append(
                PostgresDestinationResult(
                    destination_id=fence.destination_id,
                    status="FAILED",
                    error=type(exc).__name__,
                )
            )
    return tuple(results)


def _application_specs(
    options: ExportOptions,
) -> tuple[ProjectionSpec, ...]:
    if not options.full_rebuild:
        raise ValueError("application projections require full_rebuild=True")
    if not options.projections:
        raise ValueError("application publication requires projections")
    names = [spec.member for spec in options.projections]
    if len(names) != len(set(names)):
        raise ValueError("application projection members must be unique")
    for spec in options.projections:
        if spec.full_rebuild_builder is None:
            raise ValueError("every application member requires a builder")
        names = spec.column_names
        if len(names) != len(set(names)):
            raise ValueError(f"{spec.member} column names must be unique")
        if tuple(column.name for column in spec.column_schema) != names:
            raise ValueError(
                f"{spec.member} schema must type every column in order"
            )
        if not spec.unique_key or not set(spec.unique_key).issubset(names):
            raise ValueError(f"{spec.member} has an invalid unique key")
        for local, target, target_column in spec.references:
            target_spec = next(
                (
                    item
                    for item in options.projections
                    if item.member == target
                ),
                None,
            )
            if (
                local not in names
                or target_spec is None
                or target_column not in target_spec.column_names
            ):
                raise ValueError(
                    f"{spec.member} has an invalid member reference"
                )
    return options.projections


def _capture_application_source(
    source: Engine,
    specs: tuple[ProjectionSpec, ...],
    options: ExportOptions,
    *,
    reconciled_cut: _ReconciledSourceCut,
) -> tuple[str, datetime, int, dict[str, list[dict[str, Any]]]]:
    sequence = _pg_identifier(options.source_change_sequence)
    with source.connect() as connection:
        connection.execute(
            text("SELECT pg_advisory_lock(:lock_key)"),
            {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
        )
        connection.commit()
        try:
            connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                source_name = str(
                    connection.execute(
                        text("SELECT current_database()")
                    ).scalar_one()
                )
                captured_at = connection.execute(
                    text("SELECT clock_timestamp()")
                ).scalar_one()
                _validate_reconciled_capture(
                    reconciled_cut, source_name, captured_at
                )
                snapshot_seq = int(
                    connection.execute(
                        text(f"SELECT nextval('{sequence}'::regclass)")
                    ).scalar_one()
                )
                snapshot = ApplicationSnapshot(
                    source_database=source_name,
                    captured_at=captured_at,
                    snapshot_seq=snapshot_seq,
                )
                rows = {
                    spec.member: [
                        dict(row)
                        for row in spec.full_rebuild_builder(
                            connection, snapshot
                        )
                    ]
                    for spec in specs
                    if spec.full_rebuild_builder is not None
                }
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
            )
            connection.commit()
    return source_name, captured_at, snapshot_seq, rows


def _validate_application_rows(
    specs: tuple[ProjectionSpec, ...], rows: Mapping[str, list[dict[str, Any]]]
) -> tuple[dict[str, int], dict[str, str]]:
    if set(rows) != {spec.member for spec in specs}:
        raise ValueError(
            "application builders returned an incomplete inventory"
        )
    counts: dict[str, int] = {}
    checksums: dict[str, str] = {}
    for spec in specs:
        member_rows = rows[spec.member]
        if any(set(row) != set(spec.column_names) for row in member_rows):
            raise ValueError(
                f"{spec.member} does not match its declared schema"
            )
        keys = [
            tuple(row[key] for key in spec.unique_key) for row in member_rows
        ]
        if len(keys) != len({_canonical(key) for key in keys}):
            raise ValueError(f"{spec.member} contains duplicate declared keys")
        counts[spec.member] = len(member_rows)
        checksums[spec.member] = hashlib.sha256(
            _canonical(
                sorted(
                    member_rows,
                    key=lambda row: _canonical(
                        tuple(row[key] for key in spec.unique_key)
                    ),
                )
            ).encode()
        ).hexdigest()
    for spec in specs:
        for local, target, target_column in spec.references:
            target_values = {row[target_column] for row in rows[target]}
            if any(
                row[local] is not None and row[local] not in target_values
                for row in rows[spec.member]
            ):
                raise ValueError(f"{spec.member} failed reference validation")
    return counts, checksums


def _quoted(identifier: str) -> str:
    if not _IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"unsafe identifier: {identifier}")
    return f'"{identifier}"'


def _pg_identifier(identifier: str) -> str:
    """Quote a library-derived Postgres identifier after the same validation."""

    return _quoted(identifier)


def _duckdb_type(source_type: sqltypes.TypeEngine[Any]) -> str:
    if isinstance(source_type, sqltypes.DateTime):
        return "TIMESTAMPTZ" if source_type.timezone else "TIMESTAMP"
    if isinstance(source_type, sqltypes.Boolean):
        return "BOOLEAN"
    if isinstance(source_type, sqltypes.Integer):
        return "BIGINT"
    if isinstance(source_type, sqltypes.Float):
        return "DOUBLE"
    if isinstance(source_type, sqltypes.Numeric):
        return "DECIMAL"
    return "VARCHAR"


def _remote_kernel_sql_type(source_type: sqltypes.TypeEngine[Any]) -> str:
    if isinstance(source_type, sqltypes.DateTime):
        return "TIMESTAMPTZ" if source_type.timezone else "TIMESTAMP"
    if isinstance(source_type, sqltypes.Boolean):
        return "BOOLEAN"
    if isinstance(source_type, sqltypes.Integer):
        return "BIGINT"
    if isinstance(source_type, sqltypes.Float):
        return "DOUBLE PRECISION"
    if isinstance(source_type, sqltypes.Numeric):
        return "NUMERIC"
    return "TEXT"


def _storage_value(value: Any, source_type: sqltypes.TypeEngine[Any]) -> Any:
    if value is None:
        return None
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(source_type, sqltypes.JSON):
        return _canonical(value)
    if isinstance(source_type, sqltypes.DateTime) and isinstance(
        value, datetime
    ):
        # Bind an ISO value so DuckDB performs the typed cast without its
        # optional pytz conversion dependency.
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _application_sql_type(
    column_type: ProjectionColumnType,
    *,
    remote: bool,
    motherduck: bool = False,
) -> str:
    if column_type is ProjectionColumnType.TEXT:
        return "TEXT" if remote else "VARCHAR"
    if column_type is ProjectionColumnType.INTEGER:
        return "BIGINT"
    if column_type is ProjectionColumnType.NUMERIC:
        return "DOUBLE PRECISION" if remote else "DOUBLE"
    if column_type is ProjectionColumnType.BOOLEAN:
        return "BOOLEAN"
    if column_type is ProjectionColumnType.TIMESTAMP:
        return "TIMESTAMPTZ"
    if column_type is ProjectionColumnType.JSON:
        return "JSON" if motherduck or not remote else "JSONB"
    raise ValueError(f"unsupported projection column type: {column_type}")


def _duckdb_application_describe_type(
    column_type: ProjectionColumnType,
) -> str:
    if column_type is ProjectionColumnType.TIMESTAMP:
        return "TIMESTAMP WITH TIME ZONE"
    return _application_sql_type(column_type, remote=False)


def _application_storage_value(
    value: Any, column_type: ProjectionColumnType
) -> Any:
    if value is None:
        return None
    if column_type is ProjectionColumnType.JSON:
        return _canonical(value)
    if column_type is ProjectionColumnType.TIMESTAMP:
        return value.isoformat()
    return value


def _normalize_application_rows(
    specs: tuple[ProjectionSpec, ...],
    rows: Mapping[str, list[dict[str, Any]]],
    *,
    destination: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    if set(rows) != {spec.member for spec in specs}:
        return {
            member: list(member_rows) for member, member_rows in rows.items()
        }
    normalized: dict[str, list[dict[str, Any]]] = {}
    for spec in specs:
        normalized[spec.member] = [
            {
                column.name: _normalize_application_value(
                    row.get(column.name), column.type, destination=destination
                )
                for column in spec.column_schema
            }
            if set(row) == set(spec.column_names)
            else dict(row)
            for row in rows.get(spec.member, [])
        ]
    return normalized


def _normalize_application_value(
    value: Any,
    column_type: ProjectionColumnType,
    *,
    destination: bool,
) -> Any:
    if value is None:
        return None
    if column_type is ProjectionColumnType.TEXT:
        if not isinstance(value, str):
            raise ValueError("text projection values must be strings")
        return value
    if column_type is ProjectionColumnType.INTEGER:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("integer projection values must be integers")
        return value
    if column_type is ProjectionColumnType.NUMERIC:
        if isinstance(value, bool) or not isinstance(
            value, (int, float, Decimal)
        ):
            raise ValueError("numeric projection values must be numbers")
        numeric = float(value)
        if not isfinite(numeric):
            raise ValueError("numeric projection values must be finite")
        return numeric
    if column_type is ProjectionColumnType.BOOLEAN:
        if not isinstance(value, bool):
            raise ValueError("boolean projection values must be booleans")
        return value
    if column_type is ProjectionColumnType.TIMESTAMP:
        if destination and isinstance(value, str):
            value = datetime.fromisoformat(value)
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError(
                "timestamp projection values must be timezone-aware datetimes"
            )
        return value.astimezone(UTC)
    if column_type is ProjectionColumnType.JSON:
        if destination and isinstance(value, str):
            value = json.loads(value)
        if not isinstance(value, (dict, list)):
            raise ValueError(
                "json projection values must be objects or arrays"
            )
        try:
            return json.loads(
                json.dumps(
                    value,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "json projection values must be structured JSON"
            ) from exc
    raise ValueError(f"unsupported projection column type: {column_type}")


def _duckdb_scalar(connection: duckdb.DuckDBPyConnection, query: str) -> Any:
    row = connection.execute(query).fetchone()
    if row is None:
        raise RuntimeError("DuckDB scalar query returned no row")
    return row[0]


def _canonical(value: Any) -> str:
    def default(item: Any) -> str:
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        return str(item)

    return json.dumps(
        value, default=default, sort_keys=True, separators=(",", ":")
    )
