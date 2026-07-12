"""Incremental kernel export and local DuckDB publication.

Only the platform kernel is owned here.  Application projections, DBOS data,
and remote destinations deliberately remain outside this module.
"""
# ruff: noqa: BLE001, E501, FBT001, PLR0911, PLR0912, PLR0913, PLR0915, S608, TRY300

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Literal

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
from dr_platform.submission import EXPORT_BARRIER_ADVISORY_KEY
from dr_platform.telemetry import validated_telemetry_attributes

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
    unique_key: tuple[NonEmptyStr, ...]
    references: tuple[tuple[NonEmptyStr, NonEmptyStr, NonEmptyStr], ...] = ()
    full_rebuild_builder: FullRebuildBuilder | None = None


class ApplicationSnapshot(BaseModel):
    """One repeatable-read application source cut shared by every builder."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_database: NonEmptyStr
    captured_at: datetime
    snapshot_seq: NonNegativeInt


class ExportOptions(BaseModel):
    """Options for one local kernel publication attempt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    destination_path: NonEmptyStr
    destination_id: NonEmptyStr = "local-duckdb"
    bundle_key: NonEmptyStr = "platform-kernel"
    run_id: NonEmptyStr = Field(default_factory=lambda: uuid.uuid4().hex)
    lease_seconds: PositiveInt = 60
    full_rebuild: StrictBool = False
    projections: tuple[ProjectionSpec, ...] = ()


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
    schema: PlatformSchema | None = None,
) -> ExportResult:
    """Capture and publish the seven platform kernel members to local DuckDB.

    The source barrier is intentionally released before destination work.  A
    failed destination therefore leaves its prior pointer and cursors intact,
    while a retry captures the same uncommitted delta again.
    """

    if options.projections and any(
        spec.full_rebuild_builder is not None for spec in options.projections
    ):
        return _export_application(source, options)
    selected = schema or PlatformSchema()
    members = _kernel_specs(selected, options.projections)
    database_path = Path(options.destination_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with _duckdb_lock(database_path):
        connection = duckdb.connect(str(database_path))
        try:
            _create_destination_tables(connection)
            lease = _acquire_lease(connection, options)
            if lease is None:
                return _empty_result(
                    selected, options, "", 0, {}, {}, "LEASE_HELD"
                )
            token, cursors = lease
            source_name = ""
            captured_at: datetime | None = None
            high_water = 0
            counts: dict[str, int] = {}
            checksums: dict[str, str] = {}
            try:
                source_name, captured_at, high_water, rows = _capture_source(
                    source, selected, members, cursors, options.full_rebuild
                )
                counts, checksums = _validate_members(members, rows)
                status, bundle_id, counts, checksums = _stage_and_promote(
                    connection,
                    options,
                    token,
                    high_water,
                    members,
                    rows,
                )
                return ExportResult(
                    source_database=source_name,
                    source_captured_at=captured_at,
                    snapshot_seq=high_water,
                    member_counts=counts,
                    member_checksums=checksums,
                    destinations=(
                        LocalDestinationResult(
                            destination_id=options.destination_id,
                            status=status,
                            bundle_id=bundle_id,
                            fencing_token=token,
                        ),
                    ),
                )
            except Exception as exc:  # destination errors are structured
                _release_lease(connection, options, token)
                return _empty_result(
                    selected,
                    options,
                    source_name,
                    high_water,
                    counts,
                    checksums,
                    "FAILED",
                    token=token,
                    error=type(exc).__name__,
                    captured_at=captured_at,
                )
        finally:
            connection.close()


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
        if spec.columns != columns or spec.unique_key != key:
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
) -> tuple[str, datetime, int, dict[str, list[dict[str, Any]]]]:
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
                for spec, table in members:
                    statement = (
                        table.select()
                        .with_only_columns(
                            *(table.c[column] for column in spec.columns)
                        )
                        .where(table.c.change_seq <= high_water)
                    )
                    if not full_rebuild:
                        statement = statement.where(
                            table.c.change_seq > cursors.get(spec.member, 0)
                        )
                    output[spec.member] = [
                        dict(row)
                        for row in connection.execute(statement).mappings()
                    ]
        finally:
            connection.execute(
                text("SELECT pg_advisory_unlock(:lock_key)"),
                {"lock_key": EXPORT_BARRIER_ADVISORY_KEY},
            )
            connection.commit()
    return source_name, captured_at, high_water, output


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
        connection.execute(
            f"INSERT INTO {_BUNDLE_TABLE} VALUES (?, ?, ?, ?, ?, epoch_ms(now()))",
            [
                options.destination_id,
                options.bundle_key,
                bundle_id,
                snapshot_seq,
                _canonical(manifest),
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
                    f"{_quoted(column)} VARCHAR" for column in spec.columns
                )
                + ")"
            )
            values = [
                tuple(
                    _application_storage_value(row[column])
                    for column in spec.columns
                )
                for row in rows[spec.member]
            ]
            if values:
                connection.executemany(
                    f"INSERT INTO {stage} VALUES ({', '.join('?' for _ in spec.columns)})",
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
                "columns": list(spec.columns),
                "unique_key": list(spec.unique_key),
                "checksum": candidate_checksums[spec.member],
            }
            for spec in specs
        }
        connection.execute(
            f"INSERT INTO {_BUNDLE_TABLE} VALUES (?, ?, ?, ?, ?, epoch_ms(now()))",
            [
                options.destination_id,
                options.bundle_key,
                bundle_id,
                snapshot_seq,
                _canonical(manifest),
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
        table = _quoted(candidate_tables[spec.member])
        actual_columns = tuple(
            item[0]
            for item in connection.execute(f"DESCRIBE {table}").fetchall()
        )
        if actual_columns != spec.columns:
            raise ValueError(
                f"{spec.member} failed destination schema validation"
            )
        rows[spec.member] = [
            dict(zip(spec.columns, row, strict=True))
            for row in connection.execute(
                f"SELECT {', '.join(_quoted(column) for column in spec.columns)} FROM {table}"
            ).fetchall()
        ]
    return _validate_application_rows(specs, rows)


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
) -> ExportResult:
    return ExportResult(
        source_database=source_name or schema.prefix,
        source_captured_at=captured_at or datetime.now().astimezone(),
        snapshot_seq=snapshot_seq,
        member_counts=counts,
        member_checksums=checksums,
        destinations=(
            LocalDestinationResult(
                destination_id=options.destination_id,
                status=status,
                fencing_token=token,
                error=error,
            ),
        ),
    )


def _export_application(
    source: Engine, options: ExportOptions
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
                source_name, captured_at, snapshot_seq, rows = (
                    _capture_application_source(source, specs)
                )
                counts, checksums = _validate_application_rows(specs, rows)
                status, bundle_id, counts, checksums = (
                    _stage_and_promote_application(
                        connection, options, token, snapshot_seq, specs, rows
                    )
                )
                return ExportResult(
                    source_database=source_name,
                    source_captured_at=captured_at,
                    snapshot_seq=snapshot_seq,
                    member_counts=counts,
                    member_checksums=checksums,
                    destinations=(
                        LocalDestinationResult(
                            destination_id=options.destination_id,
                            status=status,
                            bundle_id=bundle_id,
                            fencing_token=token,
                        ),
                    ),
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
                )
        finally:
            connection.close()


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
        if not spec.unique_key or not set(spec.unique_key).issubset(
            spec.columns
        ):
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
                local not in spec.columns
                or target_spec is None
                or target_column not in target_spec.columns
            ):
                raise ValueError(
                    f"{spec.member} has an invalid member reference"
                )
    return options.projections


def _capture_application_source(
    source: Engine, specs: tuple[ProjectionSpec, ...]
) -> tuple[str, datetime, int, dict[str, list[dict[str, Any]]]]:
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
                snapshot_seq = int(
                    connection.execute(
                        text("SELECT nextval('platform_change_seq'::regclass)")
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
        if any(set(row) != set(spec.columns) for row in member_rows):
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


def _application_storage_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return _canonical(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


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
