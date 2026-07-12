"""Remote publication fences, source compatibility, and local bundle pins."""
# ruff: noqa: E501, PLR0913, S608, TC003

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import duckdb
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    model_validator,
)
from sqlalchemy import Connection, Engine, text

from dr_platform.export import (
    _BUNDLE_TABLE,
    _PIN_TABLE,
    _STATE_TABLE,
    _canonical,
    _create_destination_tables,
    _duckdb_lock,
    _quoted,
)

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_MINIMUM_COMPATIBLE_SOURCES = 2


class SourceCoordinate(BaseModel):
    """A truthful coordinate for one independently captured source."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: NonEmptyStr
    captured_at: datetime
    snapshot_seq: NonNegativeInt | None = None

    @model_validator(mode="after")
    def require_database_timestamp_shape(self) -> SourceCoordinate:
        if self.captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        return self


def capture_source_coordinate(
    engine: Engine,
    *,
    source_id: str,
    snapshot_seq: int | None = None,
) -> SourceCoordinate:
    """Capture a database-server timestamp and non-secret source identity."""

    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT current_database(), clock_timestamp()")
        ).one()
    return SourceCoordinate(
        source_id=f"{source_id}:{row[0]}",
        captured_at=row[1],
        snapshot_seq=snapshot_seq,
    )


class SnapshotCompatibility(BaseModel):
    """The measured relationship between independent source cuts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: Literal["COMPATIBLE", "INCOMPATIBLE", "MISSING_COORDINATE"]
    observed_skew_ms: float | None
    max_capture_skew_ms: NonNegativeInt
    source_ids: tuple[NonEmptyStr, ...]


class IncompatibleSnapshotError(RuntimeError):
    """Raised when a combined read cannot prove bounded compatibility."""

    def __init__(self, result: SnapshotCompatibility) -> None:
        super().__init__(result.disposition)
        self.result = result


def check_snapshot_compatibility(
    coordinates: tuple[SourceCoordinate, ...],
    *,
    max_capture_skew_ms: int = 100,
) -> SnapshotCompatibility:
    """Compare database-server timestamps; sequence equality is irrelevant."""

    if max_capture_skew_ms < 0:
        raise ValueError("max_capture_skew_ms must be non-negative")
    source_ids = tuple(coordinate.source_id for coordinate in coordinates)
    if len(coordinates) < _MINIMUM_COMPATIBLE_SOURCES or len(
        set(source_ids)
    ) != len(source_ids):
        return SnapshotCompatibility(
            disposition="MISSING_COORDINATE",
            observed_skew_ms=None,
            max_capture_skew_ms=max_capture_skew_ms,
            source_ids=source_ids,
        )
    moments = [
        coordinate.captured_at.timestamp() for coordinate in coordinates
    ]
    skew_ms = (max(moments) - min(moments)) * 1000
    return SnapshotCompatibility(
        disposition=(
            "COMPATIBLE" if skew_ms <= max_capture_skew_ms else "INCOMPATIBLE"
        ),
        observed_skew_ms=skew_ms,
        max_capture_skew_ms=max_capture_skew_ms,
        source_ids=source_ids,
    )


def require_compatible_snapshot(
    coordinates: tuple[SourceCoordinate, ...],
    *,
    max_capture_skew_ms: int = 100,
) -> SnapshotCompatibility:
    result = check_snapshot_compatibility(
        coordinates, max_capture_skew_ms=max_capture_skew_ms
    )
    if result.disposition != "COMPATIBLE":
        raise IncompatibleSnapshotError(result)
    return result


class RemoteLeaseResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: Literal["ACQUIRED", "LEASE_HELD"]
    fencing_token: NonNegativeInt | None = None
    cursors: Mapping[StrictStr, NonNegativeInt] = Field(default_factory=dict)


class RemotePromotionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: Literal["PROMOTED", "IDEMPOTENT", "STALE_PROMOTION"]
    bundle_id: StrictStr | None = None
    snapshot_seq: NonNegativeInt | None = None


class RemoteBundleMember(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    member: NonEmptyStr
    schema_name: NonEmptyStr = "public"
    table_name: NonEmptyStr
    key_columns: tuple[NonEmptyStr, ...]
    row_count: NonNegativeInt
    checksum: NonEmptyStr

    @model_validator(mode="after")
    def validate_identifiers(self) -> RemoteBundleMember:
        if (
            _IDENTIFIER.fullmatch(self.schema_name) is None
            or _IDENTIFIER.fullmatch(self.table_name) is None
            or not self.key_columns
            or any(
                _IDENTIFIER.fullmatch(column) is None
                for column in self.key_columns
            )
        ):
            raise ValueError("remote member identifiers must be safe")
        return self


class RemoteBundleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    members: tuple[RemoteBundleMember, ...]

    @model_validator(mode="after")
    def validate_members(self) -> RemoteBundleManifest:
        names = [member.member for member in self.members]
        if not names or len(names) != len(set(names)):
            raise ValueError(
                "remote bundle members must be non-empty and unique"
            )
        return self

    @property
    def checksums(self) -> dict[str, str]:
        return {member.member: member.checksum for member in self.members}


class _StalePromotionError(RuntimeError):
    pass


@dataclass(frozen=True)
class PostgresPublicationFence:
    """Destination-local Lease/fence state for MotherDuck or Neon Postgres."""

    engine: Engine
    destination_id: str
    table_name: str = "dr_platform_publication_state"
    kind: Literal["motherduck", "neon"] = "neon"

    def __post_init__(self) -> None:
        if not self.destination_id:
            raise ValueError("destination_id must not be empty")
        if _IDENTIFIER.fullmatch(self.table_name) is None:
            raise ValueError("table_name must be a safe SQL identifier")

    @property
    def _table(self) -> str:
        return f'"{self.table_name}"'

    @property
    def _bundles_table(self) -> str:
        return f'"{self.table_name}_bundles"'

    @property
    def _pins_table(self) -> str:
        return f'"{self.table_name}_pins"'

    @property
    def _now(self) -> str:
        # MotherDuck's Postgres endpoint implements CURRENT_TIMESTAMP but not
        # PostgreSQL's clock_timestamp(). Neon supports the stronger clock.
        return (
            "CURRENT_TIMESTAMP"
            if self.kind == "motherduck"
            else "clock_timestamp()"
        )

    def ensure_schema(self) -> None:
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {self._table} ("
                    "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, "
                    "committed_snapshot_seq BIGINT NOT NULL DEFAULT 0, "
                    "cursors_json TEXT NOT NULL DEFAULT '{}', "
                    "checksums_json TEXT NOT NULL DEFAULT '{}', bundle_id TEXT, "
                    "owner TEXT, lease_expires_at TIMESTAMPTZ, "
                    "fencing_token BIGINT NOT NULL DEFAULT 0, "
                    f"updated_at TIMESTAMPTZ NOT NULL DEFAULT {self._now}, "
                    "PRIMARY KEY(destination_id, bundle_key))"
                )
            )
            connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {self._bundles_table} ("
                    "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, "
                    "bundle_id TEXT NOT NULL, snapshot_seq BIGINT NOT NULL, "
                    "source_coordinates_json TEXT NOT NULL, "
                    "manifest_json TEXT NOT NULL, status TEXT NOT NULL, "
                    "owner TEXT NOT NULL, fencing_token BIGINT NOT NULL, "
                    f"created_at TIMESTAMPTZ NOT NULL DEFAULT {self._now}, "
                    "PRIMARY KEY(destination_id, bundle_key, bundle_id))"
                )
            )
            connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {self._pins_table} ("
                    "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, "
                    "pin_id TEXT NOT NULL, bundle_id TEXT NOT NULL, "
                    "expires_at TIMESTAMPTZ NOT NULL, "
                    f"created_at TIMESTAMPTZ NOT NULL DEFAULT {self._now}, "
                    "PRIMARY KEY(destination_id, bundle_key, pin_id))"
                )
            )

    def acquire_lease(
        self, *, bundle_key: str, run_id: str, lease_seconds: int
    ) -> RemoteLeaseResult:
        if not bundle_key or not run_id or lease_seconds <= 0:
            raise ValueError(
                "bundle_key, run_id, and positive TTL are required"
            )
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"INSERT INTO {self._table} (destination_id, bundle_key) "
                    "VALUES (CAST(:destination AS TEXT), CAST(:bundle AS TEXT)) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"destination": self.destination_id, "bundle": bundle_key},
            )
            row = (
                connection.execute(
                    text(
                        f"UPDATE {self._table} SET owner = CAST(:run AS TEXT), "
                        "fencing_token = fencing_token + 1, "
                        f"lease_expires_at = {self._now} + "
                        "(CAST(:ttl AS BIGINT) * INTERVAL '1 second'), "
                        f"updated_at = {self._now} "
                        "WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT) "
                        "AND (owner = CAST(:run AS TEXT) "
                        "OR lease_expires_at IS NULL "
                        f"OR lease_expires_at <= {self._now}) "
                        "RETURNING fencing_token, cursors_json"
                    ),
                    {
                        "destination": self.destination_id,
                        "bundle": bundle_key,
                        "run": run_id,
                        "ttl": lease_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return RemoteLeaseResult(disposition="LEASE_HELD")
        return RemoteLeaseResult(
            disposition="ACQUIRED",
            fencing_token=int(row["fencing_token"]),
            cursors={
                str(key): int(value)
                for key, value in json.loads(row["cursors_json"]).items()
            },
        )

    def renew_lease(
        self,
        *,
        bundle_key: str,
        run_id: str,
        fencing_token: int,
        lease_seconds: int,
    ) -> bool:
        with self.engine.begin() as connection:
            row = connection.execute(
                text(
                    f"UPDATE {self._table} SET lease_expires_at = {self._now} + "
                    "(CAST(:ttl AS BIGINT) * INTERVAL '1 second'), "
                    f"updated_at = {self._now} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND owner = CAST(:run AS TEXT) "
                    "AND fencing_token = CAST(:token AS BIGINT) "
                    f"AND lease_expires_at > {self._now} "
                    "RETURNING fencing_token"
                ),
                {
                    "destination": self.destination_id,
                    "bundle": bundle_key,
                    "run": run_id,
                    "token": fencing_token,
                    "ttl": lease_seconds,
                },
            ).one_or_none()
        return row == (fencing_token,)

    def promote(
        self,
        *,
        bundle_key: str,
        run_id: str,
        fencing_token: int,
        snapshot_seq: int,
        bundle_id: str,
        cursors: Mapping[str, int],
        source_coordinates: tuple[SourceCoordinate, ...],
        stage: Callable[[Connection], RemoteBundleManifest],
    ) -> RemotePromotionResult:
        """Stage, validate, persist, and fenced-promote one remote bundle."""

        if not source_coordinates:
            raise ValueError("source_coordinates must not be empty")
        if self.kind == "motherduck":
            # CURRENT_TIMESTAMP is transaction-stable on MotherDuck.  Commit
            # staging first so the final fence transaction gets a fresh
            # database timestamp and cannot outlive its Lease invisibly.
            with self.engine.begin() as stage_connection:
                manifest = stage(stage_connection)
                self._validate_remote_manifest(stage_connection, manifest)
                self._record_remote_stage(
                    stage_connection,
                    bundle_key=bundle_key,
                    bundle_id=bundle_id,
                    snapshot_seq=snapshot_seq,
                    run_id=run_id,
                    fencing_token=fencing_token,
                    source_coordinates=source_coordinates,
                    manifest=manifest,
                )
            try:
                with self.engine.begin() as connection:
                    return self._promote_remote_manifest(
                        connection,
                        bundle_key=bundle_key,
                        run_id=run_id,
                        fencing_token=fencing_token,
                        snapshot_seq=snapshot_seq,
                        bundle_id=bundle_id,
                        cursors=cursors,
                        source_coordinates=source_coordinates,
                        manifest=manifest,
                    )
            except _StalePromotionError:
                return RemotePromotionResult(disposition="STALE_PROMOTION")

        try:
            with self.engine.begin() as connection:
                manifest = stage(connection)
                self._validate_remote_manifest(connection, manifest)
                self._record_remote_stage(
                    connection,
                    bundle_key=bundle_key,
                    bundle_id=bundle_id,
                    snapshot_seq=snapshot_seq,
                    run_id=run_id,
                    fencing_token=fencing_token,
                    source_coordinates=source_coordinates,
                    manifest=manifest,
                )
                return self._promote_remote_manifest(
                    connection,
                    bundle_key=bundle_key,
                    run_id=run_id,
                    fencing_token=fencing_token,
                    snapshot_seq=snapshot_seq,
                    bundle_id=bundle_id,
                    cursors=cursors,
                    source_coordinates=source_coordinates,
                    manifest=manifest,
                )
        except _StalePromotionError:
            return RemotePromotionResult(disposition="STALE_PROMOTION")

    def _record_remote_stage(
        self,
        connection: Connection,
        *,
        bundle_key: str,
        bundle_id: str,
        snapshot_seq: int,
        run_id: str,
        fencing_token: int,
        source_coordinates: tuple[SourceCoordinate, ...],
        manifest: RemoteBundleManifest,
    ) -> None:
        connection.execute(
            text(
                f"INSERT INTO {self._bundles_table} (destination_id, bundle_key, "
                "bundle_id, snapshot_seq, source_coordinates_json, manifest_json, "
                "status, owner, fencing_token) VALUES ("
                "CAST(:destination AS TEXT), CAST(:bundle AS TEXT), "
                "CAST(:bundle_id AS TEXT), CAST(:snapshot AS BIGINT), "
                "CAST(:coordinates AS TEXT), CAST(:manifest AS TEXT), "
                "'STAGED', CAST(:run AS TEXT), CAST(:token AS BIGINT)) "
                "ON CONFLICT DO NOTHING"
            ),
            {
                "destination": self.destination_id,
                "bundle": bundle_key,
                "bundle_id": bundle_id,
                "snapshot": snapshot_seq,
                "coordinates": _canonical(
                    [
                        coordinate.model_dump(mode="json")
                        for coordinate in source_coordinates
                    ]
                ),
                "manifest": manifest.model_dump_json(),
                "run": run_id,
                "token": fencing_token,
            },
        )

    def _validate_remote_manifest(
        self, connection: Connection, manifest: RemoteBundleManifest
    ) -> None:
        for member in manifest.members:
            table = f'"{member.schema_name}"."{member.table_name}"'
            rows = [
                dict(row)
                for row in connection.execute(
                    text(
                        f"SELECT * FROM {table} ORDER BY "
                        + ", ".join(
                            f'"{column}"' for column in member.key_columns
                        )
                    )
                ).mappings()
            ]
            if len(rows) != member.row_count:
                raise ValueError(
                    f"remote member {member.member} row count mismatch"
                )
            checksum = hashlib.sha256(_canonical(rows).encode()).hexdigest()
            if checksum != member.checksum:
                raise ValueError(
                    f"remote member {member.member} checksum mismatch"
                )

    def _promote_remote_manifest(
        self,
        connection: Connection,
        *,
        bundle_key: str,
        run_id: str,
        fencing_token: int,
        snapshot_seq: int,
        bundle_id: str,
        cursors: Mapping[str, int],
        source_coordinates: tuple[SourceCoordinate, ...],
        manifest: RemoteBundleManifest,
    ) -> RemotePromotionResult:
        checksums = manifest.checksums
        values = {
            "destination": self.destination_id,
            "bundle": bundle_key,
            "run": run_id,
            "token": fencing_token,
            "snapshot": snapshot_seq,
            "bundle_id": bundle_id,
            "cursors": _canonical(dict(cursors)),
            "checksums": _canonical(dict(checksums)),
            "coordinates": _canonical(
                [
                    coordinate.model_dump(mode="json")
                    for coordinate in source_coordinates
                ]
            ),
            "manifest": manifest.model_dump_json(),
        }
        previous = connection.execute(
            text(
                f"SELECT committed_snapshot_seq, bundle_id FROM {self._table} "
                "WHERE destination_id = CAST(:destination AS TEXT) "
                "AND bundle_key = CAST(:bundle AS TEXT)"
            ),
            values,
        ).one_or_none()
        promoted = connection.execute(
            text(
                f"UPDATE {self._table} SET "
                "committed_snapshot_seq = CAST(:snapshot AS BIGINT), "
                "cursors_json = CAST(:cursors AS TEXT), "
                "checksums_json = CAST(:checksums AS TEXT), "
                "bundle_id = CASE WHEN committed_snapshot_seq = "
                "CAST(:snapshot AS BIGINT) THEN bundle_id "
                "ELSE CAST(:bundle_id AS TEXT) END, owner = NULL, "
                f"lease_expires_at = NULL, updated_at = {self._now} "
                "WHERE destination_id = CAST(:destination AS TEXT) "
                "AND bundle_key = CAST(:bundle AS TEXT) "
                "AND owner = CAST(:run AS TEXT) "
                "AND fencing_token = CAST(:token AS BIGINT) "
                f"AND lease_expires_at > {self._now} AND "
                "(committed_snapshot_seq < CAST(:snapshot AS BIGINT) OR "
                "(committed_snapshot_seq = CAST(:snapshot AS BIGINT) "
                "AND checksums_json = CAST(:checksums AS TEXT))) "
                "RETURNING bundle_id, committed_snapshot_seq"
            ),
            values,
        ).one_or_none()
        if promoted is None:
            raise _StalePromotionError
        disposition = (
            "IDEMPOTENT"
            if previous is not None and int(previous[0]) == snapshot_seq
            else "PROMOTED"
        )
        if disposition == "PROMOTED":
            recorded = connection.execute(
                text(
                    f"UPDATE {self._bundles_table} SET status = 'PROMOTED' "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND bundle_id = CAST(:bundle_id AS TEXT) "
                    "AND owner = CAST(:run AS TEXT) "
                    "AND fencing_token = CAST(:token AS BIGINT) "
                    "RETURNING bundle_id"
                ),
                values,
            ).one_or_none()
            if recorded is None:
                raise _StalePromotionError
        else:
            # Exact replay retains the existing reader-visible pointer.  The
            # candidate stage remains token-owned for later cleanup.
            current = connection.execute(
                text(
                    f"SELECT manifest_json FROM {self._bundles_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND bundle_id = CAST(:current_bundle AS TEXT) "
                    "AND status = 'PROMOTED'"
                ),
                {**values, "current_bundle": promoted[0]},
            ).one_or_none()
            if current is None:
                raise _StalePromotionError
            self._validate_remote_manifest(
                connection,
                RemoteBundleManifest.model_validate_json(current[0]),
            )
        return RemotePromotionResult(
            disposition=disposition,
            bundle_id=str(promoted[0]),
            snapshot_seq=int(promoted[1]),
        )

    def pin_bundle(
        self,
        *,
        bundle_key: str,
        bundle_id: str | None = None,
        pin_id: str | None = None,
        ttl_seconds: int = 3600,
    ) -> BundlePin:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        allocated = pin_id or uuid.uuid4().hex
        with self.engine.begin() as connection:
            selected = bundle_id
            if selected is None:
                selected = connection.execute(
                    text(
                        f"SELECT bundle_id FROM {self._table} "
                        "WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT)"
                    ),
                    {"destination": self.destination_id, "bundle": bundle_key},
                ).scalar_one_or_none()
            exists = connection.execute(
                text(
                    f"SELECT 1 FROM {self._bundles_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND bundle_id = CAST(:bundle_id AS TEXT) "
                    "AND status = 'PROMOTED'"
                ),
                {
                    "destination": self.destination_id,
                    "bundle": bundle_key,
                    "bundle_id": selected,
                },
            ).one_or_none()
            if selected is None or exists is None:
                raise PinnedBundleGoneError(allocated)
            expiry = connection.execute(
                text(
                    f"INSERT INTO {self._pins_table} (destination_id, bundle_key, "
                    "pin_id, bundle_id, expires_at) VALUES ("
                    "CAST(:destination AS TEXT), CAST(:bundle AS TEXT), "
                    "CAST(:pin AS TEXT), CAST(:bundle_id AS TEXT), "
                    f"{self._now} + (CAST(:ttl AS BIGINT) * INTERVAL '1 second')) "
                    "RETURNING CAST(extract(epoch FROM expires_at) * 1000 AS BIGINT)"
                ),
                {
                    "destination": self.destination_id,
                    "bundle": bundle_key,
                    "pin": allocated,
                    "bundle_id": selected,
                    "ttl": ttl_seconds,
                },
            ).scalar_one()
        return BundlePin(
            destination_id=self.destination_id,
            bundle_key=bundle_key,
            pin_id=allocated,
            bundle_id=str(selected),
            expires_at_ms=int(expiry),
        )

    def resolve_pin(self, pin: BundlePin) -> PinnedBundle:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    f"SELECT b.snapshot_seq, b.manifest_json FROM {self._pins_table} p "
                    f"JOIN {self._bundles_table} b ON "
                    "b.destination_id = p.destination_id "
                    "AND b.bundle_key = p.bundle_key AND b.bundle_id = p.bundle_id "
                    "WHERE p.destination_id = CAST(:destination AS TEXT) "
                    "AND p.bundle_key = CAST(:bundle AS TEXT) "
                    "AND p.pin_id = CAST(:pin AS TEXT) "
                    f"AND p.expires_at > {self._now} AND b.status = 'PROMOTED'"
                ),
                {
                    "destination": pin.destination_id,
                    "bundle": pin.bundle_key,
                    "pin": pin.pin_id,
                },
            ).one_or_none()
            if row is None:
                raise PinnedBundleGoneError(pin.pin_id)
            manifest = RemoteBundleManifest.model_validate_json(row[1])
            try:
                self._validate_remote_manifest(connection, manifest)
            except Exception as exc:
                raise PinnedBundleGoneError(pin.pin_id) from exc
        return PinnedBundle(
            bundle_id=pin.bundle_id,
            snapshot_seq=int(row[0]),
            members={
                member.member: f"{member.schema_name}.{member.table_name}"
                for member in manifest.members
            },
        )

    def cleanup_bundles(
        self,
        *,
        bundle_key: str,
        run_id: str,
        fencing_token: int,
        retain_latest: int = 1,
    ) -> tuple[str, ...]:
        if retain_latest < 1:
            raise ValueError("retain_latest must be at least one")
        deleted: list[str] = []
        with self.engine.begin() as connection:
            state = connection.execute(
                text(
                    f"SELECT bundle_id FROM {self._table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND owner = CAST(:run AS TEXT) "
                    "AND fencing_token = CAST(:token AS BIGINT) "
                    f"AND lease_expires_at > {self._now}"
                ),
                {
                    "destination": self.destination_id,
                    "bundle": bundle_key,
                    "run": run_id,
                    "token": fencing_token,
                },
            ).one_or_none()
            if state is None:
                raise _StalePromotionError
            connection.execute(
                text(
                    f"DELETE FROM {self._pins_table} WHERE expires_at <= {self._now}"
                )
            )
            pinned = {
                row[0]
                for row in connection.execute(
                    text(
                        f"SELECT bundle_id FROM {self._pins_table} "
                        "WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT) "
                        f"AND expires_at > {self._now}"
                    ),
                    {"destination": self.destination_id, "bundle": bundle_key},
                ).fetchall()
            }
            rows = connection.execute(
                text(
                    f"SELECT bundle_id, manifest_json, status, fencing_token "
                    f"FROM {self._bundles_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "ORDER BY snapshot_seq DESC, created_at DESC"
                ),
                {"destination": self.destination_id, "bundle": bundle_key},
            ).fetchall()
            latest_promoted = [row[0] for row in rows if row[2] == "PROMOTED"][
                :retain_latest
            ]
            protected = set(latest_promoted) | pinned
            if state[0] is not None:
                protected.add(state[0])
            protected_tables: set[tuple[str, str]] = set()
            for candidate, manifest_json, _status, _candidate_token in rows:
                if candidate not in protected:
                    continue
                protected_manifest = RemoteBundleManifest.model_validate_json(
                    manifest_json
                )
                protected_tables.update(
                    (member.schema_name, member.table_name)
                    for member in protected_manifest.members
                )
            for candidate, manifest_json, status, candidate_token in rows:
                if candidate in protected or (
                    status == "STAGED"
                    and int(candidate_token) >= fencing_token
                ):
                    continue
                manifest = RemoteBundleManifest.model_validate_json(
                    manifest_json
                )
                for member in manifest.members:
                    if (
                        member.schema_name,
                        member.table_name,
                    ) in protected_tables:
                        continue
                    table = f'"{member.schema_name}"."{member.table_name}"'
                    connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
                connection.execute(
                    text(
                        f"DELETE FROM {self._bundles_table} "
                        "WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT) "
                        "AND bundle_id = CAST(:bundle_id AS TEXT)"
                    ),
                    {
                        "destination": self.destination_id,
                        "bundle": bundle_key,
                        "bundle_id": candidate,
                    },
                )
                deleted.append(str(candidate))
        return tuple(deleted)


class BundlePin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    destination_id: NonEmptyStr
    bundle_key: NonEmptyStr
    pin_id: NonEmptyStr
    bundle_id: NonEmptyStr
    expires_at_ms: NonNegativeInt


class PinnedBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    bundle_id: NonEmptyStr
    snapshot_seq: NonNegativeInt
    members: Mapping[StrictStr, StrictStr]


class PinnedBundleGoneError(RuntimeError):
    code = "PINNED_BUNDLE_GONE"

    def __init__(self, pin_id: str) -> None:
        super().__init__(self.code)
        self.pin_id = pin_id


def pin_local_bundle(
    database_path: str | Path,
    *,
    destination_id: str = "local-duckdb",
    bundle_key: str = "platform-kernel",
    bundle_id: str | None = None,
    pin_id: str | None = None,
    ttl_seconds: int = 3600,
) -> BundlePin:
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    path = Path(database_path)
    with _duckdb_lock(path), duckdb.connect(str(path)) as connection:
        _create_destination_tables(connection)
        selected = bundle_id
        if selected is None:
            row = connection.execute(
                f"SELECT bundle_id FROM {_STATE_TABLE} "
                "WHERE destination_id = ? AND bundle_key = ?",
                [destination_id, bundle_key],
            ).fetchone()
            selected = None if row is None else row[0]
        exists = connection.execute(
            f"SELECT 1 FROM {_BUNDLE_TABLE} WHERE destination_id = ? "
            "AND bundle_key = ? AND bundle_id = ?",
            [destination_id, bundle_key, selected],
        ).fetchone()
        if selected is None or exists is None:
            raise PinnedBundleGoneError(pin_id or "unallocated")
        allocated = pin_id or uuid.uuid4().hex
        expiry_row = connection.execute(
            "SELECT epoch_ms(now()) + ?", [ttl_seconds * 1000]
        ).fetchone()
        if expiry_row is None:
            raise RuntimeError("DuckDB did not return its current time")
        expires_at = int(expiry_row[0])
        connection.execute(
            f"INSERT INTO {_PIN_TABLE} VALUES (?, ?, ?, ?, ?, epoch_ms(now()))",
            [destination_id, bundle_key, allocated, selected, expires_at],
        )
    return BundlePin(
        destination_id=destination_id,
        bundle_key=bundle_key,
        pin_id=allocated,
        bundle_id=str(selected),
        expires_at_ms=expires_at,
    )


def resolve_local_pin(
    database_path: str | Path,
    pin: BundlePin,
) -> PinnedBundle:
    path = Path(database_path)
    with _duckdb_lock(path), duckdb.connect(str(path)) as connection:
        row = connection.execute(
            f"SELECT bundle_id FROM {_PIN_TABLE} WHERE destination_id = ? "
            "AND bundle_key = ? AND pin_id = ? AND expires_at > epoch_ms(now())",
            [pin.destination_id, pin.bundle_key, pin.pin_id],
        ).fetchone()
        manifest_row = connection.execute(
            f"SELECT snapshot_seq, manifest_json FROM {_BUNDLE_TABLE} "
            "WHERE destination_id = ? AND bundle_key = ? AND bundle_id = ?",
            [pin.destination_id, pin.bundle_key, pin.bundle_id],
        ).fetchone()
        if row is None or manifest_row is None or row[0] != pin.bundle_id:
            raise PinnedBundleGoneError(pin.pin_id)
        manifest = json.loads(manifest_row[1])
        members: dict[str, str] = {}
        for member, facts in manifest.items():
            table_name = str(facts["table"])
            try:
                table = _quoted(table_name)
                columns = tuple(str(value) for value in facts["columns"])
                unique_key = tuple(str(value) for value in facts["unique_key"])
                rows = [
                    dict(zip(columns, values, strict=True))
                    for values in connection.execute(
                        f"SELECT {', '.join(f'CAST({_quoted(column)} AS VARCHAR)' for column in columns)} "
                        f"FROM {table} ORDER BY {', '.join(_quoted(key) for key in unique_key)}"
                    ).fetchall()
                ]
            except (duckdb.Error, KeyError, TypeError, ValueError) as exc:
                raise PinnedBundleGoneError(pin.pin_id) from exc
            checksum = hashlib.sha256(_canonical(rows).encode()).hexdigest()
            if checksum != facts["checksum"]:
                raise PinnedBundleGoneError(pin.pin_id)
            members[str(member)] = table_name
    return PinnedBundle(
        bundle_id=pin.bundle_id,
        snapshot_seq=int(manifest_row[0]),
        members=members,
    )


def cleanup_local_bundles(
    database_path: str | Path,
    *,
    destination_id: str = "local-duckdb",
    bundle_key: str = "platform-kernel",
    retain_latest: int = 1,
) -> tuple[str, ...]:
    if retain_latest < 1:
        raise ValueError("retain_latest must be at least one")
    path = Path(database_path)
    deleted: list[str] = []
    with _duckdb_lock(path), duckdb.connect(str(path)) as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"DELETE FROM {_PIN_TABLE} WHERE expires_at <= epoch_ms(now())"
            )
            current = connection.execute(
                f"SELECT bundle_id FROM {_STATE_TABLE} WHERE destination_id = ? "
                "AND bundle_key = ?",
                [destination_id, bundle_key],
            ).fetchone()
            pinned = {
                row[0]
                for row in connection.execute(
                    f"SELECT bundle_id FROM {_PIN_TABLE} WHERE destination_id = ? "
                    "AND bundle_key = ? AND expires_at > epoch_ms(now())",
                    [destination_id, bundle_key],
                ).fetchall()
            }
            bundles = connection.execute(
                f"SELECT bundle_id, manifest_json FROM {_BUNDLE_TABLE} "
                "WHERE destination_id = ? AND bundle_key = ? "
                "ORDER BY snapshot_seq DESC, created_at DESC",
                [destination_id, bundle_key],
            ).fetchall()
            protected = {row[0] for row in bundles[:retain_latest]} | pinned
            if current is not None and current[0] is not None:
                protected.add(current[0])
            for candidate, manifest_json in bundles:
                if candidate in protected:
                    continue
                manifest = json.loads(manifest_json)
                for facts in manifest.values():
                    connection.execute(
                        f"DROP TABLE IF EXISTS {_quoted(facts['table'])}"
                    )
                connection.execute(
                    f"DELETE FROM {_BUNDLE_TABLE} WHERE destination_id = ? "
                    "AND bundle_key = ? AND bundle_id = ?",
                    [destination_id, bundle_key, candidate],
                )
                deleted.append(str(candidate))
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    return tuple(deleted)
