"""Remote publication fences, source compatibility, and local bundle pins."""
# ruff: noqa: E501, PLR0912, PLR0913, PLR0915, S608, TC003, TRY004, TRY301

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
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
    ProjectionColumn,
    ProjectionSpec,
    _application_destination_rows,
    _canonical,
    _create_destination_tables,
    _duckdb_lock,
    _duckdb_scalar,
    _normalize_application_value,
    _quoted,
    _validate_application_rows,
)

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_MINIMUM_COMPATIBLE_SOURCES = 2
_COMBINED_SOURCE_FAMILIES = frozenset({"application", "dbos"})
_MAX_JAVASCRIPT_SAFE_INTEGER = 9_007_199_254_740_991
_OPENSSL = shutil.which("openssl") or "/usr/bin/openssl"


def _quote_identifier(identifier: str) -> str:
    if _IDENTIFIER.fullmatch(identifier) is None:
        raise ValueError("unsafe SQL identifier")
    return f'"{identifier}"'


class SourceCoordinate(BaseModel):
    """A truthful coordinate for one independently captured source."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: NonEmptyStr
    database_server: NonEmptyStr
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
        database_server=str(row[0]),
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
    captured_in_future = any(
        coordinate.captured_at > datetime.now(UTC)
        for coordinate in coordinates
    )
    return SnapshotCompatibility(
        disposition=(
            "COMPATIBLE"
            if skew_ms <= max_capture_skew_ms and not captured_in_future
            else "INCOMPATIBLE"
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
    physical_digest: NonEmptyStr | None = None
    column_schema: tuple[ProjectionColumn, ...] = ()
    columns: tuple[NonEmptyStr, ...] = ()

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
    source_families: tuple[Literal["application", "dbos"], ...]

    @model_validator(mode="after")
    def validate_members(self) -> RemoteBundleManifest:
        names = [member.member for member in self.members]
        if not names or len(names) != len(set(names)):
            raise ValueError(
                "remote bundle members must be non-empty and unique"
            )
        if not self.source_families or len(self.source_families) != len(
            set(self.source_families)
        ):
            raise ValueError("remote bundle source families must be explicit")
        return self

    @property
    def checksums(self) -> dict[str, str]:
        return {member.member: member.checksum for member in self.members}


class SignedBundleIntegrityPayload(BaseModel):
    """The complete, signed v1 reader contract for one physical bundle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    destination_id: NonEmptyStr
    bundle_key: NonEmptyStr
    bundle_id: NonEmptyStr
    snapshot_seq: NonNegativeInt
    integrity_version: Literal["dr-platform.bundle-integrity.v1"]
    source_coordinates_sha256: NonEmptyStr
    physical_digest_algorithm: NonEmptyStr
    members: tuple[RemoteBundleMember, ...]

    @model_validator(mode="after")
    def validate_members(self) -> SignedBundleIntegrityPayload:
        if len({member.member for member in self.members}) != len(
            self.members
        ):
            raise ValueError("integrity attestation members must be unique")
        if any(member.physical_digest is None for member in self.members):
            raise ValueError(
                "integrity attestation members require physical digests"
            )
        return self


class BundleIntegritySigner:
    """Injected signer; private key material never enters publication state."""

    key_id: str

    def sign(self, message: bytes) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True)
class OpenSslEd25519Signer(BundleIntegritySigner):
    """Ed25519 signer backed by an operator-provided PEM file."""

    key_id: str
    private_key_path: Path

    def sign(self, message: bytes) -> bytes:
        with tempfile.NamedTemporaryFile() as message_file:
            message_file.write(message)
            message_file.flush()
            completed = subprocess.run(  # noqa: S603
                [
                    _OPENSSL,
                    "pkeyutl",
                    "-sign",
                    "-rawin",
                    "-inkey",
                    str(self.private_key_path),
                    "-in",
                    message_file.name,
                ],
                capture_output=True,
                check=False,
            )
        if completed.returncode != 0:
            raise RuntimeError("Ed25519 signing failed")
        return completed.stdout


def canonical_integrity_json(value: object) -> str:
    """Constrained JCS contract shared with Unitbench.

    Integrity facts deliberately contain no floating-point values.  This keeps
    Python and JavaScript number rendering out of the signed boundary; callers
    must encode 64-bit facts as decimal strings before constructing a payload.
    """

    def encode(value: object) -> str:  # noqa: PLR0911 -- constrained JSON types
        if value is None:
            return "null"
        if value is True:
            return "true"
        if value is False:
            return "false"
        if isinstance(value, str):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if isinstance(value, int):
            if abs(value) > _MAX_JAVASCRIPT_SAFE_INTEGER:
                raise ValueError("unsafe integer in signed integrity payload")
            return str(value)
        if isinstance(value, float):
            raise TypeError("floats are forbidden in signed integrity payload")
        if isinstance(value, tuple | list):
            return "[" + ",".join(encode(item) for item in value) + "]"
        if isinstance(value, dict):
            if not all(isinstance(key, str) for key in value):
                raise ValueError(
                    "signed integrity object keys must be strings"
                )
            return (
                "{"
                + ",".join(
                    # RFC 8785 orders property names by UTF-16 code units, not
                    # Python Unicode code points.  This also matches ECMAScript's
                    # Array#sort used by Unitbench.
                    f"{encode(key)}:{encode(value[key])}"
                    for key in sorted(
                        value,
                        key=lambda item: item.encode(
                            "utf-16-be", "surrogatepass"
                        ),
                    )
                )
                + "}"
            )
        raise ValueError(
            f"unsupported signed integrity value: {type(value).__name__}"
        )

    return encode(value)


def canonical_integrity_payload(
    payload: SignedBundleIntegrityPayload,
) -> bytes:
    return canonical_integrity_json(payload.model_dump(mode="json")).encode(
        "utf-8"
    )


def integrity_message(payload: SignedBundleIntegrityPayload) -> bytes:
    return b"dr-platform.bundle-integrity.v1\0" + canonical_integrity_payload(
        payload
    )


def _verify_spki_ed25519(
    key_der: bytes, message: bytes, signature: bytes
) -> bool:
    """Use OpenSSL for the same DER/SPKI wire form consumed by Unitbench."""

    with (
        tempfile.NamedTemporaryFile() as key_file,
        tempfile.NamedTemporaryFile() as signature_file,
        tempfile.NamedTemporaryFile() as message_file,
    ):
        key_file.write(key_der)
        key_file.flush()
        signature_file.write(signature)
        signature_file.flush()
        message_file.write(message)
        message_file.flush()
        completed = subprocess.run(  # noqa: S603
            [
                _OPENSSL,
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                key_file.name,
                "-keyform",
                "DER",
                "-sigfile",
                signature_file.name,
                "-in",
                message_file.name,
            ],
            capture_output=True,
            check=False,
        )
    return completed.returncode == 0


class _StalePromotionError(RuntimeError):
    pass


@dataclass(frozen=True)
class PostgresPublicationFence:
    """Destination-local Lease/fence state for MotherDuck or Neon Postgres."""

    engine: Engine
    destination_id: str
    table_name: str = "dr_platform_publication_state"
    kind: Literal["motherduck", "neon"] = "neon"
    signer: BundleIntegritySigner | None = None
    # Readers receive a replaceable public-only ring. Rotation is an atomic
    # caller-side replacement: keep old IDs during overlap, remove revoked IDs
    # to make their existing pins fail closed. Private material is never kept.
    public_key_ring: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.destination_id:
            raise ValueError("destination_id must not be empty")
        if _IDENTIFIER.fullmatch(self.table_name) is None:
            raise ValueError("table_name must be a safe SQL identifier")
        for key_id, encoded in self.public_key_ring.items():
            if not key_id or not isinstance(encoded, str) or not encoded:
                raise ValueError("public key ring entries must be non-empty")

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
                    "destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, bundle_id TEXT NOT NULL, "
                    "snapshot_seq BIGINT NOT NULL, source_coordinates_json TEXT NOT NULL, manifest_json TEXT NOT NULL, "
                    "status TEXT NOT NULL, owner TEXT NOT NULL, fencing_token BIGINT NOT NULL, "
                    f"created_at TIMESTAMPTZ NOT NULL DEFAULT {self._now}, PRIMARY KEY(destination_id, bundle_key, bundle_id))"
                )
            )
            for column in (
                "integrity_version TEXT",
                "integrity_key_id TEXT",
                "integrity_payload_json TEXT",
                "integrity_signature TEXT",
                "physical_digest_algorithm TEXT",
            ):
                connection.execute(
                    text(
                        f"ALTER TABLE {self._bundles_table} ADD COLUMN IF NOT EXISTS {column}"
                    )
                )
            connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {self._pins_table} (destination_id TEXT NOT NULL, bundle_key TEXT NOT NULL, "
                    "pin_id TEXT NOT NULL, bundle_id TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, "
                    f"created_at TIMESTAMPTZ NOT NULL DEFAULT {self._now}, PRIMARY KEY(destination_id, bundle_key, pin_id))"
                )
            )

    def backfill_protected_integrity(
        self, *, bundle_key: str, run_id: str, fencing_token: int
    ) -> tuple[str, ...]:
        """Fence and sign every currently readable legacy bundle.

        This intentionally includes all promoted rows (a safe superset of the
        current, retention-window, and active-pin sets).  Pins are neither
        deleted nor retargeted.  One invalid member aborts the transaction.
        Run this before enabling reader enforcement.
        """

        if self.signer is None:
            raise ValueError(
                "backfill requires an injected BundleIntegritySigner"
            )
        with self.engine.begin() as connection:
            # Lock the owner state row first.  Every subsequent mutation
            # re-checks this exact fence so an expired/replaced lease cannot
            # backfill a bundle while a successor promotes or cleans up.
            connection.execute(
                text(
                    f"SELECT 1 FROM {self._table} WHERE destination_id = "
                    "CAST(:destination AS TEXT) AND bundle_key = "
                    "CAST(:bundle AS TEXT) FOR UPDATE"
                ),
                {"destination": self.destination_id, "bundle": bundle_key},
            ).one()
            self._require_current_lease(
                connection, bundle_key, run_id, fencing_token
            )
            rows = (
                connection.execute(
                    text(
                        f"SELECT bundle_key, bundle_id, snapshot_seq, source_coordinates_json, manifest_json "
                        f"FROM {self._bundles_table} WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT) AND status = 'PROMOTED' FOR UPDATE"
                    ),
                    {"destination": self.destination_id, "bundle": bundle_key},
                )
                .mappings()
                .all()
            )
            completed: list[str] = []
            for row in rows:
                self._require_current_lease(
                    connection, bundle_key, run_id, fencing_token
                )
                manifest = RemoteBundleManifest.model_validate_json(
                    row["manifest_json"]
                )
                coordinates = tuple(
                    SourceCoordinate.model_validate(value)
                    for value in json.loads(row["source_coordinates_json"])
                )
                signed_manifest = self._with_physical_digests(
                    connection, manifest
                )
                payload, signature = self._signed_payload(
                    bundle_key=str(row["bundle_key"]),
                    bundle_id=str(row["bundle_id"]),
                    snapshot_seq=int(row["snapshot_seq"]),
                    manifest=signed_manifest,
                    source_coordinates=coordinates,
                )
                self._require_current_lease(
                    connection, bundle_key, run_id, fencing_token
                )
                updated = connection.execute(
                    text(
                        f"UPDATE {self._bundles_table} SET manifest_json = CAST(:manifest AS TEXT), "
                        "integrity_version = CAST(:version AS TEXT), integrity_key_id = CAST(:key_id AS TEXT), "
                        "integrity_payload_json = CAST(:payload AS TEXT), integrity_signature = CAST(:signature AS TEXT), "
                        "physical_digest_algorithm = CAST(:algorithm AS TEXT) WHERE destination_id = CAST(:destination AS TEXT) "
                        "AND bundle_key = CAST(:bundle AS TEXT) AND bundle_id = CAST(:bundle_id AS TEXT) "
                        "AND status = 'PROMOTED'"
                    ),
                    {
                        "manifest": signed_manifest.model_dump_json(),
                        "version": payload.integrity_version,
                        "key_id": self.signer.key_id,
                        "payload": canonical_integrity_payload(
                            payload
                        ).decode(),
                        "signature": base64.b64encode(signature).decode(),
                        "algorithm": payload.physical_digest_algorithm,
                        "destination": self.destination_id,
                        "bundle": row["bundle_key"],
                        "bundle_id": row["bundle_id"],
                    },
                )
                if updated.rowcount != 1:
                    raise _StalePromotionError
                completed.append(str(row["bundle_id"]))
            return tuple(completed)

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
        source_families: tuple[Literal["application", "dbos"], ...],
        stage: Callable[[Connection], RemoteBundleManifest],
    ) -> RemotePromotionResult:
        """Stage, validate, persist, and fenced-promote one remote bundle."""

        if not source_coordinates:
            raise ValueError("source_coordinates must not be empty")
        self._require_manifest_coordinates(source_coordinates, source_families)
        if self.kind == "motherduck":
            # CURRENT_TIMESTAMP is transaction-stable on MotherDuck.  Commit
            # a pre-stage lease check prevents a stale writer from creating a
            # separately committed stage; the final fence gets a fresh
            # database timestamp and cannot outlive its Lease invisibly.
            try:
                with self.engine.begin() as connection:
                    self._require_current_lease(
                        connection, bundle_key, run_id, fencing_token
                    )
                with self.engine.begin() as stage_connection:
                    manifest = stage(stage_connection)
                    self._validate_remote_manifest(
                        stage_connection,
                        manifest,
                        expected_tables=self._candidate_tables(
                            manifest, run_id, fencing_token, snapshot_seq
                        ),
                        reject_reader_tables=True,
                        bundle_key=bundle_key,
                    )
                    if manifest.source_families != source_families:
                        raise ValueError(
                            "manifest source families do not match promotion"
                        )
                with self.engine.begin() as connection:
                    # The stage transaction may have committed long ago.  Read
                    # physical facts again, sign them, persist the immutable
                    # record, and advance the pointer under one final fence.
                    signed_manifest = self._record_remote_stage(
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
                        manifest=signed_manifest,
                    )
            except _StalePromotionError:
                return RemotePromotionResult(disposition="STALE_PROMOTION")

        try:
            with self.engine.begin() as connection:
                manifest = stage(connection)
                self._validate_remote_manifest(
                    connection,
                    manifest,
                    expected_tables=self._candidate_tables(
                        manifest, run_id, fencing_token, snapshot_seq
                    ),
                    reject_reader_tables=True,
                    bundle_key=bundle_key,
                )
                if manifest.source_families != source_families:
                    raise ValueError(
                        "manifest source families do not match promotion"
                    )
                signed_manifest = self._record_remote_stage(
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
                    manifest=signed_manifest,
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
    ) -> RemoteBundleManifest:
        manifest = self._with_physical_digests(connection, manifest)
        payload, signature = self._signed_payload(
            bundle_key=bundle_key,
            bundle_id=bundle_id,
            snapshot_seq=snapshot_seq,
            manifest=manifest,
            source_coordinates=source_coordinates,
        )
        connection.execute(
            text(
                f"INSERT INTO {self._bundles_table} (destination_id, bundle_key, "
                "bundle_id, snapshot_seq, source_coordinates_json, manifest_json, "
                "integrity_version, integrity_key_id, integrity_payload_json, integrity_signature, physical_digest_algorithm, status, owner, fencing_token) VALUES ("
                "CAST(:destination AS TEXT), CAST(:bundle AS TEXT), "
                "CAST(:bundle_id AS TEXT), CAST(:snapshot AS BIGINT), "
                "CAST(:coordinates AS TEXT), CAST(:manifest AS TEXT), CAST(:version AS TEXT), CAST(:key_id AS TEXT), CAST(:payload AS TEXT), CAST(:signature AS TEXT), CAST(:algorithm AS TEXT), "
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
                "version": payload.integrity_version,
                "key_id": self.signer.key_id if self.signer else "",
                "payload": canonical_integrity_payload(payload).decode(),
                "signature": base64.b64encode(signature).decode(),
                "algorithm": payload.physical_digest_algorithm,
                "run": run_id,
                "token": fencing_token,
            },
        )
        return manifest

    def _with_physical_digests(
        self, connection: Connection, manifest: RemoteBundleManifest
    ) -> RemoteBundleManifest:
        """Read the destination-native aggregate before the member is signed."""

        algorithm = (
            "postgres-pgcrypto-row-json-length-framed-sha256-v1"
            if self.kind == "neon"
            else "duckdb-json-length-framed-sha256-v1"
        )
        members: list[RemoteBundleMember] = []
        for member in manifest.members:
            table = f"{_quote_identifier(member.schema_name)}.{_quote_identifier(member.table_name)}"
            ordering = ", ".join(
                _quote_identifier(key) for key in member.key_columns
            )
            if self.kind == "neon":
                aggregate = (
                    "encode(digest(COALESCE(string_agg(length(row_to_json(t)::text)::text "
                    f"|| ':' || row_to_json(t)::text, '' ORDER BY {ordering}), ''), "
                    "'sha256'), 'hex')"
                )
            else:
                aggregate = (
                    "sha256(COALESCE(string_agg(length(to_json(t)::VARCHAR)::VARCHAR "
                    f"|| ':' || to_json(t)::VARCHAR, '' ORDER BY {ordering}), ''))"
                )
            row = (
                connection.execute(
                    text(
                        f"SELECT COUNT(*) AS row_count, {aggregate} AS physical_digest FROM {table} t"
                    )
                )
                .mappings()
                .one()
            )
            digest = row["physical_digest"]
            if (
                int(row["row_count"]) != member.row_count
                or not isinstance(digest, str)
                or not digest
            ):
                raise ValueError(
                    "destination physical digest validation failed"
                )
            members.append(
                member.model_copy(update={"physical_digest": digest})
            )
        # The algorithm is carried by the signed payload; this method exists to
        # make capability failures happen before any immutable bundle record.
        if not members or not algorithm:
            raise ValueError("destination lacks a physical digest capability")
        return manifest.model_copy(update={"members": tuple(members)})

    def _signed_payload(
        self,
        *,
        bundle_key: str,
        bundle_id: str,
        snapshot_seq: int,
        manifest: RemoteBundleManifest,
        source_coordinates: tuple[SourceCoordinate, ...],
    ) -> tuple[SignedBundleIntegrityPayload, bytes]:
        if self.signer is None:
            raise ValueError(
                "promotion requires an injected BundleIntegritySigner"
            )
        algorithm = (
            "postgres-pgcrypto-row-json-length-framed-sha256-v1"
            if self.kind == "neon"
            else "duckdb-json-length-framed-sha256-v1"
        )
        payload = SignedBundleIntegrityPayload(
            destination_id=self.destination_id,
            bundle_key=bundle_key,
            bundle_id=bundle_id,
            snapshot_seq=snapshot_seq,
            integrity_version="dr-platform.bundle-integrity.v1",
            source_coordinates_sha256=hashlib.sha256(
                _canonical(
                    [
                        coordinate.model_dump(mode="json")
                        for coordinate in source_coordinates
                    ]
                ).encode()
            ).hexdigest(),
            physical_digest_algorithm=algorithm,
            members=manifest.members,
        )
        return payload, self.signer.sign(integrity_message(payload))

    def stage_table_name(
        self,
        *,
        member: str,
        run_id: str,
        fencing_token: int,
        snapshot_seq: int,
    ) -> str:
        """Return the deterministic, candidate-owned staging table name."""

        if _IDENTIFIER.fullmatch(member) is None:
            raise ValueError("member must be a safe SQL identifier")
        run_digest = hashlib.sha256(run_id.encode()).hexdigest()[:16]
        member_digest = hashlib.sha256(member.encode()).hexdigest()[:12]
        return f"drp_stage_{fencing_token}_{snapshot_seq}_{run_digest}_{member_digest}"

    def _candidate_tables(
        self,
        manifest: RemoteBundleManifest,
        run_id: str,
        fencing_token: int,
        snapshot_seq: int,
    ) -> dict[str, str]:
        return {
            member.member: self.stage_table_name(
                member=member.member,
                run_id=run_id,
                fencing_token=fencing_token,
                snapshot_seq=snapshot_seq,
            )
            for member in manifest.members
        }

    def _require_current_lease(
        self,
        connection: Connection,
        bundle_key: str,
        run_id: str,
        fencing_token: int,
    ) -> None:
        current = connection.execute(
            text(
                f"SELECT 1 FROM {self._table} "
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
        if current is None:
            raise _StalePromotionError

    def _require_manifest_coordinates(
        self,
        coordinates: tuple[SourceCoordinate, ...],
        source_families: tuple[Literal["application", "dbos"], ...],
    ) -> None:
        """Fail closed before a separately committed MotherDuck stage."""

        if not source_families or len(source_families) != len(
            set(source_families)
        ):
            raise ValueError("source families must be explicit and unique")
        coordinate_families = tuple(
            coordinate.source_id.partition(":")[0]
            for coordinate in coordinates
        )
        valid_identities = all(
            separator == ":" and identity
            for coordinate in coordinates
            for _, separator, identity in (
                coordinate.source_id.partition(":"),
            )
        )
        if (
            tuple(sorted(coordinate_families))
            != tuple(sorted(source_families))
            or not valid_identities
        ):
            raise IncompatibleSnapshotError(
                SnapshotCompatibility(
                    disposition="MISSING_COORDINATE",
                    observed_skew_ms=None,
                    max_capture_skew_ms=100,
                    source_ids=tuple(
                        coordinate.source_id for coordinate in coordinates
                    ),
                )
            )
        if frozenset(source_families) == _COMBINED_SOURCE_FAMILIES:
            require_compatible_snapshot(coordinates)

    def _reader_table_names(
        self, connection: Connection, bundle_key: str
    ) -> set[str]:
        current = connection.execute(
            text(
                f"SELECT bundle_id FROM {self._table} "
                "WHERE destination_id = CAST(:destination AS TEXT) "
                "AND bundle_key = CAST(:bundle AS TEXT)"
            ),
            {"destination": self.destination_id, "bundle": bundle_key},
        ).scalar_one_or_none()
        bundles = {str(current)} if current is not None else set()
        bundles.update(
            str(row[0])
            for row in connection.execute(
                text(
                    f"SELECT bundle_id FROM {self._pins_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    f"AND expires_at > {self._now}"
                ),
                {"destination": self.destination_id, "bundle": bundle_key},
            )
        )
        if not bundles:
            return set()
        return {
            member.table_name
            for row in connection.execute(
                text(
                    f"SELECT manifest_json FROM {self._bundles_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND bundle_id = ANY(CAST(:bundle_ids AS TEXT[]))"
                ),
                {
                    "destination": self.destination_id,
                    "bundle": bundle_key,
                    "bundle_ids": list(bundles),
                },
            )
            for member in RemoteBundleManifest.model_validate_json(
                row[0]
            ).members
        }

    def _validate_remote_manifest(
        self,
        connection: Connection,
        manifest: RemoteBundleManifest,
        *,
        expected_tables: Mapping[str, str] | None = None,
        reject_reader_tables: bool = False,
        bundle_key: str = "",
    ) -> None:
        reader_tables = (
            self._reader_table_names(connection, bundle_key=bundle_key)
            if reject_reader_tables
            else set()
        )
        for member in manifest.members:
            if (
                expected_tables is not None
                and member.table_name != expected_tables.get(member.member)
            ):
                raise ValueError(
                    "remote manifest names a table outside its candidate"
                )
            if member.table_name in reader_tables:
                raise ValueError(
                    "remote manifest names a current or pinned table"
                )
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
            if member.column_schema:
                expected_columns = tuple(
                    column.name for column in member.column_schema
                )
                if not rows or tuple(rows[0]) == expected_columns:
                    rows = [
                        {
                            column.name: _normalize_application_value(
                                row[column.name],
                                column.type,
                                destination=True,
                            )
                            for column in member.column_schema
                        }
                        for row in rows
                    ]
                else:
                    raise ValueError(
                        f"remote member {member.member} schema mismatch"
                    )
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
        try:
            with self.engine.connect() as connection:
                row = connection.execute(
                    text(
                        f"SELECT b.snapshot_seq, b.manifest_json, b.integrity_version, b.integrity_key_id, "
                        "b.integrity_payload_json, b.integrity_signature, b.physical_digest_algorithm "
                        f"FROM {self._pins_table} p "
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
                # Resolve only the pinned bundle's immutable record.  Never
                # consult mutable current publication state here: a successor
                # promotion must not change an existing pin's proof.
                manifest = RemoteBundleManifest.model_validate_json(row[1])
                payload = SignedBundleIntegrityPayload.model_validate_json(
                    row[4]
                )
                if (
                    row[2] != "dr-platform.bundle-integrity.v1"
                    or not isinstance(row[3], str)
                    or not isinstance(row[5], str)
                    or not isinstance(row[6], str)
                    or payload.destination_id != pin.destination_id
                    or payload.bundle_key != pin.bundle_key
                    or payload.bundle_id != pin.bundle_id
                    or payload.snapshot_seq != int(row[0])
                    or payload.physical_digest_algorithm != row[6]
                ):
                    raise ValueError(
                        "missing or mismatched signed integrity payload"
                    )
                encoded_key = self.public_key_ring.get(row[3])
                if encoded_key is None or not _verify_spki_ed25519(
                    base64.b64decode(encoded_key, validate=True),
                    integrity_message(payload),
                    base64.b64decode(row[5], validate=True),
                ):
                    raise ValueError("invalid remote signed integrity")
                if tuple(manifest.members) != tuple(payload.members):
                    raise ValueError(
                        "remote manifest is not the signed payload"
                    )
                signed_manifest = RemoteBundleManifest(
                    members=payload.members,
                    source_families=manifest.source_families,
                )
                self._validate_remote_manifest(connection, signed_manifest)
                physical = self._with_physical_digests(
                    connection, signed_manifest
                )
                if tuple(physical.members) != tuple(payload.members):
                    raise ValueError("remote physical facts no longer match")
        except PinnedBundleGoneError:
            raise
        except Exception as exc:
            raise PinnedBundleGoneError(pin.pin_id) from exc
        return PinnedBundle(
            bundle_id=pin.bundle_id,
            snapshot_seq=int(row[0]),
            members={
                member.member: f"{member.schema_name}.{member.table_name}"
                for member in payload.members
            },
        )

    def release_pin(self, pin: BundlePin) -> None:
        """Release one reader pin without touching any successor bundle."""

        with self.engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {self._pins_table} "
                    "WHERE destination_id = CAST(:destination AS TEXT) "
                    "AND bundle_key = CAST(:bundle AS TEXT) "
                    "AND pin_id = CAST(:pin AS TEXT) "
                    "AND bundle_id = CAST(:bundle_id AS TEXT)"
                ),
                {
                    "destination": pin.destination_id,
                    "bundle": pin.bundle_key,
                    "pin": pin.pin_id,
                    "bundle_id": pin.bundle_id,
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
    *,
    public_key_ring: Mapping[str, str] | None = None,
) -> PinnedBundle:
    path = Path(database_path)
    with _duckdb_lock(path), duckdb.connect(str(path)) as connection:
        row = connection.execute(
            f"SELECT bundle_id FROM {_PIN_TABLE} WHERE destination_id = ? "
            "AND bundle_key = ? AND pin_id = ? AND expires_at > epoch_ms(now())",
            [pin.destination_id, pin.bundle_key, pin.pin_id],
        ).fetchone()
        manifest_row = connection.execute(
            f"SELECT snapshot_seq, manifest_json, integrity_version, integrity_key_id, "
            "integrity_payload_json, integrity_signature, physical_digest_algorithm "
            f"FROM {_BUNDLE_TABLE} "
            "WHERE destination_id = ? AND bundle_key = ? AND bundle_id = ?",
            [pin.destination_id, pin.bundle_key, pin.bundle_id],
        ).fetchone()
        if row is None or manifest_row is None or row[0] != pin.bundle_id:
            raise PinnedBundleGoneError(pin.pin_id)
        try:
            if public_key_ring is None:
                raise ValueError("local resolution requires a public key ring")
            integrity = manifest_row[2:]
            if (
                any(
                    not isinstance(value, str) or not value
                    for value in integrity
                )
                or integrity[0] != "dr-platform.bundle-integrity.v1"
            ):
                raise ValueError("missing or malformed local signed integrity")
            payload = SignedBundleIntegrityPayload.model_validate_json(
                integrity[2]
            )
            if (
                payload.destination_id != pin.destination_id
                or payload.bundle_key != pin.bundle_key
                or payload.bundle_id != pin.bundle_id
                or payload.snapshot_seq != int(manifest_row[0])
                or payload.physical_digest_algorithm != integrity[4]
                or payload.physical_digest_algorithm
                != "duckdb-json-length-framed-sha256-v1"
            ):
                raise ValueError("mismatched local signed integrity")
            encoded_key = public_key_ring.get(integrity[1])
            if encoded_key is None or not _verify_spki_ed25519(
                base64.b64decode(encoded_key, validate=True),
                integrity_message(payload),
                base64.b64decode(integrity[3], validate=True),
            ):
                raise ValueError("invalid local signed integrity")
            manifest = json.loads(manifest_row[1])
        except (binascii.Error, TypeError, ValueError) as exc:
            raise PinnedBundleGoneError(pin.pin_id) from exc
        if not isinstance(manifest, dict):
            raise PinnedBundleGoneError(pin.pin_id)
        signed_facts = {
            signed.member: {
            "table": signed.table_name,
                **(
                    {
                        "column_schema": [
                            column.model_dump(mode="json")
                            for column in signed.column_schema
                        ]
                    }
                    if signed.column_schema
                    else {"columns": list(signed.columns)}
                ),
                "unique_key": list(signed.key_columns),
                "checksum": signed.checksum,
            }
            for signed in payload.members
        }
        if manifest != signed_facts:
            raise PinnedBundleGoneError(pin.pin_id)
        members: dict[str, str] = {}
        for signed in payload.members:
            member = signed.member
            facts = signed_facts[member]
            try:
                table_name = str(facts["table"])
                table = _quoted(table_name)
                unique_key = tuple(str(value) for value in facts["unique_key"])
                if "column_schema" in facts:
                    column_schema = tuple(
                        ProjectionColumn.model_validate(value)
                        for value in facts["column_schema"]
                    )
                    spec = ProjectionSpec(
                        member=str(member),
                        columns=tuple(column.name for column in column_schema),
                        column_schema=column_schema,
                        unique_key=unique_key,
                    )
                    rows = _application_destination_rows(
                        connection,
                        member=spec.member,
                        table_name=table_name,
                        column_schema=column_schema,
                    )
                    _, checksums = _validate_application_rows(
                        (spec,), {spec.member: rows}
                    )
                    checksum = checksums[spec.member]
                else:
                    columns = tuple(str(value) for value in facts["columns"])
                    rows = [
                        dict(zip(columns, values, strict=True))
                        for values in connection.execute(
                            f"SELECT {', '.join(f'CAST({_quoted(column)} AS VARCHAR)' for column in columns)} "
                            f"FROM {table} ORDER BY {', '.join(_quoted(key) for key in unique_key)}"
                        ).fetchall()
                    ]
                    checksum = hashlib.sha256(
                        _canonical(rows).encode()
                    ).hexdigest()
            except (duckdb.Error, KeyError, TypeError, ValueError) as exc:
                raise PinnedBundleGoneError(pin.pin_id) from exc
            if checksum != signed.checksum:
                raise PinnedBundleGoneError(pin.pin_id)
            ordering = ", ".join(_quoted(key) for key in unique_key)
            physical = connection.execute(
                "SELECT COUNT(*), sha256(COALESCE(string_agg("
                "length(to_json(t)::VARCHAR)::VARCHAR || ':' || "
                "to_json(t)::VARCHAR, '' ORDER BY "
                f"{ordering}), '')) FROM {table} t"
            ).fetchone()
            if (
                physical is None
                or signed.row_count != int(physical[0])
                or signed.physical_digest != physical[1]
            ):
                raise PinnedBundleGoneError(pin.pin_id)
            members[str(member)] = table_name
    return PinnedBundle(
        bundle_id=pin.bundle_id,
        snapshot_seq=int(manifest_row[0]),
        members=members,
    )


def backfill_local_protected_integrity(
    database_path: str | Path,
    *,
    signer: BundleIntegritySigner,
    destination_id: str = "local-duckdb",
    bundle_key: str = "platform-kernel",
    run_id: str,
    fencing_token: int,
) -> tuple[str, ...]:
    """Sign legacy local bundles while holding the current local lease.

    Invoke this migration API before passing a public key ring to readers.
    It signs all retained rows for the destination (a safe superset of active
    pins), never retargets pins, and checks the lease before each update.
    """

    path = Path(database_path)
    with _duckdb_lock(path), duckdb.connect(str(path)) as connection:
        _create_destination_tables(connection)
        connection.execute("BEGIN TRANSACTION")
        try:
            state = connection.execute(
                f"SELECT owner, fencing_token, lease_expires_at FROM {_STATE_TABLE} "
                "WHERE destination_id = ? AND bundle_key = ?",
                [destination_id, bundle_key],
            ).fetchone()
            if (
                state is None
                or state[0] != run_id
                or int(state[1]) != fencing_token
                or int(state[2])
                <= _duckdb_scalar(connection, "SELECT epoch_ms(now())")
            ):
                raise ValueError(
                    "local integrity backfill lease ownership lost"
                )
            rows = connection.execute(
                f"SELECT bundle_id, snapshot_seq, manifest_json FROM {_BUNDLE_TABLE} "
                "WHERE destination_id = ? AND bundle_key = ? "
                "AND integrity_version IS NULL",
                [destination_id, bundle_key],
            ).fetchall()
            completed: list[str] = []
            for bundle_id, snapshot_seq, manifest_json in rows:
                current = connection.execute(
                    f"SELECT 1 FROM {_STATE_TABLE} WHERE destination_id = ? "
                    "AND bundle_key = ? AND owner = ? AND fencing_token = ? "
                    "AND lease_expires_at > epoch_ms(now())",
                    [destination_id, bundle_key, run_id, fencing_token],
                ).fetchone()
                if current is None:
                    raise ValueError(
                        "local integrity backfill lease ownership lost"
                    )
                manifest = json.loads(manifest_json)
                if not isinstance(manifest, dict):
                    raise ValueError("legacy local manifest is malformed")
                signed_members: list[RemoteBundleMember] = []
                for member, facts in manifest.items():
                    if not isinstance(facts, dict):
                        raise ValueError("legacy local member is malformed")
                    column_schema = tuple(
                        ProjectionColumn.model_validate(column)
                        for column in facts.get("column_schema", ())
                    )
                    raw_columns = tuple(
                        str(column) for column in facts.get("columns", ())
                    )
                    if not column_schema and not raw_columns:
                        raise ValueError("legacy local member has no columns")
                    unique_key = tuple(str(key) for key in facts["unique_key"])
                    table_name = str(facts["table"])
                    table = _quoted(table_name)
                    if column_schema:
                        rows_for_member = _application_destination_rows(
                            connection,
                            member=str(member),
                            table_name=table_name,
                            column_schema=column_schema,
                        )
                        spec = ProjectionSpec(
                            member=str(member),
                            columns=tuple(column.name for column in column_schema),
                            column_schema=column_schema,
                            unique_key=unique_key,
                        )
                        _, checksums = _validate_application_rows(
                            (spec,), {str(member): rows_for_member}
                        )
                        checksum = checksums[str(member)]
                    else:
                        rows = [
                            dict(zip(raw_columns, values, strict=True))
                            for values in connection.execute(
                                f"SELECT {', '.join(f'CAST({_quoted(column)} AS VARCHAR)' for column in raw_columns)} "
                                f"FROM {table} ORDER BY {', '.join(_quoted(key) for key in unique_key)}"
                            ).fetchall()
                        ]
                        checksum = hashlib.sha256(_canonical(rows).encode()).hexdigest()
                    ordering = ", ".join(_quoted(key) for key in unique_key)
                    physical = connection.execute(
                        "SELECT COUNT(*), sha256(COALESCE(string_agg("
                        "length(to_json(t)::VARCHAR)::VARCHAR || ':' || "
                        "to_json(t)::VARCHAR, '' ORDER BY "
                        f"{ordering}), '')) FROM {table} t"
                    ).fetchone()
                    if physical is None:
                        raise ValueError("legacy local member disappeared")
                    signed_members.append(
                        RemoteBundleMember(
                            member=str(member),
                            schema_name="main",
                            table_name=table_name,
                            key_columns=unique_key,
                            row_count=int(physical[0]),
                            checksum=checksum,
                            physical_digest=str(physical[1]),
                            column_schema=column_schema,
                            columns=raw_columns if not column_schema else (),
                        )
                    )
                payload = SignedBundleIntegrityPayload(
                    destination_id=destination_id,
                    bundle_key=bundle_key,
                    bundle_id=str(bundle_id),
                    snapshot_seq=int(snapshot_seq),
                    integrity_version="dr-platform.bundle-integrity.v1",
                    source_coordinates_sha256=hashlib.sha256(
                        b"[]"
                    ).hexdigest(),
                    physical_digest_algorithm="duckdb-json-length-framed-sha256-v1",
                    members=tuple(signed_members),
                )
                updated = connection.execute(
                    f"UPDATE {_BUNDLE_TABLE} SET integrity_version = ?, integrity_key_id = ?, "
                    "integrity_payload_json = ?, integrity_signature = ?, "
                    "physical_digest_algorithm = ? WHERE destination_id = ? "
                    "AND bundle_key = ? AND bundle_id = ? AND integrity_version IS NULL "
                    "RETURNING bundle_id",
                    [
                        payload.integrity_version,
                        signer.key_id,
                        canonical_integrity_payload(payload).decode(),
                        base64.b64encode(
                            signer.sign(integrity_message(payload))
                        ).decode(),
                        payload.physical_digest_algorithm,
                        destination_id,
                        bundle_key,
                        str(bundle_id),
                    ],
                )
                if updated.fetchone() != (str(bundle_id),):
                    raise ValueError("local integrity backfill became stale")
                completed.append(str(bundle_id))
            connection.execute("COMMIT")
            return tuple(completed)
        except Exception:
            connection.execute("ROLLBACK")
            raise


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
