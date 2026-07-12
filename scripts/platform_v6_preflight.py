"""Secret-safe live probes for the Platform v6 contract preflight."""

from __future__ import annotations

import hashlib
import math
import os
import secrets
import time
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, LiteralString

import duckdb
import psycopg
import typer
from psycopg import sql
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

from dr_platform.dbos_config import normalize_postgresql_driver_url

if TYPE_CHECKING:
    from datetime import datetime

DEFAULT_MOTHERDUCK_HOST = "pg.us-east-1-aws.motherduck.com"
DEFAULT_MOTHERDUCK_DATABASE = "md:"
DEFAULT_MOTHERDUCK_USER = "postgres"
DEFAULT_SAMPLE_COUNT = 100
MAX_CAPTURE_SKEW_CAP_MS = 5_000
BOUND_INCREMENT_MS = 100
PINNED_MAX_CAPTURE_SKEW_MS = 100
PROJECT_HASH_LENGTH = 12
TEMP_SCHEMA_PREFIX = "platform_v6_preflight"
PUBLISHED_PROBE_VALUE = 42

app = typer.Typer(no_args_is_help=True)


class ProviderKind(StrEnum):
    LOCAL_POSTGRES = "local-postgres"
    MOTHERDUCK_POSTGRES = "motherduck-postgres"
    NEON_POSTGRES = "neon-postgres"
    POSTGRES = "postgres"


class EndpointRelationship(StrEnum):
    SAME_ENDPOINT_FALLBACK = "same-endpoint-fallback"
    SAME_ENDPOINT_EXPLICIT = "same-endpoint-explicit"
    DISTINCT_ENDPOINTS = "distinct-endpoints"


class FencingProbeResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: ProviderKind
    project_hash: str
    renewal_cas: bool
    current_promotion_succeeded: bool
    stale_promotion_rejected: bool
    atomic_bundle_pointer: bool
    independent_writer_connections: bool


class CaptureSkewResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    application_project_hash: str
    system_project_hash: str
    sample_count: int
    p99_skew_ms: float
    median_query_quantum_ms: float
    max_capture_skew_ms: int
    cap_exceeded: bool
    system_url_fell_back_to_application: bool
    raw_skew_ms: tuple[float, ...]


class CaptureSkewVerification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_matches: bool
    relationship_matches: bool
    configured_bound_matches: bool
    measured_within_bound: bool


class ParityViewModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    label: str
    amount: Decimal
    count: int


def parity_view_model(row: tuple[Any, ...]) -> ParityViewModel:
    label, amount, count = row
    return ParityViewModel(label=label, amount=amount, count=count)


def require_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise typer.BadParameter(
            f"required environment variable {name} is absent"
        )
    return value


def connect_motherduck(token_env: str) -> psycopg.Connection[Any]:
    token = require_environment(token_env)
    return psycopg.connect(
        host=DEFAULT_MOTHERDUCK_HOST,
        port=5432,
        user=DEFAULT_MOTHERDUCK_USER,
        password=token,
        dbname=DEFAULT_MOTHERDUCK_DATABASE,
        sslmode="require",
        autocommit=True,
    )


def schema_sql(template: LiteralString, schema: str) -> sql.Composed:
    return sql.SQL(template).format(schema=sql.Identifier(schema))


def engine_from_environment(url_env: str) -> Engine:
    database_url = normalize_postgresql_driver_url(
        require_environment(url_env)
    )
    return create_engine(database_url)


def provider_kind(engine: Engine) -> ProviderKind:
    host = make_url(engine.url).host or "local-socket"
    if "motherduck.com" in host:
        return ProviderKind.MOTHERDUCK_POSTGRES
    if "neon.tech" in host:
        return ProviderKind.NEON_POSTGRES
    if host in {"local-socket", "localhost", "127.0.0.1", "::1"}:
        return ProviderKind.LOCAL_POSTGRES
    return ProviderKind.POSTGRES


def project_hash(engine: Engine) -> str:
    parsed = make_url(engine.url)
    identity = (
        f"{parsed.host or 'local-socket'}:{parsed.port}:{parsed.database}"
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:PROJECT_HASH_LENGTH]


def run_fencing_probe(engine: Engine) -> FencingProbeResult:
    schema = f"{TEMP_SCHEMA_PREFIX}_{secrets.token_hex(6)}"
    owner = "phase0-owner"
    token = 7
    try:
        with engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            connection.execute(
                text(
                    f'CREATE TABLE "{schema}".lease ('
                    "destination TEXT PRIMARY KEY, owner TEXT NOT NULL, "
                    "fencing_token BIGINT NOT NULL, "
                    "expires_at TIMESTAMPTZ NOT NULL)"
                )
            )
            connection.execute(
                text(
                    f'CREATE TABLE "{schema}".bundle ('
                    "bundle_id TEXT PRIMARY KEY, value BIGINT NOT NULL)"
                )
            )
            connection.execute(
                text(
                    f'CREATE TABLE "{schema}".pointer ('
                    "destination TEXT PRIMARY KEY, bundle_id TEXT NOT NULL)"
                )
            )
            connection.execute(
                text(
                    f'INSERT INTO "{schema}".lease '
                    "(destination, owner, fencing_token, expires_at) "
                    "VALUES ('analysis', :owner, :token, "
                    "CURRENT_TIMESTAMP + INTERVAL '5 minutes')"
                ),
                {"owner": owner, "token": token},
            )
            connection.execute(
                text(
                    f'INSERT INTO "{schema}".bundle (bundle_id, value) '
                    "VALUES ('bundle-0', 0)"
                )
            )
            connection.execute(
                text(
                    f'INSERT INTO "{schema}".pointer '
                    "(destination, bundle_id) "
                    "VALUES ('analysis', 'bundle-0')"
                )
            )

        with (
            engine.connect() as owner_connection,
            engine.connect() as stale_connection,
        ):
            independent_writer_connections = (
                owner_connection.connection.dbapi_connection
                is not stale_connection.connection.dbapi_connection
            )
            with owner_connection.begin():
                renewal = owner_connection.execute(
                    text(
                        f'UPDATE "{schema}".lease '
                        "SET expires_at = CURRENT_TIMESTAMP + "
                        "INTERVAL '5 minutes' "
                        "WHERE destination = 'analysis' AND owner = :owner "
                        "AND fencing_token = :token "
                        "AND expires_at > CURRENT_TIMESTAMP"
                    ),
                    {"owner": owner, "token": token},
                ).rowcount
            with owner_connection.begin():
                owner_connection.execute(
                    text(
                        f'INSERT INTO "{schema}".bundle (bundle_id, value) '
                        "VALUES ('bundle-1', 42)"
                    )
                )
                current_promotion = owner_connection.execute(
                    text(
                        f'UPDATE "{schema}".pointer AS pointer '
                        "SET bundle_id = 'bundle-1' "
                        "WHERE pointer.destination = 'analysis' "
                        "AND pointer.bundle_id = 'bundle-0' AND EXISTS ("
                        f'SELECT 1 FROM "{schema}".lease AS lease '
                        "WHERE lease.destination = pointer.destination "
                        "AND lease.owner = :owner "
                        "AND lease.fencing_token = :token "
                        "AND lease.expires_at > CURRENT_TIMESTAMP) "
                        "RETURNING pointer.bundle_id"
                    ),
                    {"owner": owner, "token": token},
                ).one_or_none()
            with stale_connection.begin():
                stale_connection.execute(
                    text(
                        f'INSERT INTO "{schema}".bundle (bundle_id, value) '
                        "VALUES ('bundle-stale', -1)"
                    )
                )
                stale_promotion = stale_connection.execute(
                    text(
                        f'UPDATE "{schema}".pointer AS pointer '
                        "SET bundle_id = 'bundle-stale' "
                        "WHERE pointer.destination = 'analysis' "
                        "AND pointer.bundle_id = 'bundle-1' AND EXISTS ("
                        f'SELECT 1 FROM "{schema}".lease AS lease '
                        "WHERE lease.destination = pointer.destination "
                        "AND lease.owner = :owner "
                        "AND lease.fencing_token = :token "
                        "AND lease.expires_at > CURRENT_TIMESTAMP) "
                        "RETURNING pointer.bundle_id"
                    ),
                    {"owner": owner, "token": token - 1},
                ).one_or_none()
            published = stale_connection.execute(
                text(
                    f'SELECT b.value FROM "{schema}".pointer p '
                    f'JOIN "{schema}".bundle b USING (bundle_id) '
                    "WHERE p.destination = 'analysis'"
                )
            ).scalar_one()
        return FencingProbeResult(
            provider=provider_kind(engine),
            project_hash=project_hash(engine),
            renewal_cas=renewal == 1,
            current_promotion_succeeded=current_promotion == ("bundle-1",),
            stale_promotion_rejected=stale_promotion is None,
            atomic_bundle_pointer=published == PUBLISHED_PROBE_VALUE,
            independent_writer_connections=independent_writer_connections,
        )
    finally:
        with engine.begin() as connection:
            connection.execute(
                text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            )


def run_motherduck_fencing_probe(token_env: str) -> FencingProbeResult:
    """Use raw psycopg because the endpoint does not implement SAVEPOINT."""
    schema = f"{TEMP_SCHEMA_PREFIX}_{secrets.token_hex(6)}"
    owner = "phase0-owner"
    token = 7
    identity = f"{DEFAULT_MOTHERDUCK_HOST}:5432:{DEFAULT_MOTHERDUCK_DATABASE}"
    with (
        connect_motherduck(token_env) as owner_connection,
        connect_motherduck(token_env) as stale_connection,
    ):
        independent_writer_connections = (
            owner_connection is not stale_connection
        )
        try:
            owner_connection.execute(
                schema_sql("CREATE SCHEMA {schema}", schema)
            )
            owner_connection.execute(
                schema_sql(
                    "CREATE TABLE {schema}.lease ("
                    "destination TEXT PRIMARY KEY, owner TEXT NOT NULL, "
                    "fencing_token BIGINT NOT NULL, "
                    "expires_at TIMESTAMPTZ NOT NULL)",
                    schema,
                )
            )
            owner_connection.execute(
                schema_sql(
                    "CREATE TABLE {schema}.bundle ("
                    "bundle_id TEXT PRIMARY KEY, value BIGINT NOT NULL)",
                    schema,
                )
            )
            owner_connection.execute(
                schema_sql(
                    "CREATE TABLE {schema}.pointer ("
                    "destination TEXT PRIMARY KEY, bundle_id TEXT NOT NULL)",
                    schema,
                )
            )
            owner_connection.execute(
                schema_sql(
                    "INSERT INTO {schema}.lease "
                    "(destination, owner, fencing_token, expires_at) "
                    "VALUES ('analysis', %s, %s, "
                    "CURRENT_TIMESTAMP + INTERVAL '5 minutes')",
                    schema,
                ),
                (owner, token),
            )
            owner_connection.execute(
                schema_sql(
                    "INSERT INTO {schema}.bundle (bundle_id, value) "
                    "VALUES ('bundle-0', 0)",
                    schema,
                )
            )
            owner_connection.execute(
                schema_sql(
                    "INSERT INTO {schema}.pointer (destination, bundle_id) "
                    "VALUES ('analysis', 'bundle-0')",
                    schema,
                )
            )
            renewal = owner_connection.execute(
                schema_sql(
                    "UPDATE {schema}.lease "
                    "SET expires_at = CURRENT_TIMESTAMP + "
                    "INTERVAL '5 minutes' "
                    "WHERE destination = 'analysis' AND owner = %s "
                    "AND fencing_token = %s "
                    "AND expires_at > CURRENT_TIMESTAMP "
                    "RETURNING fencing_token",
                    schema,
                ),
                (owner, token),
            ).fetchall()
            with owner_connection.transaction():
                owner_connection.execute(
                    schema_sql(
                        "INSERT INTO {schema}.bundle (bundle_id, value) "
                        "VALUES ('bundle-1', 42)",
                        schema,
                    )
                )
                current_promotion = owner_connection.execute(
                    schema_sql(
                        "UPDATE {schema}.pointer AS pointer "
                        "SET bundle_id = 'bundle-1' "
                        "WHERE pointer.destination = 'analysis' "
                        "AND pointer.bundle_id = 'bundle-0' AND EXISTS ("
                        "SELECT 1 FROM {schema}.lease AS lease "
                        "WHERE lease.destination = pointer.destination "
                        "AND lease.owner = %s AND lease.fencing_token = %s "
                        "AND lease.expires_at > CURRENT_TIMESTAMP) "
                        "RETURNING pointer.bundle_id",
                        schema,
                    ),
                    (owner, token),
                ).fetchall()
            with stale_connection.transaction():
                stale_connection.execute(
                    schema_sql(
                        "INSERT INTO {schema}.bundle (bundle_id, value) "
                        "VALUES ('bundle-stale', -1)",
                        schema,
                    )
                )
                stale_promotion = stale_connection.execute(
                    schema_sql(
                        "UPDATE {schema}.pointer AS pointer "
                        "SET bundle_id = 'bundle-stale' "
                        "WHERE pointer.destination = 'analysis' "
                        "AND pointer.bundle_id = 'bundle-1' AND EXISTS ("
                        "SELECT 1 FROM {schema}.lease AS lease "
                        "WHERE lease.destination = pointer.destination "
                        "AND lease.owner = %s AND lease.fencing_token = %s "
                        "AND lease.expires_at > CURRENT_TIMESTAMP) "
                        "RETURNING pointer.bundle_id",
                        schema,
                    ),
                    (owner, token - 1),
                ).fetchall()
            published = stale_connection.execute(
                schema_sql(
                    "SELECT b.value FROM {schema}.pointer p "
                    "JOIN {schema}.bundle b USING (bundle_id) "
                    "WHERE p.destination = 'analysis'",
                    schema,
                )
            ).fetchone()
            assert published is not None
            return FencingProbeResult(
                provider=ProviderKind.MOTHERDUCK_POSTGRES,
                project_hash=hashlib.sha256(identity.encode()).hexdigest()[
                    :PROJECT_HASH_LENGTH
                ],
                renewal_cas=renewal == [(token,)],
                current_promotion_succeeded=current_promotion
                == [("bundle-1",)],
                stale_promotion_rejected=stale_promotion == [],
                atomic_bundle_pointer=published[0] == PUBLISHED_PROBE_VALUE,
                independent_writer_connections=independent_writer_connections,
            )
        finally:
            owner_connection.execute(
                schema_sql("DROP SCHEMA IF EXISTS {schema} CASCADE", schema)
            )


def percentile_99(samples: list[float]) -> float:
    ordered = sorted(samples)
    index = math.ceil(0.99 * len(ordered)) - 1
    return ordered[index]


def median(samples: list[float]) -> float:
    ordered = sorted(samples)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def capture_database_timestamp(engine: Engine) -> tuple[datetime, float]:
    started_ns = time.monotonic_ns()
    with engine.connect() as connection:
        captured = connection.execute(
            text("SELECT clock_timestamp()")
        ).scalar_one()
    elapsed_ms = (time.monotonic_ns() - started_ns) / 1_000_000
    return captured, elapsed_ms


def measure_capture_skew(
    application_engine: Engine,
    system_engine: Engine,
    *,
    sample_count: int,
    system_url_fell_back_to_application: bool,
) -> CaptureSkewResult:
    skew_samples: list[float] = []
    query_quanta: list[float] = []
    for _index in range(sample_count):
        application_time, application_quantum = capture_database_timestamp(
            application_engine
        )
        system_time, system_quantum = capture_database_timestamp(system_engine)
        skew_samples.append(
            abs((system_time - application_time).total_seconds() * 1_000)
        )
        query_quanta.extend((application_quantum, system_quantum))

    p99_skew_ms = percentile_99(skew_samples)
    median_query_quantum_ms = median(query_quanta)
    raw_bound = p99_skew_ms + (2 * median_query_quantum_ms)
    max_capture_skew_ms = max(
        BOUND_INCREMENT_MS,
        math.ceil(raw_bound / BOUND_INCREMENT_MS) * BOUND_INCREMENT_MS,
    )
    return CaptureSkewResult(
        application_project_hash=project_hash(application_engine),
        system_project_hash=project_hash(system_engine),
        sample_count=sample_count,
        p99_skew_ms=p99_skew_ms,
        median_query_quantum_ms=median_query_quantum_ms,
        max_capture_skew_ms=max_capture_skew_ms,
        cap_exceeded=max_capture_skew_ms > MAX_CAPTURE_SKEW_CAP_MS,
        system_url_fell_back_to_application=system_url_fell_back_to_application,
        raw_skew_ms=tuple(skew_samples),
    )


def endpoint_relationship(result: CaptureSkewResult) -> EndpointRelationship:
    if result.application_project_hash != result.system_project_hash:
        return EndpointRelationship.DISTINCT_ENDPOINTS
    if result.system_url_fell_back_to_application:
        return EndpointRelationship.SAME_ENDPOINT_FALLBACK
    return EndpointRelationship.SAME_ENDPOINT_EXPLICIT


def verify_capture_skew_result(
    result: CaptureSkewResult,
    *,
    expected_application_project_hash: str,
    expected_system_project_hash: str,
    expected_relationship: EndpointRelationship,
    configured_max_capture_skew_ms: int,
) -> CaptureSkewVerification:
    return CaptureSkewVerification(
        identity_matches=(
            result.application_project_hash
            == expected_application_project_hash
            and result.system_project_hash == expected_system_project_hash
        ),
        relationship_matches=(
            endpoint_relationship(result) == expected_relationship
        ),
        configured_bound_matches=(
            configured_max_capture_skew_ms == PINNED_MAX_CAPTURE_SKEW_MS
            and result.max_capture_skew_ms == PINNED_MAX_CAPTURE_SKEW_MS
        ),
        measured_within_bound=(
            result.p99_skew_ms <= PINNED_MAX_CAPTURE_SKEW_MS
        ),
    )


def render_fencing(result: FencingProbeResult) -> None:
    typer.echo(f"provider={result.provider}")
    typer.echo(f"project_hash={result.project_hash}")
    typer.echo(f"renewal_cas={'PASS' if result.renewal_cas else 'FAIL'}")
    typer.echo(
        "current_promotion_succeeded="
        f"{'PASS' if result.current_promotion_succeeded else 'FAIL'}"
    )
    typer.echo(
        "stale_promotion_rejected="
        f"{'PASS' if result.stale_promotion_rejected else 'FAIL'}"
    )
    typer.echo(
        "atomic_bundle_pointer="
        f"{'PASS' if result.atomic_bundle_pointer else 'FAIL'}"
    )
    typer.echo(
        "independent_writer_connections="
        f"{'PASS' if result.independent_writer_connections else 'FAIL'}"
    )
    if not (
        result.renewal_cas
        and result.current_promotion_succeeded
        and result.stale_promotion_rejected
        and result.atomic_bundle_pointer
        and result.independent_writer_connections
    ):
        raise typer.Exit(code=2)


@app.command("postgres-fencing")
def postgres_fencing(
    url_env: Annotated[str, typer.Option()] = "DATABASE_URL",
) -> None:
    """Probe the Postgres destination named by an environment variable."""
    engine = engine_from_environment(url_env)
    try:
        render_fencing(run_fencing_probe(engine))
    finally:
        engine.dispose()


@app.command("motherduck-fencing")
def motherduck_fencing(
    token_env: Annotated[str, typer.Option()] = "MOTHERDUCK_TOKEN",
) -> None:
    """Probe the official MotherDuck us-east-1 Postgres endpoint."""
    render_fencing(run_motherduck_fencing_probe(token_env))


@app.command("motherduck-parity")
def motherduck_parity(
    token_env: Annotated[str, typer.Option()] = "MOTHERDUCK_TOKEN",
) -> None:
    """Read one physical MotherDuck fixture through DuckDB and Postgres."""
    token = require_environment(token_env)
    schema = f"{TEMP_SCHEMA_PREFIX}_{secrets.token_hex(6)}"
    os.environ["MOTHERDUCK_TOKEN"] = token
    motherduck = duckdb.connect(DEFAULT_MOTHERDUCK_DATABASE)
    with connect_motherduck(token_env) as postgres_connection:
        try:
            motherduck.execute(f'CREATE SCHEMA "{schema}"')
            motherduck.execute(
                f'CREATE TABLE "{schema}".parity '
                "(label VARCHAR, amount DECIMAL(12, 2), count BIGINT)"
            )
            motherduck.execute(
                f'INSERT INTO "{schema}".parity VALUES (?, ?, ?)',
                ["phase0", Decimal("12.34"), 42],
            )
            parity_query = schema_sql(
                "SELECT label, amount, count FROM {schema}.parity",
                schema,
            )
            parity_query_text = parity_query.as_string(postgres_connection)
            duckdb_row = motherduck.execute(parity_query_text).fetchone()
            postgres_row = postgres_connection.execute(parity_query).fetchone()
            assert duckdb_row is not None
            assert postgres_row is not None
            duckdb_view = parity_view_model(tuple(duckdb_row))
            postgres_view = parity_view_model(tuple(postgres_row))
            assert duckdb_view == postgres_view
            identity = (
                f"{DEFAULT_MOTHERDUCK_HOST}:5432:{DEFAULT_MOTHERDUCK_DATABASE}"
            )
            identity_hash = hashlib.sha256(identity.encode()).hexdigest()[
                :PROJECT_HASH_LENGTH
            ]
            typer.echo("provider=motherduck-duckdb-and-postgres")
            typer.echo(f"project_hash={identity_hash}")
            typer.echo("physical_fixture_parity=PASS")
            typer.echo("same_query=PASS")
            typer.echo("view_model_parity=PASS")
            typer.echo(
                "query_hash="
                + hashlib.sha256(parity_query_text.encode()).hexdigest()[
                    :PROJECT_HASH_LENGTH
                ]
            )
            typer.echo("types=str,Decimal,int")
        finally:
            motherduck.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            motherduck.close()


@app.command("capture-skew")
def capture_skew(
    application_url_env: Annotated[str, typer.Option()] = "DATABASE_URL",
    system_url_env: Annotated[
        str, typer.Option()
    ] = "DBOS_SYSTEM_DATABASE_URL",
    sample_count: Annotated[int, typer.Option(min=1)] = DEFAULT_SAMPLE_COUNT,
) -> None:
    """Measure independent application and DBOS timestamp captures."""
    application_url = require_environment(application_url_env)
    system_url = os.environ.get(system_url_env)
    fell_back = system_url is None
    if system_url is None:
        system_url = application_url
    application_engine = create_engine(
        normalize_postgresql_driver_url(application_url)
    )
    system_engine = create_engine(normalize_postgresql_driver_url(system_url))
    try:
        result = measure_capture_skew(
            application_engine,
            system_engine,
            sample_count=sample_count,
            system_url_fell_back_to_application=fell_back,
        )
    finally:
        application_engine.dispose()
        system_engine.dispose()

    typer.echo(f"application_project_hash={result.application_project_hash}")
    typer.echo(f"system_project_hash={result.system_project_hash}")
    typer.echo(f"sample_count={result.sample_count}")
    typer.echo(f"p99_skew_ms={result.p99_skew_ms:.3f}")
    typer.echo(f"median_query_quantum_ms={result.median_query_quantum_ms:.3f}")
    typer.echo(f"max_capture_skew_ms={result.max_capture_skew_ms}")
    typer.echo(f"cap_exceeded={str(result.cap_exceeded).lower()}")
    typer.echo(
        "system_url_fell_back_to_application="
        f"{str(result.system_url_fell_back_to_application).lower()}"
    )
    typer.echo(
        "raw_skew_ms="
        + ",".join(f"{sample:.3f}" for sample in result.raw_skew_ms)
    )


@app.command("verify-capture-skew")
def verify_capture_skew(
    expected_application_project_hash: Annotated[str, typer.Option()],
    expected_system_project_hash: Annotated[str, typer.Option()],
    expected_relationship: Annotated[
        EndpointRelationship, typer.Option()
    ] = EndpointRelationship.SAME_ENDPOINT_FALLBACK,
    configured_max_capture_skew_ms: Annotated[
        int, typer.Option(min=0)
    ] = PINNED_MAX_CAPTURE_SKEW_MS,
    sample_count: Annotated[int, typer.Option(min=1)] = DEFAULT_SAMPLE_COUNT,
) -> None:
    """Verify the pinned endpoint topology and 100 ms capture-skew contract."""
    application_url = require_environment("DATABASE_URL")
    system_url = os.environ.get("DBOS_SYSTEM_DATABASE_URL")
    fell_back = system_url is None
    if system_url is None:
        system_url = application_url
    application_engine = create_engine(
        normalize_postgresql_driver_url(application_url)
    )
    system_engine = create_engine(normalize_postgresql_driver_url(system_url))
    try:
        result = measure_capture_skew(
            application_engine,
            system_engine,
            sample_count=sample_count,
            system_url_fell_back_to_application=fell_back,
        )
    finally:
        application_engine.dispose()
        system_engine.dispose()

    verification = verify_capture_skew_result(
        result,
        expected_application_project_hash=expected_application_project_hash,
        expected_system_project_hash=expected_system_project_hash,
        expected_relationship=expected_relationship,
        configured_max_capture_skew_ms=configured_max_capture_skew_ms,
    )
    typer.echo(f"application_project_hash={result.application_project_hash}")
    typer.echo(f"system_project_hash={result.system_project_hash}")
    typer.echo(f"relationship={endpoint_relationship(result)}")
    typer.echo(f"sample_count={result.sample_count}")
    typer.echo(f"p99_skew_ms={result.p99_skew_ms:.3f}")
    typer.echo(f"pinned_max_capture_skew_ms={PINNED_MAX_CAPTURE_SKEW_MS}")
    for field, passed in verification.model_dump().items():
        typer.echo(f"{field}={'PASS' if passed else 'FAIL'}")
    if not all(verification.model_dump().values()):
        raise typer.Exit(code=2)


if __name__ == "__main__":
    app()
