"""Durable throttle pacing over the v6 ``throttle_state`` table."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from dr_serialize import sha256_json_digest
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

from dr_platform.records import ThrottleState, validate_payload_size
from dr_platform.status import FailureClass

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy import Connection
    from sqlalchemy.engine import RowMapping

    from dr_platform.db import PlatformSchema

DEFAULT_INITIAL_BACKOFF_SECONDS = 5.0
DEFAULT_MAX_BACKOFF_SECONDS = 300.0
DEFAULT_JITTER_SECONDS = 3.0
BACKOFF_JITTER_DIGEST_LENGTH = 8
MAX_BACKOFF_EXPONENT = 1022
RETRYABLE_BACKOFF_FAILURES = frozenset(
    {FailureClass.TRANSIENT, FailureClass.RATE_LIMITED}
)

ThrottleBackoffState = ThrottleState


def should_backoff_failure(failure_class: FailureClass) -> bool:
    return failure_class in RETRYABLE_BACKOFF_FAILURES


def delay_until_unblocked_seconds(
    state: ThrottleState | None, *, now: datetime
) -> float:
    if state is None:
        return 0.0
    deadlines = [
        value for value in (state.blocked_until, state.hold_until) if value
    ]
    return (
        max(0.0, (max(deadlines) - now).total_seconds()) if deadlines else 0.0
    )


def deterministic_jitter_seconds(
    *, throttle_key: str, consecutive_failures: int, max_jitter_seconds: float
) -> float:
    if max_jitter_seconds <= 0:
        return 0.0
    digest = sha256_json_digest(
        {
            "throttle_key": throttle_key,
            "consecutive_failures": consecutive_failures,
        },
        length=BACKOFF_JITTER_DIGEST_LENGTH,
    )
    return (
        int(digest, 16)
        / float(16**BACKOFF_JITTER_DIGEST_LENGTH - 1)
        * max_jitter_seconds
    )


def next_backoff_delay_seconds(  # noqa: PLR0913 -- retained tuning surface
    *,
    throttle_key: str,
    consecutive_failures: int,
    failure_class: FailureClass,
    initial_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS,
    max_seconds: float = DEFAULT_MAX_BACKOFF_SECONDS,
    jitter_seconds: float = DEFAULT_JITTER_SECONDS,
) -> float:
    if not should_backoff_failure(failure_class):
        return 0.0
    count = max(1, consecutive_failures)
    base = min(
        max_seconds,
        initial_seconds * (2 ** min(count - 1, MAX_BACKOFF_EXPONENT)),
    )
    return min(
        max_seconds,
        base
        + deterministic_jitter_seconds(
            throttle_key=throttle_key,
            consecutive_failures=count,
            max_jitter_seconds=jitter_seconds,
        ),
    )


def load_throttle_backoff_state(
    connection: Connection, *, throttle_key: str, schema: PlatformSchema
) -> ThrottleState | None:
    row = (
        connection.execute(
            schema.throttle_state.select().where(
                schema.throttle_state.c.throttle_key == throttle_key
            )
        )
        .mappings()
        .one_or_none()
    )
    return None if row is None else _decode_throttle_state(row)


def throttle_delay_seconds(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> float:
    return delay_until_unblocked_seconds(
        load_throttle_backoff_state(
            connection, throttle_key=throttle_key, schema=schema
        ),
        now=now,
    )


def hold_throttle_delay(
    connection: Connection,
    *,
    throttle_key: str,
    schema: PlatformSchema,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleeper: Callable[[float], None],
) -> float:
    """Sleep at the DBOS-compatible callable boundary when a target is held."""
    delay = throttle_delay_seconds(
        connection, throttle_key=throttle_key, now=clock(), schema=schema
    )
    if delay > 0:
        sleeper(delay)
    return delay


def record_throttle_failure(  # noqa: PLR0913 -- retained failure surface
    connection: Connection,
    *,
    throttle_key: str,
    failure_class: FailureClass,
    error_type: str | None = None,
    message: str | None = None,
    metadata: dict[str, Any] | None = None,
    now: datetime,
    schema: PlatformSchema,
) -> ThrottleState | None:
    validated_throttle_key = _validate_non_empty_string(
        throttle_key, label="throttle key"
    )
    validated_error_type = (
        None
        if error_type is None
        else _validate_non_empty_string(error_type, label="error type")
    )
    validated_metadata = _validate_throttle_metadata(metadata or {})
    if not should_backoff_failure(failure_class):
        return None
    table = schema.throttle_state
    count = int(
        connection.execute(
            insert(table)
            .values(
                **_fresh(
                    validated_throttle_key,
                    now,
                    consecutive_failures=1,
                )
            )
            .on_conflict_do_update(
                index_elements=["throttle_key"],
                set_={
                    "consecutive_failures": table.c.consecutive_failures + 1,
                    "updated_at": now,
                },
            )
            .returning(table.c.consecutive_failures)
        ).scalar_one()
    )
    connection.execute(
        update(table)
        .where(table.c.throttle_key == validated_throttle_key)
        .values(
            blocked_until=now
            + timedelta(
                seconds=next_backoff_delay_seconds(
                    throttle_key=validated_throttle_key,
                    consecutive_failures=count,
                    failure_class=failure_class,
                )
            ),
            failure_class=failure_class.value,
            last_error_type=validated_error_type,
            last_message=message,
            metadata=validated_metadata,
            updated_at=now,
        )
    )
    return load_throttle_backoff_state(
        connection, throttle_key=validated_throttle_key, schema=schema
    )


def clear_throttle_backoff(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> None:
    validated_throttle_key = _validate_non_empty_string(
        throttle_key, label="throttle key"
    )
    _upsert(
        connection,
        schema=schema,
        throttle_key=validated_throttle_key,
        now=now,
        values={
            "blocked_until": None,
            "consecutive_failures": 0,
            "failure_class": None,
            "last_error_type": None,
            "last_message": None,
            "metadata": {},
        },
    )


def set_throttle_hold(  # noqa: PLR0913 -- retained operator surface
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
    until: datetime | None = None,
    duration_seconds: float | None = None,
    reason: str | None = None,
) -> ThrottleState:
    validated_throttle_key = _validate_non_empty_string(
        throttle_key, label="throttle key"
    )
    if (until is None) == (duration_seconds is None) or reason is None:
        raise ValueError(
            "set_throttle_hold requires one deadline and a reason"
        )
    validated_reason = _validate_non_empty_string(
        reason, label="throttle hold reason"
    )
    _upsert(
        connection,
        schema=schema,
        throttle_key=validated_throttle_key,
        now=now,
        values={
            "hold_until": until
            or now + timedelta(seconds=duration_seconds or 0),
            "hold_reason": validated_reason,
        },
    )
    state = load_throttle_backoff_state(
        connection, throttle_key=validated_throttle_key, schema=schema
    )
    assert state is not None
    return state


def clear_throttle_hold(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> None:
    validated_throttle_key = _validate_non_empty_string(
        throttle_key, label="throttle key"
    )
    connection.execute(
        update(schema.throttle_state)
        .where(schema.throttle_state.c.throttle_key == validated_throttle_key)
        .values(hold_until=None, hold_reason=None, updated_at=now)
    )


def set_throttle_tags(
    connection: Connection,
    *,
    throttle_key: str,
    tags: dict[str, str],
    now: datetime,
    schema: PlatformSchema,
) -> None:
    validated_throttle_key = _validate_non_empty_string(
        throttle_key, label="throttle key"
    )
    validated_tags = _validate_throttle_tags(tags)
    _upsert(
        connection,
        schema=schema,
        throttle_key=validated_throttle_key,
        now=now,
        values={"tags": validated_tags},
    )


def list_throttle_states(
    connection: Connection,
    *,
    schema: PlatformSchema,
    tag_filter: dict[str, str] | None = None,
) -> tuple[ThrottleState, ...]:
    statement = schema.throttle_state.select().order_by(
        schema.throttle_state.c.throttle_key
    )
    if tag_filter:
        statement = statement.where(
            schema.throttle_state.c.tags.contains(tag_filter)
        )
    return tuple(
        _decode_throttle_state(row)
        for row in connection.execute(statement).mappings()
    )


def _validate_throttle_metadata(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) for key in value
    ):
        raise ValueError(
            "throttle metadata must be a mapping with string keys"
        )
    validate_payload_size(value, label="throttle metadata")
    return dict(cast("dict[str, Any]", value))


def _validate_non_empty_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def _validate_throttle_tags(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(item, str)
        for key, item in value.items()
    ):
        raise ValueError("throttle tags must map strings to strings")
    validate_payload_size(value, label="throttle tags")
    return dict(cast("dict[str, str]", value))


def _decode_throttle_state(row: RowMapping) -> ThrottleState:
    values = dict(row)
    failure_class = values["failure_class"]
    values["failure_class"] = (
        FailureClass(failure_class) if failure_class is not None else None
    )
    return ThrottleState.model_construct(**values)


def _upsert(
    connection: Connection,
    *,
    schema: PlatformSchema,
    throttle_key: str,
    now: datetime,
    values: dict[str, Any],
) -> None:
    table = schema.throttle_state
    connection.execute(
        insert(table)
        .values(**(_fresh(throttle_key, now) | values))
        .on_conflict_do_update(
            index_elements=["throttle_key"], set_={**values, "updated_at": now}
        )
    )


def _fresh(
    throttle_key: str, now: datetime, *, consecutive_failures: int = 0
) -> dict[str, Any]:
    return {
        "throttle_key": throttle_key,
        "blocked_until": None,
        "consecutive_failures": consecutive_failures,
        "failure_class": None,
        "last_error_type": None,
        "last_message": None,
        "metadata": {},
        "updated_at": now,
        "hold_until": None,
        "hold_reason": None,
        "tags": {},
    }
