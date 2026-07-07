"""Throttle/backoff state keyed by operator-visible throttle keys.

Automatic exponential backoff (deterministic jitter, retryable
failure classes only) plus operator holds and target tags on the same
table — ``delay_until_unblocked_seconds`` composes both, so a hold and
a backoff never race each other.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from dr_providers.kernel.failures import FailureClass
from dr_serialize import sha256_json_digest
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from dr_platform.db.schema import PlatformSchema

DEFAULT_INITIAL_BACKOFF_SECONDS = 5.0
DEFAULT_MAX_BACKOFF_SECONDS = 300.0
DEFAULT_JITTER_SECONDS = 3.0
BACKOFF_JITTER_DIGEST_LENGTH = 8
MAX_BACKOFF_EXPONENT = 1022
RETRYABLE_BACKOFF_FAILURES = frozenset(
    {
        FailureClass.TRANSIENT,
        FailureClass.RATE_LIMITED,
    }
)


class ThrottleBackoffState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    throttle_key: StrictStr
    blocked_until: datetime | None = None
    consecutive_failures: StrictInt = 0
    failure_class: FailureClass | None = None
    last_error_type: StrictStr | None = None
    last_message: StrictStr | None = None
    metadata: dict[StrictStr, Any] = Field(default_factory=dict)
    updated_at: datetime
    hold_until: datetime | None = None
    hold_reason: StrictStr | None = None
    tags: dict[StrictStr, StrictStr] = Field(default_factory=dict)


def should_backoff_failure(failure_class: FailureClass) -> bool:
    return failure_class in RETRYABLE_BACKOFF_FAILURES


def delay_until_unblocked_seconds(
    state: ThrottleBackoffState | None,
    *,
    now: datetime,
) -> float:
    if state is None:
        return 0.0
    candidates = [
        candidate
        for candidate in (state.blocked_until, state.hold_until)
        if candidate is not None
    ]
    if not candidates:
        return 0.0
    remaining = (max(candidates) - now).total_seconds()
    return max(0.0, remaining)


def next_backoff_delay_seconds(  # noqa: PLR0913 -- ported tuning knobs
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
    failure_count = max(1, consecutive_failures)
    exponent = min(failure_count - 1, MAX_BACKOFF_EXPONENT)
    exponential = initial_seconds * (2**exponent)
    base_delay = min(max_seconds, exponential)
    jitter = deterministic_jitter_seconds(
        throttle_key=throttle_key,
        consecutive_failures=failure_count,
        max_jitter_seconds=jitter_seconds,
    )
    return min(max_seconds, base_delay + jitter)


def deterministic_jitter_seconds(
    *,
    throttle_key: str,
    consecutive_failures: int,
    max_jitter_seconds: float,
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
    fraction = int(digest, 16) / float(16**BACKOFF_JITTER_DIGEST_LENGTH - 1)
    return fraction * max_jitter_seconds


def load_throttle_backoff_state(
    connection: Connection,
    *,
    throttle_key: str,
    schema: PlatformSchema,
) -> ThrottleBackoffState | None:
    row = (
        connection.execute(
            schema.throttle_backoff.select().where(
                schema.throttle_backoff.c.throttle_key == throttle_key
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return throttle_backoff_state_from_row(dict(row))


def throttle_delay_seconds(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> float:
    state = load_throttle_backoff_state(
        connection,
        throttle_key=throttle_key,
        schema=schema,
    )
    return delay_until_unblocked_seconds(state, now=now)


def record_throttle_failure(  # noqa: PLR0913 -- explicit failure fields by design
    connection: Connection,
    *,
    throttle_key: str,
    failure_class: FailureClass,
    error_type: str | None = None,
    message: str | None = None,
    metadata: dict[str, Any] | None = None,
    now: datetime,
    schema: PlatformSchema,
) -> ThrottleBackoffState | None:
    if not should_backoff_failure(failure_class):
        return None

    consecutive_failures = increment_throttle_consecutive_failures(
        connection,
        throttle_key=throttle_key,
        now=now,
        schema=schema,
    )
    delay = next_backoff_delay_seconds(
        throttle_key=throttle_key,
        consecutive_failures=consecutive_failures,
        failure_class=failure_class,
    )
    connection.execute(
        update(schema.throttle_backoff)
        .where(schema.throttle_backoff.c.throttle_key == throttle_key)
        .values(
            blocked_until=now + timedelta(seconds=delay),
            failure_class=failure_class.value,
            last_error_type=error_type,
            last_message=message,
            metadata=dict(metadata or {}),
            updated_at=now,
        )
    )
    return load_throttle_backoff_state(
        connection,
        throttle_key=throttle_key,
        schema=schema,
    )


def increment_throttle_consecutive_failures(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> int:
    inserted = connection.execute(
        insert(schema.throttle_backoff)
        .values(
            {
                "throttle_key": throttle_key,
                "blocked_until": None,
                "consecutive_failures": 1,
                "failure_class": None,
                "last_error_type": None,
                "last_message": None,
                "metadata": {},
                "updated_at": now,
            }
        )
        .on_conflict_do_update(
            index_elements=["throttle_key"],
            set_={
                "consecutive_failures": (
                    schema.throttle_backoff.c.consecutive_failures + 1
                ),
                "updated_at": now,
            },
        )
        .returning(schema.throttle_backoff.c.consecutive_failures)
    )
    return int(inserted.scalar_one())


def clear_throttle_backoff(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> None:
    """Reset automatic backoff state; holds and tags are preserved."""
    connection.execute(
        insert(schema.throttle_backoff)
        .values(_fresh_row(throttle_key=throttle_key, now=now))
        .on_conflict_do_update(
            index_elements=["throttle_key"],
            set_={
                "blocked_until": None,
                "consecutive_failures": 0,
                "failure_class": None,
                "last_error_type": None,
                "last_message": None,
                "metadata": {},
                "updated_at": now,
            },
        )
    )


def set_throttle_hold(  # noqa: PLR0913 -- operator surface: until/duration/reason
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
    until: datetime | None = None,
    duration_seconds: float | None = None,
    reason: str | None = None,
) -> ThrottleBackoffState:
    """Operator hold: block a target until an absolute or relative time.

    Exactly one of ``until`` / ``duration_seconds`` is required
    (relative form covers the "+30m" operator gesture).
    """
    if (until is None) == (duration_seconds is None):
        raise ValueError(
            "set_throttle_hold requires exactly one of until or "
            "duration_seconds"
        )
    hold_until = (
        until
        if until is not None
        else now + timedelta(seconds=float(duration_seconds or 0.0))
    )
    connection.execute(
        insert(schema.throttle_backoff)
        .values(
            _fresh_row(throttle_key=throttle_key, now=now)
            | {"hold_until": hold_until, "hold_reason": reason}
        )
        .on_conflict_do_update(
            index_elements=["throttle_key"],
            set_={
                "hold_until": hold_until,
                "hold_reason": reason,
                "updated_at": now,
            },
        )
    )
    state = load_throttle_backoff_state(
        connection,
        throttle_key=throttle_key,
        schema=schema,
    )
    assert state is not None  # just upserted
    return state


def clear_throttle_hold(
    connection: Connection,
    *,
    throttle_key: str,
    now: datetime,
    schema: PlatformSchema,
) -> None:
    connection.execute(
        update(schema.throttle_backoff)
        .where(schema.throttle_backoff.c.throttle_key == throttle_key)
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
    connection.execute(
        insert(schema.throttle_backoff)
        .values(
            _fresh_row(throttle_key=throttle_key, now=now)
            | {"tags": dict(tags)}
        )
        .on_conflict_do_update(
            index_elements=["throttle_key"],
            set_={"tags": dict(tags), "updated_at": now},
        )
    )


def list_throttle_states(
    connection: Connection,
    *,
    schema: PlatformSchema,
    tag_filter: dict[str, str] | None = None,
) -> tuple[ThrottleBackoffState, ...]:
    statement = schema.throttle_backoff.select().order_by(
        schema.throttle_backoff.c.throttle_key
    )
    if tag_filter:
        statement = statement.where(
            schema.throttle_backoff.c.tags.contains(dict(tag_filter))
        )
    rows = connection.execute(statement).mappings().all()
    return tuple(throttle_backoff_state_from_row(dict(row)) for row in rows)


def _fresh_row(*, throttle_key: str, now: datetime) -> dict[str, Any]:
    return {
        "throttle_key": throttle_key,
        "blocked_until": None,
        "consecutive_failures": 0,
        "failure_class": None,
        "last_error_type": None,
        "last_message": None,
        "metadata": {},
        "updated_at": now,
    }


def throttle_backoff_state_from_row(
    row: dict[str, Any],
) -> ThrottleBackoffState:
    failure_class = row.get("failure_class")
    return ThrottleBackoffState(
        throttle_key=row["throttle_key"],
        blocked_until=row["blocked_until"],
        consecutive_failures=row["consecutive_failures"],
        failure_class=(
            FailureClass(failure_class) if failure_class is not None else None
        ),
        last_error_type=row["last_error_type"],
        last_message=row["last_message"],
        metadata=row["metadata"],
        updated_at=row["updated_at"],
        hold_until=row.get("hold_until"),
        hold_reason=row.get("hold_reason"),
        tags=row.get("tags") or {},
    )


def utc_now() -> datetime:
    return datetime.now(UTC)
