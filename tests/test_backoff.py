from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from dr_providers.kernel.failures import FailureClass
from sqlalchemy import Engine

from dr_platform import (
    PlatformSchema,
    ThrottleBackoffState,
    clear_throttle_backoff,
    clear_throttle_hold,
    delay_until_unblocked_seconds,
    list_throttle_states,
    load_throttle_backoff_state,
    next_backoff_delay_seconds,
    record_throttle_failure,
    set_throttle_hold,
    set_throttle_tags,
    should_backoff_failure,
    throttle_delay_seconds,
    upgrade_platform_schema,
)
from dr_platform.backoff import deterministic_jitter_seconds

NOW = datetime(2026, 7, 4, 12, 0, tzinfo=UTC)


# --- pure math --------------------------------------------------------------


def test_should_backoff_only_retryable_classes() -> None:
    assert should_backoff_failure(FailureClass.TRANSIENT)
    assert should_backoff_failure(FailureClass.RATE_LIMITED)
    assert not should_backoff_failure(FailureClass.PERMANENT)
    assert not should_backoff_failure(FailureClass.UNKNOWN)


def test_backoff_delay_grows_exponentially_and_caps() -> None:
    delays = [
        next_backoff_delay_seconds(
            throttle_key="k",
            consecutive_failures=failures,
            failure_class=FailureClass.RATE_LIMITED,
            jitter_seconds=0,
        )
        for failures in (1, 2, 3, 10, 1000, 10_000)
    ]
    assert delays[:3] == [5.0, 10.0, 20.0]
    assert delays[3] == 300.0
    assert delays[4] == 300.0
    assert delays[5] == 300.0  # exponent capped, no OverflowError


def test_jitter_is_deterministic_and_bounded() -> None:
    first = deterministic_jitter_seconds(
        throttle_key="k",
        consecutive_failures=2,
        max_jitter_seconds=3.0,
    )
    second = deterministic_jitter_seconds(
        throttle_key="k",
        consecutive_failures=2,
        max_jitter_seconds=3.0,
    )
    other_key = deterministic_jitter_seconds(
        throttle_key="other",
        consecutive_failures=2,
        max_jitter_seconds=3.0,
    )
    assert first == second
    assert 0.0 <= first <= 3.0
    assert first != other_key


def test_delay_composes_backoff_and_hold() -> None:
    state = ThrottleBackoffState(
        throttle_key="k",
        blocked_until=NOW + timedelta(seconds=10),
        hold_until=NOW + timedelta(seconds=120),
        updated_at=NOW,
    )
    assert delay_until_unblocked_seconds(state, now=NOW) == 120.0

    hold_expired = ThrottleBackoffState(
        throttle_key="k",
        blocked_until=NOW + timedelta(seconds=10),
        hold_until=NOW - timedelta(seconds=1),
        updated_at=NOW,
    )
    assert delay_until_unblocked_seconds(hold_expired, now=NOW) == 10.0
    assert delay_until_unblocked_seconds(None, now=NOW) == 0.0


# --- against Postgres -------------------------------------------------------


@pytest.fixture
def schema(pg_engine: Engine) -> PlatformSchema:
    upgrade_platform_schema(str(pg_engine.url))
    return PlatformSchema()


def test_record_throttle_failure_roundtrip(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.begin() as connection:
        state = record_throttle_failure(
            connection,
            throttle_key="openai:gpt",
            failure_class=FailureClass.RATE_LIMITED,
            error_type="httpx.HTTPStatusError",
            message="429",
            metadata={"status": 429},
            now=NOW,
            schema=schema,
        )
    assert state is not None
    assert state.consecutive_failures == 1
    assert state.blocked_until is not None
    assert state.failure_class is FailureClass.RATE_LIMITED

    with pg_engine.connect() as connection:
        delay = throttle_delay_seconds(
            connection,
            throttle_key="openai:gpt",
            now=NOW,
            schema=schema,
        )
    assert delay > 0.0


def test_permanent_failures_do_not_backoff(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.begin() as connection:
        state = record_throttle_failure(
            connection,
            throttle_key="openai:gpt",
            failure_class=FailureClass.PERMANENT,
            now=NOW,
            schema=schema,
        )
        assert state is None
        assert (
            load_throttle_backoff_state(
                connection,
                throttle_key="openai:gpt",
                schema=schema,
            )
            is None
        )


def test_consecutive_failures_increment_and_clear(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    for attempt in range(3):
        with pg_engine.begin() as connection:
            state = record_throttle_failure(
                connection,
                throttle_key="k",
                failure_class=FailureClass.TRANSIENT,
                now=NOW + timedelta(seconds=attempt),
                schema=schema,
            )
    assert state is not None
    assert state.consecutive_failures == 3

    with pg_engine.begin() as connection:
        clear_throttle_backoff(
            connection,
            throttle_key="k",
            now=NOW + timedelta(seconds=10),
            schema=schema,
        )
        cleared = load_throttle_backoff_state(
            connection,
            throttle_key="k",
            schema=schema,
        )
    assert cleared is not None
    assert cleared.consecutive_failures == 0
    assert cleared.blocked_until is None


def test_hold_blocks_and_survives_backoff_clear(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.begin() as connection:
        held = set_throttle_hold(
            connection,
            throttle_key="openrouter",
            duration_seconds=1800.0,
            reason="provider incident",
            now=NOW,
            schema=schema,
        )
    assert held.hold_until == NOW + timedelta(minutes=30)
    assert held.hold_reason == "provider incident"

    with pg_engine.begin() as connection:
        clear_throttle_backoff(
            connection,
            throttle_key="openrouter",
            now=NOW,
            schema=schema,
        )
        state = load_throttle_backoff_state(
            connection,
            throttle_key="openrouter",
            schema=schema,
        )
    assert state is not None
    assert state.hold_until == NOW + timedelta(minutes=30)
    assert delay_until_unblocked_seconds(state, now=NOW) == 1800.0

    with pg_engine.begin() as connection:
        clear_throttle_hold(
            connection,
            throttle_key="openrouter",
            now=NOW,
            schema=schema,
        )
        released = load_throttle_backoff_state(
            connection,
            throttle_key="openrouter",
            schema=schema,
        )
    assert released is not None
    assert released.hold_until is None
    assert delay_until_unblocked_seconds(released, now=NOW) == 0.0


def test_hold_requires_exactly_one_time_form(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.begin() as connection:
        with pytest.raises(ValueError, match="exactly one"):
            set_throttle_hold(
                connection,
                throttle_key="k",
                now=NOW,
                schema=schema,
            )
        with pytest.raises(ValueError, match="exactly one"):
            set_throttle_hold(
                connection,
                throttle_key="k",
                until=NOW,
                duration_seconds=5.0,
                now=NOW,
                schema=schema,
            )


def test_tags_filter_throttle_states(
    pg_engine: Engine,
    schema: PlatformSchema,
) -> None:
    with pg_engine.begin() as connection:
        set_throttle_tags(
            connection,
            throttle_key="openai:gpt",
            tags={"provider": "openai"},
            now=NOW,
            schema=schema,
        )
        set_throttle_tags(
            connection,
            throttle_key="gemini:flash",
            tags={"provider": "gemini"},
            now=NOW,
            schema=schema,
        )

    with pg_engine.connect() as connection:
        everything = list_throttle_states(connection, schema=schema)
        openai_only = list_throttle_states(
            connection,
            schema=schema,
            tag_filter={"provider": "openai"},
        )
    assert [state.throttle_key for state in everything] == [
        "gemini:flash",
        "openai:gpt",
    ]
    assert [state.throttle_key for state in openai_only] == ["openai:gpt"]
    assert openai_only[0].tags == {"provider": "openai"}
