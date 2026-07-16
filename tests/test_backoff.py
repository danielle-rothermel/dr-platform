"""Deterministic throttle pacing behavior."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import Connection

import dr_platform.backoff as backoff_module
from dr_platform.backoff import (
    clear_throttle_backoff,
    clear_throttle_hold,
    delay_until_unblocked_seconds,
    deterministic_jitter_seconds,
    hold_throttle_delay,
    next_backoff_delay_seconds,
    record_throttle_failure,
    set_throttle_hold,
    set_throttle_tags,
    should_backoff_failure,
)
from dr_platform.db import PlatformSchema
from dr_platform.records import ThrottleState
from dr_platform.status import FailureClass

NOW = datetime(2026, 1, 1, tzinfo=UTC)
ThrottleMutation = Callable[[Connection, PlatformSchema], object]


def _state(
    *,
    blocked_until: datetime | None = None,
    hold_until: datetime | None = None,
) -> ThrottleState:
    return ThrottleState(
        throttle_key="provider:model",
        blocked_until=blocked_until,
        updated_at=NOW,
        hold_until=hold_until,
        change_seq=1,
    )


@pytest.mark.parametrize(
    ("failure_class", "expected"),
    [
        (FailureClass.TRANSIENT, True),
        (FailureClass.RATE_LIMITED, True),
        (FailureClass.PERMANENT, False),
        (FailureClass.UNKNOWN, False),
    ],
)
def test_only_retryable_failures_trigger_backoff(
    failure_class: FailureClass,
    expected: object,
) -> None:
    assert should_backoff_failure(failure_class) is expected


@pytest.mark.parametrize(
    ("consecutive_failures", "failure_class", "expected"),
    [
        (1, FailureClass.TRANSIENT, 5.0),
        (2, FailureClass.TRANSIENT, 10.0),
        (20, FailureClass.RATE_LIMITED, 20.0),
        (2, FailureClass.PERMANENT, 0.0),
    ],
)
def test_backoff_grows_exponentially_and_respects_the_cap(
    consecutive_failures: int,
    failure_class: FailureClass,
    expected: float,
) -> None:
    assert (
        next_backoff_delay_seconds(
            throttle_key="provider:model",
            consecutive_failures=consecutive_failures,
            failure_class=failure_class,
            initial_seconds=5,
            max_seconds=20,
            jitter_seconds=0,
        )
        == expected
    )


def test_jitter_is_stable_and_bounded_for_a_throttle_attempt() -> None:
    first = deterministic_jitter_seconds(
        throttle_key="provider:model",
        consecutive_failures=3,
        max_jitter_seconds=7,
    )
    second = deterministic_jitter_seconds(
        throttle_key="provider:model",
        consecutive_failures=3,
        max_jitter_seconds=7,
    )

    assert first == second
    assert 0 <= first <= 7


def test_unblocked_delay_uses_the_latest_active_deadline() -> None:
    state = _state(
        blocked_until=NOW + timedelta(seconds=15),
        hold_until=NOW + timedelta(seconds=60),
    )

    assert delay_until_unblocked_seconds(state, now=NOW) == 60
    assert (
        delay_until_unblocked_seconds(
            state,
            now=NOW + timedelta(seconds=61),
        )
        == 0
    )


def test_hold_boundary_sleeps_for_the_computed_delay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []
    state = _state(blocked_until=NOW + timedelta(seconds=12))

    def load_state(*args: Any, **kwargs: Any) -> ThrottleState:
        return state

    monkeypatch.setattr(
        backoff_module,
        "load_throttle_backoff_state",
        load_state,
    )

    delay = hold_throttle_delay(
        cast("Connection", object()),
        throttle_key="provider:model",
        schema=cast("PlatformSchema", object()),
        clock=lambda: NOW,
        sleeper=sleeps.append,
    )

    assert delay == 12
    assert sleeps == [12]


@pytest.mark.parametrize(
    ("mutation", "error_match"),
    [
        (
            lambda connection, schema: record_throttle_failure(
                connection,
                throttle_key="",
                failure_class=FailureClass.RATE_LIMITED,
                now=NOW,
                schema=schema,
            ),
            "throttle key",
        ),
        (
            lambda connection, schema: clear_throttle_backoff(
                connection, throttle_key="", now=NOW, schema=schema
            ),
            "throttle key",
        ),
        (
            lambda connection, schema: set_throttle_hold(
                connection,
                throttle_key="provider:model",
                duration_seconds=60,
                reason="",
                now=NOW,
                schema=schema,
            ),
            "throttle hold reason",
        ),
        (
            lambda connection, schema: clear_throttle_hold(
                connection, throttle_key="", now=NOW, schema=schema
            ),
            "throttle key",
        ),
        (
            lambda connection, schema: set_throttle_tags(
                connection,
                throttle_key="",
                tags={},
                now=NOW,
                schema=schema,
            ),
            "throttle key",
        ),
    ],
)
def test_throttle_mutations_reject_empty_scalars_before_storage(
    mutation: ThrottleMutation,
    error_match: str,
) -> None:
    with pytest.raises(ValueError, match=error_match):
        mutation(
            cast("Connection", None),
            cast("PlatformSchema", None),
        )
