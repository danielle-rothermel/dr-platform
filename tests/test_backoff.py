"""Focused PostgreSQL tests for retained throttle pacing."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import Engine

from dr_platform.backoff import (
    clear_throttle_backoff,
    delay_until_unblocked_seconds,
    load_throttle_backoff_state,
    record_throttle_failure,
    set_throttle_hold,
    set_throttle_tags,
)
from dr_platform.db import PlatformSchema, upgrade_platform_schema
from dr_platform.records import JSONB_PAYLOAD_MAX_BYTES
from dr_platform.status import FailureClass
from tests.conftest import engine_dsn

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_throttle_failure_hold_tags_and_clear_share_v6_state(
    pg_engine: Engine,
) -> None:
    upgrade_platform_schema(engine_dsn(pg_engine))
    schema = PlatformSchema()
    with pg_engine.begin() as connection:
        first = record_throttle_failure(
            connection,
            throttle_key="provider:model",
            failure_class=FailureClass.RATE_LIMITED,
            now=NOW,
            schema=schema,
        )
        assert first is not None
        assert first.consecutive_failures == 1
        set_throttle_hold(
            connection,
            throttle_key="provider:model",
            duration_seconds=60,
            reason="operator",
            now=NOW,
            schema=schema,
        )
        set_throttle_tags(
            connection,
            throttle_key="provider:model",
            tags={"provider": "test"},
            now=NOW,
            schema=schema,
        )
        clear_throttle_backoff(
            connection,
            throttle_key="provider:model",
            now=NOW,
            schema=schema,
        )
        state = load_throttle_backoff_state(
            connection, throttle_key="provider:model", schema=schema
        )
    assert state is not None
    assert state.consecutive_failures == 0
    assert state.tags == {"provider": "test"}
    assert delay_until_unblocked_seconds(state, now=NOW) == 60


@pytest.mark.parametrize(
    "metadata",
    [
        {1: "non-string key"},
        {"detail": "x" * JSONB_PAYLOAD_MAX_BYTES},
    ],
)
def test_throttle_failure_rejects_metadata_before_mutation(
    pg_engine: Engine,
    metadata: dict[object, object],
) -> None:
    upgrade_platform_schema(engine_dsn(pg_engine))
    schema = PlatformSchema()
    with pg_engine.begin() as connection:
        with pytest.raises(ValueError, match="throttle metadata"):
            record_throttle_failure(
                connection,
                throttle_key="provider:model",
                failure_class=FailureClass.RATE_LIMITED,
                metadata=metadata,  # type: ignore[arg-type]
                now=NOW,
                schema=schema,
            )
        state = load_throttle_backoff_state(
            connection, throttle_key="provider:model", schema=schema
        )

    assert state is None


@pytest.mark.parametrize(
    "tags",
    [
        {"provider": 1},
        {"provider": "x" * JSONB_PAYLOAD_MAX_BYTES},
    ],
)
def test_throttle_tags_reject_invalid_payload_before_mutation(
    pg_engine: Engine,
    tags: dict[str, object],
) -> None:
    upgrade_platform_schema(engine_dsn(pg_engine))
    schema = PlatformSchema()
    with pg_engine.begin() as connection:
        with pytest.raises(ValueError, match="throttle tags"):
            set_throttle_tags(
                connection,
                throttle_key="provider:model",
                tags=tags,  # type: ignore[arg-type]
                now=NOW,
                schema=schema,
            )
        state = load_throttle_backoff_state(
            connection, throttle_key="provider:model", schema=schema
        )

    assert state is None
