"""Database-backed coordination shared by platform writers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Connection, text

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime


def database_now(connection: Connection) -> datetime:
    """Return the database server's current wall-clock time."""
    return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def acquire_workflow_reference_locks(
    connection: Connection,
    workflow_ids: Sequence[str],
) -> None:
    """Lock workflow references in stable order for the current transaction."""
    for workflow_id in sorted(set(workflow_ids)):
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 0))"),
            {"id": workflow_id},
        )
