"""Shared durable predicates for unresolved cancellation work."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import Connection, and_, func, or_, select

from dr_platform.status import (
    CancellationDisposition,
    EnqueueClaimDisposition,
    EnqueueCompensationDisposition,
)

if TYPE_CHECKING:
    from dr_platform.db import PlatformSchema


def unresolved_cancellation_attempt(attempts: Any) -> Any:
    """Return the SQL predicate for cancellation intent needing resolution."""
    return and_(
        attempts.c.cancellation_request_id.is_not(None),
        or_(
            attempts.c.cancellation_disposition.is_(None),
            attempts.c.cancellation_disposition
            == CancellationDisposition.FAILED.value,
        ),
    )


def unresolved_compensation(compensations: Any) -> Any:
    """Return the SQL predicate for a present unresolved compensation row."""
    return compensations.c.cancel_disposition.in_(
        [
            EnqueueCompensationDisposition.PENDING.value,
            EnqueueCompensationDisposition.FAILED.value,
        ]
    )


def unresolved_invalidated_claim(schema: PlatformSchema) -> Any:
    """Return the SQL predicate for invalidated call-started Claim repair."""
    return and_(
        schema.enqueue_claims.c.disposition
        == EnqueueClaimDisposition.INVALIDATED.value,
        schema.enqueue_claims.c.enqueue_call_started_at.is_not(None),
        or_(
            schema.enqueue_compensations.c.claim_id.is_(None),
            unresolved_compensation(schema.enqueue_compensations),
        ),
    )


def workflow_reference_conflict(
    connection: Connection,
    *,
    schema: PlatformSchema,
    workflow_ids: list[str],
) -> str | None:
    """Describe the first unresolved fact blocking a new workflow reference."""
    if not workflow_ids:
        return None
    if (
        connection.execute(
            select(schema.item_attempts.c.workflow_id)
            .where(
                and_(
                    schema.item_attempts.c.workflow_id.in_(workflow_ids),
                    unresolved_cancellation_attempt(schema.item_attempts),
                )
            )
            .limit(1)
        ).first()
        is not None
    ):
        return "workflow has unresolved cancellation intent"
    if (
        connection.execute(
            select(schema.enqueue_claims.c.workflow_id)
            .select_from(schema.enqueue_claims)
            .outerjoin(
                schema.enqueue_compensations,
                and_(
                    schema.enqueue_compensations.c.item_id
                    == schema.enqueue_claims.c.item_id,
                    schema.enqueue_compensations.c.attempt
                    == schema.enqueue_claims.c.attempt,
                    schema.enqueue_compensations.c.claim_id
                    == schema.enqueue_claims.c.claim_id,
                ),
            )
            .where(
                and_(
                    schema.enqueue_claims.c.workflow_id.in_(workflow_ids),
                    unresolved_invalidated_claim(schema),
                )
            )
            .limit(1)
        ).first()
        is not None
    ):
        return "workflow has unresolved late-enqueue compensation"
    if (
        connection.execute(
            select(schema.enqueue_compensation_hazards.c.workflow_id)
            .where(
                and_(
                    schema.enqueue_compensation_hazards.c.workflow_id.in_(
                        workflow_ids
                    ),
                    unresolved_compensation(
                        schema.enqueue_compensation_hazards
                    ),
                )
            )
            .limit(1)
        ).first()
        is not None
    ):
        return "workflow has unresolved late-enqueue successor hazard"
    return None


def operation_has_unresolved_cancellation(
    connection: Connection,
    *,
    schema: PlatformSchema,
    operation_key: str,
) -> bool:
    """Return whether an Operation has any unresolved cancellation work."""
    if (
        connection.execute(
            select(schema.item_attempts.c.item_id)
            .select_from(schema.items)
            .join(
                schema.item_attempts,
                schema.item_attempts.c.item_id == schema.items.c.item_id,
            )
            .where(
                and_(
                    schema.items.c.operation_key == operation_key,
                    unresolved_cancellation_attempt(schema.item_attempts),
                )
            )
            .limit(1)
        ).first()
        is not None
    ):
        return True
    if (
        connection.execute(
            select(schema.enqueue_claims.c.item_id)
            .select_from(schema.enqueue_claims)
            .join(
                schema.items,
                schema.items.c.item_id == schema.enqueue_claims.c.item_id,
            )
            .outerjoin(
                schema.enqueue_compensations,
                and_(
                    schema.enqueue_compensations.c.item_id
                    == schema.enqueue_claims.c.item_id,
                    schema.enqueue_compensations.c.attempt
                    == schema.enqueue_claims.c.attempt,
                    schema.enqueue_compensations.c.claim_id
                    == schema.enqueue_claims.c.claim_id,
                ),
            )
            .where(
                and_(
                    schema.items.c.operation_key == operation_key,
                    unresolved_invalidated_claim(schema),
                )
            )
            .limit(1)
        ).first()
        is not None
    ):
        return True
    return (
        connection.execute(
            select(schema.enqueue_compensation_hazards.c.item_id)
            .select_from(schema.enqueue_compensation_hazards)
            .join(
                schema.items,
                schema.items.c.item_id
                == schema.enqueue_compensation_hazards.c.item_id,
            )
            .where(
                and_(
                    schema.items.c.operation_key == operation_key,
                    unresolved_compensation(
                        schema.enqueue_compensation_hazards
                    ),
                )
            )
            .limit(1)
        ).first()
        is not None
    )


def incomplete_cancellation_count(
    connection: Connection, *, schema: PlatformSchema
) -> int:
    return int(
        connection.scalar(
            select(func.count())
            .select_from(schema.item_attempts)
            .where(unresolved_cancellation_attempt(schema.item_attempts))
        )
        or 0
    )


def incomplete_compensation_count(
    connection: Connection, *, schema: PlatformSchema
) -> int:
    claims = int(
        connection.scalar(
            select(func.count())
            .select_from(schema.enqueue_claims)
            .outerjoin(
                schema.enqueue_compensations,
                and_(
                    schema.enqueue_compensations.c.item_id
                    == schema.enqueue_claims.c.item_id,
                    schema.enqueue_compensations.c.attempt
                    == schema.enqueue_claims.c.attempt,
                    schema.enqueue_compensations.c.claim_id
                    == schema.enqueue_claims.c.claim_id,
                ),
            )
            .where(unresolved_invalidated_claim(schema))
        )
        or 0
    )
    hazards = int(
        connection.scalar(
            select(func.count())
            .select_from(schema.enqueue_compensation_hazards)
            .where(
                unresolved_compensation(schema.enqueue_compensation_hazards)
            )
        )
        or 0
    )
    return claims + hazards
