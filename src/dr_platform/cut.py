"""Atomic platform-operation cut snapshots and comparisons."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from sqlalchemy import select

if TYPE_CHECKING:
    from sqlalchemy import Connection

    from dr_platform.db import PlatformSchema

NonEmptyStr = Annotated[StrictStr, Field(min_length=1)]
PositiveInt = Annotated[StrictInt, Field(gt=0)]


class PlatformOperationCut(BaseModel):
    """One immutable member of an externally pinned operation cut."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_key: NonEmptyStr
    platform_cut_version: PositiveInt


class OperationCutMismatchDisposition(StrEnum):
    OPERATION_MISSING = "operation_missing"
    PLATFORM_CUT_ADVANCED = "platform_cut_advanced"


class OperationCutMismatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expected: PlatformOperationCut
    actual: PlatformOperationCut | None = None
    disposition: OperationCutMismatchDisposition


class OperationCutComparison(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    matches: bool
    mismatches: tuple[OperationCutMismatch, ...] = ()


def compare_operation_cuts(
    connection: Connection,
    *,
    expected: tuple[PlatformOperationCut, ...],
    schema: PlatformSchema,
) -> OperationCutComparison:
    """Lock expected Operations in lexical order and compare their cuts.

    The caller owns the transaction, so these locks remain held through its
    dependent acceptance mutation.
    """
    keys = [cut.operation_key for cut in expected]
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise ValueError("expected operation cuts must be unique and sorted")
    rows = {
        str(row["operation_key"]): int(row["platform_cut_version"])
        for row in connection.execute(
            select(
                schema.operations.c.operation_key,
                schema.operations.c.platform_cut_version,
            )
            .where(schema.operations.c.operation_key.in_(keys))
            .order_by(schema.operations.c.operation_key)
            .with_for_update()
        ).mappings()
    }
    mismatches: list[OperationCutMismatch] = []
    for cut in expected:
        current = rows.get(cut.operation_key)
        if current is None:
            mismatches.append(
                OperationCutMismatch(
                    expected=cut,
                    disposition=OperationCutMismatchDisposition.OPERATION_MISSING,
                )
            )
        elif current != cut.platform_cut_version:
            mismatches.append(
                OperationCutMismatch(
                    expected=cut,
                    actual=PlatformOperationCut(
                        operation_key=cut.operation_key,
                        platform_cut_version=current,
                    ),
                    disposition=(
                        OperationCutMismatchDisposition.PLATFORM_CUT_ADVANCED
                    ),
                )
            )
    return OperationCutComparison(
        matches=not mismatches, mismatches=tuple(mismatches)
    )
